# coding: utf-8
import csv
import sys
import time
import traceback
import pika
import json
import pandas as pd

from datetime import datetime, timedelta
from typing import List, Dict, Optional

from threading import Thread
from multiprocessing import Queue as ProcQueue
from queue import Queue, Empty

from vnpy.event import EVENT_TIMER, Event
from vnpy.trader.engine import BaseEngine, MainEngine, EventEngine
from vnpy.trader.constant import Interval, Exchange
from vnpy.trader.object import BarData, TickData, ContractData, HistoryRequest
from vnpy.trader.database import BaseDatabase, get_database, BarOverview, DB_TZ
from vnpy.trader.datafeed import BaseDatafeed, get_datafeed
from vnpy.trader.utility import ZoneInfo, get_file_logger

isDebug = True if sys.gettrace() else False

APP_NAME = "DataManager"

from trading_signal.process import logger, s_type_to_count
from trading_signal.process import MessageThread, DingThread, QueueThread, BarProcess, is_future_id, get_security_type


# g_bdb_dict: Dict[str, StockBarDataBackend] = {}

g_bar_proc_dict: Dict[str, tuple] = {}
g_last_factors: Dict[str, Dict] = {} # 最后一个趋势因子全局缓存



class ManagerEngine(BaseEngine):
    """"""

    def __init__(
        self,
        main_engine: MainEngine,
        event_engine: EventEngine,
    ) -> None:
        """"""
        global logger
        super().__init__(main_engine, event_engine, APP_NAME)

        # 重新定位log
        self.app_type = main_engine.engine_name
        logger = get_file_logger("%s_%s" % (APP_NAME, self.app_type))
        # self.queue: Queue = Queue()
        # self.thread: Thread = Thread(target=self.run)
        self.queue_mq: Queue = ProcQueue()
        self.queue_ding: Queue = Queue()
        self.thread_msg: Thread = MessageThread(self.app_type, g_last_factors, self.queue_mq, self.queue_ding)
        self.thread_ding: Thread = DingThread(self.app_type, g_last_factors, self.queue_ding)
        self.active: bool = False
        # 香港等市场用rabbit
        if self.app_type in ('hk','out','future_zt'):
            self.thread_mq: Thread = QueueThread(self.app_type, self.queue_mq)
        self.database: BaseDatabase = get_database()
        # FIX ME  用上证指数，获取上一个交易日
        curr_time = datetime.now()
        start_time = curr_time - timedelta(days=30)
        bars: List[BarData] = self.database.load_bar_data(
            "000001",
            Exchange.SSE,
            Interval.DAILY,
            start_time,
            curr_time
        )
        if len(bars) > 1:
            if curr_time.date() != bars[-1].datetime.date():
                self.last_dt = bars[-1].datetime
            else:
                self.last_dt = bars[-2].datetime
        else:
            if curr_time.weekday() == 0:
                self.last_dt = curr_time - timedelta(days=3)
            else:
                self.last_dt = curr_time - timedelta(days=1)

        self.datafeed: BaseDatafeed = get_datafeed()
        # self.check_dict = {}
        #
        self.s_type = None
        self.timer_count: int = 0
        self.last_end_time = {}
        self.inited = False

        self.event_engine.register(EVENT_TIMER, self.process_timer_event)
        self.start()

    def load_history_df(self, symbol, exchange, interval, start_dt, end_dt):
        """"""
        req: HistoryRequest = HistoryRequest(
            symbol=symbol,
            exchange=exchange,
            interval=interval,
            start=start_dt,
            end=end_dt
        )
        bardata: List[BarData] = self.datafeed.query_bar_history(req)
        if len(bardata) > 0:
            his_df = pd.DataFrame([(bar.datetime, bar.open_price, bar.high_price, bar.low_price,
                bar.close_price, bar.volume, bar.turnover, bar.open_interest) for bar in bardata],
                columns=['datetime', 'open', 'high', 'low', 'close', 'volume', 'turnover', 'open_interest'])
            return his_df
        else:
            return pd.DataFrame()

    def close(self) -> None:
        """"""
        self.active = False

        # 
        for q,p in g_bar_proc_dict.values():
            p.terminate()

        pass

    def start(self) -> None:
        """"""
        self.active = True
        # 
        self.thread_msg.start()
        self.thread_ding.start()

    def get_queue(self, s_type, tv_symbol, count=2):
        """"""
        if s_type in s_type_to_count:
            count = s_type_to_count[s_type]
        s_key = "%s-%s" % (s_type, hash(tv_symbol) % count)
        if s_key not in g_bar_proc_dict:
            q = ProcQueue()
            p = BarProcess(s_key, q, self.queue_mq)
            p.start()
            g_bar_proc_dict[s_key] = (q,p)
        return g_bar_proc_dict[s_key][0]

    def process_timer_event(self, event: Event) -> None:
        """"""
        self.timer_count += 1
        if self.timer_count >= 10:
            self.timer_count = 0
            # 只处理近期更新的bar，减少数据库压力
            if self.s_type is None:
                if self.main_engine.engine_name != "":
                    self.s_type = self.main_engine.engine_name
                else:
                    self.s_type = 'all'
                # 第一次，前推多一点确保有数据
                begin_time = datetime.now() - timedelta(days=3)
            else:
                begin_time = datetime.now() - timedelta(minutes=3)
            try:
                overviews: List[BarOverview] = self.database.get_bar_overview_ex(begin_time)
                # print(count, total, overviews)
                for overview in overviews:
                    # 期货只处理指数
                    if is_future_id(overview.symbol) and not overview.symbol.endswith('8888'):
                        continue
                    # if not overview.symbol.startswith('SA8888'):
                    #     continue
                    # 只处理本进程标的类型
                    s_type = get_security_type(overview.symbol)
                    if self.s_type not in ('all', s_type):
                        continue
                    # 时间更新，才处理
                    tv_symbol = f"{overview.symbol}.{overview.exchange.value}"
                    # bar时间没更新不处理
                    if tv_symbol in self.last_end_time and self.last_end_time[tv_symbol] >= overview.end:
                        continue
                    # 第一次从上一个交易日结束开始，统一取下午收盘后数据
                    if tv_symbol not in self.last_end_time:
                        # 当天夜盘必须用今日时间；港股和A股，16点30分后都交易日结束
                        curr_time = datetime.now()
                        if curr_time.hour <= 18:
                            start_time = self.last_dt.replace(hour=16, minute=31, second=0, microsecond=0, tzinfo=None)
                        else:
                            start_time = curr_time.replace(hour=16, minute=31, second=0, microsecond=0)
                        # 为了期货加权短周期连续，统一向前加一天
                        # start_time = start_time - timedelta(days=1) if is_future_id(tv_symbol) else start_time
                        end_time = overview.end # .replace(hour=9, minute=31)
                    else:
                        # 下一秒，即不包含上次的最后一分钟
                        start_time = self.last_end_time[tv_symbol].replace(second=1)
                        end_time = overview.end
                    #
                    self.last_end_time[tv_symbol] = end_time
                    # 给进程处理
                    bars: List[BarData] = self.database.load_bar_data(
                        overview.symbol,
                        overview.exchange,
                        overview.interval,
                        start_time,
                        end_time
                    )
                    # if len(bars) > 0:
                    #     bars = bars[0:1]
                    #     end_time = bars[0].datetime.replace(tzinfo=None)
                    #     self.last_end_time[tv_symbol] = end_time
                    queue = self.get_queue(s_type, tv_symbol)
                    queue.put((tv_symbol, start_time, end_time, bars))
            except Exception as e:
                logger.error("process_timer_event Exception %s", e)
            #
            last_time = datetime.now()
            if last_time.hour == 16 and last_time.minute == 50:
                self.inited = False
            if last_time.hour == 20 and last_time.minute == 50:
                if not self.inited:
                    self.inited = True

    def import_data_from_csv(
        self,
        file_path: str,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        tz_name: str,
        datetime_head: str,
        open_head: str,
        high_head: str,
        low_head: str,
        close_head: str,
        volume_head: str,
        turnover_head: str,
        open_interest_head: str,
        datetime_format: str
    ) -> tuple:
        """"""
        with open(file_path, "rt") as f:
            buf: list = [line.replace("\0", "") for line in f]

        reader: csv.DictReader = csv.DictReader(buf, delimiter=",")

        bars: List[BarData] = []
        start: datetime = None
        count: int = 0
        tz = ZoneInfo(tz_name)

        for item in reader:
            if datetime_format:
                dt: datetime = datetime.strptime(item[datetime_head], datetime_format)
            else:
                dt: datetime = datetime.fromisoformat(item[datetime_head])
            dt = dt.replace(tzinfo=tz)

            turnover = item.get(turnover_head, 0)
            open_interest = item.get(open_interest_head, 0)

            bar: BarData = BarData(
                symbol=symbol,
                exchange=exchange,
                datetime=dt,
                interval=interval,
                volume=float(item[volume_head]),
                open_price=float(item[open_head]),
                high_price=float(item[high_head]),
                low_price=float(item[low_head]),
                close_price=float(item[close_head]),
                turnover=float(turnover),
                open_interest=float(open_interest),
                gateway_name="DB",
            )

            bars.append(bar)

            # do some statistics
            count += 1
            if not start:
                start = bar.datetime

        end: datetime = bar.datetime

        # insert into database
        self.database.save_bar_data(bars)

        return start, end, count

    def output_data_to_csv(
        self,
        file_path: str,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> bool:
        """"""
        bars: List[BarData] = self.load_bar_data(symbol, exchange, interval, start, end)

        fieldnames: list = [
            "symbol",
            "exchange",
            "datetime",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "turnover",
            "open_interest"
        ]

        try:
            with open(file_path, "w") as f:
                writer: csv.DictWriter = csv.DictWriter(f, fieldnames=fieldnames, lineterminator="\n")
                writer.writeheader()

                for bar in bars:
                    d: dict = {
                        "symbol": bar.symbol,
                        "exchange": bar.exchange.value,
                        "datetime": bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),
                        "open": bar.open_price,
                        "high": bar.high_price,
                        "low": bar.low_price,
                        "close": bar.close_price,
                        "turnover": bar.turnover,
                        "volume": bar.volume,
                        "open_interest": bar.open_interest,
                    }
                    writer.writerow(d)

            return True
        except PermissionError:
            return False

    def get_bar_overview(self) -> List[BarOverview]:
        """"""
        return self.database.get_bar_overview()

    def load_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> List[BarData]:
        """"""
        bars: List[BarData] = self.database.load_bar_data(
            symbol,
            exchange,
            interval,
            start,
            end
        )

        return bars

    def delete_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval
    ) -> int:
        """"""
        count: int = self.database.delete_bar_data(
            symbol,
            exchange,
            interval
        )

        return count

    def download_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: str,
        start: datetime
    ) -> int:
        """
        Query bar data from datafeed.
        """
        req: HistoryRequest = HistoryRequest(
            symbol=symbol,
            exchange=exchange,
            interval=Interval(interval),
            start=start,
            end=datetime.now(DB_TZ)
        )

        vt_symbol: str = f"{symbol}.{exchange.value}"
        contract: Optional[ContractData] = self.main_engine.get_contract(vt_symbol)

        # If history data provided in gateway, then query
        if contract and contract.history_data:
            data: List[BarData] = self.main_engine.query_history(
                req, contract.gateway_name
            )
        # Otherwise use datafeed to query data
        else:
            data: List[BarData] = self.datafeed.query_bar_history(req)

        if data:
            self.database.save_bar_data(data)
            return(len(data))

        return 0

    def download_tick_data(
        self,
        symbol: str,
        exchange: Exchange,
        start: datetime
    ) -> int:
        """
        Query tick data from datafeed.
        """
        req: HistoryRequest = HistoryRequest(
            symbol=symbol,
            exchange=exchange,
            start=start,
            end=datetime.now(DB_TZ)
        )

        data: List[TickData] = self.datafeed.query_tick_history(req)

        if data:
            self.database.save_tick_data(data)
            return(len(data))

        return 0
