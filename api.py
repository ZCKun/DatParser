import struct

from enum import IntEnum
from dataclasses import dataclass
from typing import Tuple, Union

header_size = 20


class DataType(IntEnum):
    SNAPSHOT_DATA_TYPE = 0x00202001
    TICK_DATA_TYPE = 0x00202002
    ORDER_BOOK_DATA_TYPE = 0x00202003
    STATIC_DATA_TYPE = 0x00202004


@dataclass
class XTPMarketData:
    exchange_id: int
    ticker: str
    last_price: float
    pre_close_price: float
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    pre_total_long_position: int
    total_long_position: int
    pre_settl_price: float
    settl_price: float
    upper_limit_price: float
    lower_limit_price: float
    pre_delta: float
    curr_delta: float
    data_time: int
    qty: int
    turnover: float
    avg_price: float
    # 10
    bid: list[float]
    ask: list[float]
    bid_qty: list[int]
    ask_qty: list[int]
    trades_count: int
    ticker_status: str


@dataclass
class Snapshot:
    market_data: XTPMarketData
    recv_time: int
    bid1_qty: list[int]
    bid1_count: int
    max_bid1_count: int
    ask1_qty: list[int]
    ask1_count: int
    max_ask1_count: int


@dataclass
class Entrust:
    # 4 + 3*8 + 2*1 + 8 = 38
    channel_no: int
    seq: int
    price: float
    qty: int
    side: chr
    ord_type: chr
    order_no: int


@dataclass
class Transaction:
    # 4 + 6*8 + 1 = 53
    channel_no: int
    seq: int
    price: float
    qty: int
    money: float
    bid_no: int
    ask_no: int
    trade_flat: chr


@dataclass
class XTPTickByTick:
    exchange_id: int
    ticker: str
    seq: int
    data_time: int
    _type: int
    data: Union[Entrust, Transaction]


def entrust_parser(buffer: bytes) -> Entrust:
    """ 解析逐笔委托 """
    items = struct.unpack("<i4cqdq2c6cq", buffer)
    ret = Entrust(items[0], *items[5:10], items[-1])
    return ret


def transaction_parser(buffer: bytes) -> Transaction:
    """ 解析逐笔成交 """
    items = struct.unpack("<iqdqd2qc", buffer)
    ret = Transaction(*items)
    return ret


def tbt_parser(buffer: bytes) -> XTPTickByTick:
    """ 解析逐笔行情 """
    items = struct.unpack("<i16c4c2qi4c", buffer[:48])
    tbt_type = items[-5]
    data = None
    if tbt_type == 1:
        data = entrust_parser(buffer[48:48+48])
    elif tbt_type == 2:
        data = transaction_parser(buffer[48:48+53])
    ret = XTPTickByTick(
        items[0], 
        b''.join(items[1:17]).decode("utf-8").rstrip("\x00"),
        items[21],
        items[22],
        items[23],
        data
    )
    return ret


def market_data_parser(buffer: bytes) -> XTPMarketData:
    """ 解析XTPMarketData """
    items = struct.unpack("<i16c4c6d2q6d2q22d21q8c", buffer[:504])
    data = XTPMarketData(
        items[0],
        b''.join(items[1:17]).decode().rstrip('\x00'),
        *items[21:39],
        items[39:49],
        items[49:59],
        items[59:69],
        items[69:79],
        items[79],
        b''.join(items[80:]).decode().rstrip('\x00')
    )
    return data


def snapshot_parser(buffer: bytes) -> Snapshot:
    """ 解析行情快照 """
    market_data = market_data_parser(buffer[:-184])
    # 8 8*10 4 4 8*10 4 4
    items = struct.unpack("<qq10iiq10ii", buffer[-112:])
    snap = Snapshot(
        market_data,
        items[0],
        items[1:11], 
        items[11],
        items[12],
        items[13:23],
        items[23],
        items[24]
    )
    return snap


class Header:
    data_type: DataType
    data_size: int
    magic: int
    unix_time: str

    def __init__(self, buffer: bytes):
        items = struct.unpack("<IHH12c", buffer)
        self.data_type = items[0]
        self.data_size = items[1]
        self.magic  = items[2]
        self.unix_time = b''.join(items[3:]).decode('utf-8')


class Dat:

    def __init__(self, f):
        self._buf = f
        self._parser_func = {
            DataType.SNAPSHOT_DATA_TYPE: snapshot_parser,
            DataType.TICK_DATA_TYPE: tbt_parser,
        }

    def read_header(self):
        buf = self.read(header_size)
        header = Header(buf)
        return header

    def read_next_data(self) -> Tuple[DataType, Union[Snapshot, XTPTickByTick]]:
        header = self.read_header()
        data = self.read(header.data_size)
        if header.data_type in self._parser_func:
            data = self._parser_func[header.data_type](data)
        else:
            data = None
        return header.data_type, data

    def read(self, size: int):
        ret = self._buf.read(size)
        return ret


import sys
import os

def test():
    if not os.path.exists(sys.argv[1]):
        print(f"file {sys.argv[1]} not exists")
        exit(1)

    f = open(sys.argv[1], "rb")
    dat = Dat(f)
    while True:
        data_type, data = dat.read_next_data()
        #if data_type == DataType.TICK_DATA_TYPE:
        print(f"data_type={data_type}")
        if data is not None:
            print(data.__dict__)
        print("-"*50)
    f.close()


test()

