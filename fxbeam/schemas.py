import typing


class TickDataWithTimestampSchema(typing.NamedTuple):
    """Schema for tick data that includes a timestamp"""
    timestamp: int
    bid: float
    ask: float
    volume: int


class TickDataSchema(typing.NamedTuple):
    """Schema for tick data that does not include a timestamp"""
    bid: float
    ask: float
    volume: int


class OHLCVDataWithTimestampSchema(typing.NamedTuple):
    """Schema for OHLCV data that includes a timestamp"""
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: int


class OHLCVDataSchema(typing.NamedTuple):
    """Schema for OHLCV data that does not include a timestamp"""
    open: float
    high: float
    low: float
    close: float
    volume: int
