from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Literal


class Venue(str, Enum):
    BYBIT = "BYBIT"
    BINANCE = "BINANCE"
    MT5 = "MT5"
    IBKR = "IBKR"


class EventType(str, Enum):
    QUOTE = "QUOTE"
    BOOK_SNAPSHOT = "BOOK_SNAPSHOT"
    BOOK_DELTA = "BOOK_DELTA"
    TRADE = "TRADE"
    FEATURE = "FEATURE"
    EVENT_SIGNAL = "EVENT_SIGNAL"
    TRADE_INTENT = "TRADE_INTENT"
    ORDER_REQUEST = "ORDER_REQUEST"
    ORDER_ACK = "ORDER_ACK"
    ORDER_FILL = "ORDER_FILL"
    POSITION_SNAPSHOT = "POSITION_SNAPSHOT"
    DATA_QUALITY = "DATA_QUALITY"
    ENGINE_STATE = "ENGINE_STATE"


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class BookLevel:
    price: float
    size: float


@dataclass(frozen=True)
class BookSnapshot:
    symbol: str
    venue: Venue
    ts_utc: datetime
    bids: list[BookLevel]
    asks: list[BookLevel]
    depth: int


@dataclass(frozen=True)
class BookDelta:
    symbol: str
    venue: Venue
    ts_utc: datetime
    bids: list[BookLevel]
    asks: list[BookLevel]
    is_snapshot: bool
    update_id: int | None


@dataclass(frozen=True)
class TradePrint:
    symbol: str
    venue: Venue
    ts_utc: datetime
    price: float
    size: float
    aggressor_side: Side


@dataclass(frozen=True)
class QuoteTick:
    symbol: str
    venue: Venue
    ts_utc: datetime
    bid: float
    ask: float
    last: float


@dataclass(frozen=True)
class SignalEvent:
    event_name: Literal["E1", "E2", "E3", "E4"]
    symbol: str
    venue: Venue
    ts_utc: datetime
    score_0_1: float
    reason_codes: list[str] = field(default_factory=list)
    meta: dict = field(default_factory=dict)


@dataclass(frozen=True)
class TradeIntent:
    symbol: str
    direction: Side
    score: float
    risk_frac: float
    entry_plan: dict
    stop_plan: dict
    ts_utc: datetime
    take_plan: dict = field(default_factory=dict)
    reason_codes: list[str] = field(default_factory=list)
    meta: dict = field(default_factory=dict)


@dataclass(frozen=True)
class OrderRequest:
    id: str
    symbol: str
    venue: Venue
    ts_utc: datetime
    side: Side
    order_type: str
    price: float | None
    qty: float
    intent_id: str
    meta: dict = field(default_factory=dict)


@dataclass(frozen=True)
class OrderAck:
    request_id: str
    ts_utc: datetime
    status: str
    reason: str | None
    order_id: str
    meta: dict = field(default_factory=dict)


@dataclass(frozen=True)
class OrderFill:
    request_id: str
    ts_utc: datetime
    price: float
    qty: float
    fee: float
    meta: dict = field(default_factory=dict)


@dataclass(frozen=True)
class PositionSnapshot:
    symbol: str
    ts_utc: datetime
    qty: float
    avg_price: float
    unrealized_pnl: float
    meta: dict = field(default_factory=dict)


from sublimine.health.health import DataQualitySnapshot
from sublimine.health.state import EngineStateEvent
