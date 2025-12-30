from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import threading
import time
from typing import Any, Callable

try:
    import websocket  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - exercised in live environments
    websocket = None

from sublimine.contracts.types import BookDelta, BookLevel, BookSnapshot, EventType, Side, TradePrint, Venue
from sublimine.feeds.book import OrderBook
from sublimine.feeds.ws_common import ReconnectPolicy


def _parse_levels(raw_levels: list[list[Any]]) -> list[BookLevel]:
    levels: list[BookLevel] = []
    for price, size in raw_levels:
        levels.append(BookLevel(price=float(price), size=float(size)))
    return levels


def _ts_from_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


def parse_bybit_message(msg: dict) -> BookSnapshot | BookDelta | None:
    topic = msg.get("topic")
    if topic is None or not str(topic).startswith("orderbook."):
        return None
    msg_type = msg.get("type")
    data = msg.get("data") or {}
    if msg_type not in {"snapshot", "delta"}:
        return None

    symbol = data.get("s")
    if symbol is None:
        return None

    ts_ms = msg.get("ts")
    if ts_ms is None:
        return None

    bids = _parse_levels(data.get("b", []))
    asks = _parse_levels(data.get("a", []))
    update_id = data.get("u")

    if msg_type == "snapshot":
        depth = int(data.get("depth", max(len(bids), len(asks))))
        return BookSnapshot(
            symbol=symbol,
            venue=Venue.BYBIT,
            ts_utc=_ts_from_ms(int(ts_ms)),
            bids=bids,
            asks=asks,
            depth=depth,
        )

    is_snapshot = bool(update_id == 1)
    return BookDelta(
        symbol=symbol,
        venue=Venue.BYBIT,
        ts_utc=_ts_from_ms(int(ts_ms)),
        bids=bids,
        asks=asks,
        is_snapshot=is_snapshot,
        update_id=int(update_id) if update_id is not None else None,
    )

def parse_bybit_trade_message(msg: dict) -> list[TradePrint] | None:
    topic = msg.get("topic")
    if topic is None or not str(topic).startswith("publicTrade."):
        return None
    raw = msg.get("data")
    if not raw:
        return []
    if isinstance(raw, dict):
        raw_trades = [raw]
    else:
        raw_trades = list(raw)

    trades: list[TradePrint] = []
    for item in raw_trades:
        symbol = item.get("s")
        if symbol is None:
            continue
        ts_ms = item.get("T") or msg.get("ts")
        if ts_ms is None:
            continue
        price_raw = item.get("p")
        size_raw = item.get("v") or item.get("q")
        if price_raw is None or size_raw is None:
            continue
        side_raw = str(item.get("S", "")).lower()
        if side_raw == "buy":
            side = Side.BUY
        elif side_raw == "sell":
            side = Side.SELL
        else:
            side = Side.UNKNOWN
        trades.append(
            TradePrint(
                symbol=symbol,
                venue=Venue.BYBIT,
                ts_utc=_ts_from_ms(int(ts_ms)),
                price=float(price_raw),
                size=float(size_raw),
                aggressor_side=side,
            )
        )
    return trades


EventSink = Callable[[EventType, object], None]


@dataclass
class BybitConnector:
    symbol: str
    depth: int
    ws_url: str
    reconnect: ReconnectPolicy = field(default_factory=ReconnectPolicy)
    ping_interval: int = 20
    ping_timeout: int = 10
    _book: OrderBook = field(init=False)
    _stop_event: threading.Event = field(init=False, default_factory=threading.Event)
    _thread: threading.Thread | None = field(init=False, default=None)
    _ws: websocket.WebSocketApp | None = field(init=False, default=None)
    _sink: EventSink | None = field(init=False, default=None)

    def start(self, sink: EventSink) -> None:
        if websocket is None:
            raise RuntimeError("websocket-client is required for live Bybit feeds")
        self._sink = sink
        self._book = OrderBook.empty(self.symbol, Venue.BYBIT, self.depth)
        self._thread = threading.Thread(target=self._run, name="bybit-ws", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._ws is not None:
            self._ws.close()

    def join(self, timeout: float | None = None) -> None:
        if self._thread is not None:
            self._thread.join(timeout)

    def _run(self) -> None:
        while not self._stop_event.is_set():
            self._ws = websocket.WebSocketApp(
                self.ws_url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )
            self._ws.run_forever(ping_interval=self.ping_interval, ping_timeout=self.ping_timeout)
            if self._stop_event.is_set():
                break
            time.sleep(self.reconnect.next_delay())

    def _on_open(self, ws: websocket.WebSocketApp) -> None:
        self.reconnect.reset()
        self._book = OrderBook.empty(self.symbol, Venue.BYBIT, self.depth)
        args = [f"orderbook.{self.depth}.{self.symbol}", f"publicTrade.{self.symbol}"]
        ws.send(json.dumps({"op": "subscribe", "args": args}, separators=(",", ":")))

    def _on_message(self, _ws: websocket.WebSocketApp, message: str) -> None:
        if self._sink is None:
            return
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            return

        book_event = parse_bybit_message(payload)
        if isinstance(book_event, BookSnapshot):
            self._book.apply_snapshot(book_event)
            self._sink(EventType.BOOK_SNAPSHOT, book_event)
            return
        if isinstance(book_event, BookDelta):
            if book_event.is_snapshot:
                snapshot = BookSnapshot(
                    symbol=book_event.symbol,
                    venue=book_event.venue,
                    ts_utc=book_event.ts_utc,
                    bids=book_event.bids,
                    asks=book_event.asks,
                    depth=self._book.depth,
                )
                self._book.apply_snapshot(snapshot)
                self._sink(EventType.BOOK_SNAPSHOT, snapshot)
            else:
                self._book.apply_delta(book_event)
                self._sink(EventType.BOOK_DELTA, book_event)
            return

        trades = parse_bybit_trade_message(payload)
        if trades is None:
            return
        for trade in trades:
            self._sink(EventType.TRADE, trade)

    def _on_error(self, _ws: websocket.WebSocketApp, _error: object) -> None:
        return None

    def _on_close(self, _ws: websocket.WebSocketApp, _status: object, _msg: object) -> None:
        return None
