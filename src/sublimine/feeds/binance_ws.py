from __future__ import annotations

from dataclasses import dataclass, field
from collections import deque
from datetime import datetime, timezone
import json
import threading
import time
from typing import Any, Callable
import urllib.parse
import urllib.request

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


@dataclass(frozen=True)
class BinanceDiffEvent:
    symbol: str
    first_update_id: int
    final_update_id: int
    ts_utc: datetime
    delta: BookDelta


def parse_binance_diff_event(msg: dict) -> BinanceDiffEvent | None:
    if msg.get("e") != "depthUpdate":
        return None

    symbol = msg.get("s")
    if symbol is None:
        return None

    first_id = msg.get("U")
    final_id = msg.get("u")
    if first_id is None or final_id is None:
        return None

    bids = _parse_levels(msg.get("b", []))
    asks = _parse_levels(msg.get("a", []))
    ts_ms = msg.get("E")
    ts_utc = _ts_from_ms(int(ts_ms)) if ts_ms is not None else datetime.fromtimestamp(0, tz=timezone.utc)

    delta = BookDelta(
        symbol=symbol,
        venue=Venue.BINANCE,
        ts_utc=ts_utc,
        bids=bids,
        asks=asks,
        is_snapshot=False,
        update_id=int(final_id),
    )

    return BinanceDiffEvent(
        symbol=symbol,
        first_update_id=int(first_id),
        final_update_id=int(final_id),
        ts_utc=ts_utc,
        delta=delta,
    )


def parse_binance_trade_message(msg: dict) -> TradePrint | None:
    if msg.get("e") != "trade":
        return None
    symbol = msg.get("s")
    if symbol is None:
        return None
    price_raw = msg.get("p")
    size_raw = msg.get("q")
    ts_ms = msg.get("T") or msg.get("E")
    if price_raw is None or size_raw is None or ts_ms is None:
        return None
    is_buyer_maker = bool(msg.get("m"))
    side = Side.SELL if is_buyer_maker else Side.BUY
    return TradePrint(
        symbol=symbol,
        venue=Venue.BINANCE,
        ts_utc=_ts_from_ms(int(ts_ms)),
        price=float(price_raw),
        size=float(size_raw),
        aggressor_side=side,
    )


def fetch_binance_snapshot(
    symbol: str,
    depth: int,
    rest_url: str,
    timeout_s: float = 10.0,
) -> tuple[BookSnapshot, int]:
    params = urllib.parse.urlencode({"symbol": symbol, "limit": depth})
    joiner = "&" if "?" in rest_url else "?"
    url = f"{rest_url}{joiner}{params}"
    with urllib.request.urlopen(url, timeout=timeout_s) as response:
        payload = json.loads(response.read().decode("utf-8"))
    last_update_id = int(payload["lastUpdateId"])
    bids = _parse_levels(payload.get("bids", []))
    asks = _parse_levels(payload.get("asks", []))
    snapshot = BookSnapshot(
        symbol=symbol,
        venue=Venue.BINANCE,
        ts_utc=datetime.now(timezone.utc),
        bids=bids,
        asks=asks,
        depth=depth,
    )
    return snapshot, last_update_id


class BinanceBookSynchronizer:
    def __init__(self, symbol: str, depth: int) -> None:
        self._symbol = symbol
        self._depth = depth
        self._book = OrderBook.empty(symbol, Venue.BINANCE, depth)
        self._last_update_id: int | None = None
        self._buffer: list[BinanceDiffEvent] = []
        self._synced = False
        self.desynced = False

    @property
    def book(self) -> OrderBook:
        return self._book

    @property
    def last_update_id(self) -> int | None:
        return self._last_update_id

    def apply_snapshot(self, snapshot: BookSnapshot, last_update_id: int) -> list[BookDelta]:
        self._book.apply_snapshot(snapshot)
        self._last_update_id = int(last_update_id)
        self._synced = False
        applied: list[BookDelta] = []
        if self._buffer:
            buffered = sorted(self._buffer, key=lambda item: item.final_update_id)
            self._buffer.clear()
            for event in buffered:
                if self.desynced:
                    break
                if self.on_diff_event(event):
                    applied.append(event.delta)
        return applied

    def on_diff_event(self, event: BinanceDiffEvent) -> bool:
        if self._last_update_id is None:
            self._buffer.append(event)
            return False

        if event.final_update_id < self._last_update_id:
            return False

        if not self._synced:
            if not (event.first_update_id <= self._last_update_id <= event.final_update_id):
                self.desynced = True
                return False
            self._synced = True
        elif event.first_update_id != self._last_update_id + 1:
            self.desynced = True
            return False

        self._book.apply_delta(event.delta)
        self._last_update_id = event.final_update_id
        return True

    def needs_resync(self) -> bool:
        return self.desynced

    def reset_for_resync(self) -> None:
        self._buffer.clear()
        self._last_update_id = None
        self._synced = False
        self.desynced = False


EventSink = Callable[[EventType, object], None]


@dataclass
class BinanceConnector:
    symbol: str
    depth: int
    depth_interval_ms: int
    ws_url: str
    rest_url: str
    reconnect: ReconnectPolicy = field(default_factory=ReconnectPolicy)
    resync: ReconnectPolicy = field(default_factory=ReconnectPolicy)
    ping_interval: int = 20
    ping_timeout: int = 10
    snapshot_fetcher: Callable[[str, int, str, float], tuple[BookSnapshot, int]] = fetch_binance_snapshot
    _sync: BinanceBookSynchronizer = field(init=False)
    _sink: EventSink | None = field(init=False, default=None)
    _thread: threading.Thread | None = field(init=False, default=None)
    _stop_event: threading.Event = field(init=False, default_factory=threading.Event)
    _ws: websocket.WebSocketApp | None = field(init=False, default=None)
    _sync_lock: threading.Lock = field(init=False, default_factory=threading.Lock)
    _resync_lock: threading.Lock = field(init=False, default_factory=threading.Lock)
    _health_lock: threading.Lock = field(init=False, default_factory=threading.Lock)
    _resync_events: deque[datetime] = field(init=False, default_factory=deque)
    _desync_events: deque[datetime] = field(init=False, default_factory=deque)
    _resync_count: int = field(init=False, default=0)
    _desync_count: int = field(init=False, default=0)
    _desync_reported: bool = field(init=False, default=False)

    def __post_init__(self) -> None:
        self._sync = BinanceBookSynchronizer(symbol=self.symbol, depth=self.depth)

    def start(self, sink: EventSink) -> None:
        if websocket is None:
            raise RuntimeError("websocket-client is required for live Binance feeds")
        self._sink = sink
        self._thread = threading.Thread(target=self._run, name="binance-ws", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._ws is not None:
            self._ws.close()

    def join(self, timeout: float | None = None) -> None:
        if self._thread is not None:
            self._thread.join(timeout)

    @property
    def resync_count(self) -> int:
        with self._health_lock:
            return self._resync_count

    @property
    def desync_count(self) -> int:
        with self._health_lock:
            return self._desync_count

    def drain_health_events(self) -> tuple[list[datetime], list[datetime]]:
        with self._health_lock:
            resync_events = list(self._resync_events)
            desync_events = list(self._desync_events)
            self._resync_events.clear()
            self._desync_events.clear()
        return resync_events, desync_events

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
        params = [
            f"{self.symbol.lower()}@depth@{self.depth_interval_ms}ms",
            f"{self.symbol.lower()}@trade",
        ]
        ws.send(json.dumps({"method": "SUBSCRIBE", "params": params, "id": 1}, separators=(",", ":")))
        self._request_resync()

    def _on_message(self, _ws: websocket.WebSocketApp, message: str) -> None:
        if self._sink is None:
            return
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            return
        data = payload.get("data", payload)
        diff_event = parse_binance_diff_event(data)
        if diff_event is not None:
            self._handle_diff_event(diff_event)
            return

        trade = parse_binance_trade_message(data)
        if trade is not None:
            self._sink(EventType.TRADE, trade)

    def _handle_diff_event(self, diff_event: BinanceDiffEvent) -> None:
        if self._sink is None:
            return
        with self._sync_lock:
            applied = self._sync.on_diff_event(diff_event)
            needs_resync = self._sync.needs_resync()
        if applied:
            self._sink(EventType.BOOK_DELTA, diff_event.delta)
        if needs_resync:
            self._record_desync_resync(diff_event.ts_utc)
            self._request_resync()

    def _request_resync(self) -> None:
        if self._stop_event.is_set():
            return
        if not self._resync_lock.acquire(blocking=False):
            return
        thread = threading.Thread(target=self._resync, name="binance-resync", daemon=True)
        thread.start()

    def _resync(self) -> None:
        try:
            with self._sync_lock:
                self._sync.reset_for_resync()
            snapshot, last_update_id = self._fetch_snapshot_with_backoff()
            if self._stop_event.is_set():
                return
            with self._sync_lock:
                buffered = self._sync.apply_snapshot(snapshot, last_update_id)
            if self._sink is not None:
                self._sink(EventType.BOOK_SNAPSHOT, snapshot)
                for delta in buffered:
                    self._sink(EventType.BOOK_DELTA, delta)
        finally:
            with self._health_lock:
                self._desync_reported = False
            self._resync_lock.release()

    def _fetch_snapshot_with_backoff(self) -> tuple[BookSnapshot, int]:
        while not self._stop_event.is_set():
            try:
                snapshot, last_update_id = self.snapshot_fetcher(self.symbol, self.depth, self.rest_url, 10.0)
            except Exception:
                time.sleep(self.resync.next_delay())
                continue
            self.resync.reset()
            return snapshot, last_update_id
        return BookSnapshot(self.symbol, Venue.BINANCE, datetime.now(timezone.utc), [], [], self.depth), 0

    def _on_error(self, _ws: websocket.WebSocketApp, _error: object) -> None:
        return None

    def _on_close(self, _ws: websocket.WebSocketApp, _status: object, _msg: object) -> None:
        return None

    def _record_desync_resync(self, ts_utc: datetime) -> None:
        with self._health_lock:
            if self._desync_reported:
                return
            self._desync_reported = True
            self._desync_count += 1
            self._resync_count += 1
            self._desync_events.append(ts_utc)
            self._resync_events.append(ts_utc)
