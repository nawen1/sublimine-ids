from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sublimine.contracts.types import BookDelta, BookLevel, BookSnapshot, Venue
from sublimine.feeds.book import OrderBook


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

    def apply_snapshot(self, snapshot: BookSnapshot, last_update_id: int) -> None:
        self._book.apply_snapshot(snapshot)
        self._last_update_id = int(last_update_id)
        self._synced = False
        if self._buffer:
            buffered = sorted(self._buffer, key=lambda item: item.final_update_id)
            self._buffer.clear()
            for event in buffered:
                self.on_diff_event(event)

    def on_diff_event(self, event: BinanceDiffEvent) -> None:
        if self._last_update_id is None:
            self._buffer.append(event)
            return

        if event.final_update_id < self._last_update_id:
            return

        if not self._synced:
            if not (event.first_update_id <= self._last_update_id <= event.final_update_id):
                self.desynced = True
                return
            self._synced = True
        elif event.first_update_id != self._last_update_id + 1:
            self.desynced = True
            return

        self._book.apply_delta(event.delta)
        self._last_update_id = event.final_update_id

    def needs_resync(self) -> bool:
        return self.desynced

    def reset_for_resync(self) -> None:
        self._buffer.clear()
        self._last_update_id = None
        self._synced = False
        self.desynced = False
