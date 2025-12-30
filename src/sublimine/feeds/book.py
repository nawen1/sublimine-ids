from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable

from sublimine.contracts.types import BookDelta, BookLevel, BookSnapshot, Side, Venue


@dataclass
class OrderBook:
    symbol: str
    venue: Venue
    depth: int
    bids: Dict[float, float]
    asks: Dict[float, float]

    @classmethod
    def empty(cls, symbol: str, venue: Venue, depth: int) -> "OrderBook":
        return cls(symbol=symbol, venue=venue, depth=depth, bids={}, asks={})

    def apply_snapshot(self, snapshot: BookSnapshot) -> None:
        self.symbol = snapshot.symbol
        self.venue = snapshot.venue
        self.depth = snapshot.depth
        self.bids = {level.price: level.size for level in snapshot.bids}
        self.asks = {level.price: level.size for level in snapshot.asks}
        self._trim()

    def apply_delta(self, delta: BookDelta) -> None:
        self.symbol = delta.symbol
        self.venue = delta.venue
        if delta.is_snapshot:
            snapshot = BookSnapshot(
                symbol=delta.symbol,
                venue=delta.venue,
                ts_utc=delta.ts_utc,
                bids=delta.bids,
                asks=delta.asks,
                depth=self.depth,
            )
            self.apply_snapshot(snapshot)
        else:
            self._apply_levels(self.bids, delta.bids)
            self._apply_levels(self.asks, delta.asks)
        self._trim()

    def _trim(self) -> None:
        if self.depth <= 0:
            self.bids = {}
            self.asks = {}
            return
        if len(self.bids) > self.depth:
            prices = sorted(self.bids.keys(), reverse=True)[: self.depth]
            self.bids = {price: self.bids[price] for price in prices}
        if len(self.asks) > self.depth:
            prices = sorted(self.asks.keys())[: self.depth]
            self.asks = {price: self.asks[price] for price in prices}

    def _apply_levels(self, book: Dict[float, float], levels: Iterable[BookLevel]) -> None:
        for level in levels:
            if level.size == 0:
                book.pop(level.price, None)
            else:
                book[level.price] = level.size

    def top_n(self, side: Side, n: int) -> list[BookLevel]:
        if side == Side.BUY:
            prices = sorted(self.bids.keys(), reverse=True)
            levels = [BookLevel(price=p, size=self.bids[p]) for p in prices[:n]]
        else:
            prices = sorted(self.asks.keys())
            levels = [BookLevel(price=p, size=self.asks[p]) for p in prices[:n]]
        return levels

    def best_bid(self) -> BookLevel | None:
        if not self.bids:
            return None
        price = max(self.bids.keys())
        return BookLevel(price=price, size=self.bids[price])

    def best_ask(self) -> BookLevel | None:
        if not self.asks:
            return None
        price = min(self.asks.keys())
        return BookLevel(price=price, size=self.asks[price])
