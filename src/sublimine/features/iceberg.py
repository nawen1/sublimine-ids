from __future__ import annotations

from collections import deque
from dataclasses import dataclass

from sublimine.contracts.types import BookLevel


@dataclass
class IcebergTracker:
    window: int

    def __post_init__(self) -> None:
        self._last_bid: BookLevel | None = None
        self._last_ask: BookLevel | None = None
        self._scores: deque[float] = deque(maxlen=self.window)

    def update(self, best_bid: BookLevel | None, best_ask: BookLevel | None) -> float:
        score = 0.0
        if self._last_bid and best_bid:
            if best_bid.price == self._last_bid.price and best_bid.size > self._last_bid.size:
                score += 1.0
        if self._last_ask and best_ask:
            if best_ask.price == self._last_ask.price and best_ask.size > self._last_ask.size:
                score += 1.0
        if best_bid or best_ask:
            self._scores.append(score)
        self._last_bid = best_bid
        self._last_ask = best_ask
        return self.value

    @property
    def value(self) -> float:
        if not self._scores:
            return 0.0
        return sum(self._scores) / len(self._scores)
