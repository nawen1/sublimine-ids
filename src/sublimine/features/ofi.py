from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from math import sqrt

from sublimine.contracts.types import BookLevel


class RollingStats:
    def __init__(self, window: int) -> None:
        self._window = window
        self._values: deque[float] = deque(maxlen=window)

    def update(self, value: float) -> None:
        self._values.append(value)

    def mean_std(self) -> tuple[float, float]:
        if not self._values:
            return 0.0, 0.0
        mean = sum(self._values) / len(self._values)
        var = sum((v - mean) ** 2 for v in self._values) / len(self._values)
        return mean, sqrt(var)

    def zscore(self, value: float) -> float:
        mean, std = self.mean_std()
        if std == 0:
            return 0.0
        return (value - mean) / std


@dataclass
class OFIState:
    window: int
    last_bid: BookLevel | None = None
    last_ask: BookLevel | None = None

    def __post_init__(self) -> None:
        self._stats = RollingStats(self.window)

    def update(self, best_bid: BookLevel | None, best_ask: BookLevel | None) -> tuple[float, float]:
        if best_bid is None or best_ask is None:
            return 0.0, 0.0
        if self.last_bid is None or self.last_ask is None:
            self.last_bid = best_bid
            self.last_ask = best_ask
            return 0.0, 0.0

        ofi = _compute_ofi(self.last_bid, self.last_ask, best_bid, best_ask)
        self._stats.update(ofi)
        z = self._stats.zscore(ofi)
        self.last_bid = best_bid
        self.last_ask = best_ask
        return ofi, z


def _compute_ofi(prev_bid: BookLevel, prev_ask: BookLevel, curr_bid: BookLevel, curr_ask: BookLevel) -> float:
    ofi = 0.0
    if curr_bid.price > prev_bid.price:
        ofi += curr_bid.size
    elif curr_bid.price == prev_bid.price:
        ofi += curr_bid.size - prev_bid.size
    else:
        ofi -= prev_bid.size

    if curr_ask.price < prev_ask.price:
        ofi -= curr_ask.size
    elif curr_ask.price == prev_ask.price:
        ofi -= curr_ask.size - prev_ask.size
    else:
        ofi += prev_ask.size

    return ofi
