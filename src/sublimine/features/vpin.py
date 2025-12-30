from __future__ import annotations

from collections import deque
from dataclasses import dataclass

from sublimine.contracts.types import Side, TradePrint


@dataclass
class VPINTracker:
    bucket_size: float
    window: int

    def __post_init__(self) -> None:
        self._bucket_buy = 0.0
        self._bucket_sell = 0.0
        self._buckets: deque[float] = deque(maxlen=self.window)

    def update(self, trade: TradePrint) -> float:
        if trade.aggressor_side == Side.BUY:
            self._bucket_buy += trade.size
        elif trade.aggressor_side == Side.SELL:
            self._bucket_sell += trade.size

        total = self._bucket_buy + self._bucket_sell
        while total >= self.bucket_size and self.bucket_size > 0:
            buy = min(self._bucket_buy, self.bucket_size)
            sell = min(self._bucket_sell, self.bucket_size - buy)
            imbalance = abs(buy - sell) / self.bucket_size
            self._buckets.append(imbalance)
            self._bucket_buy = max(0.0, self._bucket_buy - buy)
            self._bucket_sell = max(0.0, self._bucket_sell - sell)
            total = self._bucket_buy + self._bucket_sell

        return self.value

    @property
    def value(self) -> float:
        if not self._buckets:
            return 0.0
        return sum(self._buckets) / len(self._buckets)
