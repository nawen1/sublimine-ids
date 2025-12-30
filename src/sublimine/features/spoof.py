from __future__ import annotations

from collections import deque
from dataclasses import dataclass

from sublimine.contracts.types import BookDelta


@dataclass
class SpoofTracker:
    window: int

    def __post_init__(self) -> None:
        self._scores: deque[float] = deque(maxlen=self.window)

    def update(self, delta: BookDelta) -> float:
        levels = list(delta.bids) + list(delta.asks)
        if not levels:
            return self.value
        removed = sum(1 for level in levels if level.size == 0)
        score = removed / len(levels)
        self._scores.append(score)
        return self.value

    @property
    def value(self) -> float:
        if not self._scores:
            return 0.0
        return sum(self._scores) / len(self._scores)
