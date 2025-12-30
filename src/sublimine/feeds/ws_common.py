from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ReconnectPolicy:
    base_delay: float = 1.0
    max_delay: float = 30.0
    factor: float = 2.0
    _attempts: int = 0

    def next_delay(self) -> float:
        delay = min(self.base_delay * (self.factor**self._attempts), self.max_delay)
        self._attempts += 1
        return delay

    def reset(self) -> None:
        self._attempts = 0
