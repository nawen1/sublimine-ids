from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass
class RiskGates:
    max_trades_per_day: int = 1

    def __post_init__(self) -> None:
        self._trade_dates: dict[str, int] = {}

    def allow_trade(self, ts_utc: datetime) -> bool:
        date_key = ts_utc.date().isoformat()
        return self._trade_dates.get(date_key, 0) < self.max_trades_per_day

    def record_trade(self, ts_utc: datetime) -> None:
        date_key = ts_utc.date().isoformat()
        self._trade_dates[date_key] = self._trade_dates.get(date_key, 0) + 1

    def correlation_bucket_ok(self, bucket: str) -> bool:
        return True
