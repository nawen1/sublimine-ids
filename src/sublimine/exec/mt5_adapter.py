from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from sublimine.contracts.types import TradeIntent


class MT5Adapter(Protocol):
    def place_order(self, intent: TradeIntent) -> str:
        ...


@dataclass
class MockMT5Adapter:
    def place_order(self, intent: TradeIntent) -> str:
        return f"mock_order_{intent.ts_utc.timestamp():.0f}"
