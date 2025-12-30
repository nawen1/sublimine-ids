from __future__ import annotations

from dataclasses import dataclass

from sublimine.contracts.types import TradeIntent
from sublimine.exec.mt5_adapter import MT5Adapter


@dataclass
class OrderRouter:
    adapter: MT5Adapter
    shadow: bool = True

    def submit(self, intent: TradeIntent) -> str | None:
        if self.shadow:
            return None
        return self.adapter.place_order(intent)
