from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass, field
from datetime import datetime

from sublimine.contracts.types import OrderRequest, Side, TradeIntent, Venue

_TINY = 1e-12


def intent_id(intent: TradeIntent) -> str:
    direction = getattr(intent.direction, "value", str(intent.direction))
    raw = "|".join(
        [
            intent.symbol,
            direction,
            intent.ts_utc.isoformat(),
            f"{intent.score:.6f}",
            f"{intent.risk_frac:.6f}",
        ]
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]


def size_lots(
    equity: float,
    risk_frac: float,
    entry_price: float,
    stop_price: float,
    tick_size: float,
    tick_value_per_lot: float,
    vol_min: float,
    vol_step: float,
) -> float:
    risk_amount = float(equity) * float(risk_frac)
    stop_distance = abs(float(entry_price) - float(stop_price))
    ticks = stop_distance / float(tick_size)
    loss_per_lot = ticks * float(tick_value_per_lot)
    lots = risk_amount / max(loss_per_lot, _TINY)
    if vol_step > 0:
        steps = math.floor(lots / float(vol_step))
        lots = steps * float(vol_step)
    if lots < float(vol_min):
        lots = float(vol_min)
    return float(lots)


@dataclass
class OMS:
    venue: Venue
    equity: float
    tick_size: float
    tick_value_per_lot: float
    vol_min: float
    vol_step: float
    _requests: dict[str, OrderRequest] = field(default_factory=dict)

    def build_request(self, intent: TradeIntent, *, ts_utc: datetime | None = None) -> OrderRequest | None:
        intent_key = intent_id(intent)
        if intent_key in self._requests:
            return None

        entry_plan = intent.entry_plan if isinstance(intent.entry_plan, dict) else {}
        stop_plan = intent.stop_plan if isinstance(intent.stop_plan, dict) else {}

        order_type = str(entry_plan.get("type", "MARKET")).upper()
        price_raw = entry_plan.get("price")
        stop_price_raw = stop_plan.get("stop_price")

        price = float(price_raw) if price_raw is not None else None
        stop_price = float(stop_price_raw) if stop_price_raw is not None else None

        qty = 0.0
        if price is not None and stop_price is not None:
            qty = size_lots(
                self.equity,
                intent.risk_frac,
                price,
                stop_price,
                self.tick_size,
                self.tick_value_per_lot,
                self.vol_min,
                self.vol_step,
            )

        meta = dict(intent.meta)
        meta.update(
            {
                "entry_plan": dict(entry_plan),
                "stop_plan": dict(stop_plan),
                "take_plan": dict(intent.take_plan) if isinstance(intent.take_plan, dict) else {},
                "risk_frac": float(intent.risk_frac),
                "score": float(intent.score),
            }
        )

        request = OrderRequest(
            id=f"{intent_key}-1",
            symbol=intent.symbol,
            venue=self.venue,
            ts_utc=ts_utc or intent.ts_utc,
            side=intent.direction,
            order_type=order_type,
            price=price,
            qty=qty,
            intent_id=intent_key,
            meta=meta,
        )
        self._requests[intent_key] = request
        return request
