from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from sublimine.contracts.types import OrderAck, OrderFill, OrderRequest


class MT5Adapter(Protocol):
    def submit(self, request: OrderRequest) -> tuple[OrderAck, OrderFill]:
        ...


def _resolve_price(request: OrderRequest) -> float:
    if request.price is not None:
        return float(request.price)
    if isinstance(request.meta, dict):
        for key in ("market_price", "mid_price", "last_price", "mark_price"):
            value = request.meta.get(key)
            if isinstance(value, (int, float)):
                return float(value)
    return 0.0


@dataclass
class PaperAdapter:
    fee_bps: float = 0.0

    def submit(self, request: OrderRequest) -> tuple[OrderAck, OrderFill]:
        price = _resolve_price(request)
        fee = abs(float(request.qty)) * price * (self.fee_bps / 10_000.0)
        ack = OrderAck(
            request_id=request.id,
            ts_utc=request.ts_utc,
            status="ACK",
            reason=None,
            order_id=f"paper_{request.id}",
            meta={},
        )
        fill = OrderFill(
            request_id=request.id,
            ts_utc=request.ts_utc,
            price=price,
            qty=float(request.qty),
            fee=fee,
            meta={},
        )
        return ack, fill


@dataclass
class MockMT5Adapter:
    def submit(self, request: OrderRequest) -> tuple[OrderAck, OrderFill]:
        price = _resolve_price(request)
        ack = OrderAck(
            request_id=request.id,
            ts_utc=request.ts_utc,
            status="ACK",
            reason="mock",
            order_id=f"mock_{request.id}",
            meta={},
        )
        fill = OrderFill(
            request_id=request.id,
            ts_utc=request.ts_utc,
            price=price,
            qty=float(request.qty),
            fee=0.0,
            meta={"mock": True},
        )
        return ack, fill
