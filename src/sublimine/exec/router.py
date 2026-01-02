from __future__ import annotations

from dataclasses import dataclass, field

from sublimine.contracts.types import EventType, OrderFill, PositionSnapshot, Side, TradeIntent
from sublimine.core.bus import EventBus
from sublimine.exec.mt5_adapter import MT5Adapter
from sublimine.exec.oms import OMS


@dataclass
class OrderRouter:
    adapter: MT5Adapter
    oms: OMS
    bus: EventBus | None = None
    shadow: bool = True
    _positions: dict[str, tuple[float, float]] = field(default_factory=dict)

    def submit(self, intent: TradeIntent) -> str | None:
        request = self.oms.build_request(intent)
        if request is None:
            return None

        self._publish(EventType.ORDER_REQUEST, request)
        if self.shadow:
            return request.id

        ack, fill = self.adapter.submit(request)
        self._publish(EventType.ORDER_ACK, ack)
        self._publish(EventType.ORDER_FILL, fill)
        snapshot = self._apply_fill(intent.symbol, intent.direction, fill)
        if snapshot is not None:
            self._publish(EventType.POSITION_SNAPSHOT, snapshot)
        return request.id

    def _publish(self, event_type: EventType, payload: object) -> None:
        if self.bus is None:
            return
        self.bus.publish(event_type, payload)

    def _apply_fill(self, symbol: str, side: Side, fill: OrderFill) -> PositionSnapshot | None:
        qty = float(fill.qty)
        price = float(fill.price)
        if qty <= 0.0:
            return None

        pos_qty, pos_avg = self._positions.get(symbol, (0.0, 0.0))
        signed_qty = qty if side == Side.BUY else -qty
        new_qty = pos_qty + signed_qty

        if pos_qty == 0.0 or (pos_qty > 0.0 and signed_qty > 0.0) or (pos_qty < 0.0 and signed_qty < 0.0):
            total = abs(pos_qty) + abs(signed_qty)
            new_avg = ((abs(pos_qty) * pos_avg) + (abs(signed_qty) * price)) / max(total, 1.0e-12)
        else:
            if abs(signed_qty) >= abs(pos_qty):
                new_avg = price if new_qty != 0.0 else 0.0
            else:
                new_avg = pos_avg

        self._positions[symbol] = (new_qty, new_avg)
        unrealized = (price - new_avg) * new_qty
        return PositionSnapshot(
            symbol=symbol,
            ts_utc=fill.ts_utc,
            qty=new_qty,
            avg_price=new_avg,
            unrealized_pnl=unrealized,
            meta={"mark_price": price},
        )
