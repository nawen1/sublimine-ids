from __future__ import annotations

from datetime import datetime, timezone

from sublimine.contracts.types import EventType, Side, TradeIntent, Venue
from sublimine.core.bus import EventBus
from sublimine.exec.mt5_adapter import PaperAdapter
from sublimine.exec.oms import OMS
from sublimine.exec.router import OrderRouter


def test_router_paper_exec_emits_execution_events() -> None:
    bus = EventBus()
    captured: dict[EventType, list] = {
        EventType.ORDER_REQUEST: [],
        EventType.ORDER_ACK: [],
        EventType.ORDER_FILL: [],
        EventType.POSITION_SNAPSHOT: [],
    }
    for event_type, bucket in captured.items():
        bus.subscribe(event_type, bucket.append)

    oms = OMS(
        venue=Venue.MT5,
        equity=10_000.0,
        tick_size=1.0,
        tick_value_per_lot=1.0,
        vol_min=0.1,
        vol_step=0.1,
    )
    router = OrderRouter(adapter=PaperAdapter(), oms=oms, bus=bus, shadow=False)
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    intent = TradeIntent(
        symbol="BTCUSD",
        direction=Side.BUY,
        score=0.9,
        risk_frac=0.01,
        entry_plan={"type": "STOP", "price": 100.0, "max_slippage_bps": 2.0},
        stop_plan={"stop_price": 95.0},
        ts_utc=ts,
        take_plan={"take_price": 107.5},
    )

    order_id = router.submit(intent)

    assert order_id is not None
    assert len(captured[EventType.ORDER_REQUEST]) == 1
    assert len(captured[EventType.ORDER_ACK]) == 1
    assert len(captured[EventType.ORDER_FILL]) == 1
    assert len(captured[EventType.POSITION_SNAPSHOT]) == 1

    req = captured[EventType.ORDER_REQUEST][0]
    ack = captured[EventType.ORDER_ACK][0]
    fill = captured[EventType.ORDER_FILL][0]
    snapshot = captured[EventType.POSITION_SNAPSHOT][0]

    assert ack.request_id == req.id
    assert fill.request_id == req.id
    assert snapshot.qty == fill.qty
    assert fill.price == req.price
