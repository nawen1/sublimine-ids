from __future__ import annotations

from dataclasses import dataclass

from sublimine.contracts.types import SignalEvent, Side, TradeIntent


@dataclass
class BTCPlaybook:
    exec_symbol: str = "BTCUSD_CFD"

    def on_signal(self, signal: SignalEvent, risk_frac: float) -> TradeIntent | None:
        if signal.event_name not in {"E1", "E2", "E3", "E4"}:
            return None

        direction_meta = signal.meta.get("direction")
        if isinstance(direction_meta, Side):
            direction = direction_meta
        elif isinstance(direction_meta, str) and direction_meta.strip().upper() in {"BUY", "SELL"}:
            direction = Side(direction_meta.strip().upper())
        else:
            direction = Side.BUY
            bias = signal.meta.get("microprice_bias")
            if isinstance(bias, (int, float)) and bias < 0:
                direction = Side.SELL

        return TradeIntent(
            symbol=self.exec_symbol,
            direction=direction,
            score=signal.score_0_1,
            risk_frac=risk_frac,
            entry_plan={"mode": "shadow", "event": signal.event_name},
            stop_plan={"mode": "shadow"},
            ts_utc=signal.ts_utc,
        )
