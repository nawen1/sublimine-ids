from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RiskPhase:
    name: str
    risk_frac: float
    max_daily_loss: float


BASE_RISK_PHASES = {
    "F0": RiskPhase(name="F0", risk_frac=0.0020, max_daily_loss=0.0100),
    "F1": RiskPhase(name="F1", risk_frac=0.0025, max_daily_loss=0.0125),
    "F2": RiskPhase(name="F2", risk_frac=0.0030, max_daily_loss=0.0150),
    "F3": RiskPhase(name="F3", risk_frac=0.0035, max_daily_loss=0.0175),
    "F4": RiskPhase(name="F4", risk_frac=0.0040, max_daily_loss=0.0200),
}
