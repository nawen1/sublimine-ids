from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SignalQualityScore:
    score_0_1: float
    reason_codes: list[str]


def clamp_score(value: float) -> float:
    return max(0.0, min(1.0, value))
