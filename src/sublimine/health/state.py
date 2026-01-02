from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING

from sublimine.config import ThresholdsConfig

if TYPE_CHECKING:
    from sublimine.health.health import DataQualitySnapshot


class EngineState(str, Enum):
    RUN = "RUN"
    DEGRADED = "DEGRADED"
    FREEZE = "FREEZE"
    KILL = "KILL"


@dataclass(frozen=True)
class EngineStateEvent:
    ts_utc: datetime
    state: str
    prev_state: str
    score_0_1: float
    reason_codes: list[str]
    meta: dict


class EngineGuard:
    def __init__(self, thresholds: ThresholdsConfig) -> None:
        self._thresholds = thresholds
        self.current_state = EngineState.RUN
        self.last_transition_ts: datetime | None = None
        self.kill_latched = False

    def update(self, snapshot: DataQualitySnapshot) -> EngineStateEvent | None:
        now = snapshot.ts_utc
        prev_state = self.current_state

        if self.kill_latched:
            target = EngineState.KILL
        else:
            reasons = snapshot.reason_codes
            has_missing_feed = any(code.startswith("missing_feed_") for code in reasons)
            has_stale = any(code.startswith("stale_") for code in reasons)
            has_soft = any(
                code.startswith(prefix)
                for prefix in ("low_eps_", "resync_rate_high_", "desync_rate_high_", "gaps_high_")
                for code in reasons
            )

            if snapshot.score_0_1 <= self._thresholds.health_kill_score or "mid_diff_high" in reasons or has_missing_feed:
                target = EngineState.KILL
            elif (
                snapshot.score_0_1 <= self._thresholds.health_freeze_score
                or "queue_depth_high" in reasons
                or has_stale
            ):
                target = EngineState.FREEZE
            elif snapshot.score_0_1 <= self._thresholds.health_degraded_score or has_soft:
                target = EngineState.DEGRADED
            else:
                target = EngineState.RUN

            if target == EngineState.KILL:
                self.kill_latched = True

        if prev_state == EngineState.FREEZE and target == EngineState.RUN:
            if snapshot.score_0_1 < self._thresholds.health_recover_score:
                target = EngineState.FREEZE
            else:
                if self.last_transition_ts is None:
                    target = EngineState.FREEZE
                else:
                    elapsed_ms = max((now - self.last_transition_ts).total_seconds() * 1000.0, 0.0)
                    if elapsed_ms < self._thresholds.health_recover_window_ms:
                        target = EngineState.FREEZE

        if prev_state == EngineState.DEGRADED and target == EngineState.RUN:
            if snapshot.score_0_1 < self._thresholds.health_recover_score:
                target = EngineState.DEGRADED

        if target == prev_state:
            return None

        self.current_state = target
        self.last_transition_ts = now

        return EngineStateEvent(
            ts_utc=now,
            state=target.value,
            prev_state=prev_state.value,
            score_0_1=snapshot.score_0_1,
            reason_codes=list(snapshot.reason_codes),
            meta={"kill_latched": self.kill_latched},
        )
