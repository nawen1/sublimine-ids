from __future__ import annotations

from dataclasses import dataclass

from sublimine.features.ofi import RollingStats


@dataclass
class BasisTracker:
    window: int

    def __post_init__(self) -> None:
        self._stats = RollingStats(self.window)
        self._last_leader: float | None = None
        self._last_follower: float | None = None

    def update(self, leader_mid: float, follower_mid: float) -> tuple[float, float, float]:
        basis = leader_mid - follower_mid
        self._stats.update(basis)
        basis_z = self._stats.zscore(basis)

        lead_lag = 0.0
        if self._last_leader is not None and self._last_follower is not None:
            leader_ret = leader_mid - self._last_leader
            follower_ret = follower_mid - self._last_follower
            if follower_ret == 0:
                lead_lag = 1.0 if leader_ret != 0 else 0.0
            else:
                lead_lag = min(abs(leader_ret / follower_ret), 3.0) / 3.0

        self._last_leader = leader_mid
        self._last_follower = follower_mid
        return basis, basis_z, lead_lag
