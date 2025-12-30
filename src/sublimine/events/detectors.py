from __future__ import annotations

from collections import deque
from dataclasses import dataclass

from sublimine.contracts.types import SignalEvent
from sublimine.features.feature_engine import FeatureFrame


class RollingQuantile:
    def __init__(self, window: int) -> None:
        self._values: deque[float] = deque(maxlen=window)

    def update(self, value: float) -> None:
        self._values.append(value)

    def quantile(self, q: float) -> float | None:
        if not self._values:
            return None
        values = sorted(self._values)
        idx = int(q * (len(values) - 1))
        return values[idx]

    def ready(self, min_samples: int) -> bool:
        return len(self._values) >= min_samples


@dataclass
class DetectorConfig:
    window: int
    quantile_high: float
    quantile_low: float
    min_samples: int


class DetectorEngine:
    def __init__(self, config: DetectorConfig) -> None:
        self._config = config
        self._depth = RollingQuantile(config.window)
        self._ofi = RollingQuantile(config.window)
        self._bias = RollingQuantile(config.window)
        self._delta = RollingQuantile(config.window)
        self._progress = RollingQuantile(config.window)
        self._replen = RollingQuantile(config.window)
        self._sweep = RollingQuantile(config.window)
        self._return_speed = RollingQuantile(config.window)
        self._post_abs = RollingQuantile(config.window)
        self._basis = RollingQuantile(config.window)
        self._lead_lag = RollingQuantile(config.window)

    def evaluate(self, frame: FeatureFrame) -> list[SignalEvent]:
        self._depth.update(frame.depth_near)
        self._ofi.update(frame.ofi_z)
        self._bias.update(frame.microprice_bias)
        self._delta.update(frame.delta_size)
        self._progress.update(frame.price_progress)
        self._replen.update(frame.replenishment)
        self._sweep.update(frame.sweep_distance)
        self._return_speed.update(frame.return_speed)
        self._post_abs.update(frame.post_sweep_absorption)
        self._basis.update(abs(frame.basis_z))
        self._lead_lag.update(frame.lead_lag)

        if not self._depth.ready(self._config.min_samples):
            return []

        depth_low = self._depth.quantile(self._config.quantile_low)
        ofi_high = self._ofi.quantile(self._config.quantile_high)
        bias_high = self._bias.quantile(self._config.quantile_high)
        delta_high = self._delta.quantile(self._config.quantile_high)
        progress_low = self._progress.quantile(self._config.quantile_low)
        replen_high = self._replen.quantile(self._config.quantile_high)
        sweep_high = self._sweep.quantile(self._config.quantile_high)
        return_high = self._return_speed.quantile(self._config.quantile_high)
        post_abs_high = self._post_abs.quantile(self._config.quantile_high)
        basis_high = self._basis.quantile(self._config.quantile_high)
        lead_lag_high = self._lead_lag.quantile(self._config.quantile_high)

        signals: list[SignalEvent] = []
        if depth_low is not None and ofi_high is not None and bias_high is not None:
            if frame.depth_near <= depth_low and frame.ofi_z >= ofi_high and frame.microprice_bias >= bias_high:
                score = _avg(
                    _score_low(frame.depth_near, depth_low),
                    _score_high(frame.ofi_z, ofi_high),
                    _score_high(frame.microprice_bias, bias_high),
                )
                signals.append(
                    SignalEvent(
                        event_name="E1",
                        symbol=frame.symbol,
                        venue=frame.venue,
                        ts_utc=frame.ts_utc,
                        score_0_1=score,
                        reason_codes=["depth_near_low", "ofi_z_high", "microprice_bias_high"],
                        meta={"depth_near": frame.depth_near, "ofi_z": frame.ofi_z, "microprice_bias": frame.microprice_bias},
                    )
                )

        if delta_high is not None and progress_low is not None and replen_high is not None:
            if frame.delta_size >= delta_high and frame.price_progress <= progress_low and frame.replenishment >= replen_high:
                score = _avg(
                    _score_high(frame.delta_size, delta_high),
                    _score_low(frame.price_progress, progress_low),
                    _score_high(frame.replenishment, replen_high),
                )
                signals.append(
                    SignalEvent(
                        event_name="E2",
                        symbol=frame.symbol,
                        venue=frame.venue,
                        ts_utc=frame.ts_utc,
                        score_0_1=score,
                        reason_codes=["delta_high", "price_progress_low", "replenishment_high"],
                        meta={"delta_size": frame.delta_size, "price_progress": frame.price_progress, "replenishment": frame.replenishment},
                    )
                )

        if sweep_high is not None and return_high is not None and post_abs_high is not None:
            if frame.sweep_distance >= sweep_high and frame.return_speed >= return_high and frame.post_sweep_absorption >= post_abs_high:
                score = _avg(
                    _score_high(frame.sweep_distance, sweep_high),
                    _score_high(frame.return_speed, return_high),
                    _score_high(frame.post_sweep_absorption, post_abs_high),
                )
                signals.append(
                    SignalEvent(
                        event_name="E3",
                        symbol=frame.symbol,
                        venue=frame.venue,
                        ts_utc=frame.ts_utc,
                        score_0_1=score,
                        reason_codes=["sweep_distance_high", "return_speed_high", "post_sweep_absorption_high"],
                        meta={
                            "sweep_distance": frame.sweep_distance,
                            "return_speed": frame.return_speed,
                            "post_sweep_absorption": frame.post_sweep_absorption,
                        },
                    )
                )

        if basis_high is not None and lead_lag_high is not None:
            if abs(frame.basis_z) >= basis_high and frame.lead_lag >= lead_lag_high:
                score = _avg(
                    _score_high(abs(frame.basis_z), basis_high),
                    _score_high(frame.lead_lag, lead_lag_high),
                )
                signals.append(
                    SignalEvent(
                        event_name="E4",
                        symbol=frame.symbol,
                        venue=frame.venue,
                        ts_utc=frame.ts_utc,
                        score_0_1=score,
                        reason_codes=["basis_z_extreme", "lead_lag_high"],
                        meta={"basis_z": frame.basis_z, "lead_lag": frame.lead_lag},
                    )
                )

        return signals


def _score_high(value: float, threshold: float) -> float:
    if threshold == 0:
        return 0.0
    return min(max(value / threshold, 0.0), 1.0)


def _score_low(value: float, threshold: float) -> float:
    if threshold == 0:
        return 0.0
    if value <= threshold:
        return 1.0
    return min(max(threshold / value, 0.0), 1.0)


def _avg(*values: float) -> float:
    return sum(values) / len(values) if values else 0.0
