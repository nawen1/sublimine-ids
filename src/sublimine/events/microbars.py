from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from sublimine.features.feature_engine import FeatureFrame


@dataclass(frozen=True)
class MicroBar:
    open: float
    high: float
    low: float
    close: float
    ts_start: datetime
    ts_end: datetime
    n: int
    ofi_mean: float
    ofi_abs_mean: float
    replenishment_mean: float


def _epoch_ms(ts_utc: datetime) -> int:
    if ts_utc.tzinfo is None:
        ts_utc = ts_utc.replace(tzinfo=timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = ts_utc - epoch
    return delta.days * 86_400_000 + delta.seconds * 1_000 + delta.microseconds // 1_000


class MicroBarBuilder:
    def __init__(self, bar_interval_ms: int) -> None:
        if bar_interval_ms <= 0:
            raise ValueError("bar_interval_ms must be > 0")
        self._bar_interval_ms = int(bar_interval_ms)
        self._bucket: int | None = None
        self._ts_start: datetime | None = None
        self._ts_end: datetime | None = None
        self._open: float | None = None
        self._high: float | None = None
        self._low: float | None = None
        self._close: float | None = None
        self._n: int = 0
        self._ofi_sum: float = 0.0
        self._ofi_abs_sum: float = 0.0
        self._replen_sum: float = 0.0

    def update(self, frame: FeatureFrame) -> MicroBar | None:
        epoch_ms = _epoch_ms(frame.ts_utc)
        bucket = epoch_ms // self._bar_interval_ms

        if self._bucket is None:
            self._start_bucket(bucket, frame)
            return None

        if bucket == self._bucket:
            self._accumulate(frame)
            return None

        completed = self._finalize()
        self._start_bucket(bucket, frame)
        return completed

    def _start_bucket(self, bucket: int, frame: FeatureFrame) -> None:
        self._bucket = bucket
        self._ts_start = frame.ts_utc
        self._ts_end = frame.ts_utc
        self._open = frame.mid
        self._high = frame.mid
        self._low = frame.mid
        self._close = frame.mid
        self._n = 1
        self._ofi_sum = frame.ofi_z
        self._ofi_abs_sum = abs(frame.ofi_z)
        self._replen_sum = frame.replenishment

    def _accumulate(self, frame: FeatureFrame) -> None:
        if self._ts_end is None or self._high is None or self._low is None:
            self._start_bucket(self._bucket if self._bucket is not None else 0, frame)
            return

        self._ts_end = frame.ts_utc
        self._high = max(self._high, frame.mid)
        self._low = min(self._low, frame.mid)
        self._close = frame.mid
        self._n += 1
        self._ofi_sum += frame.ofi_z
        self._ofi_abs_sum += abs(frame.ofi_z)
        self._replen_sum += frame.replenishment

    def _finalize(self) -> MicroBar:
        if (
            self._ts_start is None
            or self._ts_end is None
            or self._open is None
            or self._high is None
            or self._low is None
            or self._close is None
            or self._n <= 0
        ):
            raise RuntimeError("MicroBarBuilder finalized without active bucket")

        n = self._n
        return MicroBar(
            open=self._open,
            high=self._high,
            low=self._low,
            close=self._close,
            ts_start=self._ts_start,
            ts_end=self._ts_end,
            n=n,
            ofi_mean=self._ofi_sum / n,
            ofi_abs_mean=self._ofi_abs_sum / n,
            replenishment_mean=self._replen_sum / n,
        )

