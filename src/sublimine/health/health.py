from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from sublimine.config import ThresholdsConfig

if TYPE_CHECKING:
    from sublimine.contracts.types import Venue


@dataclass(frozen=True)
class DataQualitySnapshot:
    ts_utc: datetime
    symbol: str
    per_venue: dict[str, dict]
    queue_depth: int
    mid_by_venue: dict[str, float]
    mid_diff_bps: float | None
    score_0_1: float
    reason_codes: list[str]
    meta: dict


class HealthMonitor:
    def __init__(self, thresholds: ThresholdsConfig) -> None:
        self._thresholds = thresholds
        self._queue_depth = 0
        self._required_venues = ("BYBIT", "BINANCE")
        self._book_events: dict[str, deque[datetime]] = {}
        self._trade_events: dict[str, deque[datetime]] = {}
        self._feature_events: dict[str, deque[datetime]] = {}
        self._resync_events: dict[str, deque[datetime]] = {}
        self._desync_events: dict[str, deque[datetime]] = {}
        self._gap_events: dict[str, deque[datetime]] = {}
        self._last_book_ts: dict[str, datetime] = {}
        self._last_trade_ts: dict[str, datetime] = {}
        self._last_feature_ts: dict[str, datetime] = {}
        self._last_trade_price: dict[str, float] = {}
        self._last_mid: dict[str, float] = {}

    def observe_book(self, venue: Venue, ts_utc: datetime) -> None:
        venue_key = self._venue_key(venue)
        self._last_book_ts[venue_key] = ts_utc
        self._book_events.setdefault(venue_key, deque()).append(ts_utc)

    def observe_trade(self, venue: Venue, ts_utc: datetime, price: float | None = None) -> None:
        venue_key = self._venue_key(venue)
        self._last_trade_ts[venue_key] = ts_utc
        self._trade_events.setdefault(venue_key, deque()).append(ts_utc)
        if price is not None:
            self._last_trade_price[venue_key] = float(price)

    def observe_feature(self, venue: Venue, ts_utc: datetime, mid: float) -> None:
        venue_key = self._venue_key(venue)
        self._last_feature_ts[venue_key] = ts_utc
        self._feature_events.setdefault(venue_key, deque()).append(ts_utc)
        self._last_mid[venue_key] = float(mid)

    def observe_resync(self, venue: Venue, ts_utc: datetime) -> None:
        venue_key = self._venue_key(venue)
        self._resync_events.setdefault(venue_key, deque()).append(ts_utc)

    def observe_desync(self, venue: Venue, ts_utc: datetime) -> None:
        venue_key = self._venue_key(venue)
        self._desync_events.setdefault(venue_key, deque()).append(ts_utc)

    def observe_gap(self, venue: Venue, ts_utc: datetime) -> None:
        venue_key = self._venue_key(venue)
        self._gap_events.setdefault(venue_key, deque()).append(ts_utc)

    def set_queue_depth(self, depth: int) -> None:
        self._queue_depth = int(depth)

    def snapshot(self, symbol: str, ref_ts: datetime) -> DataQualitySnapshot:
        per_venue: dict[str, dict] = {}
        reason_codes: list[str] = []
        score = 1.0
        hard_fail = False
        mid_by_venue: dict[str, float] = {}

        for venue_key in self._required_venues:
            book_ts = self._last_book_ts.get(venue_key)
            trade_ts = self._last_trade_ts.get(venue_key)
            feature_ts = self._last_feature_ts.get(venue_key)
            latest_ts = self._max_ts(book_ts, trade_ts, feature_ts)

            if latest_ts is None:
                staleness_ms = None
                reason_codes.append(f"missing_feed_{venue_key}")
                hard_fail = True
            else:
                staleness_ms = max((ref_ts - latest_ts).total_seconds() * 1000.0, 0.0)
                if staleness_ms > self._thresholds.max_stale_ms:
                    reason_codes.append(f"stale_{venue_key}")
                    hard_fail = True

            eps = self._compute_eps(venue_key, ref_ts)
            if eps < self._thresholds.health_min_eps:
                reason_codes.append(f"low_eps_{venue_key}")
                score *= 0.5

            resync_per_min = self._compute_rate(self._resync_events, venue_key, ref_ts)
            if resync_per_min > self._thresholds.health_max_resync_per_min:
                reason_codes.append(f"resync_rate_high_{venue_key}")
                score *= 0.6

            desync_per_min = self._compute_rate(self._desync_events, venue_key, ref_ts)
            if desync_per_min > self._thresholds.health_max_desync_per_min:
                reason_codes.append(f"desync_rate_high_{venue_key}")
                score *= 0.6

            gap_count = self._count_window(self._gap_events, venue_key, ref_ts, self._thresholds.health_rate_window_ms)
            if gap_count > self._thresholds.health_max_gaps_in_window:
                reason_codes.append(f"gaps_high_{venue_key}")
                score *= 0.7

            mid = self._last_mid.get(venue_key)
            if mid is None:
                mid = self._last_trade_price.get(venue_key)
            if mid is not None:
                mid_by_venue[venue_key] = float(mid)

            per_venue[venue_key] = {
                "last_book_ts_utc": self._iso_ts(book_ts),
                "last_trade_ts_utc": self._iso_ts(trade_ts),
                "last_feature_ts_utc": self._iso_ts(feature_ts),
                "staleness_ms": staleness_ms,
                "eps": eps,
                "resync_per_min": resync_per_min,
                "desync_per_min": desync_per_min,
                "gap_count": gap_count,
            }

        missing_mid = [venue for venue in self._required_venues if venue not in mid_by_venue]
        if missing_mid:
            reason_codes.append("mid_missing")
            hard_fail = True
            mid_diff_bps = None
        else:
            mid_a = mid_by_venue[self._required_venues[0]]
            mid_b = mid_by_venue[self._required_venues[1]]
            mid_avg = (mid_a + mid_b) / 2.0
            mid_diff_bps = abs(mid_a - mid_b) / max(mid_avg, 1e-12) * 10_000.0
            if mid_diff_bps > self._thresholds.max_mid_diff_bps:
                reason_codes.append("mid_diff_high")
                hard_fail = True

        if self._queue_depth > self._thresholds.health_max_queue_depth:
            reason_codes.append("queue_depth_high")
            hard_fail = True

        if hard_fail:
            score = 0.0

        score = self._clamp(score)
        reason_codes = self._dedupe(reason_codes)

        meta = {
            "eps_window_ms": self._thresholds.health_eps_window_ms,
            "rate_window_ms": self._thresholds.health_rate_window_ms,
            "missing_mid_venues": missing_mid,
        }

        return DataQualitySnapshot(
            ts_utc=ref_ts,
            symbol=symbol,
            per_venue=per_venue,
            queue_depth=self._queue_depth,
            mid_by_venue=mid_by_venue,
            mid_diff_bps=mid_diff_bps,
            score_0_1=score,
            reason_codes=reason_codes,
            meta=meta,
        )

    @staticmethod
    def _venue_key(venue: object) -> str:
        return str(getattr(venue, "value", venue))

    @staticmethod
    def _iso_ts(ts: datetime | None) -> str | None:
        return ts.isoformat() if ts is not None else None

    @staticmethod
    def _max_ts(*values: datetime | None) -> datetime | None:
        candidates = [value for value in values if value is not None]
        if not candidates:
            return None
        return max(candidates)

    def _compute_eps(self, venue_key: str, ref_ts: datetime) -> float:
        window_ms = self._thresholds.health_eps_window_ms
        if window_ms <= 0:
            return 0.0
        total = 0
        total += self._count_window(self._book_events, venue_key, ref_ts, window_ms)
        total += self._count_window(self._trade_events, venue_key, ref_ts, window_ms)
        total += self._count_window(self._feature_events, venue_key, ref_ts, window_ms)
        return total / (window_ms / 1000.0)

    def _compute_rate(self, store: dict[str, deque[datetime]], venue_key: str, ref_ts: datetime) -> float:
        window_ms = self._thresholds.health_rate_window_ms
        if window_ms <= 0:
            return 0.0
        count = self._count_window(store, venue_key, ref_ts, window_ms)
        return count / (window_ms / 60000.0)

    def _count_window(
        self,
        store: dict[str, deque[datetime]],
        venue_key: str,
        ref_ts: datetime,
        window_ms: int,
    ) -> int:
        cutoff = ref_ts - timedelta(milliseconds=window_ms)
        dq = store.setdefault(venue_key, deque())
        while dq and dq[0] < cutoff:
            dq.popleft()
        return sum(1 for ts in dq if ts <= ref_ts)

    @staticmethod
    def _dedupe(values: list[str]) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []
        for item in values:
            if item in seen:
                continue
            seen.add(item)
            ordered.append(item)
        return ordered

    @staticmethod
    def _clamp(score: float) -> float:
        if score < 0.0:
            return 0.0
        if score > 1.0:
            return 1.0
        return score
