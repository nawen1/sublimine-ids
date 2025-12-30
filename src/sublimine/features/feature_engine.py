from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sublimine.contracts.types import BookDelta, BookSnapshot, TradePrint, Venue
from sublimine.feeds.book import OrderBook
from sublimine.features.basis import BasisTracker
from sublimine.features.book_features import compute_book_features
from sublimine.features.iceberg import IcebergTracker
from sublimine.features.ofi import OFIState
from sublimine.features.spoof import SpoofTracker
from sublimine.features.vpin import VPINTracker


@dataclass(frozen=True)
class FeatureFrame:
    symbol: str
    venue: Venue
    ts_utc: datetime
    depth_near: float
    microprice_bias: float
    ofi_z: float
    delta_size: float
    price_progress: float
    replenishment: float
    sweep_distance: float
    return_speed: float
    post_sweep_absorption: float
    basis_z: float
    lead_lag: float
    microprice: float
    mid: float


@dataclass
class FeatureEngine:
    symbol: str
    depth_k: int
    window: int

    def __post_init__(self) -> None:
        self._book = OrderBook.empty(self.symbol, venue=Venue.BYBIT, depth=self.depth_k)
        self._ofi = OFIState(window=self.window)
        self._iceberg = IcebergTracker(window=self.window)
        self._spoof = SpoofTracker(window=self.window)
        self._vpin = VPINTracker(bucket_size=10.0, window=self.window)
        self._basis = BasisTracker(window=self.window)
        self._last_mid: float | None = None
        self._last_ts: datetime | None = None

    def on_book_snapshot(self, snapshot: BookSnapshot) -> FeatureFrame | None:
        self._book.apply_snapshot(snapshot)
        return self._compute_features(snapshot.ts_utc, None)

    def on_book_delta(self, delta: BookDelta) -> FeatureFrame | None:
        prev_mid = self._last_mid
        prev_ts = self._last_ts
        delta_size = sum(abs(level.size) for level in delta.bids + delta.asks)
        self._spoof.update(delta)
        self._book.apply_delta(delta)
        return self._compute_features(delta.ts_utc, (prev_mid, prev_ts, delta_size))

    def on_trade(self, trade: TradePrint) -> float:
        return self._vpin.update(trade)

    def _compute_features(
        self,
        ts_utc: datetime,
        delta_context: tuple[float | None, datetime | None, float] | None,
    ) -> FeatureFrame | None:
        features = compute_book_features(self._book, self.depth_k, ts_utc)
        if features is None:
            return None

        best_bid = self._book.best_bid()
        best_ask = self._book.best_ask()
        _, ofi_z = self._ofi.update(best_bid, best_ask)
        replenishment = self._iceberg.update(best_bid, best_ask)

        prev_mid = None
        prev_ts = None
        delta_size = 0.0
        if delta_context is not None:
            prev_mid, prev_ts, delta_size = delta_context

        price_progress = 0.0
        sweep_distance = 0.0
        return_speed = 0.0
        if prev_mid is not None:
            price_progress = abs(features.mid - prev_mid)
            sweep_distance = price_progress
            if prev_ts is not None:
                dt = max((ts_utc - prev_ts).total_seconds(), 1e-6)
                return_speed = price_progress / dt

        post_sweep_absorption = replenishment if sweep_distance > 0 else 0.0

        _, basis_z, lead_lag = self._basis.update(features.mid, features.mid)

        self._last_mid = features.mid
        self._last_ts = ts_utc

        return FeatureFrame(
            symbol=features.symbol,
            venue=features.venue,
            ts_utc=ts_utc,
            depth_near=features.depth_near,
            microprice_bias=features.microprice_bias,
            ofi_z=ofi_z,
            delta_size=delta_size,
            price_progress=price_progress,
            replenishment=replenishment,
            sweep_distance=sweep_distance,
            return_speed=return_speed,
            post_sweep_absorption=post_sweep_absorption,
            basis_z=basis_z,
            lead_lag=lead_lag,
            microprice=features.microprice,
            mid=features.mid,
        )
