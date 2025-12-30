from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sublimine.contracts.types import Side, Venue
from sublimine.feeds.book import OrderBook


@dataclass(frozen=True)
class BookFeatureSet:
    symbol: str
    venue: Venue
    ts_utc: datetime
    mid: float
    spread: float
    microprice: float
    microprice_bias: float
    imbalance: float
    depth_near: float
    slope: float
    convexity: float


def compute_book_features(book: OrderBook, depth_k: int, ts_utc: datetime) -> BookFeatureSet | None:
    best_bid = book.best_bid()
    best_ask = book.best_ask()
    if best_bid is None or best_ask is None:
        return None

    mid = (best_bid.price + best_ask.price) / 2.0
    spread = max(best_ask.price - best_bid.price, 0.0)
    microprice = _microprice(best_bid.price, best_bid.size, best_ask.price, best_ask.size)
    microprice_bias = (microprice - mid) / spread if spread > 0 else 0.0

    bids = book.top_n(Side.BUY, depth_k)
    asks = book.top_n(Side.SELL, depth_k)

    bid_depth = sum(level.size for level in bids)
    ask_depth = sum(level.size for level in asks)
    depth_near = bid_depth + ask_depth

    imbalance = 0.0
    if bid_depth + ask_depth > 0:
        imbalance = (bid_depth - ask_depth) / (bid_depth + ask_depth)

    slope = _liquidity_slope(mid, bids, asks)
    convexity = _liquidity_convexity(bids, asks)

    return BookFeatureSet(
        symbol=book.symbol,
        venue=book.venue,
        ts_utc=ts_utc,
        mid=mid,
        spread=spread,
        microprice=microprice,
        microprice_bias=microprice_bias,
        imbalance=imbalance,
        depth_near=depth_near,
        slope=slope,
        convexity=convexity,
    )


def _microprice(bid_px: float, bid_sz: float, ask_px: float, ask_sz: float) -> float:
    denom = bid_sz + ask_sz
    if denom == 0:
        return (bid_px + ask_px) / 2.0
    return (bid_px * ask_sz + ask_px * bid_sz) / denom


def _liquidity_slope(mid: float, bids: list, asks: list) -> float:
    weighted_dist = 0.0
    total_size = 0.0
    for level in bids + asks:
        dist = abs(level.price - mid)
        weighted_dist += dist * level.size
        total_size += level.size
    if total_size == 0:
        return 0.0
    avg_dist = weighted_dist / total_size
    return 1.0 / (avg_dist + 1e-9)


def _liquidity_convexity(bids: list, asks: list) -> float:
    sizes = [level.size for level in bids + asks]
    if not sizes:
        return 0.0
    total = sum(sizes)
    top = sizes[0] + sizes[len(sizes) // 2] if len(sizes) > 1 else sizes[0]
    return top / total if total > 0 else 0.0
