from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import yaml


@dataclass(frozen=True)
class SymbolsConfig:
    leader: str
    exec_symbol: str


@dataclass(frozen=True)
class ThresholdsConfig:
    window: int
    depth_k: int
    quantile_high: float
    quantile_low: float
    min_samples: int
    signal_score_min: float


@dataclass(frozen=True)
class RiskPhaseConfig:
    risk_frac: float
    max_daily_loss: float


@dataclass(frozen=True)
class RiskConfig:
    phases: dict[str, RiskPhaseConfig]


@dataclass(frozen=True)
class LiveConfig:
    out_dir: str
    journal_filename: str
    bybit_ws: str
    bybit_depth: int
    binance_ws: str
    binance_rest: str
    binance_depth: int
    binance_depth_interval_ms: int


@dataclass(frozen=True)
class EngineConfig:
    symbols: SymbolsConfig
    thresholds: ThresholdsConfig
    risk: RiskConfig
    live: LiveConfig | None = None


def _require(data: dict, key: str) -> Any:
    if key not in data:
        raise KeyError(f"Missing config key: {key}")
    return data[key]


def load_config(path: str) -> EngineConfig:
    with open(path, "r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}

    symbols_raw = _require(raw, "symbols")
    thresholds_raw = _require(raw, "thresholds")
    risk_raw = _require(raw, "risk_phases")

    symbols = SymbolsConfig(
        leader=str(_require(symbols_raw, "leader")),
        exec_symbol=str(_require(symbols_raw, "exec")),
    )

    thresholds = ThresholdsConfig(
        window=int(_require(thresholds_raw, "window")),
        depth_k=int(_require(thresholds_raw, "depth_k")),
        quantile_high=float(_require(thresholds_raw, "quantile_high")),
        quantile_low=float(_require(thresholds_raw, "quantile_low")),
        min_samples=int(_require(thresholds_raw, "min_samples")),
        signal_score_min=float(_require(thresholds_raw, "signal_score_min")),
    )

    phases = {}
    for name, values in risk_raw.items():
        phases[str(name)] = RiskPhaseConfig(
            risk_frac=float(_require(values, "risk_frac")),
            max_daily_loss=float(_require(values, "max_daily_loss")),
        )

    risk = RiskConfig(phases=phases)

    live_raw = raw.get("live", {}) or {}
    out_dir = str(live_raw.get("out_dir", "_out/live"))
    journal_filename = str(live_raw.get("journal_filename", f"{symbols.leader.lower()}_live.jsonl"))
    live = LiveConfig(
        out_dir=out_dir,
        journal_filename=journal_filename,
        bybit_ws=str(live_raw.get("bybit_ws", "wss://stream.bybit.com/v5/public/spot")),
        bybit_depth=int(live_raw.get("bybit_depth", 50)),
        binance_ws=str(live_raw.get("binance_ws", "wss://stream.binance.com:9443/ws")),
        binance_rest=str(live_raw.get("binance_rest", "https://api.binance.com/api/v3/depth")),
        binance_depth=int(live_raw.get("binance_depth", 50)),
        binance_depth_interval_ms=int(live_raw.get("binance_depth_interval_ms", 100)),
    )

    return EngineConfig(symbols=symbols, thresholds=thresholds, risk=risk, live=live)
