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
class EngineConfig:
    symbols: SymbolsConfig
    thresholds: ThresholdsConfig
    risk: RiskConfig


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

    return EngineConfig(symbols=symbols, thresholds=thresholds, risk=risk)
