import tempfile
from pathlib import Path
from uuid import uuid4

import yaml

from sublimine.config import load_config


def _load_config(*, active_phase: str, phases: dict):
    data = {
        "symbols": {"leader": "BTCUSDT", "exec": "BTCUSD_CFD"},
        "thresholds": {
            "window": 5,
            "depth_k": 1,
            "quantile_high": 0.6,
            "quantile_low": 0.4,
            "min_samples": 2,
            "signal_score_min": 0.2,
        },
        "risk": {"active_phase": active_phase},
        "risk_phases": phases,
    }
    base_tmp = Path(tempfile.gettempdir())
    path = base_tmp / f"sublimine_config_{uuid4().hex}.yaml"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    try:
        return load_config(str(path))
    finally:
        try:
            path.unlink()
        except FileNotFoundError:
            pass


def test_invalid_active_phase_falls_back_to_f0():
    phases = {
        "F0": {"risk_frac": 0.002, "max_daily_loss": 0.01},
        "F1": {"risk_frac": 0.003, "max_daily_loss": 0.02},
    }
    config = _load_config(active_phase="F9", phases=phases)

    assert config.risk.active_phase == "F0"
    assert config.risk.phases[config.risk.active_phase].risk_frac == 0.002


def test_invalid_active_phase_falls_back_to_first_sorted_key():
    phases = {
        "F2": {"risk_frac": 0.003, "max_daily_loss": 0.015},
        "F1": {"risk_frac": 0.0025, "max_daily_loss": 0.0125},
    }
    config = _load_config(active_phase="FX", phases=phases)

    assert config.risk.active_phase == "F1"
    assert config.risk.phases[config.risk.active_phase].max_daily_loss == 0.0125
