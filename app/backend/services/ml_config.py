from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

from app.core.config import config as core_config


@dataclass(frozen=True)
class MLConfig:
    neutral_band_pct: float = 0.005
    p_up_threshold: float = 0.55
    top_n: int = 30
    cost_bps: float = 20.0
    train_days: int = 504
    test_days: int = 63
    step_days: int = 63
    embargo_days: int = 20
    cls_boost_round: int = 200
    reg_boost_round: int = 200
    rule_weight: float = 0.2
    ev_weight: float = 0.5
    prob_weight: float = 0.3
    min_prob_up: float = 0.55
    min_prob_down: float = 0.55
    turn_weight: float = 0.25
    min_turn_prob_up: float = 0.58
    min_turn_prob_down: float = 0.58
    min_turn_margin: float = 0.06

    @property
    def cost_rate(self) -> float:
        return self.cost_bps / 10_000.0


def _to_float(value: object, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _to_int(value: object, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _default_config_path() -> Path:
    return Path(core_config.REPO_ROOT) / "app" / "backend" / "ml_config.json"


def load_ml_config() -> MLConfig:
    path = Path(os.getenv("ML_CONFIG_PATH") or _default_config_path())
    payload: dict[str, object] = {}
    try:
        if path.exists():
            payload = json.loads(path.read_text(encoding="utf-8")) or {}
    except Exception:
        payload = {}

    cfg = MLConfig(
        neutral_band_pct=_to_float(payload.get("neutral_band_pct"), 0.005),
        p_up_threshold=_to_float(payload.get("p_up_threshold"), 0.55),
        top_n=max(1, _to_int(payload.get("top_n"), 30)),
        cost_bps=max(0.0, _to_float(payload.get("cost_bps"), 20.0)),
        train_days=max(30, _to_int(payload.get("train_days"), 504)),
        test_days=max(5, _to_int(payload.get("test_days"), 63)),
        step_days=max(1, _to_int(payload.get("step_days"), 63)),
        embargo_days=max(0, _to_int(payload.get("embargo_days"), 20)),
        cls_boost_round=max(20, _to_int(payload.get("cls_boost_round"), 200)),
        reg_boost_round=max(20, _to_int(payload.get("reg_boost_round"), 200)),
        rule_weight=max(0.0, _to_float(payload.get("rule_weight"), 0.2)),
        ev_weight=max(0.0, _to_float(payload.get("ev_weight"), 0.5)),
        prob_weight=max(0.0, _to_float(payload.get("prob_weight"), 0.3)),
        min_prob_up=min(1.0, max(0.0, _to_float(payload.get("min_prob_up"), 0.55))),
        min_prob_down=min(1.0, max(0.0, _to_float(payload.get("min_prob_down"), 0.55))),
        turn_weight=min(1.0, max(0.0, _to_float(payload.get("turn_weight"), 0.25))),
        min_turn_prob_up=min(1.0, max(0.0, _to_float(payload.get("min_turn_prob_up"), 0.58))),
        min_turn_prob_down=min(1.0, max(0.0, _to_float(payload.get("min_turn_prob_down"), 0.58))),
        min_turn_margin=min(1.0, max(0.0, _to_float(payload.get("min_turn_margin"), 0.06))),
    )
    weight_sum = cfg.rule_weight + cfg.ev_weight + cfg.prob_weight
    if weight_sum <= 0:
        return MLConfig(
            neutral_band_pct=cfg.neutral_band_pct,
            p_up_threshold=cfg.p_up_threshold,
            top_n=cfg.top_n,
            cost_bps=cfg.cost_bps,
            train_days=cfg.train_days,
            test_days=cfg.test_days,
            step_days=cfg.step_days,
            embargo_days=cfg.embargo_days,
            cls_boost_round=cfg.cls_boost_round,
            reg_boost_round=cfg.reg_boost_round,
            rule_weight=0.2,
            ev_weight=0.5,
            prob_weight=0.3,
            min_prob_up=cfg.min_prob_up,
            min_prob_down=cfg.min_prob_down,
            turn_weight=cfg.turn_weight,
            min_turn_prob_up=cfg.min_turn_prob_up,
            min_turn_prob_down=cfg.min_turn_prob_down,
            min_turn_margin=cfg.min_turn_margin,
        )
    return MLConfig(
        neutral_band_pct=cfg.neutral_band_pct,
        p_up_threshold=cfg.p_up_threshold,
        top_n=cfg.top_n,
        cost_bps=cfg.cost_bps,
        train_days=cfg.train_days,
        test_days=cfg.test_days,
        step_days=cfg.step_days,
        embargo_days=cfg.embargo_days,
        cls_boost_round=cfg.cls_boost_round,
        reg_boost_round=cfg.reg_boost_round,
        rule_weight=cfg.rule_weight / weight_sum,
        ev_weight=cfg.ev_weight / weight_sum,
        prob_weight=cfg.prob_weight / weight_sum,
        min_prob_up=cfg.min_prob_up,
        min_prob_down=cfg.min_prob_down,
        turn_weight=cfg.turn_weight,
        min_turn_prob_up=cfg.min_turn_prob_up,
        min_turn_prob_down=cfg.min_turn_prob_down,
        min_turn_margin=cfg.min_turn_margin,
    )
