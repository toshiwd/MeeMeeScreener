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
    train_days: int = 1260
    test_days: int = 63
    step_days: int = 63
    embargo_days: int = 20
    cls_boost_round: int = 200
    reg_boost_round: int = 200
    rank_boost_round: int = 300
    rule_weight: float = 0.2
    ev_weight: float = 0.5
    prob_weight: float = 0.3
    rank_weight: float = 0.5
    min_prob_up: float = 0.55
    min_prob_down: float = 0.55
    turn_weight: float = 0.25
    min_turn_prob_up: float = 0.58
    min_turn_prob_down: float = 0.58
    min_turn_margin: float = 0.06
    auto_promote: bool = True
    allow_bootstrap_promotion: bool = True
    min_wf_fold_count: int = 3
    min_wf_daily_count: int = 120
    min_wf_mean_ret20_net: float = 0.0
    min_wf_win_rate: float = 0.52
    min_wf_p05_ret20_net: float = -0.08
    min_wf_cvar05_ret20_net: float = -0.12
    robust_lb_lambda: float = 1.0
    min_wf_robust_lb: float = -0.09
    max_wf_p_value_mean_gt0: float = 0.10
    min_wf_lcb95_ret20_net: float = 0.0
    min_wf_up_mean_ret20_net: float = 0.0
    min_wf_down_mean_ret20_net: float = -0.03
    min_wf_combined_mean_ret20_net: float = 0.0
    require_champion_improvement: bool = True
    min_delta_mean_ret20_net: float = 0.0
    min_delta_robust_lb: float = -0.002
    min_delta_lcb95_ret20_net: float = 0.0
    live_guard_enabled: bool = True
    live_guard_lookback_days: int = 126
    live_guard_min_daily_count: int = 40
    live_guard_min_mean_ret20_net: float = -0.0005
    live_guard_min_robust_lb: float = -0.005
    live_guard_max_p_value_mean_gt0: float = 0.25
    live_guard_min_lcb95_ret20_net: float = -0.01
    live_guard_allow_rollback: bool = True
    wf_use_expanding_train: bool = True
    wf_max_train_days: int = 2520

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


def _to_bool(value: object, default: bool) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return bool(default)


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
        train_days=max(1260, _to_int(payload.get("train_days"), 1260)),
        test_days=max(5, _to_int(payload.get("test_days"), 63)),
        step_days=max(1, _to_int(payload.get("step_days"), 63)),
        embargo_days=max(0, _to_int(payload.get("embargo_days"), 20)),
        cls_boost_round=max(20, _to_int(payload.get("cls_boost_round"), 200)),
        reg_boost_round=max(20, _to_int(payload.get("reg_boost_round"), 200)),
        rank_boost_round=max(20, _to_int(payload.get("rank_boost_round"), 300)),
        rule_weight=max(0.0, _to_float(payload.get("rule_weight"), 0.2)),
        ev_weight=max(0.0, _to_float(payload.get("ev_weight"), 0.5)),
        prob_weight=max(0.0, _to_float(payload.get("prob_weight"), 0.3)),
        rank_weight=min(1.0, max(0.0, _to_float(payload.get("rank_weight"), 0.5))),
        min_prob_up=min(1.0, max(0.0, _to_float(payload.get("min_prob_up"), 0.55))),
        min_prob_down=min(1.0, max(0.0, _to_float(payload.get("min_prob_down"), 0.55))),
        turn_weight=min(1.0, max(0.0, _to_float(payload.get("turn_weight"), 0.25))),
        min_turn_prob_up=min(1.0, max(0.0, _to_float(payload.get("min_turn_prob_up"), 0.58))),
        min_turn_prob_down=min(1.0, max(0.0, _to_float(payload.get("min_turn_prob_down"), 0.58))),
        min_turn_margin=min(1.0, max(0.0, _to_float(payload.get("min_turn_margin"), 0.06))),
        auto_promote=_to_bool(payload.get("auto_promote"), True),
        allow_bootstrap_promotion=_to_bool(payload.get("allow_bootstrap_promotion"), True),
        min_wf_fold_count=max(0, _to_int(payload.get("min_wf_fold_count"), 3)),
        min_wf_daily_count=max(0, _to_int(payload.get("min_wf_daily_count"), 120)),
        min_wf_mean_ret20_net=_to_float(payload.get("min_wf_mean_ret20_net"), 0.0),
        min_wf_win_rate=min(1.0, max(0.0, _to_float(payload.get("min_wf_win_rate"), 0.52))),
        min_wf_p05_ret20_net=_to_float(payload.get("min_wf_p05_ret20_net"), -0.08),
        min_wf_cvar05_ret20_net=_to_float(payload.get("min_wf_cvar05_ret20_net"), -0.12),
        robust_lb_lambda=max(0.0, _to_float(payload.get("robust_lb_lambda"), 1.0)),
        min_wf_robust_lb=_to_float(payload.get("min_wf_robust_lb"), -0.09),
        max_wf_p_value_mean_gt0=min(1.0, max(0.0, _to_float(payload.get("max_wf_p_value_mean_gt0"), 0.10))),
        min_wf_lcb95_ret20_net=_to_float(payload.get("min_wf_lcb95_ret20_net"), 0.0),
        min_wf_up_mean_ret20_net=_to_float(payload.get("min_wf_up_mean_ret20_net"), 0.0),
        min_wf_down_mean_ret20_net=_to_float(payload.get("min_wf_down_mean_ret20_net"), -0.03),
        min_wf_combined_mean_ret20_net=_to_float(payload.get("min_wf_combined_mean_ret20_net"), 0.0),
        require_champion_improvement=_to_bool(payload.get("require_champion_improvement"), True),
        min_delta_mean_ret20_net=_to_float(payload.get("min_delta_mean_ret20_net"), 0.0),
        min_delta_robust_lb=_to_float(payload.get("min_delta_robust_lb"), -0.002),
        min_delta_lcb95_ret20_net=_to_float(payload.get("min_delta_lcb95_ret20_net"), 0.0),
        live_guard_enabled=_to_bool(payload.get("live_guard_enabled"), True),
        live_guard_lookback_days=max(5, _to_int(payload.get("live_guard_lookback_days"), 126)),
        live_guard_min_daily_count=max(0, _to_int(payload.get("live_guard_min_daily_count"), 40)),
        live_guard_min_mean_ret20_net=_to_float(payload.get("live_guard_min_mean_ret20_net"), -0.0005),
        live_guard_min_robust_lb=_to_float(payload.get("live_guard_min_robust_lb"), -0.005),
        live_guard_max_p_value_mean_gt0=min(
            1.0, max(0.0, _to_float(payload.get("live_guard_max_p_value_mean_gt0"), 0.25))
        ),
        live_guard_min_lcb95_ret20_net=_to_float(payload.get("live_guard_min_lcb95_ret20_net"), -0.01),
        live_guard_allow_rollback=_to_bool(payload.get("live_guard_allow_rollback"), True),
        wf_use_expanding_train=_to_bool(payload.get("wf_use_expanding_train"), True),
        wf_max_train_days=max(1260, _to_int(payload.get("wf_max_train_days"), 2520)),
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
            rank_boost_round=cfg.rank_boost_round,
            rule_weight=0.2,
            ev_weight=0.5,
            prob_weight=0.3,
            rank_weight=cfg.rank_weight,
            min_prob_up=cfg.min_prob_up,
            min_prob_down=cfg.min_prob_down,
            turn_weight=cfg.turn_weight,
            min_turn_prob_up=cfg.min_turn_prob_up,
            min_turn_prob_down=cfg.min_turn_prob_down,
            min_turn_margin=cfg.min_turn_margin,
            auto_promote=cfg.auto_promote,
            allow_bootstrap_promotion=cfg.allow_bootstrap_promotion,
            min_wf_fold_count=cfg.min_wf_fold_count,
            min_wf_daily_count=cfg.min_wf_daily_count,
            min_wf_mean_ret20_net=cfg.min_wf_mean_ret20_net,
            min_wf_win_rate=cfg.min_wf_win_rate,
            min_wf_p05_ret20_net=cfg.min_wf_p05_ret20_net,
            min_wf_cvar05_ret20_net=cfg.min_wf_cvar05_ret20_net,
            robust_lb_lambda=cfg.robust_lb_lambda,
            min_wf_robust_lb=cfg.min_wf_robust_lb,
            max_wf_p_value_mean_gt0=cfg.max_wf_p_value_mean_gt0,
            min_wf_lcb95_ret20_net=cfg.min_wf_lcb95_ret20_net,
            min_wf_up_mean_ret20_net=cfg.min_wf_up_mean_ret20_net,
            min_wf_down_mean_ret20_net=cfg.min_wf_down_mean_ret20_net,
            min_wf_combined_mean_ret20_net=cfg.min_wf_combined_mean_ret20_net,
            require_champion_improvement=cfg.require_champion_improvement,
            min_delta_mean_ret20_net=cfg.min_delta_mean_ret20_net,
            min_delta_robust_lb=cfg.min_delta_robust_lb,
            min_delta_lcb95_ret20_net=cfg.min_delta_lcb95_ret20_net,
            live_guard_enabled=cfg.live_guard_enabled,
            live_guard_lookback_days=cfg.live_guard_lookback_days,
            live_guard_min_daily_count=cfg.live_guard_min_daily_count,
            live_guard_min_mean_ret20_net=cfg.live_guard_min_mean_ret20_net,
            live_guard_min_robust_lb=cfg.live_guard_min_robust_lb,
            live_guard_max_p_value_mean_gt0=cfg.live_guard_max_p_value_mean_gt0,
            live_guard_min_lcb95_ret20_net=cfg.live_guard_min_lcb95_ret20_net,
            live_guard_allow_rollback=cfg.live_guard_allow_rollback,
            wf_use_expanding_train=cfg.wf_use_expanding_train,
            wf_max_train_days=cfg.wf_max_train_days,
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
        rank_boost_round=cfg.rank_boost_round,
        rule_weight=cfg.rule_weight / weight_sum,
        ev_weight=cfg.ev_weight / weight_sum,
        prob_weight=cfg.prob_weight / weight_sum,
        rank_weight=cfg.rank_weight,
        min_prob_up=cfg.min_prob_up,
        min_prob_down=cfg.min_prob_down,
        turn_weight=cfg.turn_weight,
        min_turn_prob_up=cfg.min_turn_prob_up,
        min_turn_prob_down=cfg.min_turn_prob_down,
        min_turn_margin=cfg.min_turn_margin,
        auto_promote=cfg.auto_promote,
        allow_bootstrap_promotion=cfg.allow_bootstrap_promotion,
        min_wf_fold_count=cfg.min_wf_fold_count,
        min_wf_daily_count=cfg.min_wf_daily_count,
        min_wf_mean_ret20_net=cfg.min_wf_mean_ret20_net,
        min_wf_win_rate=cfg.min_wf_win_rate,
        min_wf_p05_ret20_net=cfg.min_wf_p05_ret20_net,
        min_wf_cvar05_ret20_net=cfg.min_wf_cvar05_ret20_net,
        robust_lb_lambda=cfg.robust_lb_lambda,
        min_wf_robust_lb=cfg.min_wf_robust_lb,
        max_wf_p_value_mean_gt0=cfg.max_wf_p_value_mean_gt0,
        min_wf_lcb95_ret20_net=cfg.min_wf_lcb95_ret20_net,
        min_wf_up_mean_ret20_net=cfg.min_wf_up_mean_ret20_net,
        min_wf_down_mean_ret20_net=cfg.min_wf_down_mean_ret20_net,
        min_wf_combined_mean_ret20_net=cfg.min_wf_combined_mean_ret20_net,
        require_champion_improvement=cfg.require_champion_improvement,
        min_delta_mean_ret20_net=cfg.min_delta_mean_ret20_net,
        min_delta_robust_lb=cfg.min_delta_robust_lb,
        min_delta_lcb95_ret20_net=cfg.min_delta_lcb95_ret20_net,
        live_guard_enabled=cfg.live_guard_enabled,
        live_guard_lookback_days=cfg.live_guard_lookback_days,
        live_guard_min_daily_count=cfg.live_guard_min_daily_count,
        live_guard_min_mean_ret20_net=cfg.live_guard_min_mean_ret20_net,
        live_guard_min_robust_lb=cfg.live_guard_min_robust_lb,
        live_guard_max_p_value_mean_gt0=cfg.live_guard_max_p_value_mean_gt0,
        live_guard_min_lcb95_ret20_net=cfg.live_guard_min_lcb95_ret20_net,
        live_guard_allow_rollback=cfg.live_guard_allow_rollback,
        wf_use_expanding_train=cfg.wf_use_expanding_train,
        wf_max_train_days=cfg.wf_max_train_days,
    )
