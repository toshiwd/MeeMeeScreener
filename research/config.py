from __future__ import annotations

from dataclasses import asdict, dataclass, field
from hashlib import sha1
import json
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class CostConfig:
    enabled: bool = True
    rate_per_side: float = 0.001


@dataclass(frozen=True)
class StopLossConfig:
    enabled: bool = False
    rate: float = 0.0


@dataclass(frozen=True)
class SplitConfig:
    train_years: int = 5
    valid_months: int = 12
    test_months: int = 12


@dataclass(frozen=True)
class ModelConfig:
    candidate_pool: int = 150
    top_k: int = 20
    ridge_alpha: float = 1.0
    risk_penalty: float = 0.35
    use_lightgbm: bool = True
    lgbm_n_estimators: int = 1000
    lgbm_learning_rate: float = 0.02
    lgbm_max_depth: int = 6
    lgbm_use_dart: bool = False
    lgbm_optuna_trials: int = 0
    confidence_threshold: float = 0.0
    use_high_conf_labels: bool = False
    regime_strategy_enabled: bool = True
    regime_strategy_long_enabled: bool = True
    regime_strategy_short_enabled: bool = True
    regime_min_rows: int = 120
    regime_min_months: int = 4
    regime_min_improvement: float = 0.0005
    short_score_return_base: float = 0.45
    short_score_prob_weight: float = 0.20
    short_score_prob_alpha: float = 0.01
    short_score_risk_scale: float = 1.20
    short_mt_bear_bonus: float = 0.0030
    short_mt_bull_penalty: float = 0.0060
    short_vr1_penalty: float = 0.0040
    short_vr2_bonus: float = 0.0010
    short_regime_topk_caps: dict[str, int] = field(default_factory=dict)
    short_month_gate_enabled: bool = False
    short_month_gate_allowed_regimes: list[str] = field(default_factory=list)
    short_month_gate_pred_return_max: float | None = None
    short_month_gate_prob_min: float | None = None
    short_month_gate_risk_max: float | None = None
    short_month_gate_auto: bool = False
    short_month_gate_auto_max_regimes: int = 4
    short_month_gate_auto_min_months: int = 6
    short_month_gate_auto_min_improvement: float = 0.0002


@dataclass(frozen=True)
class PublishGateConfig:
    min_test_overall_return_at20: float = 0.0030
    min_test_long_return_at20: float = 0.0150
    min_test_short_return_at20: float = -0.0100
    max_test_risk_mae_p90: float = 0.1200
    min_test_months: int = 6
    regime_overrides: dict[str, dict[str, float | int]] = field(default_factory=dict)


@dataclass(frozen=True)
class LoopVariant:
    name: str
    candidate_pool: int
    ridge_alpha: float
    risk_penalty: float


@dataclass(frozen=True)
class StudyRetentionGates:
    min_profit_factor: float = 1.05
    min_positive_window_ratio: float = 0.52
    max_worst_drawdown: float = 0.35
    min_samples: int = 60
    top_hypotheses_per_combo: int = 5


@dataclass(frozen=True)
class StudyAdoptionGates:
    min_oos_return: float = 0.02
    min_pf: float = 1.15
    min_positive_window_ratio: float = 0.55
    max_worst_drawdown: float = 0.25
    min_stability: float = 0.45
    min_cluster_consistency: float = 0.45
    min_fold_months: int = 12


def _default_study_seed_weights() -> dict[str, dict[str, float]]:
    return {
        "reversal": {
            "Candle": 0.12,
            "Pivot": 0.10,
            "MA": 0.20,
            "Volume": 0.16,
            "WeeklyContext": 0.18,
            "MonthlyContext": 0.10,
            "Regime": 0.08,
            "Cluster": 0.06,
        },
        "continuation": {
            "MA": 0.24,
            "BreakoutShape": 0.12,
            "Pivot": 0.06,
            "Volume": 0.18,
            "WeeklyContext": 0.16,
            "Regime": 0.12,
            "MonthlyContext": 0.06,
            "Cluster": 0.06,
        },
    }


@dataclass(frozen=True)
class StudyConfig:
    timeframes: tuple[str, ...] = ("daily", "weekly", "monthly")
    families: tuple[str, ...] = (
        "bottom",
        "top",
        "bottom_negation",
        "top_negation",
        "up_cont",
        "down_cont",
    )
    trials_per_family: dict[str, int] = field(
        default_factory=lambda: {"daily": 256, "weekly": 192, "monthly": 96}
    )
    refinement_trials_per_family: dict[str, int] = field(
        default_factory=lambda: {"daily": 640, "weekly": 480, "monthly": 240}
    )
    retention_gates: StudyRetentionGates = field(default_factory=StudyRetentionGates)
    adoption_gates: StudyAdoptionGates = field(default_factory=StudyAdoptionGates)
    seed_weights: dict[str, dict[str, float]] = field(default_factory=_default_study_seed_weights)
    negation_penalties: tuple[float, ...] = (-0.20, -0.30, -0.45)
    selection_cutoffs: tuple[float, ...] = (0.005, 0.01, 0.02, 0.05, 0.10)
    top_refinement_parents: int = 10
    random_seed: int = 42
    resume: bool = True


@dataclass(frozen=True)
class ResearchConfig:
    tp_long: float = 0.10
    tp_short: float = 0.10
    cost: CostConfig = field(default_factory=CostConfig)
    stop_loss: StopLossConfig = field(default_factory=StopLossConfig)
    split: SplitConfig = field(default_factory=SplitConfig)
    model: ModelConfig = field(default_factory=ModelConfig)
    publish_gate: PublishGateConfig = field(default_factory=PublishGateConfig)
    study: StudyConfig = field(default_factory=StudyConfig)
    feature_version: str = "monthly_feature_v1"
    label_version: str = "tp_next_month_v1"
    model_version: str = "top20_ranker_v1"
    loop_variants: tuple[LoopVariant, ...] = field(
        default_factory=lambda: (
            LoopVariant(name="baseline", candidate_pool=150, ridge_alpha=1.0, risk_penalty=0.35),
            LoopVariant(name="risk_tight", candidate_pool=120, ridge_alpha=1.4, risk_penalty=0.50),
            LoopVariant(name="recall_wide", candidate_pool=200, ridge_alpha=0.8, risk_penalty=0.25),
        )
    )


def default_config_path() -> Path:
    return Path(__file__).resolve().parent / "default_config.json"


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(dict(merged[key]), value)
        else:
            merged[key] = value
    return merged


def config_to_dict(config: ResearchConfig) -> dict[str, Any]:
    payload = asdict(config)
    payload["loop_variants"] = [asdict(v) for v in config.loop_variants]
    payload["study"]["timeframes"] = list(config.study.timeframes)
    payload["study"]["families"] = list(config.study.families)
    payload["study"]["negation_penalties"] = list(config.study.negation_penalties)
    payload["study"]["selection_cutoffs"] = list(config.study.selection_cutoffs)
    return payload


def params_hash(config: ResearchConfig) -> str:
    payload_obj = config_to_dict(config)
    model_obj = dict(payload_obj.get("model", {}))
    for key in (
        "regime_strategy_enabled",
        "regime_strategy_long_enabled",
        "regime_strategy_short_enabled",
        "regime_min_rows",
        "regime_min_months",
        "regime_min_improvement",
        "short_score_return_base",
        "short_score_prob_weight",
        "short_score_prob_alpha",
        "short_score_risk_scale",
        "short_mt_bear_bonus",
        "short_mt_bull_penalty",
        "short_vr1_penalty",
        "short_vr2_bonus",
        "short_regime_topk_caps",
        "short_month_gate_enabled",
        "short_month_gate_allowed_regimes",
        "short_month_gate_pred_return_max",
        "short_month_gate_prob_min",
        "short_month_gate_risk_max",
        "short_month_gate_auto",
        "short_month_gate_auto_max_regimes",
        "short_month_gate_auto_min_months",
        "short_month_gate_auto_min_improvement",
    ):
        model_obj.pop(key, None)
    payload_obj["model"] = model_obj
    payload_obj.pop("publish_gate", None)
    payload_obj.pop("study", None)
    payload = json.dumps(payload_obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return sha1(payload.encode("utf-8")).hexdigest()[:12]


def _as_clean_str_list(raw: Any, default: tuple[str, ...]) -> tuple[str, ...]:
    if not isinstance(raw, list):
        return tuple(default)
    values = [str(item).strip() for item in raw if str(item).strip()]
    return tuple(values) if values else tuple(default)


def _normalize_weight_groups(raw: Any, fallback: dict[str, dict[str, float]]) -> dict[str, dict[str, float]]:
    if not isinstance(raw, dict):
        return fallback
    out: dict[str, dict[str, float]] = {}
    for key, group in raw.items():
        if not isinstance(key, str) or not isinstance(group, dict):
            continue
        normalized: dict[str, float] = {}
        for gk, gv in group.items():
            try:
                group_name = str(gk).strip()
                if not group_name:
                    continue
                normalized[group_name] = float(gv)
            except Exception:
                continue
        if normalized:
            out[key.strip()] = normalized
    return out or fallback


def from_dict(payload: dict[str, Any]) -> ResearchConfig:
    cost_raw = payload.get("cost") if isinstance(payload.get("cost"), dict) else {}
    stop_raw = payload.get("stop_loss") if isinstance(payload.get("stop_loss"), dict) else {}
    split_raw = payload.get("split") if isinstance(payload.get("split"), dict) else {}
    model_raw = payload.get("model") if isinstance(payload.get("model"), dict) else {}
    publish_gate_raw = payload.get("publish_gate") if isinstance(payload.get("publish_gate"), dict) else {}
    study_raw = payload.get("study") if isinstance(payload.get("study"), dict) else {}

    regime_overrides_raw = (
        publish_gate_raw.get("regime_overrides")
        if isinstance(publish_gate_raw.get("regime_overrides"), dict)
        else {}
    )
    regime_overrides: dict[str, dict[str, float | int]] = {}
    for key, value in regime_overrides_raw.items():
        if not isinstance(key, str) or not isinstance(value, dict):
            continue
        normalized: dict[str, float | int] = {}
        for gate_key, gate_value in value.items():
            if not isinstance(gate_key, str):
                continue
            if gate_key == "min_test_months":
                try:
                    normalized[gate_key] = int(gate_value)
                except Exception:
                    continue
            else:
                try:
                    normalized[gate_key] = float(gate_value)
                except Exception:
                    continue
        if normalized:
            regime_overrides[key.strip()] = normalized

    loop_raw = payload.get("loop_variants")
    variants: list[LoopVariant] = []
    if isinstance(loop_raw, list):
        for idx, item in enumerate(loop_raw):
            if not isinstance(item, dict):
                continue
            variants.append(
                LoopVariant(
                    name=str(item.get("name") or f"variant_{idx+1}"),
                    candidate_pool=max(20, int(item.get("candidate_pool") or 150)),
                    ridge_alpha=max(0.0, float(item.get("ridge_alpha") or 1.0)),
                    risk_penalty=max(0.0, float(item.get("risk_penalty") or 0.35)),
                )
            )

    short_month_gate_allowed_regimes: list[str] = []
    if isinstance(model_raw.get("short_month_gate_allowed_regimes"), list):
        for item in model_raw.get("short_month_gate_allowed_regimes", []) or []:
            key = str(item).strip()
            if key:
                short_month_gate_allowed_regimes.append(key)

    def _opt_float(raw: Any) -> float | None:
        if raw is None:
            return None
        try:
            return float(raw)
        except Exception:
            return None

    default_study = StudyConfig()
    retention_raw = study_raw.get("retention_gates") if isinstance(study_raw.get("retention_gates"), dict) else {}
    adoption_raw = study_raw.get("adoption_gates") if isinstance(study_raw.get("adoption_gates"), dict) else {}

    config = ResearchConfig(
        tp_long=max(0.0001, float(payload.get("tp_long") or 0.10)),
        tp_short=max(0.0001, float(payload.get("tp_short") or 0.10)),
        cost=CostConfig(
            enabled=bool(cost_raw.get("enabled", True)),
            rate_per_side=max(0.0, float(cost_raw.get("rate_per_side") or 0.0)),
        ),
        stop_loss=StopLossConfig(
            enabled=bool(stop_raw.get("enabled", False)),
            rate=max(0.0, float(stop_raw.get("rate") or 0.0)),
        ),
        split=SplitConfig(
            train_years=max(1, int(split_raw.get("train_years") or 5)),
            valid_months=max(1, int(split_raw.get("valid_months") or 12)),
            test_months=max(1, int(split_raw.get("test_months") or 12)),
        ),
        model=ModelConfig(
            candidate_pool=max(20, int(model_raw.get("candidate_pool") or 150)),
            top_k=max(1, int(model_raw.get("top_k") or 20)),
            ridge_alpha=max(0.0, float(model_raw.get("ridge_alpha") or 1.0)),
            risk_penalty=max(0.0, float(model_raw.get("risk_penalty") or 0.35)),
            use_lightgbm=bool(model_raw.get("use_lightgbm", True)),
            lgbm_n_estimators=max(10, int(model_raw.get("lgbm_n_estimators") or 1000)),
            lgbm_learning_rate=max(0.001, float(model_raw.get("lgbm_learning_rate") or 0.02)),
            lgbm_max_depth=max(2, int(model_raw.get("lgbm_max_depth") or 6)),
            lgbm_use_dart=bool(model_raw.get("lgbm_use_dart", False)),
            lgbm_optuna_trials=max(0, int(model_raw.get("lgbm_optuna_trials") or 0)),
            confidence_threshold=max(0.0, float(model_raw.get("confidence_threshold") or 0.0)),
            use_high_conf_labels=bool(model_raw.get("use_high_conf_labels", False)),
            regime_strategy_enabled=bool(model_raw.get("regime_strategy_enabled", True)),
            regime_strategy_long_enabled=bool(model_raw.get("regime_strategy_long_enabled", True)),
            regime_strategy_short_enabled=bool(model_raw.get("regime_strategy_short_enabled", True)),
            regime_min_rows=max(20, int(model_raw.get("regime_min_rows") or 120)),
            regime_min_months=max(2, int(model_raw.get("regime_min_months") or 4)),
            regime_min_improvement=max(0.0, float(model_raw.get("regime_min_improvement") or 0.0005)),
            short_score_return_base=float(model_raw.get("short_score_return_base", 0.45)),
            short_score_prob_weight=float(model_raw.get("short_score_prob_weight", 0.20)),
            short_score_prob_alpha=float(model_raw.get("short_score_prob_alpha", 0.01)),
            short_score_risk_scale=max(0.1, float(model_raw.get("short_score_risk_scale", 1.20))),
            short_mt_bear_bonus=float(model_raw.get("short_mt_bear_bonus", 0.0030)),
            short_mt_bull_penalty=float(model_raw.get("short_mt_bull_penalty", 0.0060)),
            short_vr1_penalty=float(model_raw.get("short_vr1_penalty", 0.0040)),
            short_vr2_bonus=float(model_raw.get("short_vr2_bonus", 0.0010)),
            short_regime_topk_caps=(
                {
                    str(k).strip(): max(0, int(v))
                    for k, v in (model_raw.get("short_regime_topk_caps", {}) or {}).items()
                    if str(k).strip()
                }
                if isinstance(model_raw.get("short_regime_topk_caps"), dict)
                else {}
            ),
            short_month_gate_enabled=bool(model_raw.get("short_month_gate_enabled", False)),
            short_month_gate_allowed_regimes=short_month_gate_allowed_regimes,
            short_month_gate_pred_return_max=_opt_float(model_raw.get("short_month_gate_pred_return_max")),
            short_month_gate_prob_min=_opt_float(model_raw.get("short_month_gate_prob_min")),
            short_month_gate_risk_max=_opt_float(model_raw.get("short_month_gate_risk_max")),
            short_month_gate_auto=bool(model_raw.get("short_month_gate_auto", False)),
            short_month_gate_auto_max_regimes=max(1, int(model_raw.get("short_month_gate_auto_max_regimes") or 4)),
            short_month_gate_auto_min_months=max(1, int(model_raw.get("short_month_gate_auto_min_months") or 6)),
            short_month_gate_auto_min_improvement=max(
                0.0,
                float(model_raw.get("short_month_gate_auto_min_improvement") or 0.0002),
            ),
        ),
        publish_gate=PublishGateConfig(
            min_test_overall_return_at20=float(
                publish_gate_raw.get("min_test_overall_return_at20")
                if publish_gate_raw.get("min_test_overall_return_at20") is not None
                else 0.0030
            ),
            min_test_long_return_at20=float(
                publish_gate_raw.get("min_test_long_return_at20")
                if publish_gate_raw.get("min_test_long_return_at20") is not None
                else 0.0150
            ),
            min_test_short_return_at20=float(
                publish_gate_raw.get("min_test_short_return_at20")
                if publish_gate_raw.get("min_test_short_return_at20") is not None
                else -0.0100
            ),
            max_test_risk_mae_p90=max(
                0.0,
                float(
                    publish_gate_raw.get("max_test_risk_mae_p90")
                    if publish_gate_raw.get("max_test_risk_mae_p90") is not None
                    else 0.1200
                ),
            ),
            min_test_months=max(
                1,
                int(
                    publish_gate_raw.get("min_test_months")
                    if publish_gate_raw.get("min_test_months") is not None
                    else 6
                ),
            ),
            regime_overrides=regime_overrides,
        ),
        study=StudyConfig(
            timeframes=_as_clean_str_list(study_raw.get("timeframes"), default_study.timeframes),
            families=_as_clean_str_list(study_raw.get("families"), default_study.families),
            trials_per_family=(
                {
                    str(k).strip(): max(1, int(v))
                    for k, v in (study_raw.get("trials_per_family", {}) or {}).items()
                    if str(k).strip()
                }
                if isinstance(study_raw.get("trials_per_family"), dict)
                else dict(default_study.trials_per_family)
            ),
            refinement_trials_per_family=(
                {
                    str(k).strip(): max(0, int(v))
                    for k, v in (study_raw.get("refinement_trials_per_family", {}) or {}).items()
                    if str(k).strip()
                }
                if isinstance(study_raw.get("refinement_trials_per_family"), dict)
                else dict(default_study.refinement_trials_per_family)
            ),
            retention_gates=StudyRetentionGates(
                min_profit_factor=max(
                    0.0,
                    float(retention_raw.get("min_profit_factor", default_study.retention_gates.min_profit_factor)),
                ),
                min_positive_window_ratio=max(
                    0.0,
                    float(
                        retention_raw.get(
                            "min_positive_window_ratio",
                            default_study.retention_gates.min_positive_window_ratio,
                        )
                    ),
                ),
                max_worst_drawdown=max(
                    0.0,
                    float(
                        retention_raw.get(
                            "max_worst_drawdown",
                            default_study.retention_gates.max_worst_drawdown,
                        )
                    ),
                ),
                min_samples=max(1, int(retention_raw.get("min_samples", default_study.retention_gates.min_samples))),
                top_hypotheses_per_combo=max(
                    1,
                    int(
                        retention_raw.get(
                            "top_hypotheses_per_combo",
                            default_study.retention_gates.top_hypotheses_per_combo,
                        )
                    ),
                ),
            ),
            adoption_gates=StudyAdoptionGates(
                min_oos_return=float(adoption_raw.get("min_oos_return", default_study.adoption_gates.min_oos_return)),
                min_pf=max(0.0, float(adoption_raw.get("min_pf", default_study.adoption_gates.min_pf))),
                min_positive_window_ratio=max(
                    0.0,
                    float(
                        adoption_raw.get(
                            "min_positive_window_ratio",
                            default_study.adoption_gates.min_positive_window_ratio,
                        )
                    ),
                ),
                max_worst_drawdown=max(
                    0.0,
                    float(
                        adoption_raw.get(
                            "max_worst_drawdown",
                            default_study.adoption_gates.max_worst_drawdown,
                        )
                    ),
                ),
                min_stability=float(adoption_raw.get("min_stability", default_study.adoption_gates.min_stability)),
                min_cluster_consistency=float(
                    adoption_raw.get(
                        "min_cluster_consistency",
                        default_study.adoption_gates.min_cluster_consistency,
                    )
                ),
                min_fold_months=max(
                    1,
                    int(adoption_raw.get("min_fold_months", default_study.adoption_gates.min_fold_months)),
                ),
            ),
            seed_weights=_normalize_weight_groups(study_raw.get("seed_weights"), _default_study_seed_weights()),
            negation_penalties=tuple(
                float(v) for v in (study_raw.get("negation_penalties") or list(default_study.negation_penalties))
            ),
            selection_cutoffs=tuple(
                float(v) for v in (study_raw.get("selection_cutoffs") or list(default_study.selection_cutoffs))
            ),
            top_refinement_parents=max(
                1,
                int(study_raw.get("top_refinement_parents", default_study.top_refinement_parents)),
            ),
            random_seed=int(study_raw.get("random_seed", default_study.random_seed)),
            resume=bool(study_raw.get("resume", default_study.resume)),
        ),
        feature_version=str(payload.get("feature_version") or "monthly_feature_v1"),
        label_version=str(payload.get("label_version") or "tp_next_month_v1"),
        model_version=str(payload.get("model_version") or "top20_ranker_v1"),
        loop_variants=tuple(variants) if variants else ResearchConfig().loop_variants,
    )
    return config


def load_config(path: str | Path | None) -> ResearchConfig:
    default_payload = json.loads(default_config_path().read_text(encoding="utf-8"))
    if path is None:
        return from_dict(default_payload)
    cfg_path = Path(path)
    if not cfg_path.exists():
        raise FileNotFoundError(f"config not found: {cfg_path}")
    user_payload = json.loads(cfg_path.read_text(encoding="utf-8"))
    if not isinstance(user_payload, dict):
        raise ValueError("config must be a JSON object")
    merged = _deep_merge(default_payload, user_payload)
    return from_dict(merged)


def apply_variant(config: ResearchConfig, variant: LoopVariant) -> ResearchConfig:
    payload = config_to_dict(config)
    model_payload = dict(payload.get("model", {}))
    model_payload.update(
        {
            "candidate_pool": variant.candidate_pool,
            "top_k": config.model.top_k,
            "ridge_alpha": variant.ridge_alpha,
            "risk_penalty": variant.risk_penalty,
        }
    )
    payload["model"] = model_payload
    return from_dict(payload)
