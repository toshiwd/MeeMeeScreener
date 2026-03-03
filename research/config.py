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
    lgbm_n_estimators: int = 1000      # early stoppingで自動調整されるため多めに設定
    lgbm_learning_rate: float = 0.02   # 小さいlrで安定した汎化性能
    lgbm_max_depth: int = 6
    lgbm_use_dart: bool = False        # DART boosting (slower but better generalization)
    lgbm_optuna_trials: int = 0        # 0=無効, >0でOptunaによるHPO実行
    confidence_threshold: float = 0.0  # 信頼度フィルタ閾値 (0=無効, >0で閾値以下をスコア=0)
    use_high_conf_labels: bool = False # label_high_confをメインラベルとして使用
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
class ResearchConfig:
    tp_long: float = 0.10
    tp_short: float = 0.10
    cost: CostConfig = field(default_factory=CostConfig)
    stop_loss: StopLossConfig = field(default_factory=StopLossConfig)
    split: SplitConfig = field(default_factory=SplitConfig)
    model: ModelConfig = field(default_factory=ModelConfig)
    publish_gate: PublishGateConfig = field(default_factory=PublishGateConfig)
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
    return payload


def params_hash(config: ResearchConfig) -> str:
    payload_obj = config_to_dict(config)
    # Runtime-only knobs for post-ranking strategy should not invalidate
    # historical feature/label cache hashes.
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
    payload = json.dumps(payload_obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return sha1(payload.encode("utf-8")).hexdigest()[:12]


def from_dict(payload: dict[str, Any]) -> ResearchConfig:
    cost_raw = payload.get("cost") if isinstance(payload.get("cost"), dict) else {}
    stop_raw = payload.get("stop_loss") if isinstance(payload.get("stop_loss"), dict) else {}
    split_raw = payload.get("split") if isinstance(payload.get("split"), dict) else {}
    model_raw = payload.get("model") if isinstance(payload.get("model"), dict) else {}
    publish_gate_raw = payload.get("publish_gate") if isinstance(payload.get("publish_gate"), dict) else {}
    regime_overrides_raw = (
        publish_gate_raw.get("regime_overrides")
        if isinstance(publish_gate_raw.get("regime_overrides"), dict)
        else {}
    )
    regime_overrides: dict[str, dict[str, float | int]] = {}
    for k, v in regime_overrides_raw.items():
        if not isinstance(k, str) or not isinstance(v, dict):
            continue
        normalized: dict[str, float | int] = {}
        for kk, vv in v.items():
            if not isinstance(kk, str):
                continue
            if kk == "min_test_months":
                try:
                    normalized[kk] = int(vv)
                except Exception:
                    continue
            else:
                try:
                    normalized[kk] = float(vv)
                except Exception:
                    continue
        if normalized:
            regime_overrides[k.strip()] = normalized

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
                    for k, v in (
                        model_raw.get("short_regime_topk_caps", {}) or {}
                    ).items()
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
            short_month_gate_auto_max_regimes=max(
                1,
                int(model_raw.get("short_month_gate_auto_max_regimes") or 4),
            ),
            short_month_gate_auto_min_months=max(
                1,
                int(model_raw.get("short_month_gate_auto_min_months") or 6),
            ),
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
    model_payload.update({
        "candidate_pool": variant.candidate_pool,
        "top_k": config.model.top_k,
        "ridge_alpha": variant.ridge_alpha,
        "risk_penalty": variant.risk_penalty,
    })
    payload["model"] = model_payload
    return from_dict(payload)
