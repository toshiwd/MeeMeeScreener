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
    payload = json.dumps(config_to_dict(config), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return sha1(payload.encode("utf-8")).hexdigest()[:12]


def from_dict(payload: dict[str, Any]) -> ResearchConfig:
    cost_raw = payload.get("cost") if isinstance(payload.get("cost"), dict) else {}
    stop_raw = payload.get("stop_loss") if isinstance(payload.get("stop_loss"), dict) else {}
    split_raw = payload.get("split") if isinstance(payload.get("split"), dict) else {}
    model_raw = payload.get("model") if isinstance(payload.get("model"), dict) else {}

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
    payload["model"] = {
        "candidate_pool": variant.candidate_pool,
        "top_k": config.model.top_k,
        "ridge_alpha": variant.ridge_alpha,
        "risk_penalty": variant.risk_penalty,
    }
    return from_dict(payload)
