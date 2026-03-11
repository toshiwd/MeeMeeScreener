from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha1
import json
from typing import Any

import numpy as np

from research.config import ResearchConfig


GROUPS_REVERSAL = ("Candle", "Pivot", "MA", "Volume", "WeeklyContext", "MonthlyContext", "Regime", "Cluster")
GROUPS_CONTINUATION = ("MA", "BreakoutShape", "Pivot", "Volume", "WeeklyContext", "MonthlyContext", "Regime", "Cluster")


@dataclass(frozen=True)
class FamilySpec:
    family: str
    direction: int
    seed_style: str
    allowed_groups: tuple[str, ...]
    negation_bias: float = 1.0


FAMILY_SPECS: dict[str, FamilySpec] = {
    "bottom": FamilySpec("bottom", direction=1, seed_style="reversal", allowed_groups=GROUPS_REVERSAL, negation_bias=1.0),
    "top": FamilySpec("top", direction=-1, seed_style="reversal", allowed_groups=GROUPS_REVERSAL, negation_bias=1.0),
    "bottom_negation": FamilySpec("bottom_negation", direction=-1, seed_style="reversal", allowed_groups=GROUPS_REVERSAL, negation_bias=1.2),
    "top_negation": FamilySpec("top_negation", direction=1, seed_style="reversal", allowed_groups=GROUPS_REVERSAL, negation_bias=1.2),
    "up_cont": FamilySpec("up_cont", direction=1, seed_style="continuation", allowed_groups=GROUPS_CONTINUATION, negation_bias=1.0),
    "down_cont": FamilySpec("down_cont", direction=-1, seed_style="continuation", allowed_groups=GROUPS_CONTINUATION, negation_bias=1.0),
}


def timeframe_pivot_windows(timeframe: str) -> tuple[int, ...]:
    if timeframe == "daily":
        return (3, 5, 7)
    if timeframe == "weekly":
        return (2, 3, 4)
    return (1, 2, 3)


def timeframe_breakout_windows(timeframe: str) -> tuple[int, ...]:
    if timeframe == "daily":
        return (10, 20, 40)
    if timeframe == "weekly":
        return (4, 8, 12)
    return (3, 6, 12)


def timeframe_ma_thresholds(timeframe: str) -> tuple[float, ...]:
    if timeframe == "daily":
        return (0.005, 0.01, 0.02)
    if timeframe == "weekly":
        return (0.01, 0.02, 0.04)
    return (0.02, 0.04, 0.08)


def timeframe_volume_thresholds(timeframe: str) -> tuple[float, ...]:
    if timeframe == "daily":
        return (1.1, 1.3, 1.6, 2.0)
    if timeframe == "weekly":
        return (1.0, 1.2, 1.5)
    return (1.0, 1.15, 1.3)


def trial_hash(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return sha1(raw.encode("utf-8")).hexdigest()[:12]


def _stable_seed_offset(timeframe: str, family: str) -> int:
    raw = f"{timeframe}::{family}".encode("utf-8")
    return int(sha1(raw).hexdigest()[:8], 16)


def _normalized_weights(raw: np.ndarray, groups: tuple[str, ...]) -> dict[str, float]:
    arr = np.asarray(raw, dtype=float)
    arr = np.clip(arr, 1e-6, None)
    arr = arr / arr.sum()
    return {group: float(arr[idx]) for idx, group in enumerate(groups)}


def _sample_weights(rng: np.random.Generator, seed_weights: dict[str, float], groups: tuple[str, ...]) -> dict[str, float]:
    base = np.asarray([float(seed_weights.get(group, 0.01)) for group in groups], dtype=float)
    base = np.clip(base, 1e-4, None)
    jitter = rng.lognormal(mean=0.0, sigma=0.35, size=len(groups))
    return _normalized_weights(base * jitter, groups)


def generate_base_trials(
    config: ResearchConfig,
    timeframe: str,
    family: str,
    *,
    completed_hashes: set[str],
    target_count: int | None = None,
) -> list[dict[str, Any]]:
    study_cfg = config.study
    spec = FAMILY_SPECS[family]
    groups = spec.allowed_groups
    rng = np.random.default_rng(
        int(config.study.random_seed)
        + (_stable_seed_offset(timeframe, family) % 100_000)
    )
    seed_weights = config.study.seed_weights.get(spec.seed_style, {})
    trials: list[dict[str, Any]] = []
    target_count = int(study_cfg.trials_per_family.get(timeframe, 0)) if target_count is None else max(0, int(target_count))
    if target_count <= 0:
        return []
    seen = set(completed_hashes)
    guard = 0
    while len(trials) < target_count and guard < target_count * 20:
        guard += 1
        weights = _sample_weights(rng, seed_weights, groups)
        params = {
            "weights": weights,
            "neg_penalties": {
                "trend_conflict": abs(float(rng.choice(study_cfg.negation_penalties))) * spec.negation_bias,
                "volume_dry": abs(float(rng.choice(study_cfg.negation_penalties))) * spec.negation_bias,
                "context_conflict": abs(float(rng.choice(study_cfg.negation_penalties))) * spec.negation_bias,
                "breakout_fail": abs(float(rng.choice(study_cfg.negation_penalties))) * spec.negation_bias,
            },
            "pivot_window": int(rng.choice(timeframe_pivot_windows(timeframe))),
            "breakout_window": int(rng.choice(timeframe_breakout_windows(timeframe))),
            "ma_threshold": float(rng.choice(timeframe_ma_thresholds(timeframe))),
            "volume_threshold": float(rng.choice(timeframe_volume_thresholds(timeframe))),
            "selection_cutoff": float(rng.choice(config.study.selection_cutoffs)),
        }
        payload = {
            "timeframe": timeframe,
            "family": family,
            "params": params,
        }
        sig = trial_hash(payload)
        if sig in seen:
            continue
        seen.add(sig)
        payload["param_hash"] = sig
        trials.append(payload)
    return trials


def generate_refinement_trials(
    config: ResearchConfig,
    timeframe: str,
    family: str,
    parent_trials: list[dict[str, Any]],
    *,
    completed_hashes: set[str],
    target_count: int | None = None,
) -> list[dict[str, Any]]:
    if not parent_trials:
        return []
    spec = FAMILY_SPECS[family]
    groups = spec.allowed_groups
    configured_total = int(config.study.refinement_trials_per_family.get(timeframe, 0))
    max_total = configured_total if target_count is None else max(0, int(target_count))
    if max_total <= 0:
        return []
    parent_cap = max(1, int(config.study.top_refinement_parents))
    per_parent = max(1, max_total // max(1, min(parent_cap, len(parent_trials))))
    rng = np.random.default_rng(
        int(config.study.random_seed)
        + 500_000
        + (_stable_seed_offset(timeframe, family) % 100_000)
    )
    seen = set(completed_hashes)
    out: list[dict[str, Any]] = []
    for parent in parent_trials[:parent_cap]:
        base_params = dict(parent.get("params", {}))
        base_weights = {
            group: float(base_params.get("weights", {}).get(group, 0.0))
            for group in groups
        }
        base_weight_arr = np.asarray([base_weights[g] for g in groups], dtype=float)
        for _ in range(per_parent):
            noise = rng.normal(loc=1.0, scale=0.12, size=len(groups))
            params = {
                "weights": _normalized_weights(base_weight_arr * noise, groups),
                "neg_penalties": {
                    key: max(
                        0.05,
                        float(base_params.get("neg_penalties", {}).get(key, 0.20)) * float(rng.uniform(0.9, 1.15)),
                    )
                    for key in ("trend_conflict", "volume_dry", "context_conflict", "breakout_fail")
                },
                "pivot_window": int(rng.choice(timeframe_pivot_windows(timeframe))),
                "breakout_window": int(rng.choice(timeframe_breakout_windows(timeframe))),
                "ma_threshold": float(rng.choice(timeframe_ma_thresholds(timeframe))),
                "volume_threshold": float(rng.choice(timeframe_volume_thresholds(timeframe))),
                "selection_cutoff": float(rng.choice(config.study.selection_cutoffs)),
            }
            payload = {
                "timeframe": timeframe,
                "family": family,
                "params": params,
            }
            sig = trial_hash(payload)
            if sig in seen:
                continue
            seen.add(sig)
            payload["param_hash"] = sig
            out.append(payload)
            if len(out) >= max_total:
                return out
    return out
