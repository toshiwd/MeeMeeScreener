from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
from functools import lru_cache
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from app.backend.api.dependencies import get_stock_repo
from app.backend.core.legacy_analysis_control import LEGACY_ANALYSIS_DISABLE_ENV, is_legacy_analysis_disabled
from app.backend.services import tradex_experiment_service as tradex
from app.backend.services import tradex_research_environment_readiness as tradex_environment_readiness
from app.backend.services.tradex_research_contracts import (
    TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
    TRADEX_ARTIFACT_DETAIL_LEVEL_RESEARCH_FALLBACK,
    TRADEX_DEFAULT_COST_MODEL,
    TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
    TRADEX_FALLBACK_STATUS_RESEARCH,
    TRADEX_FEATURE_FAMILIES,
    TRADEX_VICTORY_METRICS,
    TRADEX_RUN_MANIFEST_SCHEMA_VERSION,
    build_run_manifest,
    build_same_condition_contract,
    normalize_feature_family,
    validate_family_leaderboard_artifact,
    validate_run_manifest,
    validate_scope_rollup_artifact,
    validate_session_rollup_artifact,
)
from app.backend.services.tradex_experiment_store import (
    family_dir,
    family_file,
    load_family,
    load_run,
    tradex_reports_root,
    run_manifest_file,
    run_file,
    write_json,
)
from app.core.config import config as app_config
from shared.tradex_storage import tradex_research_keep_root, tradex_research_sessions_root


SESSION_SCHEMA_VERSION = "tradex_research_session_v1"
SESSION_COMPARE_SCHEMA_VERSION = "tradex_research_session_compare_v1"
SESSION_FAMILY_LEADERBOARD_SCHEMA_VERSION = "tradex_family_leaderboard_v1"
SESSION_LEADERBOARD_ROLLUP_SCHEMA_VERSION = "tradex_session_leaderboard_rollup_v1"
STABILITY_ROLLUP_SCHEMA_VERSION = "tradex_research_stability_rollup_v1"
SCOPE_STABILITY_ROLLUP_SCHEMA_VERSION = "tradex_research_scope_stability_rollup_v1"
LOGIC_SEARCH_ROLLUP_SCHEMA_VERSION = "tradex_research_logic_search_rollup_v1"
SCENARIO_SEARCH_ROLLUP_SCHEMA_VERSION = "tradex_research_scenario_search_rollup_v1"
SCENARIO_SEARCH_MEMORY_SCHEMA_VERSION = "tradex_research_scenario_memory_v1"
SCENARIO_SEARCH_GENERATION_SUMMARY_SCHEMA_VERSION = "tradex_research_scenario_generation_summary_v1"
SCENARIO_SEARCH_LINEAGE_SCHEMA_VERSION = "tradex_research_scenario_lineage_v1"
SCENARIO_SEARCH_POPULATION_SCHEMA_VERSION = "tradex_research_scenario_population_v1"
SCENARIO_SEARCH_WIDE_TOPK_SCHEMA_VERSION = "tradex_research_scenario_wide_topk_eval_v1"
SCENARIO_SEARCH_ROLLUP_REPORT_PREFIX = "tradex_research_scenario_search_rollup"
SESSION_REPORT_NAME_PREFIX = "tradex_research_session"
SESSION_FAMILY_LEADERBOARD_REPORT_PREFIX = "tradex_research_family_leaderboard"
SESSION_LEADERBOARD_ROLLUP_REPORT_PREFIX = "tradex_research_session_rollup"
STABILITY_ROLLUP_REPORT_PREFIX = "tradex_research_stability_rollup"
SCOPE_STABILITY_ROLLUP_REPORT_PREFIX = "tradex_research_scope_stability_rollup"
LOGIC_SEARCH_ROLLUP_REPORT_PREFIX = "tradex_research_logic_search_rollup"
SCENARIO_SEARCH_FILE = "research_memory.json"
SCENARIO_POPULATION_FILE = "candidate_population.json"
SCENARIO_GENERATION_SUMMARY_FILE = "generation_summary.json"
SCENARIO_LINEAGE_FILE = "evolution_lineage.json"
SCENARIO_KEEP_DROP_HOLD_FILE = "keep_drop_hold_rollup.json"
SCENARIO_SELECTOR_EVAL_FILE = "regime_selector_eval.json"
SCENARIO_CHAMPION_FILE = "champion_vs_challenger_summary.json"
SCENARIO_WIDE_TOPK_FILE = "practical_topk_eval.json"
SESSION_FAMILY_LEADERBOARD_FILE = "family_leaderboard.json"
SESSION_LEADERBOARD_ROLLUP_FILE = "session_leaderboard_rollup.json"
STABILITY_ROLLUP_FILE = "stability_rollup.json"
SCOPE_STABILITY_ROLLUP_FILE = "scope_stability_rollup.json"
DEFAULT_UNIVERSE_SIZE = 30
DEFAULT_MAX_CANDIDATES_PER_FAMILY = 2
STABILITY_SWEEP_DEFAULT_SEEDS = (7, 11, 19, 23, 29)
FEATURE_SNAPSHOT_SCOPE_ID = "rr_confirmed_20260323_fix6"
TRADEX_EXPLORATORY_EVAL_WINDOW_MIN_TRADING_DAYS = 8
SCENARIO_SEARCH_TOP_K = 5


@dataclass(frozen=True)
class CandidateMethodSpec:
    method_family: str
    method_id: str
    method_title: str
    method_thesis: str
    plan_overrides: dict[str, Any]
    feature_family: str | None = None
    population_kind: str | None = None
    scenario_definition: dict[str, Any] | None = None
    generation_index: int | None = None
    mutation_parent_ids: tuple[str, ...] | None = None


@dataclass(frozen=True)
class FamilySpec:
    method_family: str
    family_title: str
    family_thesis: str
    candidates: tuple[CandidateMethodSpec, ...]


@dataclass(frozen=True)
class ScenarioSpec:
    direction: str
    entry_family: str
    filter_family: str
    ranking_target: str
    holding_days: int
    exit_family: str
    stop_type: str
    regime_gate: str
    liquidity_gate: str
    weight_profile: str
    signal_bias: str = "balanced"
    minimum_confidence: float = 0.60
    minimum_ready_rate: float = 0.50
    bad_pick_penalty_scale: float = 1.0
    playbook_up_score_bonus: float = 0.0
    playbook_down_score_bonus: float = 0.0
    mutation_parent_ids: tuple[str, ...] = ()
    generation_index: int = 0
    population_kind: str = "long_expert_population"


FEATURE_FAMILY_BY_METHOD_FAMILY: dict[str, str] = {
    "existing-score rescaled": "common_pattern",
    "penalty-first": "bad_pick_removal",
    "bad-pick-prune": "bad_pick_removal",
    "readiness-aware": "environment_recognition",
    "liquidity-aware": "symbol_specific_adjustment",
    "regime-aware": "regime_adjustment",
}


def _feature_family_for_method_family(method_family: str) -> str:
    feature_family = FEATURE_FAMILY_BY_METHOD_FAMILY.get(_text(method_family))
    if not feature_family:
        raise ValueError(f"feature_family required for method_family={method_family}")
    return feature_family


def _scenario_hash(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _scenario_population_kind_from_legacy(*, method_family: str, feature_family: str) -> str:
    method_family_l = _text(method_family).lower()
    feature_family_l = _text(feature_family).lower()
    if "regime" in method_family_l or feature_family_l == "regime_adjustment":
        return "regime_selector_population"
    if "short" in method_family_l or feature_family_l in {"bad_pick_removal", "symbol_specific_adjustment"}:
        return "short_expert_population"
    return "long_expert_population"


def _scenario_population_label(population_kind: str) -> str:
    if population_kind == "short_expert_population":
        return "Short Experts"
    if population_kind == "regime_selector_population":
        return "Regime Selector"
    return "Long Experts"


def _scenario_direction_for_population(population_kind: str, *, fallback: str = "long") -> str:
    if population_kind == "short_expert_population":
        return "short"
    return "long" if fallback not in {"long", "short"} else fallback


def _scenario_definition_to_payload(spec: ScenarioSpec) -> dict[str, Any]:
    return {
        "direction": spec.direction,
        "entry_family": spec.entry_family,
        "filter_family": spec.filter_family,
        "ranking_target": spec.ranking_target,
        "holding_days": int(spec.holding_days),
        "exit_family": spec.exit_family,
        "stop_type": spec.stop_type,
        "regime_gate": spec.regime_gate,
        "liquidity_gate": spec.liquidity_gate,
        "weight_profile": spec.weight_profile,
        "signal_bias": spec.signal_bias,
        "minimum_confidence": float(spec.minimum_confidence),
        "minimum_ready_rate": float(spec.minimum_ready_rate),
        "bad_pick_penalty_scale": float(spec.bad_pick_penalty_scale),
        "playbook_up_score_bonus": float(spec.playbook_up_score_bonus),
        "playbook_down_score_bonus": float(spec.playbook_down_score_bonus),
        "mutation_parent_ids": list(spec.mutation_parent_ids),
        "generation_index": int(spec.generation_index),
        "population_kind": spec.population_kind,
    }


def _scenario_definition_signature(spec: ScenarioSpec) -> str:
    payload = {
        "population_kind": spec.population_kind,
        "method_thesis": _scenario_method_thesis(spec),
    }
    return _scenario_hash(payload)


def _scenario_method_id(search_id: str, population_kind: str, spec: ScenarioSpec) -> str:
    payload = {
        "search_id": _slug(search_id),
        "population_kind": population_kind,
        "scenario": _scenario_definition_to_payload(spec),
    }
    return f"{_slug(search_id)}-{_slug(population_kind)}-{_scenario_hash(payload)}"


def _scenario_method_title(spec: ScenarioSpec) -> str:
    return (
        f"{spec.population_kind} / {spec.direction} / {spec.entry_family} / "
        f"{spec.ranking_target} / g{int(spec.generation_index)}"
    )


def _scenario_method_thesis(spec: ScenarioSpec) -> str:
    return (
        f"{spec.direction} expert via {spec.entry_family}, filtered by {spec.filter_family}, "
        f"targeting {spec.ranking_target} under {spec.regime_gate} with {spec.liquidity_gate}"
    )


def _scenario_candidate_signature(spec: ScenarioSpec) -> str:
    plan = _scenario_plan_overrides(spec, search_id="scenario-search", parent_candidate_ids=[])
    payload = {
        "method_family": spec.population_kind,
        "minimum_confidence": tradex._float(plan.get("minimum_confidence")),
        "minimum_ready_rate": tradex._float(plan.get("minimum_ready_rate")),
        "signal_bias": _text(plan.get("signal_bias"), fallback="balanced"),
        "top_k": max(1, tradex._int(plan.get("top_k")) or 0),
        "bad_pick_penalty_scale": tradex._float(plan.get("bad_pick_penalty_scale")) or 0.0,
        "bad_pick_penalty_profile": _text(plan.get("bad_pick_penalty_profile"), fallback="shared"),
        "playbook_up_score_bonus": tradex._float(plan.get("playbook_up_score_bonus")) or 0.0,
        "playbook_down_score_bonus": tradex._float(plan.get("playbook_down_score_bonus")) or 0.0,
    }
    return tradex._stable_hash(payload)


def _scenario_plan_overrides(
    spec: ScenarioSpec,
    *,
    search_id: str,
    parent_candidate_ids: list[str],
    top_k: int | None = None,
) -> dict[str, Any]:
    direction = _text(spec.direction, fallback="long")
    signal_bias = _text(spec.signal_bias, fallback="sell" if direction == "short" else "buy")
    weight_profile = _text(spec.weight_profile, fallback="balanced")
    if weight_profile not in {"balanced", "boundary_heavy", "bad_pick_heavy", "regime_heavy", "liquidity_heavy"}:
        weight_profile = "balanced"
    minimum_confidence = float(spec.minimum_confidence)
    minimum_ready_rate = float(spec.minimum_ready_rate)
    bad_pick_penalty_scale = float(spec.bad_pick_penalty_scale)
    playbook_up_score_bonus = float(spec.playbook_up_score_bonus)
    playbook_down_score_bonus = float(spec.playbook_down_score_bonus)
    plan_top_k = SCENARIO_SEARCH_TOP_K if top_k is None else max(1, int(top_k))
    return {
        "minimum_confidence": minimum_confidence,
        "minimum_ready_rate": minimum_ready_rate,
        "signal_bias": signal_bias,
        "top_k": plan_top_k,
        "bad_pick_penalty_scale": bad_pick_penalty_scale,
        "bad_pick_penalty_profile": _text(getattr(spec, "bad_pick_penalty_profile", ""), fallback="shared"),
        "target_failure_bucket": _text(getattr(spec, "target_failure_bucket", "")),
        "playbook_up_score_bonus": playbook_up_score_bonus,
        "playbook_down_score_bonus": playbook_down_score_bonus,
        "scenario_definition": _scenario_definition_to_payload(spec),
        "population_kind": spec.population_kind,
        "search_id": _slug(search_id),
        "generation_index": int(spec.generation_index),
        "mutation_parent_ids": list(parent_candidate_ids),
        "regime_gate": spec.regime_gate,
        "liquidity_gate": spec.liquidity_gate,
        "entry_family": spec.entry_family,
        "filter_family": spec.filter_family,
        "ranking_target": spec.ranking_target,
        "holding_days": int(spec.holding_days),
        "exit_family": spec.exit_family,
        "stop_type": spec.stop_type,
        "weight_profile": weight_profile,
    }


def _scenario_candidate_score(record: dict[str, Any]) -> float:
    metrics = record.get("metrics") if isinstance(record.get("metrics"), dict) else {}
    comparison = record.get("comparison") if isinstance(record.get("comparison"), dict) else {}
    evaluation_metrics = record.get("evaluation_metrics") if isinstance(record.get("evaluation_metrics"), dict) else {}
    branching_metrics = record.get("branching_metrics") if isinstance(record.get("branching_metrics"), dict) else {}
    keep_bonus = 1.0 if _text(record.get("decision")) == "keep" else 0.5 if _text(record.get("decision")) == "hold" else 0.0
    sample_count = float(metrics.get("sample_count") or 0)
    top5 = float(comparison.get("challenger_top5_ret20_mean") or comparison.get("top5_ret20_mean") or 0.0)
    top10 = float(comparison.get("challenger_top10_ret20_mean") or comparison.get("top10_ret20_mean") or 0.0)
    monthly = float(comparison.get("challenger_monthly_capture_mean") or comparison.get("monthly_capture_mean") or 0.0)
    worst = float(comparison.get("challenger_worst_regime_ret20_mean") or comparison.get("worst_regime_ret20_mean") or 0.0)
    dd = float(comparison.get("challenger_dd") or comparison.get("dd") or 0.0)
    turnover = float(comparison.get("challenger_turnover") or comparison.get("turnover") or 0.0)
    liquidity = float(comparison.get("challenger_liquidity_fail_rate") or comparison.get("liquidity_fail_rate") or 0.0)
    score = (
        keep_bonus * 100.0
        + sample_count * 0.25
        + top5 * 120.0
        + top10 * 90.0
        + monthly * 70.0
        + worst * 60.0
        - abs(dd) * 100.0
        - turnover * 30.0
        - liquidity * 80.0
    )
    if bool(record.get("sample_validity")):
        score += 20.0
    if _text(record.get("fallback_status")) == "research-fallback":
        score -= 10.0
    branching_signal = 0.0
    if bool(branching_metrics.get("meaningful_topk_branching_possible")):
        branching_signal += 1.0
    branching_signal += min(
        4.0,
        max(0.0, float(evaluation_metrics.get("changed_rank_count") or 0)) * 1.5
        + max(0.0, float(evaluation_metrics.get("changed_top5_members_count") or 0)) * 0.5
        + max(0.0, float(evaluation_metrics.get("changed_top10_members_count") or 0)) * 0.5
        + max(0.0, float(evaluation_metrics.get("top5_boundary_score_gap") or 0.0)) * 50.0
        + max(0.0, float(evaluation_metrics.get("top10_boundary_score_gap") or 0.0)) * 50.0,
    )
    score += branching_signal * 15.0
    return float(score)


def _scenario_parent_priority(record: dict[str, Any]) -> tuple[float, str, str]:
    decision_rank = {"keep": 0.0, "hold": 1.0, "drop": 2.0}.get(_text(record.get("decision")), 3.0)
    evaluation_metrics = record.get("evaluation_metrics") if isinstance(record.get("evaluation_metrics"), dict) else {}
    branching_metrics = record.get("branching_metrics") if isinstance(record.get("branching_metrics"), dict) else {}
    branching_priority = 0.0
    if bool(branching_metrics.get("meaningful_topk_branching_possible")):
        branching_priority += 1.0
    branching_priority += min(
        4.0,
        max(0.0, float(evaluation_metrics.get("changed_rank_count") or 0)) * 1.5
        + max(0.0, float(evaluation_metrics.get("changed_top5_members_count") or 0)) * 0.5
        + max(0.0, float(evaluation_metrics.get("changed_top10_members_count") or 0)) * 0.5
        + max(0.0, float(evaluation_metrics.get("top5_boundary_score_gap") or 0.0)) * 50.0
        + max(0.0, float(evaluation_metrics.get("top10_boundary_score_gap") or 0.0)) * 50.0,
    )
    return (
        decision_rank,
        -branching_priority,
        -_scenario_candidate_score(record),
        _text(record.get("candidate_id"), fallback=_text(record.get("method_signature_hash"), fallback=_text(record.get("method_id")))),
    )


def _scenario_same_condition_universe_size(record: dict[str, Any]) -> int | None:
    same_condition = record.get("same_condition_contract")
    if isinstance(same_condition, dict):
        universe = same_condition.get("universe")
        if isinstance(universe, list) and universe:
            return len(universe)
    branching_metrics = record.get("branching_metrics") if isinstance(record.get("branching_metrics"), dict) else {}
    effective_universe_count = branching_metrics.get("effective_universe_count")
    if effective_universe_count is not None:
        try:
            value = int(effective_universe_count)
        except Exception:
            value = 0
        if value > 0:
            return value
    return None


def _scenario_branching_top_k(record: dict[str, Any]) -> int:
    branching_metrics = record.get("branching_metrics") if isinstance(record.get("branching_metrics"), dict) else {}
    same_condition = record.get("same_condition_contract") if isinstance(record.get("same_condition_contract"), dict) else {}
    top_k = tradex._int(branching_metrics.get("top_k"))
    if top_k is None or top_k <= 0:
        top_k = tradex._int(same_condition.get("top_k"))
    return max(0, int(top_k or 0))


def _scenario_universe_too_small_for_branching(record: dict[str, Any]) -> bool:
    universe_size = _scenario_same_condition_universe_size(record)
    top_k = _scenario_branching_top_k(record)
    if universe_size is None or top_k <= 0:
        return False
    return universe_size <= top_k


def _scenario_is_seed_eligible(record: dict[str, Any], *, minimum_universe_size: int = 30) -> bool:
    universe_size = _scenario_same_condition_universe_size(record)
    if universe_size is not None and universe_size < int(minimum_universe_size):
        return False
    if not bool(record.get("sample_validity")):
        return False
    fallback_status = _text(record.get("fallback_status"))
    if fallback_status and fallback_status not in {"authoritative", "research-fallback"}:
        return False
    return True


def _scenario_defaults_for_population(population_kind: str) -> dict[str, Any]:
    if population_kind == "short_expert_population":
        return {
            "direction": "short",
            "entry_family": "short_downtrend",
            "filter_family": "bad_pick_removal",
            "ranking_target": "bad_pick_removal",
            "holding_days": 10,
            "exit_family": "risk_exit",
            "stop_type": "hard",
            "regime_gate": "trend_short",
            "liquidity_gate": "tight",
            "weight_profile": "bad_pick_heavy",
            "signal_bias": "sell",
            "minimum_confidence": 0.52,
            "minimum_ready_rate": 0.40,
            "bad_pick_penalty_scale": 2.5,
            "playbook_up_score_bonus": 0.0,
            "playbook_down_score_bonus": 0.03,
        }
    if population_kind == "regime_selector_population":
        return {
            "direction": "long",
            "entry_family": "regime_selector",
            "filter_family": "regime_adjustment",
            "ranking_target": "regime_balance",
            "holding_days": 20,
            "exit_family": "regime_switch",
            "stop_type": "adaptive",
            "regime_gate": "trend_long",
            "liquidity_gate": "balanced",
            "weight_profile": "regime_heavy",
            "signal_bias": "balanced",
            "minimum_confidence": 0.55,
            "minimum_ready_rate": 0.45,
            "bad_pick_penalty_scale": 1.2,
            "playbook_up_score_bonus": 0.01,
            "playbook_down_score_bonus": 0.01,
        }
    return {
        "direction": "long",
        "entry_family": "long_breakout",
        "filter_family": "boundary_feature",
        "ranking_target": "topk_uplift",
        "holding_days": 20,
        "exit_family": "time_stop",
        "stop_type": "trailing",
        "regime_gate": "trend_long",
        "liquidity_gate": "balanced",
        "weight_profile": "boundary_heavy",
        "signal_bias": "buy",
        "minimum_confidence": 0.56,
        "minimum_ready_rate": 0.45,
        "bad_pick_penalty_scale": 1.0,
        "playbook_up_score_bonus": 0.02,
        "playbook_down_score_bonus": 0.0,
    }


def _scenario_spec_from_record(record: dict[str, Any], *, population_kind: str | None = None, generation_index: int = 0) -> ScenarioSpec:
    population = _text(population_kind, fallback=_text(record.get("population_kind"), fallback="long_expert_population"))
    defaults = _scenario_defaults_for_population(population)
    candidate_method = record.get("candidate_method") if isinstance(record.get("candidate_method"), dict) else {}
    effective_config = record.get("candidate_effective_config") if isinstance(record.get("candidate_effective_config"), dict) else {}
    scenario_definition = record.get("scenario_definition") if isinstance(record.get("scenario_definition"), dict) else {}
    signal_bias = _text(effective_config.get("signal_bias"), fallback="balanced")
    direction = _text(record.get("direction"), fallback=_text(scenario_definition.get("direction"), fallback=defaults["direction"]))
    if population == "short_expert_population":
        direction = "short"
    elif population == "regime_selector_population" and direction not in {"long", "short"}:
        direction = defaults["direction"]
    return ScenarioSpec(
        direction=direction,
        entry_family=_text(
            record.get("entry_family"),
            fallback=_text(
                scenario_definition.get("entry_family"),
                fallback=_text(candidate_method.get("method_family"), fallback=defaults["entry_family"]),
            ),
        ),
        filter_family=_text(
            record.get("filter_family"),
            fallback=_text(scenario_definition.get("filter_family"), fallback=_text(record.get("feature_family"), fallback=defaults["filter_family"])),
        ),
        ranking_target=_text(record.get("ranking_target"), fallback=_text(scenario_definition.get("ranking_target"), fallback=defaults["ranking_target"])),
        holding_days=max(
            1,
            int(
                record.get("holding_days")
                or scenario_definition.get("holding_days")
                or effective_config.get("holding_days")
                or defaults["holding_days"]
            ),
        ),
        exit_family=_text(record.get("exit_family"), fallback=_text(scenario_definition.get("exit_family"), fallback=defaults["exit_family"])),
        stop_type=_text(record.get("stop_type"), fallback=_text(scenario_definition.get("stop_type"), fallback=defaults["stop_type"])),
        regime_gate=_text(
            record.get("regime_gate"),
            fallback=_text(scenario_definition.get("regime_gate"), fallback=_text(record.get("regime_tag"), fallback=defaults["regime_gate"])),
        ),
        liquidity_gate=_text(record.get("liquidity_gate"), fallback=_text(scenario_definition.get("liquidity_gate"), fallback=defaults["liquidity_gate"])),
        weight_profile=_text(
            record.get("weight_profile"),
            fallback=_text(scenario_definition.get("weight_profile"), fallback=_text(signal_bias, fallback=defaults["weight_profile"])),
        ),
        signal_bias=_text(
            effective_config.get("signal_bias"),
            fallback=_text(scenario_definition.get("signal_bias"), fallback=_text(defaults["signal_bias"])),
        ),
        minimum_confidence=float(
            scenario_definition.get("minimum_confidence")
            or effective_config.get("minimum_confidence")
            or defaults["minimum_confidence"]
        ),
        minimum_ready_rate=float(
            scenario_definition.get("minimum_ready_rate")
            or effective_config.get("minimum_ready_rate")
            or defaults["minimum_ready_rate"]
        ),
        bad_pick_penalty_scale=float(
            scenario_definition.get("bad_pick_penalty_scale")
            or effective_config.get("bad_pick_penalty_scale")
            or defaults["bad_pick_penalty_scale"]
        ),
        playbook_up_score_bonus=float(
            scenario_definition.get("playbook_up_score_bonus")
            or effective_config.get("playbook_up_score_bonus")
            or defaults["playbook_up_score_bonus"]
        ),
        playbook_down_score_bonus=float(
            scenario_definition.get("playbook_down_score_bonus")
            or effective_config.get("playbook_down_score_bonus")
            or defaults["playbook_down_score_bonus"]
        ),
        mutation_parent_ids=tuple(
            _text(item)
            for item in (
                record.get("parent_candidate_ids")
                or record.get("mutation_parent_ids")
                or scenario_definition.get("mutation_parent_ids")
                or []
            )
            if _text(item)
        ),
        generation_index=int(record.get("generation_index") or scenario_definition.get("generation_index") or generation_index),
        population_kind=population,
    )


def _scenario_record_from_candidate_result(
    *,
    session_id: str,
    family_result: dict[str, Any],
    family_row: dict[str, Any] | None,
    candidate_result: dict[str, Any],
    generation_index: int,
    source_kind: str,
    mutation_parent_ids: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    candidate_method = candidate_result.get("candidate_method") if isinstance(candidate_result.get("candidate_method"), dict) else {}
    diagnostics = candidate_result.get("diagnostics") if isinstance(candidate_result.get("diagnostics"), dict) else {}
    candidate_effective_config = diagnostics.get("candidate_effective_config") if isinstance(diagnostics.get("candidate_effective_config"), dict) else {}
    comparison = candidate_result.get("comparison") if isinstance(candidate_result.get("comparison"), dict) else {}
    selection_compare = candidate_result.get("selection_compare") if isinstance(candidate_result.get("selection_compare"), dict) else {}
    evaluation_summary = candidate_result.get("evaluation_summary") if isinstance(candidate_result.get("evaluation_summary"), dict) else {}
    method_family = _text(candidate_method.get("method_family"), fallback=_text(candidate_result.get("method_family"), fallback=_text(family_result.get("method_family"), fallback="unknown")))
    feature_family = _text(candidate_method.get("feature_family"), fallback=_text(candidate_result.get("feature_family"), fallback="common_pattern"))
    population_kind = _text(
        candidate_effective_config.get("population_kind"),
        fallback=_text(candidate_result.get("population_kind"), fallback=_scenario_population_kind_from_legacy(method_family=method_family, feature_family=feature_family)),
    )
    score_source = dict(candidate_result)
    score_source["comparison"] = comparison
    score_source["metrics"] = {
        "sample_count": int(evaluation_summary.get("sample_count") or candidate_result.get("evaluation_window_count") or 0),
    }
    scenario_spec = _scenario_spec_from_record(
        {
            "candidate_method": candidate_method,
            "candidate_effective_config": candidate_effective_config,
            "feature_family": feature_family,
            "method_family": method_family,
            "population_kind": population_kind,
            "regime_tag": _text(candidate_result.get("regime_tag"), fallback=_text(evaluation_summary.get("regime_tag"), fallback="unknown")),
            "ranking_target": _text(candidate_effective_config.get("ranking_target"), fallback="topk_uplift" if population_kind == "long_expert_population" else "bad_pick_removal" if population_kind == "short_expert_population" else "regime_balance"),
            "holding_days": int(candidate_effective_config.get("holding_days") or candidate_result.get("evaluation_window_count") or 10),
            "weight_profile": _text(candidate_effective_config.get("weight_profile"), fallback=_text(candidate_effective_config.get("signal_bias"), fallback="balanced")),
            "entry_family": _text(candidate_effective_config.get("entry_family"), fallback=_text(candidate_method.get("method_family"), fallback=method_family)),
            "filter_family": _text(candidate_effective_config.get("filter_family"), fallback=feature_family),
            "exit_family": _text(candidate_effective_config.get("exit_family"), fallback="time_stop"),
            "stop_type": _text(candidate_effective_config.get("stop_type"), fallback="adaptive" if population_kind == "regime_selector_population" else "hard"),
            "liquidity_gate": _text(candidate_effective_config.get("liquidity_gate"), fallback="balanced"),
            "mutation_parent_ids": tuple(_text(item) for item in (candidate_effective_config.get("mutation_parent_ids") or []) if _text(item)),
            "generation_index": generation_index,
            "population_kind": population_kind,
        },
        population_kind=population_kind,
        generation_index=generation_index,
    )
    same_condition = candidate_result.get("same_condition_contract") if isinstance(candidate_result.get("same_condition_contract"), dict) else {}
    candidate_id = _text(candidate_result.get("method_signature_hash"), fallback=_text(candidate_result.get("plan_id"), fallback=_scenario_hash({"candidate_method": candidate_method, "scenario": _scenario_definition_to_payload(scenario_spec)})))
    record = {
        "candidate_id": candidate_id,
        "generation_index": int(generation_index),
        "population_kind": population_kind,
        "family_id": _text(family_result.get("family_id")),
        "family_title": _text(family_result.get("family_title"), fallback=_scenario_population_label(population_kind)),
        "method_family": method_family,
        "method_id": _text(candidate_method.get("method_id"), fallback=_text(candidate_result.get("plan_id"), fallback=candidate_id)),
        "method_title": _text(candidate_method.get("method_title"), fallback=_text(candidate_result.get("plan_id"), fallback=candidate_id)),
        "method_thesis": _text(candidate_method.get("method_thesis")),
        "feature_family": feature_family,
        "decision": _text(candidate_result.get("candidate_local_decision"), fallback=_text(candidate_result.get("decision"), fallback="hold")),
        "sample_validity": bool(int(evaluation_summary.get("sample_count") or 0) > 0 and not bool(candidate_result.get("insufficient_samples"))),
        "fallback_status": _text(candidate_result.get("fallback_status"), fallback=_text(candidate_result.get("artifact_detail_level"), fallback="unknown")),
        "artifact_detail_level": _text(candidate_result.get("artifact_detail_level"), fallback="unknown"),
        "ret20_source_mode": _text(candidate_result.get("ret20_source_mode"), fallback=_text(candidate_effective_config.get("ret20_source_mode"), fallback="unknown")),
        "ret20_source_mode_reason": _text(candidate_result.get("ret20_source_mode_reason"), fallback=_text(candidate_effective_config.get("ret20_source_mode_reason"), fallback="unknown")),
        "same_condition_contract_hash": _text(same_condition.get("contract_hash"), fallback=_scenario_hash(same_condition) if same_condition else ""),
        "same_condition_contract": same_condition,
        "scenario_definition": _scenario_definition_to_payload(scenario_spec),
        "evaluation_metrics": {
            "sample_count": int(evaluation_summary.get("sample_count") or candidate_result.get("evaluation_window_count") or 0),
            "promote_ready": bool(candidate_result.get("promote_ready")),
            "top5_ret20_mean": float(comparison.get("challenger_top5_ret20_mean") or comparison.get("top5_ret20_mean") or 0.0),
            "top10_ret20_mean": float(comparison.get("challenger_top10_ret20_mean") or comparison.get("top10_ret20_mean") or 0.0),
            "monthly_capture_mean": float(comparison.get("challenger_monthly_capture_mean") or comparison.get("monthly_capture_mean") or 0.0),
            "worst_regime_ret20_mean": float(comparison.get("challenger_worst_regime_ret20_mean") or comparison.get("worst_regime_ret20_mean") or 0.0),
            "dd": float(comparison.get("challenger_dd") or comparison.get("dd") or 0.0),
            "turnover": float(comparison.get("challenger_turnover") or comparison.get("turnover") or 0.0),
            "liquidity_fail_rate": float(comparison.get("challenger_liquidity_fail_rate") or comparison.get("liquidity_fail_rate") or 0.0),
            "changed_top5_members_count": int(
                candidate_result.get("changed_top5_members_count")
                or comparison.get("changed_top5_members_count")
                or selection_compare.get("changed_top5_members_count")
                or 0
            ),
            "changed_top10_members_count": int(
                candidate_result.get("changed_top10_members_count")
                or comparison.get("changed_top10_members_count")
                or selection_compare.get("changed_top10_members_count")
                or 0
            ),
            "changed_rank_count": int(
                candidate_result.get("changed_rank_count")
                or comparison.get("changed_rank_count")
                or selection_compare.get("changed_rank_count")
                or 0
            ),
            "top5_boundary_score_gap": float(
                candidate_result.get("top5_boundary_score_gap")
                or comparison.get("top5_boundary_score_gap")
                or selection_compare.get("top5_boundary_score_gap")
                or 0.0
            ),
            "top10_boundary_score_gap": float(
                candidate_result.get("top10_boundary_score_gap")
                or comparison.get("top10_boundary_score_gap")
                or selection_compare.get("top10_boundary_score_gap")
                or 0.0
            ),
            "selection_divergence_reason": _text(
                candidate_result.get("selection_divergence_reason"),
                fallback=_text(comparison.get("selection_divergence_reason"), fallback=_text(selection_compare.get("selection_divergence_reason"), fallback="no_meaningful_branching")),
            ),
        },
        "branching_metrics": {
            "meaningful_topk_branching_possible": bool(candidate_result.get("meaningful_topk_branching_possible")),
            "topk_branching_block_reason": _text(candidate_result.get("topk_branching_block_reason"), fallback=""),
            "top_k": int(candidate_result.get("top_k") or 0),
            "effective_universe_count": int(candidate_result.get("effective_universe_count") or 0),
            "candidate_in_scope_before_build_count": int(candidate_result.get("candidate_in_scope_before_build_count") or 0),
            "candidate_in_scope_after_build_count": int(candidate_result.get("candidate_in_scope_after_build_count") or 0),
            "candidate_removed_by_scope_boundary_count": int(candidate_result.get("candidate_removed_by_scope_boundary_count") or 0),
            "scope_filter_applied_stage": _text(candidate_result.get("scope_filter_applied_stage"), fallback="unknown"),
            "key_normalization_mode": _text(candidate_result.get("key_normalization_mode"), fallback="unknown"),
        },
        "regime_summary": {
            "regime_tag": _text(candidate_result.get("regime_tag"), fallback=_text(evaluation_summary.get("regime_tag"), fallback="unknown")),
            "long_horizon_regime_score": float(candidate_result.get("long_horizon_regime_score") or 0.0),
            "recent_adaptation_score": float(candidate_result.get("recent_adaptation_score") or 0.0),
        },
        "failure_reason": _text(candidate_result.get("ret20_source_mode_reason"), fallback=_text(candidate_result.get("topk_branching_block_reason"), fallback=_text(candidate_result.get("selection_divergence_reason"), fallback=""))),
        "source_artifact_refs": {
            "session_id": _text(session_id),
            "family_id": _text(family_result.get("family_id")),
            "compare_path": _text(family_result.get("compare_path")),
            "family_leaderboard_path": _text(family_result.get("family_leaderboard_path")),
        },
        "parent_candidate_ids": list(
            _text(item)
            for item in (
                scenario_spec.mutation_parent_ids
                or mutation_parent_ids
                or candidate_effective_config.get("mutation_parent_ids")
                or ()
            )
            if _text(item)
        ),
        "source_kind": source_kind,
    }
    if _scenario_universe_too_small_for_branching(record):
        record["decision"] = "drop"
        record["failure_reason"] = "same_condition_universe_too_small"
        record["sample_validity"] = False
    record["score"] = _scenario_candidate_score(record)
    return record


def _scenario_record_from_family_row(
    *,
    session_id: str,
    family_result: dict[str, Any],
    family_row: dict[str, Any],
    generation_index: int,
    source_kind: str,
) -> dict[str, Any]:
    candidate_id = _text(family_row.get("method_signature_hash"), fallback=_text(family_row.get("best_candidate_method_id"), fallback=_text(family_row.get("method_id"), fallback="candidate")))
    population_kind = _text(
        family_row.get("population_kind"),
        fallback=_scenario_population_kind_from_legacy(
            method_family=_text(family_row.get("method_family")),
            feature_family=_text(family_row.get("best_candidate_feature_family"), fallback=_text(family_row.get("feature_family"), fallback="common_pattern")),
        ),
    )
    scenario_spec = _scenario_spec_from_record(
        {
            "candidate_method": {
                "method_family": _text(family_row.get("method_family")),
                "method_id": _text(family_row.get("best_candidate_method_id"), fallback=_text(family_row.get("method_id"), fallback=candidate_id)),
                "method_title": _text(family_row.get("best_candidate_method_title"), fallback=_text(family_row.get("method_title"), fallback=candidate_id)),
                "method_thesis": _text(family_row.get("best_candidate_method_thesis"), fallback=_text(family_row.get("method_thesis"))),
                "feature_family": _text(family_row.get("best_candidate_feature_family"), fallback=_text(family_row.get("feature_family"), fallback="common_pattern")),
            },
            "candidate_effective_config": {
                "signal_bias": _text(family_row.get("best_candidate_signal_bias"), fallback="balanced"),
                "top_k": int(family_row.get("top_k") or 5),
                "population_kind": population_kind,
                "entry_family": _text(family_row.get("entry_family"), fallback=_text(family_row.get("method_family"))),
                "filter_family": _text(family_row.get("filter_family"), fallback=_text(family_row.get("best_candidate_feature_family"), fallback=_text(family_row.get("feature_family"), fallback="common_pattern"))),
            },
            "feature_family": _text(family_row.get("best_candidate_feature_family"), fallback=_text(family_row.get("feature_family"), fallback="common_pattern")),
            "method_family": _text(family_row.get("method_family")),
            "population_kind": population_kind,
            "regime_tag": _text(family_row.get("best_candidate_regime_tag"), fallback="unknown"),
            "ranking_target": _text(family_row.get("ranking_target"), fallback="topk_uplift"),
            "holding_days": int(family_row.get("holding_days") or 10),
            "weight_profile": _text(family_row.get("weight_profile"), fallback="balanced"),
            "entry_family": _text(family_row.get("method_family")),
            "filter_family": _text(family_row.get("best_candidate_feature_family"), fallback=_text(family_row.get("feature_family"), fallback="common_pattern")),
            "exit_family": _text(family_row.get("exit_family"), fallback="time_stop"),
            "stop_type": _text(family_row.get("stop_type"), fallback="hard"),
            "liquidity_gate": _text(family_row.get("liquidity_gate"), fallback="balanced"),
            "mutation_parent_ids": tuple(),
            "generation_index": generation_index,
        },
        population_kind=population_kind,
        generation_index=generation_index,
    )
    record = {
        "candidate_id": candidate_id,
        "generation_index": int(generation_index),
        "population_kind": population_kind,
        "family_id": _text(family_result.get("family_id")),
        "family_title": _text(family_result.get("family_title"), fallback=_scenario_population_label(population_kind)),
        "method_family": _text(family_row.get("method_family")),
        "method_id": _text(family_row.get("best_candidate_method_id"), fallback=_text(family_row.get("method_id"), fallback=candidate_id)),
        "method_title": _text(family_row.get("best_candidate_method_title"), fallback=_text(family_row.get("method_title"), fallback=candidate_id)),
        "method_thesis": _text(family_row.get("best_candidate_method_thesis"), fallback=_text(family_row.get("method_thesis"))),
        "feature_family": _text(family_row.get("best_candidate_feature_family"), fallback=_text(family_row.get("feature_family"), fallback="common_pattern")),
        "decision": _text(family_row.get("decision"), fallback="hold"),
        "sample_validity": not bool(family_row.get("insufficient_samples")) and int(family_row.get("candidate_count") or 0) > 0,
        "fallback_status": _text(family_row.get("latest_fallback_status"), fallback=_text(family_row.get("fallback_status"), fallback="unknown")),
        "artifact_detail_level": _text(family_row.get("artifact_detail_level"), fallback="unknown"),
        "ret20_source_mode": _text(family_row.get("ret20_source_mode"), fallback="unknown"),
        "ret20_source_mode_reason": _text(family_row.get("ret20_source_mode_reason"), fallback="unknown"),
        "same_condition_contract_hash": _text(family_row.get("same_condition_contract_hash")),
        "same_condition_contract": {},
        "scenario_definition": _scenario_definition_to_payload(scenario_spec),
        "evaluation_metrics": {
            "sample_count": int(family_row.get("sample_count") or 0),
            "promote_ready": bool(family_row.get("best_candidate_promote_ready")),
            "top5_ret20_mean": float(family_row.get("avg_top5_ret20_mean_delta") or 0.0),
            "top10_ret20_mean": float(family_row.get("avg_top10_ret20_mean_delta") or 0.0),
            "monthly_capture_mean": float(family_row.get("avg_monthly_capture_delta") or 0.0),
            "worst_regime_ret20_mean": float(family_row.get("avg_worst_regime_delta") or 0.0),
            "dd": float(family_row.get("avg_dd_delta") or 0.0),
            "turnover": float(family_row.get("avg_turnover_delta") or 0.0),
            "liquidity_fail_rate": float(family_row.get("avg_liquidity_fail_delta") or 0.0),
            "changed_top5_members_count": int(family_row.get("avg_changed_top5_members_count") or 0),
            "changed_top10_members_count": int(family_row.get("avg_changed_top10_members_count") or 0),
            "changed_rank_count": int(family_row.get("avg_changed_rank_count") or 0),
            "top5_boundary_score_gap": float(family_row.get("avg_top5_boundary_score_gap") or 0.0),
            "top10_boundary_score_gap": float(family_row.get("avg_top10_boundary_score_gap") or 0.0),
            "selection_divergence_reason": _text(family_row.get("selection_divergence_reason"), fallback="no_meaningful_branching"),
        },
        "branching_metrics": {
            "meaningful_topk_branching_possible": bool(family_row.get("meaningful_topk_branching_possible")),
            "topk_branching_block_reason": _text(family_row.get("topk_branching_block_reason"), fallback=""),
            "top_k": int(family_row.get("top_k") or 0),
            "effective_universe_count": int(family_row.get("effective_universe_count") or 0),
            "candidate_in_scope_before_build_count": int(family_row.get("candidate_in_scope_before_build_count") or 0),
            "candidate_in_scope_after_build_count": int(family_row.get("candidate_in_scope_after_build_count") or 0),
            "candidate_removed_by_scope_boundary_count": int(family_row.get("candidate_removed_by_scope_boundary_count") or 0),
            "scope_filter_applied_stage": _text(family_row.get("scope_filter_applied_stage"), fallback="unknown"),
            "key_normalization_mode": _text(family_row.get("key_normalization_mode"), fallback="unknown"),
        },
        "regime_summary": {
            "regime_tag": _text(family_row.get("latest_eval_window_mode"), fallback="unknown"),
            "long_horizon_regime_score": 0.0,
            "recent_adaptation_score": 0.0,
        },
        "failure_reason": _text(family_row.get("topk_branching_block_reason"), fallback=_text(family_row.get("selection_divergence_reason"), fallback="")),
        "source_artifact_refs": {
            "session_id": _text(session_id),
            "family_id": _text(family_result.get("family_id")),
            "compare_path": _text(family_result.get("compare_path")),
            "family_leaderboard_path": _text(family_row.get("family_leaderboard_path")),
        },
        "parent_candidate_ids": list(),
        "source_kind": source_kind,
    }
    if _scenario_universe_too_small_for_branching(record):
        record["decision"] = "drop"
        record["failure_reason"] = "same_condition_universe_too_small"
        record["sample_validity"] = False
    record["score"] = _scenario_candidate_score(record)
    return record


def _utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _slug(text: str) -> str:
    raw = re.sub(r"[^0-9A-Za-z._-]+", "-", str(text).strip())
    raw = raw.strip("-._")
    return raw or "session"


def _seed_int(session_id: str, random_seed: int) -> int:
    payload = f"{session_id}:{int(random_seed)}"
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return int(digest[:16], 16)


def _scope_session_id(session_id: str, session_scope_id: str, random_seed: int) -> str:
    payload = f"{session_id}:{session_scope_id}:{int(random_seed)}"
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return f"scope-{digest[:12]}-seed-{int(random_seed)}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    return value


def _text(value: Any, fallback: str = "") -> str:
    if value is None:
        return fallback
    if isinstance(value, str):
        text = value.strip()
        return text or fallback
    text = str(value).strip()
    return text or fallback


def _first_metric_value(*sources_and_keys: tuple[dict[str, Any], str]) -> Any:
    for source, key in sources_and_keys:
        if not isinstance(source, dict):
            continue
        value = source.get(key)
        if value is not None:
            return value
    return None


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    tmp_path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    try:
        tmp_path.replace(path)
    except PermissionError:
        # Windows can transiently block atomic replace when the target tree is under active test or AV scans.
        # Fall back to an in-place write so scenario-search artifacts are still emitted deterministically.
        path.write_text(tmp_path.read_text(encoding="utf-8"), encoding="utf-8")
        try:
            tmp_path.unlink()
        except OSError:
            pass


def _verify_json_roundtrip(path: Path, payload: dict[str, Any], *, artifact_name: str) -> None:
    stored = json.loads(path.read_text(encoding="utf-8"))
    expected = _json_ready(payload)
    if stored != expected:
        raise RuntimeError(
            f"{artifact_name} roundtrip mismatch: {json.dumps({'path': str(path), 'expected_schema_version': expected.get('schema_version'), 'stored_schema_version': stored.get('schema_version')}, ensure_ascii=False, sort_keys=True)}"
        )


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True))
        handle.write("\n")


def _session_root() -> Path:
    root = tradex_research_sessions_root()
    root.mkdir(parents=True, exist_ok=True)
    return root


def _session_dir(session_id: str) -> Path:
    path = _session_root() / _slug(session_id)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _session_state_file(session_id: str) -> Path:
    return _session_dir(session_id) / "session.json"


def _session_compare_file(session_id: str) -> Path:
    return _session_dir(session_id) / "compare.json"


def _session_events_file(session_id: str) -> Path:
    return _session_dir(session_id) / "events.jsonl"


def _session_report_file(session_id: str) -> Path:
    report_dir = tradex_reports_root()
    return report_dir / f"{SESSION_REPORT_NAME_PREFIX}_{_slug(session_id)}.md"


def _session_family_leaderboard_file(session_id: str) -> Path:
    return _session_dir(session_id) / SESSION_FAMILY_LEADERBOARD_FILE


def _session_family_leaderboard_report_file(session_id: str) -> Path:
    report_dir = tradex_reports_root()
    return report_dir / f"{SESSION_FAMILY_LEADERBOARD_REPORT_PREFIX}_{_slug(session_id)}.md"


def _session_leaderboard_rollup_file() -> Path:
    return _session_root() / SESSION_LEADERBOARD_ROLLUP_FILE


def _session_leaderboard_rollup_report_file() -> Path:
    report_dir = tradex_reports_root()
    return report_dir / f"{SESSION_LEADERBOARD_ROLLUP_REPORT_PREFIX}.md"


def _stability_rollup_file() -> Path:
    return _session_root() / STABILITY_ROLLUP_FILE


def _stability_rollup_report_file() -> Path:
    report_dir = tradex_reports_root()
    return report_dir / f"{STABILITY_ROLLUP_REPORT_PREFIX}.md"


def _scope_stability_rollup_file() -> Path:
    return _session_root() / SCOPE_STABILITY_ROLLUP_FILE


def _scope_stability_rollup_report_file() -> Path:
    report_dir = tradex_reports_root()
    return report_dir / f"{SCOPE_STABILITY_ROLLUP_REPORT_PREFIX}.md"


def _logic_search_rollup_file(search_id: str) -> Path:
    return tradex_reports_root() / f"{LOGIC_SEARCH_ROLLUP_REPORT_PREFIX}_{_slug(search_id)}.json"


def _logic_search_rollup_report_file(search_id: str) -> Path:
    return tradex_reports_root() / f"{LOGIC_SEARCH_ROLLUP_REPORT_PREFIX}_{_slug(search_id)}.md"


def _scenario_search_root() -> Path:
    root = tradex_research_keep_root() / "scenario_search" / "v1"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _scenario_search_dir(search_id: str) -> Path:
    path = _scenario_search_root() / _slug(search_id)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _scenario_search_file(search_id: str, filename: str) -> Path:
    return _scenario_search_dir(search_id) / filename


def _session_family_id(session_id: str, method_family: str) -> str:
    return f"tradex-research-{_slug(session_id)}-{_slug(method_family)}"


def _champion_family_id(session_id: str) -> str:
    return f"tradex-research-{_slug(session_id)}-champion"


@lru_cache(maxsize=1)
def _build_family_specs() -> tuple[FamilySpec, ...]:
    return (
        FamilySpec(
            method_family="existing-score rescaled",
            family_title="既存点数の再尺度化",
            family_thesis="既存スコアの差を広げて、上位候補の密度を高める。",
            candidates=(
                CandidateMethodSpec(
                    method_family="existing-score rescaled",
                    method_id="existing_score_rescaled_v1",
                    method_title="既存点数の再尺度化",
                    method_thesis="現行スコアを少し強めに再尺度化して、上位の密度を上げる。",
                    plan_overrides={
                        "minimum_confidence": 0.56,
                        "minimum_ready_rate": 0.45,
                        "signal_bias": "balanced",
                        "top_k": 5,
                        "playbook_up_score_bonus": 0.02,
                        "playbook_down_score_bonus": 0.0,
                    },
                ),
                CandidateMethodSpec(
                    method_family="existing-score rescaled",
                    method_id="existing_score_rescaled_v2",
                    method_title="既存点数の再尺度化強め",
                    method_thesis="再尺度化を少し強めて、点差が小さい候補を落としやすくする。",
                    plan_overrides={
                        "minimum_confidence": 0.60,
                        "minimum_ready_rate": 0.50,
                        "signal_bias": "balanced",
                        "top_k": 5,
                        "playbook_up_score_bonus": 0.03,
                        "playbook_down_score_bonus": 0.0,
                    },
                ),
            ),
        ),
        FamilySpec(
            method_family="penalty-first",
            family_title="減点優先型",
            family_thesis="欠損・未解決・弱い readiness を先に落として、無駄な上位残りを減らす。",
            candidates=(
                CandidateMethodSpec(
                    method_family="penalty-first",
                    method_id="penalty_first_v1",
                    method_title="減点優先型",
                    method_thesis="欠損と未解決を先に強く罰して、上位候補を締める。",
                    plan_overrides={
                        "minimum_confidence": 0.68,
                        "minimum_ready_rate": 0.60,
                        "signal_bias": "balanced",
                        "top_k": 5,
                        "playbook_up_score_bonus": 0.0,
                        "playbook_down_score_bonus": -0.01,
                    },
                ),
                CandidateMethodSpec(
                    method_family="penalty-first",
                    method_id="penalty_first_v2",
                    method_title="減点優先型厳しめ",
                    method_thesis="さらに閾値を上げて、弱い候補を上位に残しにくくする。",
                    plan_overrides={
                        "minimum_confidence": 0.74,
                        "minimum_ready_rate": 0.65,
                        "signal_bias": "balanced",
                        "top_k": 5,
                        "playbook_up_score_bonus": 0.0,
                        "playbook_down_score_bonus": -0.015,
                    },
                ),
            ),
        ),
        FamilySpec(
            method_family="bad-pick-prune",
            family_title="Bad pick prune",
            family_thesis="トップ帯の悪い銘柄を落とす方向にだけ寄せて、上位入れ替えの質を確認する。",
            candidates=(
                CandidateMethodSpec(
                    method_family="bad-pick-prune",
                    method_id="bad_pick_prune_v1",
                    method_title="Bad pick prune v1",
                    method_thesis="bad pick 系ペナルティだけを 2 倍にして、上位からの悪い残留を減らせるかを確認する。",
                    plan_overrides={
                        "minimum_confidence": 0.60,
                        "minimum_ready_rate": 0.50,
                        "signal_bias": "balanced",
                        "top_k": 5,
                        "bad_pick_penalty_scale": 2.0,
                        "playbook_up_score_bonus": 0.0,
                        "playbook_down_score_bonus": 0.0,
                    },
                ),
                CandidateMethodSpec(
                    method_family="bad-pick-prune",
                    method_id="bad_pick_prune_v2",
                    method_title="Bad pick prune v2",
                    method_thesis="bad pick 系ペナルティを 3 倍にして、上位の悪い残留をさらに減らせるかを確認する。",
                    plan_overrides={
                        "minimum_confidence": 0.60,
                        "minimum_ready_rate": 0.50,
                        "signal_bias": "balanced",
                        "top_k": 5,
                        "bad_pick_penalty_scale": 3.0,
                        "playbook_up_score_bonus": 0.0,
                        "playbook_down_score_bonus": 0.0,
                    },
                ),
                CandidateMethodSpec(
                    method_family="bad-pick-prune",
                    method_id="bad_pick_prune_v3",
                    method_title="Bad pick prune v3",
                    method_thesis="bad pick 系ペナルティを 4 倍にして、上位の悪い残留をさらに強く抑えられるかを確認する。",
                    plan_overrides={
                        "minimum_confidence": 0.60,
                        "minimum_ready_rate": 0.50,
                        "signal_bias": "balanced",
                        "top_k": 5,
                        "bad_pick_penalty_scale": 4.0,
                        "playbook_up_score_bonus": 0.0,
                        "playbook_down_score_bonus": 0.0,
                    },
                ),
            ),
        ),
        FamilySpec(
            method_family="readiness-aware",
            family_title="準備完了優先型",
            family_thesis="ready率が高い銘柄を優先して、gate 通過後の取りこぼしを減らす。",
            candidates=(
                CandidateMethodSpec(
                    method_family="readiness-aware",
                    method_id="readiness_aware_v1",
                    method_title="準備完了優先型",
                    method_thesis="ready率を少し強めに見て、通過後の安定性を上げる。",
                    plan_overrides={
                        "minimum_confidence": 0.58,
                        "minimum_ready_rate": 0.65,
                        "signal_bias": "balanced",
                        "top_k": 5,
                        "playbook_up_score_bonus": 0.01,
                        "playbook_down_score_bonus": 0.0,
                    },
                ),
                CandidateMethodSpec(
                    method_family="readiness-aware",
                    method_id="readiness_aware_v2",
                    method_title="準備完了優先型強め",
                    method_thesis="ready率への寄せをさらに強めて、零通過月を減らしにいく。",
                    plan_overrides={
                        "minimum_confidence": 0.55,
                        "minimum_ready_rate": 0.72,
                        "signal_bias": "balanced",
                        "top_k": 5,
                        "playbook_up_score_bonus": 0.01,
                        "playbook_down_score_bonus": 0.0,
                    },
                ),
            ),
        ),
        FamilySpec(
            method_family="liquidity-aware",
            family_title="流動性ふるい残し",
            family_thesis="買えない候補を上位に残しにくくして、実行可能性を上げる。",
            candidates=(
                CandidateMethodSpec(
                    method_family="liquidity-aware",
                    method_id="liquidity_aware_v1",
                    method_title="流動性ふるい残し",
                    method_thesis="流動性の弱い候補を上位から外しやすくする。",
                    plan_overrides={
                        "minimum_confidence": 0.60,
                        "minimum_ready_rate": 0.50,
                        "signal_bias": "balanced",
                        "top_k": 5,
                        "playbook_up_score_bonus": 0.015,
                        "playbook_down_score_bonus": 0.0,
                    },
                ),
                CandidateMethodSpec(
                    method_family="liquidity-aware",
                    method_id="liquidity_aware_v2",
                    method_title="流動性ふるい残し厳しめ",
                    method_thesis="流動性への寄せを強めて、上位に買いにくい銘柄を残しにくくする。",
                    plan_overrides={
                        "minimum_confidence": 0.60,
                        "minimum_ready_rate": 0.55,
                        "signal_bias": "balanced",
                        "top_k": 5,
                        "playbook_up_score_bonus": 0.025,
                        "playbook_down_score_bonus": 0.0,
                    },
                ),
            ),
        ),
        FamilySpec(
            method_family="regime-aware",
            family_title="逆風回避の順張り",
            family_thesis="相場局面に合わせて、順張りと逆風回避の強さを切り替える。",
            candidates=(
                CandidateMethodSpec(
                    method_family="regime-aware",
                    method_id="regime_aware_v1",
                    method_title="逆風回避の順張り",
                    method_thesis="相場局面を意識して、逆風局面の損失を減らす。",
                    plan_overrides={
                        "minimum_confidence": 0.58,
                        "minimum_ready_rate": 0.50,
                        "signal_bias": "balanced",
                        "top_k": 5,
                        "playbook_up_score_bonus": 0.01,
                        "playbook_down_score_bonus": -0.01,
                    },
                ),
                CandidateMethodSpec(
                    method_family="regime-aware",
                    method_id="regime_aware_v2",
                    method_title="逆風回避の順張り保守",
                    method_thesis="順張り寄りを少し強めて、弱い局面の候補を落としやすくする。",
                    plan_overrides={
                        "minimum_confidence": 0.60,
                        "minimum_ready_rate": 0.48,
                        "signal_bias": "buy",
                        "top_k": 5,
                        "playbook_up_score_bonus": 0.01,
                        "playbook_down_score_bonus": -0.015,
                    },
                ),
            ),
        ),
    )


def _load_session_state(session_id: str) -> dict[str, Any] | None:
    path = _session_state_file(session_id)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _write_session_state(session_id: str, payload: dict[str, Any]) -> None:
    _write_json(_session_state_file(session_id), payload)
    _write_json(_session_compare_file(session_id), payload)


def _write_run_manifest(session_id: str, payload: dict[str, Any]) -> None:
    validate_run_manifest(payload)
    _write_json(run_manifest_file(session_id), payload)


def _ensure_run_manifest(session_id: str, payload: dict[str, Any], *, force: bool = False) -> Path:
    path = run_manifest_file(session_id)
    if not force and path.exists():
        return path
    _write_run_manifest(session_id, payload)
    return path


def _build_manifest(
    session_id: str,
    random_seed: int,
    universe: list[str],
    period_segments: list[dict[str, Any]],
    family_specs: tuple[FamilySpec, ...],
    max_candidates_per_family: int,
    *,
    session_scope_id: str | None = None,
    runtime_meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    scope_id = _text(session_scope_id, fallback=session_id)
    manifest = {
        "schema_version": SESSION_SCHEMA_VERSION,
        "session_id": session_id,
        "session_scope_id": scope_id,
        "random_seed": int(random_seed),
        "seed_int": _seed_int(scope_id, random_seed),
        "universe": list(universe),
        "period_segments": list(period_segments),
        "method_families": [
            {
                "method_family": spec.method_family,
                "family_title": spec.family_title,
                "family_thesis": spec.family_thesis,
                "candidate_order": [candidate.method_id for candidate in spec.candidates[:max_candidates_per_family]],
            }
            for spec in family_specs
        ],
        "max_candidates_per_family": int(max_candidates_per_family),
    }
    if isinstance(runtime_meta, dict):
        manifest["runtime_meta"] = _json_ready(runtime_meta)
    return manifest


def _build_champion_plan(
    *,
    ret20_source_mode: str = tradex.TRADEX_RET20_SOURCE_MODE_PRECOMPUTED,
    top_k: int = 5,
) -> dict[str, Any]:
    return {
        "plan_id": "champion_current_ranking",
        "plan_version": "v1",
        "label": "現行ランキング",
        "method_id": "champion_current_ranking",
        "method_title": "現行ランキング",
        "method_thesis": "現行のTRADEX標準順位をそのまま再現する。",
        "method_family": "champion",
        "feature_family": "boundary_feature",
        "minimum_confidence": 0.60,
        "minimum_ready_rate": 0.50,
        "signal_bias": "balanced",
        "top_k": max(1, int(top_k)),
        "playbook_up_score_bonus": 0.0,
        "playbook_down_score_bonus": 0.0,
        "ret20_source_mode": ret20_source_mode,
        "artifact_detail_level": TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        "fallback_status": TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
        "cost_model": dict(TRADEX_DEFAULT_COST_MODEL),
        "notes": "session champion baseline",
    }


def _build_family_body(
    *,
    session_id: str,
    random_seed: int,
    session_scope_id: str | None,
    universe: list[str],
    period_segments: list[dict[str, Any]],
    family_spec: FamilySpec,
    candidate_specs: list[CandidateMethodSpec],
    ret20_source_mode: str,
    top_k: int,
) -> dict[str, Any]:
    family_id = _session_family_id(session_id, family_spec.method_family)
    candidate_plans = [
        {
            "plan_id": candidate.method_id,
            "plan_version": "v1",
            "label": candidate.method_title,
            "method_id": candidate.method_id,
            "method_title": candidate.method_title,
            "method_thesis": candidate.method_thesis,
            "method_family": candidate.method_family,
            "feature_family": normalize_feature_family(
                candidate.feature_family or _feature_family_for_method_family(candidate.method_family)
            ),
            "ret20_source_mode": ret20_source_mode,
            "artifact_detail_level": TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
            "fallback_status": TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
            "cost_model": dict(TRADEX_DEFAULT_COST_MODEL),
            **dict(candidate.plan_overrides),
            "notes": f"{family_spec.method_family}:{candidate.method_id}",
        }
        for candidate in candidate_specs
    ]
    return {
        "family_id": family_id,
        "family_name": f"{family_spec.family_title} / {family_spec.method_family}",
        "session_scope_id": _text(session_scope_id, fallback=session_id),
        "universe": list(universe),
        "period": {"segments": list(period_segments)},
        "probes": [],
        "baseline_plan": _build_champion_plan(ret20_source_mode=ret20_source_mode, top_k=top_k),
        "candidate_plans": candidate_plans,
        "confirmed_only": True,
        "input_dataset_version": f"session:{session_id}:seed:{int(random_seed)}",
        "code_revision": tradex._git_commit(),
        "timezone": "Asia/Tokyo",
        "price_source": "daily_bars",
        "cost_model": dict(TRADEX_DEFAULT_COST_MODEL),
        "data_cutoff_at": period_segments[-1]["end_date"] if period_segments else None,
        "random_seed": int(random_seed),
        "notes": family_spec.family_thesis,
    }


def _choose_universe(codes: list[str], *, session_id: str, random_seed: int, universe_size: int) -> list[str]:
    if len(codes) < universe_size:
        raise RuntimeError(f"universe_size={universe_size} exceeds available confirmed codes={len(codes)}")
    rng = random.Random(_seed_int(session_id, random_seed))
    chosen = rng.sample(sorted(codes), universe_size)
    return sorted(chosen)


def _scope_analysis_eligible_codes(
    repo,
    *,
    codes: list[str],
    period_segments: list[dict[str, Any]],
) -> tuple[list[str], dict[str, Any]]:
    selected_segments = [
        {
            "start_date": _text(segment.get("start_date")),
            "end_date": _text(segment.get("end_date")),
        }
        for segment in period_segments
        if _text(segment.get("start_date")) and _text(segment.get("end_date"))
    ]
    if not selected_segments:
        return sorted({_text(code) for code in codes if _text(code)}), {
            "requested_code_count": len([code for code in codes if _text(code)]),
            "eligible_code_count": len([code for code in codes if _text(code)]),
            "segment_coverage_counts": {},
            "segments": [],
            "source_path": "daily_bars distinct codes",
            "selection_mode": "unfiltered_daily_bars",
        }
    eligible_codes: list[str] = []
    segment_coverage_counts: dict[str, int] = {
        f"{segment['start_date']}..{segment['end_date']}": 0 for segment in selected_segments
    }
    for raw_code in codes:
        code = _text(raw_code)
        if not code:
            continue
        code_has_scope_points = False
        for segment in selected_segments:
            points = tradex._analysis_points(repo, code, segment["start_date"], segment["end_date"])
            if points:
                code_has_scope_points = True
                segment_key = f"{segment['start_date']}..{segment['end_date']}"
                segment_coverage_counts[segment_key] = int(segment_coverage_counts.get(segment_key) or 0) + 1
        if code_has_scope_points:
            eligible_codes.append(code)
    eligible_codes = sorted(set(eligible_codes))
    return eligible_codes, {
        "requested_code_count": len([code for code in codes if _text(code)]),
        "eligible_code_count": len(eligible_codes),
        "segment_coverage_counts": segment_coverage_counts,
        "segments": list(selected_segments),
        "source_path": "analysis timeline codes intersecting selected evaluation segments",
        "selection_mode": "scope_analysis_eligible",
    }


def _scope_feature_snapshot_eligible_codes(
    repo,
    *,
    codes: list[str],
    period_segments: list[dict[str, Any]],
) -> tuple[list[str], dict[str, Any]]:
    selected_segments = [
        {
            "start_date": _text(segment.get("start_date")),
            "end_date": _text(segment.get("end_date")),
        }
        for segment in period_segments
        if _text(segment.get("start_date")) and _text(segment.get("end_date"))
    ]
    if not selected_segments:
        eligible_codes = sorted({_text(code) for code in codes if _text(code)})
        return eligible_codes, {
            "requested_code_count": len(eligible_codes),
            "eligible_code_count": len(eligible_codes),
            "segment_coverage_counts": {},
            "segments": [],
            "source_path": "feature_snapshot_daily distinct codes",
            "selection_mode": "unfiltered_feature_snapshot_daily",
        }

    code_filter = sorted({_text(code) for code in codes if _text(code)})
    code_filter_set = set(code_filter)
    eligible_codes: set[str] = set()
    segment_coverage_counts: dict[str, int] = {
        f"{segment['start_date']}..{segment['end_date']}": 0 for segment in selected_segments
    }
    with repo._get_read_conn() as conn:
        for segment in selected_segments:
            segment_key = f"{segment['start_date']}..{segment['end_date']}"
            rows = conn.execute(
                """
                SELECT DISTINCT code
                FROM feature_snapshot_daily
                WHERE CASE
                    WHEN dt >= 1000000000 THEN CAST(TO_TIMESTAMP(CAST(dt AS BIGINT)) AS DATE)
                    ELSE CAST(TRY_STRPTIME(CAST(dt AS VARCHAR), '%Y%m%d') AS DATE)
                END BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
                ORDER BY code
                """,
                [segment["start_date"], segment["end_date"]],
            ).fetchall()
            segment_codes = sorted({ _text(row[0]) for row in rows if _text(row[0]) and _text(row[0]) in code_filter_set })
            segment_coverage_counts[segment_key] = len(segment_codes)
            eligible_codes.update(segment_codes)
    eligible_codes = sorted(eligible_codes)
    return eligible_codes, {
        "requested_code_count": len(code_filter),
        "eligible_code_count": len(eligible_codes),
        "segment_coverage_counts": segment_coverage_counts,
        "segments": list(selected_segments),
        "source_path": "feature_snapshot_daily codes intersecting selected evaluation segments",
        "selection_mode": "feature_snapshot_eligible",
    }


def _build_period_segments_with_mode(
    *,
    exploratory_min_trading_days: int | None = TRADEX_EXPLORATORY_EVAL_WINDOW_MIN_TRADING_DAYS,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    regime_rows, issues = tradex._load_evaluation_regime_rows()
    standard_windows, standard_issues = tradex._select_evaluation_windows(
        regime_rows,
        min_trading_days=tradex.TRADEX_STANDARD_EVAL_WINDOW_MIN_TRADING_DAYS,
    )
    fallback_windows, fallback_issues = tradex._select_evaluation_windows(
        regime_rows,
        min_trading_days=tradex.TRADEX_RESEARCH_FALLBACK_EVAL_WINDOW_MIN_TRADING_DAYS,
    )
    mode = "standard"
    selected_windows = standard_windows
    selected_issues = list(issues)
    mode_reason = "standard_windows_available"
    if len(standard_windows) < 3:
        mode = "fallback"
        selected_windows = fallback_windows
        selected_issues = [*issues, *fallback_issues]
        mode_reason = "fallback_required_standard_windows_unavailable"
    if len(selected_windows) < 3 and exploratory_min_trading_days is not None:
        exploratory_windows, exploratory_issues = tradex._select_evaluation_windows(
            regime_rows,
            min_trading_days=max(1, int(exploratory_min_trading_days)),
        )
        if len(exploratory_windows) >= 3:
            mode = "exploratory"
            selected_windows = exploratory_windows
            selected_issues = [*issues, *exploratory_issues]
            mode_reason = f"exploratory_required_standard_and_fallback_windows_unavailable_min_days={int(exploratory_min_trading_days)}"
    if selected_issues or len(selected_windows) < 3:
        raise RuntimeError(
            "evaluation windows unavailable: "
            + ",".join([*selected_issues, "windows<3" if len(selected_windows) < 3 else ""])
        )
    return [
        {
            "label": f"{window['regime_tag']}:{window['evaluation_window_id']}",
            "start_date": window["start_date"],
            "end_date": window["end_date"],
        }
        for window in selected_windows[:3]
    ], {
        "mode": mode,
        "mode_reason": mode_reason,
        "standard_window_count": len(standard_windows),
        "fallback_window_count": len(fallback_windows),
        "standard_issues": list(standard_issues),
        "fallback_issues": list(fallback_issues),
    }


def _build_period_segments() -> list[dict[str, Any]]:
    segments, _ = _build_period_segments_with_mode()
    return segments


def _configure_research_execution_context(
    *,
    repo,
    codes: list[str],
    period_segments: list[dict[str, Any]],
    preload_timelines: bool,
) -> tradex.ResearchExecutionContext | None:
    context = tradex.get_research_execution_context()
    if context is None:
        return None
    start_dates = [_text(segment.get("start_date")) for segment in period_segments if _text(segment.get("start_date"))]
    end_dates = [_text(segment.get("end_date")) for segment in period_segments if _text(segment.get("end_date"))]
    if start_dates and end_dates:
        context.configure_scope_window(
            start_date=min(start_dates),
            end_date=max(end_dates),
            limit=1000,
        )
    if preload_timelines and codes:
        context.preload_scope_timelines(repo, codes)
    return context


def _preload_research_universe_context(
    *,
    repo,
    universe: list[str],
) -> None:
    context = tradex.get_research_execution_context()
    if context is None or not universe:
        return
    context.preload_scope_timelines(repo, universe)
    context.preload_daily_rows(repo, universe, limit=10000)


def _require_legacy_analysis_enabled(*, context: str) -> None:
    if is_legacy_analysis_disabled():
        raise RuntimeError(
            f"{context}: legacy analysis is disabled; set {LEGACY_ANALYSIS_DISABLE_ENV}=0 to run TRADEX research"
        )


def _seed_family_baseline_from_reference(
    *,
    reference_run: dict[str, Any],
    family_id: str,
    family: dict[str, Any],
) -> dict[str, Any]:
    baseline_run_id = f"{family_id}-baseline"
    baseline_path = run_file(family_id, baseline_run_id)
    if baseline_path.exists():
        loaded = load_run(family_id, baseline_run_id)
        if isinstance(loaded, dict):
            return loaded
    copied = json.loads(json.dumps(reference_run, ensure_ascii=False, default=str))
    copied["family_id"] = family_id
    copied["run_id"] = baseline_run_id
    copied["run_kind"] = "baseline"
    copied["status"] = "succeeded"
    copied["started_at"] = copied.get("started_at") or _utc_now_iso()
    copied["completed_at"] = copied.get("completed_at") or _utc_now_iso()
    copied["notes"] = f"seeded_from:{reference_run.get('family_id')}/{reference_run.get('run_id')}"
    copied["diagnostics_schema_version"] = tradex.TRADEX_DIAGNOSTICS_SCHEMA_VERSION
    write_json(baseline_path, copied)
    family["baseline_run_id"] = baseline_run_id
    family["frozen"] = True
    family["frozen_at"] = family.get("frozen_at") or _utc_now_iso()
    family["run_ids"] = [baseline_run_id]
    family["candidate_run_ids"] = [str(item) for item in family.get("candidate_run_ids") or [] if str(item).strip()]
    tradex._update_family_file(family)
    return copied


def _family_best_key(candidate_result: dict[str, Any]) -> tuple[float, float, float, float, float, str]:
    evaluation = candidate_result.get("evaluation_summary") if isinstance(candidate_result.get("evaluation_summary"), dict) else {}
    selection_compare = candidate_result.get("selection_compare") if isinstance(candidate_result.get("selection_compare"), dict) else {}
    windows = evaluation.get("windows") if isinstance(evaluation.get("windows"), list) else []
    changed_rank_count = float(selection_compare.get("changed_rank_count") or 0.0)
    changed_top5_members_count = float(selection_compare.get("changed_top5_members_count") or 0.0)
    changed_top10_members_count = float(selection_compare.get("changed_top10_members_count") or 0.0)
    top5_boundary_score_gap = float(selection_compare.get("top5_boundary_score_gap") or 0.0)
    top10_boundary_score_gap = float(selection_compare.get("top10_boundary_score_gap") or 0.0)
    branching_priority = (
        changed_rank_count * 10.0
        + changed_top5_members_count * 5.0
        + changed_top10_members_count * 3.0
        + top5_boundary_score_gap * 100.0
        + top10_boundary_score_gap * 80.0
        + (1.0 if bool(selection_compare.get("meaningful_topk_branching_possible")) else 0.0)
    )
    challenger_top5 = float(evaluation.get("challenger_topk_ret20_mean") or 0.0)
    worst_regime_margin = 0.0
    if windows:
        margins: list[float] = []
        for window in windows:
            champion = float(window.get("champion_top5_ret20_mean") or 0.0)
            challenger = float(window.get("challenger_top5_ret20_mean") or 0.0)
            margins.append(challenger - champion)
        worst_regime_margin = min(margins) if margins else 0.0
    return (
        -branching_priority,
        -challenger_top5,
        -worst_regime_margin,
        float(evaluation.get("challenger_dd") or 0.0),
        float(evaluation.get("challenger_turnover") or 0.0),
        str(candidate_result.get("plan_id") or ""),
    )


def _family_result_summary(
    *,
    family_spec: FamilySpec,
    family: dict[str, Any],
    compare: dict[str, Any] | None,
) -> dict[str, Any]:
    candidate_results = compare.get("candidate_results") if isinstance(compare, dict) else []
    if candidate_results is None:
        candidate_results = []
    candidate_results = [item for item in candidate_results if isinstance(item, dict)]
    ranked_candidates = sorted(candidate_results, key=_family_best_key)
    best_candidate = ranked_candidates[0] if ranked_candidates else None
    return {
        "family_id": family.get("family_id"),
        "method_family": family_spec.method_family,
        "family_title": family_spec.family_title,
        "family_thesis": family_spec.family_thesis,
        "candidate_count": len(candidate_results),
        "candidate_order": [candidate.get("plan_id") for candidate in candidate_results],
        "compare_path": str(family_dir(family["family_id"]) / "compare.json"),
        "compare": compare or {},
        "candidate_results": candidate_results,
        "best_candidate": best_candidate,
        "promote_ready": bool(best_candidate.get("promote_ready")) if best_candidate else False,
        "promote_reasons": best_candidate.get("promote_reasons") if isinstance(best_candidate, dict) else [],
        "best_method_title": (best_candidate.get("candidate_method") or {}).get("method_title") if isinstance(best_candidate, dict) else None,
        "best_method_thesis": (best_candidate.get("candidate_method") or {}).get("method_thesis") if isinstance(best_candidate, dict) else None,
    }


def _train_phase4_ranker(*, family_results: list[dict[str, Any]], random_seed: int) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for family_result in family_results:
        compare = family_result.get("compare") if isinstance(family_result.get("compare"), dict) else {}
        candidates = compare.get("candidate_results") if isinstance(compare.get("candidate_results"), list) else []
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            evaluation = candidate.get("evaluation_summary") if isinstance(candidate.get("evaluation_summary"), dict) else {}
            method = candidate.get("candidate_method") if isinstance(candidate.get("candidate_method"), dict) else {}
            windows = evaluation.get("windows") if isinstance(evaluation.get("windows"), list) else []
            worst_regime_margin = min(
                (
                    float(window.get("challenger_top5_ret20_mean") or 0.0)
                    - float(window.get("champion_top5_ret20_mean") or 0.0)
                    for window in windows
                ),
                default=0.0,
            )
            rows.append(
                {
                    "method_family": str(method.get("method_family") or family_result.get("method_family") or "unknown"),
                    "method_title": str(method.get("method_title") or candidate.get("plan_id") or "unknown"),
                    "promote_ready": bool(evaluation.get("promote_ready")),
                    "top5_mean": float(evaluation.get("challenger_topk_ret20_mean") or 0.0),
                    "top5_median": float(evaluation.get("challenger_topk_ret20_median") or 0.0),
                    "top10_mean": float(evaluation.get("challenger_topk10_ret20_mean") or 0.0),
                    "top10_median": float(evaluation.get("challenger_topk10_ret20_median") or 0.0),
                    "monthly_capture_mean": float(evaluation.get("challenger_monthly_top5_capture_mean") or 0.0),
                    "zero_pass_months": float(evaluation.get("challenger_zero_pass_months") or 0.0),
                    "dd": float(evaluation.get("challenger_dd") or 0.0),
                    "turnover": float(evaluation.get("challenger_turnover") or 0.0),
                    "liquidity_fail_rate": float(evaluation.get("challenger_liquidity_fail_rate") or 0.0),
                    "worst_regime_margin": float(worst_regime_margin),
                }
            )

    if len(rows) < 4:
        return {"status": "skipped", "reason": "insufficient_rows", "row_count": len(rows)}
    labels = {bool(row["promote_ready"]) for row in rows}
    if len(labels) < 2:
        return {"status": "skipped", "reason": "single_class", "row_count": len(rows)}
    try:
        import lightgbm as lgb  # type: ignore
    except Exception as exc:
        return {"status": "skipped", "reason": f"lightgbm_unavailable:{exc.__class__.__name__}", "row_count": len(rows)}

    frame = pd.DataFrame(rows)
    feature_frame = pd.get_dummies(frame.drop(columns=["promote_ready", "method_title"]), columns=["method_family"], dummy_na=False)
    target = frame["promote_ready"].astype(int)
    model = lgb.LGBMClassifier(
        n_estimators=64,
        learning_rate=0.08,
        max_depth=3,
        num_leaves=15,
        random_state=_seed_int("phase4", random_seed),
        n_jobs=1,
    )
    model.fit(feature_frame, target)
    scores = model.predict_proba(feature_frame)[:, 1].tolist()
    ranked = sorted(
        [
            {
                "method_family": row["method_family"],
                "method_title": row["method_title"],
                "score": float(score),
                "promote_ready": bool(row["promote_ready"]),
            }
            for row, score in zip(rows, scores, strict=False)
        ],
        key=lambda item: (-float(item["score"]), item["method_family"], item["method_title"]),
    )
    importance_items = sorted(
        zip(feature_frame.columns, model.feature_importances_, strict=False),
        key=lambda item: (-float(item[1]), str(item[0])),
    )
    return {
        "status": "trained",
        "row_count": len(rows),
        "feature_count": len(feature_frame.columns),
        "feature_columns": list(feature_frame.columns),
        "ranked_candidates": ranked,
        "feature_importances": {str(column): float(importance) for column, importance in importance_items},
    }


def _leaderboard_metric_delta(champion_value: Any, candidate_value: Any) -> float | None:
    champion_number = tradex._float(champion_value)
    candidate_number = tradex._float(candidate_value)
    if champion_number is None or candidate_number is None:
        return None
    return float(candidate_number) - float(champion_number)


def _leaderboard_metric_underperform_bp(champion_value: Any, candidate_value: Any) -> float:
    delta = _leaderboard_metric_delta(champion_value, candidate_value)
    if delta is None:
        return 0.0
    return max(0.0, -delta * 10000.0)


def _leaderboard_candidate_signature_payload(candidate_result: dict[str, Any]) -> dict[str, Any]:
    candidate_method = candidate_result.get("candidate_method") if isinstance(candidate_result.get("candidate_method"), dict) else {}
    diagnostics = candidate_result.get("diagnostics") if isinstance(candidate_result.get("diagnostics"), dict) else {}
    candidate_effective_config = diagnostics.get("candidate_effective_config") if isinstance(diagnostics.get("candidate_effective_config"), dict) else {}
    method_family = _text(candidate_method.get("method_family"), fallback=_text(candidate_effective_config.get("method_family"), fallback="unknown"))
    return {
        "method_family": method_family,
        "minimum_confidence": tradex._float(candidate_effective_config.get("minimum_confidence")),
        "minimum_ready_rate": tradex._float(candidate_effective_config.get("minimum_ready_rate")),
        "signal_bias": _text(candidate_effective_config.get("signal_bias"), fallback="balanced"),
        "top_k": max(1, tradex._int(candidate_effective_config.get("top_k")) or 0),
        "playbook_up_score_bonus": tradex._float(candidate_effective_config.get("playbook_up_score_bonus")) or 0.0,
        "playbook_down_score_bonus": tradex._float(candidate_effective_config.get("playbook_down_score_bonus")) or 0.0,
        "ret20_source_mode": _text(candidate_effective_config.get("ret20_source_mode"), fallback="precomputed"),
    }


def _leaderboard_candidate_signature_hash(candidate_result: dict[str, Any]) -> str:
    return tradex._stable_hash(_leaderboard_candidate_signature_payload(candidate_result))


def _read_json_file(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _leaderboard_reason_entry(
    *,
    code: str,
    status: str,
    champion_value: Any,
    candidate_value: Any,
    delta: float | None,
    threshold: float | int | None = None,
    detail: dict[str, Any] | None = None,
) -> dict[str, Any]:
    entry = {
        "code": code,
        "status": status,
        "champion_value": champion_value,
        "candidate_value": candidate_value,
        "delta": delta,
    }
    if threshold is not None:
        entry["threshold"] = threshold
    if detail:
        entry.update(detail)
    return entry


def _leaderboard_candidate_reasons(evaluation: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    champion_top5_mean = tradex._float(evaluation.get("champion_topk_ret20_mean")) or 0.0
    challenger_top5_mean = tradex._float(evaluation.get("challenger_topk_ret20_mean")) or 0.0
    champion_top5_median = tradex._float(evaluation.get("champion_topk_ret20_median")) or 0.0
    challenger_top5_median = tradex._float(evaluation.get("challenger_topk_ret20_median")) or 0.0
    champion_top10_mean = tradex._float(evaluation.get("champion_topk10_ret20_mean")) or 0.0
    challenger_top10_mean = tradex._float(evaluation.get("challenger_topk10_ret20_mean")) or 0.0
    champion_top10_median = tradex._float(evaluation.get("champion_topk10_ret20_median")) or 0.0
    challenger_top10_median = tradex._float(evaluation.get("challenger_topk10_ret20_median")) or 0.0
    champion_capture = tradex._float((evaluation.get("champion_monthly_top5_capture") or {}).get("mean")) or 0.0
    challenger_capture = tradex._float((evaluation.get("challenger_monthly_top5_capture") or {}).get("mean")) or 0.0
    window_win_rate = tradex._float(evaluation.get("window_win_rate")) or 0.0
    champion_zero_pass_months = int(evaluation.get("champion_zero_pass_months") or 0)
    challenger_zero_pass_months = int(evaluation.get("challenger_zero_pass_months") or 0)
    champion_regime_means = [value for value in tradex._selection_regime_ret20_means(evaluation.get("champion_summary") or {}) if value is not None]
    challenger_regime_means = [value for value in tradex._selection_regime_ret20_means(evaluation.get("challenger_summary") or {}) if value is not None]
    champion_worst_regime = min(champion_regime_means) if champion_regime_means else 0.0
    challenger_worst_regime = min(challenger_regime_means) if challenger_regime_means else 0.0
    champion_turnover = tradex._float(evaluation.get("champion_turnover")) or 0.0
    challenger_turnover = tradex._float(evaluation.get("challenger_turnover")) or 0.0
    champion_dd = tradex._float(evaluation.get("champion_dd")) or 0.0
    challenger_dd = tradex._float(evaluation.get("challenger_dd")) or 0.0
    champion_liquidity_fail_rate = tradex._float(evaluation.get("champion_liquidity_fail_rate")) or 0.0
    challenger_liquidity_fail_rate = tradex._float(evaluation.get("challenger_liquidity_fail_rate")) or 0.0
    champion_future_ret20_source_coverage = evaluation.get("champion_future_ret20_source_coverage") if isinstance(evaluation.get("champion_future_ret20_source_coverage"), dict) else {}
    challenger_future_ret20_source_coverage = evaluation.get("challenger_future_ret20_source_coverage") if isinstance(evaluation.get("challenger_future_ret20_source_coverage"), dict) else {}
    champion_future_ret20_code_coverage = evaluation.get("champion_future_ret20_code_coverage") if isinstance(evaluation.get("champion_future_ret20_code_coverage"), dict) else {}
    challenger_future_ret20_code_coverage = evaluation.get("challenger_future_ret20_code_coverage") if isinstance(evaluation.get("challenger_future_ret20_code_coverage"), dict) else {}
    meaningful_topk_branching_possible = bool(
        evaluation.get("meaningful_topk_branching_possible")
        if "meaningful_topk_branching_possible" in evaluation
        else False
    )
    topk_branching_block_reason = _text(evaluation.get("topk_branching_block_reason"), fallback="")

    top5_underperform_bp = _leaderboard_metric_underperform_bp(champion_top5_mean, challenger_top5_mean)
    top5_median_underperform_bp = _leaderboard_metric_underperform_bp(champion_top5_median, challenger_top5_median)
    top10_underperform_bp = _leaderboard_metric_underperform_bp(champion_top10_mean, challenger_top10_mean)
    top10_median_underperform_bp = _leaderboard_metric_underperform_bp(champion_top10_median, challenger_top10_median)
    monthly_capture_degrade = max(0.0, champion_capture - challenger_capture)
    zero_pass_degrade = max(0.0, float(challenger_zero_pass_months - champion_zero_pass_months))
    worst_regime_underperform_bp = _leaderboard_metric_underperform_bp(champion_worst_regime, challenger_worst_regime)
    dd_degrade_bp = max(0.0, (challenger_dd - champion_dd) * 10000.0)
    turnover_degrade_ratio = max(0.0, challenger_turnover - champion_turnover)
    liquidity_fail_degrade_ratio = max(0.0, challenger_liquidity_fail_rate - champion_liquidity_fail_rate)

    top5_status = "pass"
    if top5_underperform_bp > 50.0 or top5_median_underperform_bp > 50.0:
        top5_status = "fail"
    elif top5_underperform_bp > 0.0 or top5_median_underperform_bp > 0.0:
        top5_status = "warn"

    top10_status = "pass"
    if top10_underperform_bp > tradex.PROMOTE_TOP10_MEAN_TOLERANCE_BP or top10_median_underperform_bp > tradex.PROMOTE_TOP10_MEAN_TOLERANCE_BP:
        top10_status = "fail"
    elif top10_underperform_bp > 0.0 or top10_median_underperform_bp > 0.0:
        top10_status = "warn"

    monthly_status = "pass"
    if monthly_capture_degrade > 0.0 or window_win_rate < 0.50:
        monthly_status = "fail"
    elif window_win_rate < tradex.PROMOTE_MIN_MONTHLY_WIN_RATE:
        monthly_status = "warn"

    zero_pass_status = "pass" if zero_pass_degrade <= 0.0 else "fail"
    worst_regime_status = "pass"
    if worst_regime_underperform_bp > tradex.PROMOTE_MAX_WORST_REGIME_UNDERPERFORM_BP:
        worst_regime_status = "fail"
    elif worst_regime_underperform_bp > 0.0:
        worst_regime_status = "warn"

    dd_status = "pass"
    if dd_degrade_bp > tradex.PROMOTE_MAX_DD_DEGRADE_BP:
        dd_status = "fail"
    elif dd_degrade_bp > 0.0:
        dd_status = "warn"

    turnover_status = "pass"
    if turnover_degrade_ratio > tradex.PROMOTE_MAX_TURNOVER_DEGRADE_RATIO:
        turnover_status = "fail"
    elif turnover_degrade_ratio > 0.0:
        turnover_status = "warn"

    liquidity_status = "pass" if liquidity_fail_degrade_ratio <= 0.0 else "fail"

    decision_reasons = [
        _leaderboard_reason_entry(
            code="top5",
            status=top5_status,
            champion_value=champion_top5_mean,
            candidate_value=challenger_top5_mean,
            delta=_leaderboard_metric_delta(champion_top5_mean, challenger_top5_mean),
            threshold=50.0,
            detail={
                "top5_underperform_bp": top5_underperform_bp,
                "top5_median_underperform_bp": top5_median_underperform_bp,
            },
        ),
        _leaderboard_reason_entry(
            code="top10",
            status=top10_status,
            champion_value=champion_top10_mean,
            candidate_value=challenger_top10_mean,
            delta=_leaderboard_metric_delta(champion_top10_mean, challenger_top10_mean),
            threshold=tradex.PROMOTE_TOP10_MEAN_TOLERANCE_BP,
            detail={
                "top10_underperform_bp": top10_underperform_bp,
                "top10_median_underperform_bp": top10_median_underperform_bp,
            },
        ),
        _leaderboard_reason_entry(
            code="monthly_capture",
            status=monthly_status,
            champion_value=champion_capture,
            candidate_value=challenger_capture,
            delta=_leaderboard_metric_delta(champion_capture, challenger_capture),
            threshold=tradex.PROMOTE_MIN_MONTHLY_WIN_RATE,
            detail={
                "monthly_capture_degrade": monthly_capture_degrade,
                "monthly_window_win_rate": window_win_rate,
            },
        ),
        _leaderboard_reason_entry(
            code="zero_pass",
            status=zero_pass_status,
            champion_value=champion_zero_pass_months,
            candidate_value=challenger_zero_pass_months,
            delta=float(challenger_zero_pass_months - champion_zero_pass_months),
            threshold=0,
            detail={
                "zero_pass_degrade": zero_pass_degrade,
            },
        ),
        _leaderboard_reason_entry(
            code="worst_regime",
            status=worst_regime_status,
            champion_value=champion_worst_regime,
            candidate_value=challenger_worst_regime,
            delta=_leaderboard_metric_delta(champion_worst_regime, challenger_worst_regime),
            threshold=tradex.PROMOTE_MAX_WORST_REGIME_UNDERPERFORM_BP,
            detail={
                "worst_regime_underperform_bp": worst_regime_underperform_bp,
            },
        ),
        _leaderboard_reason_entry(
            code="dd",
            status=dd_status,
            champion_value=champion_dd,
            candidate_value=challenger_dd,
            delta=_leaderboard_metric_delta(champion_dd, challenger_dd),
            threshold=tradex.PROMOTE_MAX_DD_DEGRADE_BP,
            detail={
                "dd_degrade_bp": dd_degrade_bp,
            },
        ),
        _leaderboard_reason_entry(
            code="turnover",
            status=turnover_status,
            champion_value=champion_turnover,
            candidate_value=challenger_turnover,
            delta=_leaderboard_metric_delta(champion_turnover, challenger_turnover),
            threshold=tradex.PROMOTE_MAX_TURNOVER_DEGRADE_RATIO,
            detail={
                "turnover_degrade_ratio": turnover_degrade_ratio,
            },
        ),
        _leaderboard_reason_entry(
            code="liquidity_fail",
            status=liquidity_status,
            champion_value=champion_liquidity_fail_rate,
            candidate_value=challenger_liquidity_fail_rate,
            delta=_leaderboard_metric_delta(champion_liquidity_fail_rate, challenger_liquidity_fail_rate),
            threshold=tradex.PROMOTE_MAX_LIQUIDITY_FAIL_DEGRADE_RATIO,
            detail={
                "liquidity_fail_degrade_ratio": liquidity_fail_degrade_ratio,
            },
        ),
    ]
    if not meaningful_topk_branching_possible:
        decision_reasons.append(
            _leaderboard_reason_entry(
                code="topk_branching",
                status="hold",
                champion_value=float(evaluation.get("top_k") or 0.0),
                candidate_value=float(evaluation.get("top_k") or 0.0),
                delta=0.0,
                detail={
                    "meaningful_topk_branching_possible": False,
                    "topk_branching_block_reason": topk_branching_block_reason or "topk_boundary_absent",
                },
            )
        )
    comparison = {
        "champion_top5_ret20_mean": champion_top5_mean,
        "challenger_top5_ret20_mean": challenger_top5_mean,
        "champion_top5_ret20_median": champion_top5_median,
        "challenger_top5_ret20_median": challenger_top5_median,
        "champion_top10_ret20_mean": champion_top10_mean,
        "challenger_top10_ret20_mean": challenger_top10_mean,
        "champion_top10_ret20_median": champion_top10_median,
        "challenger_top10_ret20_median": challenger_top10_median,
        "champion_monthly_top5_capture_mean": champion_capture,
        "challenger_monthly_top5_capture_mean": challenger_capture,
        "monthly_capture_degrade": monthly_capture_degrade,
        "window_win_rate": window_win_rate,
        "champion_zero_pass_months": champion_zero_pass_months,
        "challenger_zero_pass_months": challenger_zero_pass_months,
        "zero_pass_degrade": zero_pass_degrade,
        "champion_worst_regime_ret20_mean": champion_worst_regime,
        "challenger_worst_regime_ret20_mean": challenger_worst_regime,
        "worst_regime_underperform_bp": worst_regime_underperform_bp,
        "champion_dd": champion_dd,
        "challenger_dd": challenger_dd,
        "dd_degrade_bp": dd_degrade_bp,
        "champion_turnover": champion_turnover,
        "challenger_turnover": challenger_turnover,
        "turnover_degrade_ratio": turnover_degrade_ratio,
        "champion_liquidity_fail_rate": champion_liquidity_fail_rate,
        "challenger_liquidity_fail_rate": challenger_liquidity_fail_rate,
        "liquidity_fail_degrade_ratio": liquidity_fail_degrade_ratio,
        "champion_future_ret20_source_coverage": champion_future_ret20_source_coverage,
        "challenger_future_ret20_source_coverage": challenger_future_ret20_source_coverage,
        "champion_future_ret20_code_coverage": champion_future_ret20_code_coverage,
        "challenger_future_ret20_code_coverage": challenger_future_ret20_code_coverage,
        "future_ret20_source_coverage": challenger_future_ret20_source_coverage or champion_future_ret20_source_coverage,
        "future_ret20_code_coverage": challenger_future_ret20_code_coverage or champion_future_ret20_code_coverage,
        "changed_top5_members_count": int(evaluation.get("changed_top5_members_count") or 0),
        "changed_top10_members_count": int(evaluation.get("changed_top10_members_count") or 0),
        "changed_rank_count": int(evaluation.get("changed_rank_count") or 0),
        "top5_boundary_score_gap": float(evaluation.get("top5_boundary_score_gap") or 0.0),
        "top10_boundary_score_gap": float(evaluation.get("top10_boundary_score_gap") or 0.0),
        "selection_divergence_reason": _text(evaluation.get("selection_divergence_reason"), fallback="no_meaningful_branching"),
        "meaningful_topk_branching_possible": meaningful_topk_branching_possible,
        "topk_branching_block_reason": topk_branching_block_reason,
    }
    if bool(evaluation.get("insufficient_samples")):
        return [
            {
                "code": "insufficient_samples",
                "status": "hold",
                "detail": {
                    "sample_count": int(evaluation.get("sample_count") or 0),
                    "selection_divergence_reason": comparison["selection_divergence_reason"],
                },
            }
        ], comparison, "hold"
    if not meaningful_topk_branching_possible:
        decision = "hold"
    elif top5_status == "fail" or top10_status == "fail" or monthly_status == "fail" or zero_pass_status == "fail" or worst_regime_status == "fail" or dd_status == "fail" or turnover_status == "fail" or liquidity_status == "fail":
        decision = "drop"
    else:
        decision = "hold"
    if bool(evaluation.get("promote_ready")) and meaningful_topk_branching_possible:
        decision = "keep"
    return decision_reasons, comparison, decision


def _build_candidate_leaderboard_row(family_result: dict[str, Any], candidate_result: dict[str, Any]) -> dict[str, Any]:
    evaluation = candidate_result.get("evaluation_summary") if isinstance(candidate_result.get("evaluation_summary"), dict) else {}
    candidate_method = candidate_result.get("candidate_method") if isinstance(candidate_result.get("candidate_method"), dict) else {}
    baseline_method = candidate_result.get("baseline_method") if isinstance(candidate_result.get("baseline_method"), dict) else {}
    method_signature_hash = _leaderboard_candidate_signature_hash(candidate_result)
    decision_reasons, comparison, decision = _leaderboard_candidate_reasons(evaluation)
    champion_future_ret20_code_coverage = evaluation.get("champion_future_ret20_code_coverage") if isinstance(evaluation.get("champion_future_ret20_code_coverage"), dict) else {}
    challenger_future_ret20_code_coverage = evaluation.get("challenger_future_ret20_code_coverage") if isinstance(evaluation.get("challenger_future_ret20_code_coverage"), dict) else {}
    champion_future_ret20_source_coverage = evaluation.get("champion_future_ret20_source_coverage") if isinstance(evaluation.get("champion_future_ret20_source_coverage"), dict) else {}
    challenger_future_ret20_source_coverage = evaluation.get("challenger_future_ret20_source_coverage") if isinstance(evaluation.get("challenger_future_ret20_source_coverage"), dict) else {}
    champion_future_ret20_join_gap_coverage = evaluation.get("champion_future_ret20_join_gap_coverage") if isinstance(evaluation.get("champion_future_ret20_join_gap_coverage"), dict) else {}
    challenger_future_ret20_join_gap_coverage = evaluation.get("challenger_future_ret20_join_gap_coverage") if isinstance(evaluation.get("challenger_future_ret20_join_gap_coverage"), dict) else {}
    champion_candidate_scope_gap_coverage = evaluation.get("champion_candidate_scope_gap_coverage") if isinstance(evaluation.get("champion_candidate_scope_gap_coverage"), dict) else {}
    challenger_candidate_scope_gap_coverage = evaluation.get("challenger_candidate_scope_gap_coverage") if isinstance(evaluation.get("challenger_candidate_scope_gap_coverage"), dict) else {}
    row = {
        "family_id": _text(family_result.get("family_id")),
        "family_title": _text(family_result.get("family_title")),
        "family_thesis": _text(family_result.get("family_thesis")),
        "method_family": _text(candidate_method.get("method_family"), fallback=_text(family_result.get("method_family"))),
        "method_id": _text(candidate_method.get("method_id"), fallback=_text(candidate_result.get("plan_id"))),
        "method_title": _text(candidate_method.get("method_title"), fallback=_text(candidate_result.get("plan_id"))),
        "method_thesis": _text(candidate_method.get("method_thesis")),
        "feature_family": _text(candidate_method.get("feature_family")),
        "method_signature_hash": method_signature_hash,
        "candidate_run_id": _text(candidate_result.get("run_id"), fallback=_text(candidate_result.get("plan_id"))),
        "baseline_run_id": _text(candidate_result.get("baseline_run_id")),
        "baseline_method_title": _text(baseline_method.get("method_title")),
        "decision": decision,
        "candidate_local_decision": decision,
        "decision_reasons": decision_reasons,
        "promote_ready": bool(evaluation.get("promote_ready")),
        "promote_reasons": [str(item) for item in (evaluation.get("promote_reasons") or []) if str(item).strip()],
        "evaluation_window_count": int(evaluation.get("evaluation_window_count") or 0),
        "evaluation_window_ids": [str(item) for item in (evaluation.get("evaluation_window_ids") or []) if str(item).strip()],
        "regime_tag": _text(evaluation.get("regime_tag")),
        "artifact_detail_level": _text(evaluation.get("artifact_detail_level"), fallback=TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE),
        "fallback_status": _text(evaluation.get("fallback_status"), fallback=TRADEX_FALLBACK_STATUS_AUTHORITATIVE),
        "insufficient_samples": bool(evaluation.get("insufficient_samples")),
        "effective_universe_count": int(comparison.get("effective_universe_count") or evaluation.get("effective_universe_count") or 0),
        "top_k": int(comparison.get("top_k") or evaluation.get("top_k") or 0),
        "topk_boundary_exists": bool(comparison.get("topk_boundary_exists") if "topk_boundary_exists" in comparison else evaluation.get("topk_boundary_exists")),
        "meaningful_topk_branching_possible": bool(comparison.get("meaningful_topk_branching_possible") if "meaningful_topk_branching_possible" in comparison else evaluation.get("meaningful_topk_branching_possible")),
        "topk_branching_block_reason": _text(comparison.get("topk_branching_block_reason"), fallback=_text(evaluation.get("topk_branching_block_reason"), fallback="")),
        "victory_metrics": evaluation.get("victory_metrics") if isinstance(evaluation.get("victory_metrics"), dict) else {},
        "long_horizon_regime_score": tradex._float(evaluation.get("long_horizon_regime_score")) or 0.0,
        "recent_adaptation_score": tradex._float(evaluation.get("recent_adaptation_score")) or 0.0,
        "ret20_source_mode": _text(candidate_result.get("candidate_ret20_source_mode"), fallback=_text(evaluation.get("challenger_ret20_source_mode"), fallback="unknown")),
        "ret20_source_mode_reason": _text(candidate_result.get("candidate_ret20_source_mode_reason"), fallback=_text(evaluation.get("ret20_source_mode_reason"), fallback="unknown")),
        "champion_future_ret20_code_coverage": champion_future_ret20_code_coverage,
        "challenger_future_ret20_code_coverage": challenger_future_ret20_code_coverage,
        "champion_future_ret20_source_coverage": champion_future_ret20_source_coverage,
        "challenger_future_ret20_source_coverage": challenger_future_ret20_source_coverage,
        "champion_future_ret20_join_gap_coverage": champion_future_ret20_join_gap_coverage,
        "challenger_future_ret20_join_gap_coverage": challenger_future_ret20_join_gap_coverage,
        "champion_candidate_scope_gap_coverage": champion_candidate_scope_gap_coverage,
        "challenger_candidate_scope_gap_coverage": challenger_candidate_scope_gap_coverage,
        "future_ret20_code_coverage": challenger_future_ret20_code_coverage or champion_future_ret20_code_coverage,
        "future_ret20_source_coverage": challenger_future_ret20_source_coverage or champion_future_ret20_source_coverage,
        "future_ret20_join_gap_coverage": challenger_future_ret20_join_gap_coverage or champion_future_ret20_join_gap_coverage,
        "candidate_scope_gap_coverage": challenger_candidate_scope_gap_coverage or champion_candidate_scope_gap_coverage,
        "candidate_scope_key_mismatch_reason_counts": (challenger_candidate_scope_gap_coverage or champion_candidate_scope_gap_coverage).get("candidate_scope_key_mismatch_reason_counts")
        if isinstance((challenger_candidate_scope_gap_coverage or champion_candidate_scope_gap_coverage).get("candidate_scope_key_mismatch_reason_counts"), dict)
        else {},
        "candidate_in_scope_before_build_count": int((challenger_candidate_scope_gap_coverage or champion_candidate_scope_gap_coverage).get("candidate_in_scope_before_build_count") or 0),
        "candidate_in_scope_after_build_count": int((challenger_candidate_scope_gap_coverage or champion_candidate_scope_gap_coverage).get("candidate_in_scope_after_build_count") or 0),
        "candidate_removed_by_scope_boundary_count": int((challenger_candidate_scope_gap_coverage or champion_candidate_scope_gap_coverage).get("candidate_removed_by_scope_boundary_count") or 0),
        "scope_filter_applied_stage": _text((challenger_candidate_scope_gap_coverage or champion_candidate_scope_gap_coverage).get("scope_filter_applied_stage"), fallback="unknown"),
        "key_normalization_mode": _text((challenger_candidate_scope_gap_coverage or champion_candidate_scope_gap_coverage).get("key_normalization_mode"), fallback="unknown"),
        "changed_top5_members_count": int(comparison.get("changed_top5_members_count") or 0),
        "changed_top10_members_count": int(comparison.get("changed_top10_members_count") or 0),
        "changed_rank_count": int(comparison.get("changed_rank_count") or 0),
        "top5_boundary_score_gap": float(comparison.get("top5_boundary_score_gap") or 0.0),
        "top10_boundary_score_gap": float(comparison.get("top10_boundary_score_gap") or 0.0),
        "selection_divergence_reason": _text(comparison.get("selection_divergence_reason"), fallback="no_meaningful_branching"),
        "comparison": comparison,
    }
    return row


def _build_family_leaderboard(session_state: dict[str, Any]) -> dict[str, Any]:
    family_rows: list[dict[str, Any]] = []
    candidate_rows: list[dict[str, Any]] = []
    for family_result in session_state.get("family_results") or []:
        if not isinstance(family_result, dict):
            continue
        compare = family_result.get("compare") if isinstance(family_result.get("compare"), dict) else {}
        family_candidate_results = [item for item in (compare.get("candidate_results") or []) if isinstance(item, dict)]
        family_candidate_rows = [_build_candidate_leaderboard_row(family_result, item) for item in family_candidate_results]
        candidate_rows.extend(family_candidate_rows)
        keep_count = sum(1 for row in family_candidate_rows if row.get("decision") == "keep")
        hold_count = sum(1 for row in family_candidate_rows if row.get("decision") == "hold")
        drop_count = sum(1 for row in family_candidate_rows if row.get("decision") == "drop")
        if keep_count >= 1:
            family_decision = "keep"
            family_decision_reasons = [{"code": "candidate_keep_present", "keep_count": keep_count}]
        elif drop_count == len(family_candidate_rows) and family_candidate_rows:
            family_decision = "drop"
            family_decision_reasons = [{"code": "all_candidates_drop", "drop_count": drop_count}]
        else:
            family_decision = "hold"
            family_decision_reasons = [{"code": "additional_candidate_worth_trying", "hold_count": hold_count or 1}]
        best_candidate = next((row for row in family_candidate_rows if row.get("decision") == "keep"), None)
        if best_candidate is None and family_candidate_rows:
            best_candidate = sorted(
                family_candidate_rows,
                key=lambda row: (
                    0 if row.get("decision") == "hold" else 1,
                    -float((row.get("comparison") or {}).get("challenger_top5_ret20_mean") or 0.0),
                    -float((row.get("comparison") or {}).get("challenger_monthly_top5_capture_mean") or 0.0),
                    float((row.get("comparison") or {}).get("challenger_dd") or 0.0),
                    float((row.get("comparison") or {}).get("challenger_turnover") or 0.0),
                    _text(row.get("method_id")),
                ),
            )[0]
        family_rows.append(
            {
                "family_id": _text(family_result.get("family_id")),
                "method_family": _text(family_result.get("method_family")),
                "family_title": _text(family_result.get("family_title")),
                "family_thesis": _text(family_result.get("family_thesis")),
                "decision": family_decision,
                "session_aggregate_decision": family_decision,
                "authoritative_rollup_decision": family_decision,
                "decision_reasons": family_decision_reasons,
                "candidate_count": len(family_candidate_rows),
                "keep_count": keep_count,
                "drop_count": drop_count,
                "hold_count": hold_count,
                "hold_budget_remaining": 1 if family_decision == "hold" else 0,
                "best_candidate_method_id": _text(best_candidate.get("method_id")) if isinstance(best_candidate, dict) else None,
                "best_candidate_method_title": _text(best_candidate.get("method_title")) if isinstance(best_candidate, dict) else None,
                "best_candidate_method_thesis": _text(best_candidate.get("method_thesis")) if isinstance(best_candidate, dict) else None,
                "best_candidate_decision": _text(best_candidate.get("decision")) if isinstance(best_candidate, dict) else None,
                "best_candidate_feature_family": _text(best_candidate.get("feature_family")) if isinstance(best_candidate, dict) else None,
                "effective_universe_count": int(best_candidate.get("effective_universe_count") or 0) if isinstance(best_candidate, dict) else 0,
                "top_k": int(best_candidate.get("top_k") or 0) if isinstance(best_candidate, dict) else 0,
                "meaningful_topk_branching_possible": bool(best_candidate.get("meaningful_topk_branching_possible")) if isinstance(best_candidate, dict) else False,
                "topk_branching_block_reason": _text(best_candidate.get("topk_branching_block_reason")) if isinstance(best_candidate, dict) else "",
            }
        )
    family_rows = sorted(family_rows, key=lambda row: (_text(row.get("method_family")), _text(row.get("family_id"))))
    candidate_rows = sorted(
        candidate_rows,
        key=lambda row: (
            _text(row.get("method_family")),
            _text(row.get("family_id")),
            0 if _text(row.get("decision")) == "keep" else 1 if _text(row.get("decision")) == "hold" else 2,
            _text(row.get("method_title")),
            _text(row.get("method_id")),
        ),
    )
    coverage_waterfall = _session_coverage_summary(session_state)
    overview = {
        "family_count": len(family_rows),
        "candidate_count": len(candidate_rows),
        "keep_family_count": sum(1 for row in family_rows if row.get("decision") == "keep"),
        "hold_family_count": sum(1 for row in family_rows if row.get("decision") == "hold"),
        "drop_family_count": sum(1 for row in family_rows if row.get("decision") == "drop"),
        "keep_candidate_count": sum(1 for row in candidate_rows if row.get("decision") == "keep"),
        "hold_candidate_count": sum(1 for row in candidate_rows if row.get("decision") == "hold"),
        "drop_candidate_count": sum(1 for row in candidate_rows if row.get("decision") == "drop"),
        "insufficient_samples": bool(coverage_waterfall.get("insufficient_samples")),
    }
    return {
        "schema_version": SESSION_FAMILY_LEADERBOARD_SCHEMA_VERSION,
        "session_meta": {
            "session_id": _text(session_state.get("session_id")),
            "random_seed": int(session_state.get("random_seed") or 0),
            "generated_at": _utc_now_iso(),
            "manifest_hash": _text(session_state.get("manifest_hash")),
            "compare_schema_version": _text(session_state.get("compare_schema_version")),
            "eval_window_mode": _text(session_state.get("eval_window_mode"), fallback="unknown"),
            "eval_window_mode_reason": _text(session_state.get("eval_window_mode_reason"), fallback="unknown"),
            "ret20_source_mode": _text(session_state.get("ret20_source_mode"), fallback="unknown"),
            "ret20_source_mode_reason": _text(session_state.get("ret20_source_mode_reason"), fallback="unknown"),
            "sample_count": int(coverage_waterfall.get("sample_count") or 0),
            "insufficient_samples": bool(coverage_waterfall.get("insufficient_samples")),
            "scope_filter_applied_stage": _text(coverage_waterfall.get("scope_filter_applied_stage"), fallback="unknown"),
            "candidate_scope_gap_reason_counts": coverage_waterfall.get("candidate_scope_gap_reason_counts") if isinstance(coverage_waterfall.get("candidate_scope_gap_reason_counts"), dict) else {},
            "candidate_in_scope_before_build_count": int(coverage_waterfall.get("candidate_in_scope_before_build_count") or 0),
            "candidate_in_scope_after_build_count": int(coverage_waterfall.get("candidate_in_scope_after_build_count") or 0),
            "session_failure_reason_counts": coverage_waterfall.get("session_failure_reason_counts") if isinstance(coverage_waterfall.get("session_failure_reason_counts"), dict) else {},
        },
        "source_compare_path": str(_session_compare_file(_text(session_state.get("session_id")))),
        "source_report_path": str(_session_report_file(_text(session_state.get("session_id")))),
        "coverage_waterfall": coverage_waterfall,
        "overview": overview,
        "authoritative_rollup_decision": "keep" if overview.get("keep_family_count") else "hold" if overview.get("hold_family_count") else "drop",
        "family_summary": family_rows,
        "candidate_rows": candidate_rows,
    }


def _session_coverage_summary(session_state: dict[str, Any]) -> dict[str, Any]:
    manifest = session_state.get("manifest") if isinstance(session_state.get("manifest"), dict) else {}
    summary = session_state.get("summary") if isinstance(session_state.get("summary"), dict) else {}
    summary_future_ret20_coverage = summary.get("future_ret20_coverage") if isinstance(summary.get("future_ret20_coverage"), dict) else {}
    summary_future_ret20_source_coverage = summary.get("future_ret20_source_coverage") if isinstance(summary.get("future_ret20_source_coverage"), dict) else {}
    state_future_ret20_source_coverage = session_state.get("future_ret20_source_coverage") if isinstance(session_state.get("future_ret20_source_coverage"), dict) else {}
    family_results = [item for item in (session_state.get("family_results") or []) if isinstance(item, dict)]
    candidate_row_count = 0
    sample_counts: list[int] = []
    window_counts: list[int] = []
    eligible_candidate_count = 0
    ret20_computable_count = 0
    compare_row_count = 0
    sample_rows_retained_count = 0
    future_ret20_candidate_day_count = 0
    candidate_rows_before_future_guard = 0
    candidate_rows_after_future_guard = 0
    ret20_joinable_rows = 0
    compare_rows_emitted = 0
    sample_rows_retained = 0
    future_ret20_passed_count = 0
    future_ret20_guarded_out_count = 0
    future_ret20_failure_reason_counts: dict[str, int] = {}
    future_ret20_failure_reason_counts_by_source_mode: dict[str, dict[str, int]] = {}
    future_ret20_source_mode_counts: dict[str, int] = {"precomputed": 0, "derived_from_daily_bars": 0, "unknown": 0}
    future_ret20_source_missing_details: list[dict[str, Any]] = []
    future_ret20_missing_by_source_table: dict[str, int] = {}
    future_ret20_missing_by_code: dict[str, int] = {}
    future_ret20_missing_by_month: dict[str, int] = {}
    future_ret20_missing_near_data_end_count = 0
    future_ret20_missing_join_miss_count = 0
    future_ret20_missing_trade_sequence_shortage_count = 0
    future_ret20_candidate_guarded_by_last_valid_ret20_date_count = 0
    future_ret20_codes_with_any_candidate = 0
    future_ret20_codes_with_future_ret20_pass = 0
    future_ret20_codes_all_failed_future_ret20 = 0
    future_ret20_top_failed_codes: list[dict[str, Any]] = []
    candidate_scope_gap_reason_counts: dict[str, int] = {}
    candidate_scope_gap_examples: list[dict[str, Any]] = []
    candidate_in_scope_before_build_count = 0
    candidate_in_scope_after_build_count = 0
    candidate_removed_by_scope_boundary_count = 0
    candidate_scope_key_mismatch_reason_counts: dict[str, int] = {}
    key_normalization_mode_values: set[str] = set()
    scope_filter_applied_stage_values: set[str] = set()
    session_failure_reason_counts: dict[str, int] = {}
    future_ret20_join_gap_after_scope_filter_count = 0
    future_ret20_join_gap_reason_counts: dict[str, int] = {}
    future_ret20_join_gap_examples: list[dict[str, Any]] = []
    future_ret20_candidate_rows_before_scope_filter = 0
    future_ret20_candidate_rows_after_scope_filter = 0
    future_ret20_future_rows_before_scope_filter = 0
    future_ret20_future_rows_after_scope_filter = 0
    future_ret20_joinable_code_date_pairs_before_scope = 0
    future_ret20_joinable_code_date_pairs_after_scope = 0
    future_ret20_failure_reason_counts_by_source_mode: dict[str, dict[str, int]] = {}
    future_ret20_failure_details: list[dict[str, Any]] = []

    def add_future_ret20_coverage(payload: dict[str, Any] | None) -> None:
        nonlocal future_ret20_candidate_day_count, future_ret20_passed_count, future_ret20_guarded_out_count
        nonlocal candidate_rows_before_future_guard, candidate_rows_after_future_guard
        nonlocal ret20_joinable_rows, compare_rows_emitted, sample_rows_retained
        if not isinstance(payload, dict):
            return
        future_ret20_candidate_day_count += int(payload.get("candidate_day_count") or 0)
        candidate_rows_before_future_guard += int(payload.get("candidate_rows_before_future_guard") or 0)
        candidate_rows_after_future_guard += int(payload.get("candidate_rows_after_future_guard") or 0)
        ret20_joinable_rows += int(payload.get("ret20_joinable_rows") or 0)
        compare_rows_emitted += int(payload.get("compare_rows_emitted") or 0)
        sample_rows_retained += int(payload.get("sample_rows_retained") or 0)
        future_ret20_passed_count += int(payload.get("passed_count") or 0)
        future_ret20_guarded_out_count += int(payload.get("guarded_out_count") or 0)
        reason_counts = payload.get("failure_reason_counts")
        if isinstance(reason_counts, dict):
            for reason, count in reason_counts.items():
                key = _text(reason)
                if not key:
                    continue
                future_ret20_failure_reason_counts[key] = future_ret20_failure_reason_counts.get(key, 0) + int(count or 0)
        reason_counts_by_mode = payload.get("failure_reason_counts_by_source_mode")
        if isinstance(reason_counts_by_mode, dict):
            for source_mode, nested in reason_counts_by_mode.items():
                source_mode_key = _text(source_mode, fallback="unknown")
                if not isinstance(nested, dict):
                    continue
                source_bucket = future_ret20_failure_reason_counts_by_source_mode.setdefault(source_mode_key, {})
                for reason, count in nested.items():
                    key = _text(reason)
                    if not key:
                        continue
                    source_bucket[key] = source_bucket.get(key, 0) + int(count or 0)
        details = payload.get("failure_details")
        if isinstance(details, list):
            for item in details:
                if isinstance(item, dict) and len(future_ret20_failure_details) < 200:
                    future_ret20_failure_details.append(item)

    def add_future_ret20_source_coverage(payload: dict[str, Any] | None) -> None:
        nonlocal future_ret20_missing_near_data_end_count, future_ret20_missing_join_miss_count, future_ret20_missing_trade_sequence_shortage_count
        if not isinstance(payload, dict):
            return
        mode = _text(payload.get("ret20_source_mode"), fallback="unknown")
        future_ret20_source_mode_counts[mode if mode in {"precomputed", "derived_from_daily_bars"} else "unknown"] += 1
        source_table_counts = payload.get("missing_by_source_table") if isinstance(payload.get("missing_by_source_table"), dict) else {}
        for key, count in source_table_counts.items():
            text_key = _text(key)
            if not text_key:
                continue
            future_ret20_missing_by_source_table[text_key] = future_ret20_missing_by_source_table.get(text_key, 0) + int(count or 0)
        code_counts = payload.get("missing_by_code") if isinstance(payload.get("missing_by_code"), dict) else {}
        for key, count in code_counts.items():
            text_key = _text(key)
            if not text_key:
                continue
            future_ret20_missing_by_code[text_key] = future_ret20_missing_by_code.get(text_key, 0) + int(count or 0)
        month_counts = payload.get("missing_by_month") if isinstance(payload.get("missing_by_month"), dict) else {}
        for key, count in month_counts.items():
            text_key = _text(key)
            if not text_key:
                continue
            future_ret20_missing_by_month[text_key] = future_ret20_missing_by_month.get(text_key, 0) + int(count or 0)
        future_ret20_missing_near_data_end_count += int(payload.get("missing_near_data_end_count") or 0)
        future_ret20_missing_join_miss_count += int(payload.get("missing_join_miss_count") or 0)
        future_ret20_missing_trade_sequence_shortage_count += int(payload.get("missing_trade_sequence_shortage_count") or 0)
        examples = payload.get("missing_examples")
        if isinstance(examples, list):
            for item in examples:
                if isinstance(item, dict) and len(future_ret20_source_missing_details) < 100:
                    future_ret20_source_missing_details.append(item)

    def add_future_ret20_join_gap_coverage(payload: dict[str, Any] | None) -> None:
        nonlocal future_ret20_join_gap_after_scope_filter_count, future_ret20_candidate_rows_before_scope_filter, future_ret20_candidate_rows_after_scope_filter
        nonlocal future_ret20_future_rows_before_scope_filter, future_ret20_future_rows_after_scope_filter
        nonlocal future_ret20_joinable_code_date_pairs_before_scope, future_ret20_joinable_code_date_pairs_after_scope
        if not isinstance(payload, dict):
            return
        future_ret20_join_gap_after_scope_filter_count += int(payload.get("after_scope_filter_count") or 0)
        future_ret20_candidate_rows_before_scope_filter += int(payload.get("candidate_rows_before_scope_filter") or 0)
        future_ret20_candidate_rows_after_scope_filter += int(payload.get("candidate_rows_after_scope_filter") or 0)
        future_ret20_future_rows_before_scope_filter += int(payload.get("future_rows_before_scope_filter") or 0)
        future_ret20_future_rows_after_scope_filter += int(payload.get("future_rows_after_scope_filter") or 0)
        future_ret20_joinable_code_date_pairs_before_scope += int(payload.get("joinable_code_date_pairs_before_scope") or 0)
        future_ret20_joinable_code_date_pairs_after_scope += int(payload.get("joinable_code_date_pairs_after_scope") or 0)
        reason_counts = payload.get("reason_counts") if isinstance(payload.get("reason_counts"), dict) else {}
        for reason, count in reason_counts.items():
            key = _text(reason)
            if not key:
                continue
            future_ret20_join_gap_reason_counts[key] = future_ret20_join_gap_reason_counts.get(key, 0) + int(count or 0)
        examples = payload.get("examples")
        if isinstance(examples, list):
            for item in examples:
                if isinstance(item, dict) and len(future_ret20_join_gap_examples) < 100:
                    future_ret20_join_gap_examples.append(item)

    def add_candidate_scope_gap_coverage(payload: dict[str, Any] | None) -> None:
        nonlocal candidate_in_scope_before_build_count, candidate_in_scope_after_build_count, candidate_removed_by_scope_boundary_count
        if not isinstance(payload, dict):
            return
        candidate_in_scope_before_build_count += int(payload.get("candidate_in_scope_before_build_count") or 0)
        candidate_in_scope_after_build_count += int(payload.get("candidate_in_scope_after_build_count") or 0)
        candidate_removed_by_scope_boundary_count += int(payload.get("candidate_removed_by_scope_boundary_count") or 0)
        stage = _text(payload.get("scope_filter_applied_stage"), fallback="")
        if stage:
            scope_filter_applied_stage_values.add(stage)
        key_mode = _text(payload.get("key_normalization_mode"), fallback="")
        if key_mode:
            key_normalization_mode_values.add(key_mode)
        reason_counts = payload.get("candidate_scope_key_mismatch_reason_counts")
        if not isinstance(reason_counts, dict):
            reason_counts = payload.get("candidate_scope_gap_reason_counts") if isinstance(payload.get("candidate_scope_gap_reason_counts"), dict) else {}
        for reason, count in reason_counts.items():
            key = _text(reason)
            if not key:
                continue
            candidate_scope_gap_reason_counts[key] = candidate_scope_gap_reason_counts.get(key, 0) + int(count or 0)
            candidate_scope_key_mismatch_reason_counts[key] = candidate_scope_key_mismatch_reason_counts.get(key, 0) + int(count or 0)
        examples = payload.get("candidate_scope_gap_examples")
        if isinstance(examples, list):
            for item in examples:
                if isinstance(item, dict) and len(candidate_scope_gap_examples) < 100:
                    candidate_scope_gap_examples.append(item)

    def add_session_failure_reason(payload: dict[str, Any] | None) -> None:
        if not isinstance(payload, dict):
            return
        reason = _text(payload.get("session_failure_reason"), fallback="")
        if reason:
            session_failure_reason_counts[reason] = session_failure_reason_counts.get(reason, 0) + 1

    def add_future_ret20_code_coverage(payload: dict[str, Any] | None) -> None:
        nonlocal future_ret20_candidate_guarded_by_last_valid_ret20_date_count, future_ret20_codes_with_any_candidate, future_ret20_codes_with_future_ret20_pass, future_ret20_codes_all_failed_future_ret20, future_ret20_top_failed_codes
        if not isinstance(payload, dict):
            return
        future_ret20_candidate_guarded_by_last_valid_ret20_date_count += int(payload.get("candidate_guarded_by_last_valid_ret20_date_count") or 0)
        future_ret20_codes_with_any_candidate += int(payload.get("codes_with_any_candidate") or 0)
        future_ret20_codes_with_future_ret20_pass += int(payload.get("codes_with_future_ret20_pass") or 0)
        future_ret20_codes_all_failed_future_ret20 += int(payload.get("codes_all_failed_future_ret20") or 0)
        top_failed_codes = payload.get("top_failed_codes") if isinstance(payload.get("top_failed_codes"), list) else []
        if top_failed_codes:
            future_ret20_top_failed_codes = [item for item in top_failed_codes if isinstance(item, dict)][:25]

    for family_result in family_results:
        compare = family_result.get("compare") if isinstance(family_result.get("compare"), dict) else {}
        candidate_results = [item for item in (compare.get("candidate_results") or []) if isinstance(item, dict)]
        candidate_row_count += len(candidate_results)
        compare_row_count += len(candidate_results)
        for candidate_result in candidate_results:
            evaluation = candidate_result.get("evaluation_summary") if isinstance(candidate_result.get("evaluation_summary"), dict) else {}
            comparison = candidate_result.get("selection_compare") if isinstance(candidate_result.get("selection_compare"), dict) else {}
            future_ret20_sources = [
                evaluation.get("future_ret20_coverage") if isinstance(evaluation.get("future_ret20_coverage"), dict) else None,
                evaluation.get("challenger_future_ret20_coverage") if isinstance(evaluation.get("challenger_future_ret20_coverage"), dict) else None,
                evaluation.get("champion_future_ret20_coverage") if isinstance(evaluation.get("champion_future_ret20_coverage"), dict) else None,
                comparison.get("future_ret20_coverage") if isinstance(comparison.get("future_ret20_coverage"), dict) else None,
            ]
            selected_future_ret20 = max(
                (item for item in future_ret20_sources if isinstance(item, dict)),
                key=lambda item: int(item.get("candidate_day_count") or 0),
                default=None,
            )
            add_future_ret20_coverage(selected_future_ret20)
            if isinstance(evaluation.get("future_ret20_source_coverage"), dict):
                add_future_ret20_source_coverage(evaluation.get("future_ret20_source_coverage"))
            if isinstance(comparison.get("future_ret20_source_coverage"), dict):
                add_future_ret20_source_coverage(comparison.get("future_ret20_source_coverage"))
            if isinstance(evaluation.get("future_ret20_join_gap_coverage"), dict):
                add_future_ret20_join_gap_coverage(evaluation.get("future_ret20_join_gap_coverage"))
            if isinstance(evaluation.get("champion_future_ret20_join_gap_coverage"), dict):
                add_future_ret20_join_gap_coverage(evaluation.get("champion_future_ret20_join_gap_coverage"))
            if isinstance(evaluation.get("challenger_future_ret20_join_gap_coverage"), dict):
                add_future_ret20_join_gap_coverage(evaluation.get("challenger_future_ret20_join_gap_coverage"))
            if isinstance(comparison.get("future_ret20_join_gap_coverage"), dict):
                add_future_ret20_join_gap_coverage(comparison.get("future_ret20_join_gap_coverage"))
            if isinstance(evaluation.get("future_ret20_code_coverage"), dict):
                add_future_ret20_code_coverage(evaluation.get("future_ret20_code_coverage"))
            if isinstance(evaluation.get("champion_future_ret20_code_coverage"), dict):
                add_future_ret20_code_coverage(evaluation.get("champion_future_ret20_code_coverage"))
            if isinstance(evaluation.get("challenger_future_ret20_code_coverage"), dict):
                add_future_ret20_code_coverage(evaluation.get("challenger_future_ret20_code_coverage"))
            if isinstance(comparison.get("future_ret20_code_coverage"), dict):
                add_future_ret20_code_coverage(comparison.get("future_ret20_code_coverage"))
            if isinstance(evaluation.get("candidate_scope_gap_coverage"), dict):
                add_candidate_scope_gap_coverage(evaluation.get("candidate_scope_gap_coverage"))
            if isinstance(evaluation.get("champion_candidate_scope_gap_coverage"), dict):
                add_candidate_scope_gap_coverage(evaluation.get("champion_candidate_scope_gap_coverage"))
            if isinstance(evaluation.get("challenger_candidate_scope_gap_coverage"), dict):
                add_candidate_scope_gap_coverage(evaluation.get("challenger_candidate_scope_gap_coverage"))
            if isinstance(comparison.get("candidate_scope_gap_coverage"), dict):
                add_candidate_scope_gap_coverage(comparison.get("candidate_scope_gap_coverage"))
            champion_summary = comparison.get("champion_selection_summary") if isinstance(comparison.get("champion_selection_summary"), dict) else evaluation.get("champion_selection_summary") if isinstance(evaluation.get("champion_selection_summary"), dict) else {}
            challenger_summary = comparison.get("challenger_selection_summary") if isinstance(comparison.get("challenger_selection_summary"), dict) else evaluation.get("challenger_selection_summary") if isinstance(evaluation.get("challenger_selection_summary"), dict) else {}
            champion_sample_count = max(
                int(champion_summary.get("sample_count") or 0),
                int(comparison.get("champion_sample_count") or 0),
            )
            challenger_sample_count = max(
                int(challenger_summary.get("sample_count") or 0),
                int(comparison.get("challenger_sample_count") or 0),
            )
            sample_count = max(champion_sample_count, challenger_sample_count)
            sample_counts.append(sample_count)
            if bool(comparison.get("promote_ready")) or bool(evaluation.get("promote_ready")):
                eligible_candidate_count += 1
            if sample_count > 0:
                ret20_computable_count += 1
                sample_rows_retained_count += 1
            windows = evaluation.get("windows") if isinstance(evaluation.get("windows"), list) else []
            window_counts.append(len(windows))
    if not future_ret20_candidate_day_count and isinstance(summary_future_ret20_coverage, dict):
        future_ret20_candidate_day_count = int(summary_future_ret20_coverage.get("candidate_day_count") or 0)
        candidate_rows_before_future_guard = int(summary_future_ret20_coverage.get("candidate_rows_before_future_guard") or 0)
        candidate_rows_after_future_guard = int(summary_future_ret20_coverage.get("candidate_rows_after_future_guard") or 0)
        ret20_joinable_rows = int(summary_future_ret20_coverage.get("ret20_joinable_rows") or 0)
        compare_rows_emitted = int(summary_future_ret20_coverage.get("compare_rows_emitted") or 0)
        sample_rows_retained = int(summary_future_ret20_coverage.get("sample_rows_retained") or 0)
        future_ret20_passed_count = int(summary_future_ret20_coverage.get("passed_count") or 0)
        future_ret20_guarded_out_count = int(summary_future_ret20_coverage.get("guarded_out_count") or 0)
        reason_counts = summary_future_ret20_coverage.get("failure_reason_counts")
        if isinstance(reason_counts, dict):
            future_ret20_failure_reason_counts = { _text(reason): int(count or 0) for reason, count in reason_counts.items() if _text(reason) }
        reason_counts_by_mode = summary_future_ret20_coverage.get("failure_reason_counts_by_source_mode")
        if isinstance(reason_counts_by_mode, dict):
            for source_mode, nested in reason_counts_by_mode.items():
                source_mode_key = _text(source_mode, fallback="unknown")
                if not isinstance(nested, dict):
                    continue
                bucket = future_ret20_failure_reason_counts_by_source_mode.setdefault(source_mode_key, {})
                for reason, count in nested.items():
                    key = _text(reason)
                    if key:
                        bucket[key] = bucket.get(key, 0) + int(count or 0)
        details = summary_future_ret20_coverage.get("failure_details")
        if isinstance(details, list):
            for item in details:
                if isinstance(item, dict) and len(future_ret20_failure_details) < 200:
                    future_ret20_failure_details.append(item)
    if isinstance(summary_future_ret20_source_coverage, dict):
        add_future_ret20_source_coverage(summary_future_ret20_source_coverage)
    if isinstance(state_future_ret20_source_coverage, dict):
        add_future_ret20_source_coverage(state_future_ret20_source_coverage)
    if isinstance(summary.get("candidate_scope_gap_coverage"), dict):
        add_candidate_scope_gap_coverage(summary.get("candidate_scope_gap_coverage"))
    if isinstance(session_state.get("candidate_scope_gap_coverage"), dict):
        add_candidate_scope_gap_coverage(session_state.get("candidate_scope_gap_coverage"))
    if isinstance(summary.get("session_failure_reason_counts"), dict):
        for reason, count in summary.get("session_failure_reason_counts", {}).items():
            key = _text(reason)
            if key:
                session_failure_reason_counts[key] = session_failure_reason_counts.get(key, 0) + int(count or 0)
    if isinstance(session_state.get("session_failure_reason_counts"), dict):
        for reason, count in session_state.get("session_failure_reason_counts", {}).items():
            key = _text(reason)
            if key:
                session_failure_reason_counts[key] = session_failure_reason_counts.get(key, 0) + int(count or 0)
    if not future_ret20_failure_reason_counts_by_source_mode and future_ret20_failure_reason_counts:
        source_mode = _text(
            summary_future_ret20_source_coverage.get("ret20_source_mode"),
            fallback=_text(session_state.get("ret20_source_mode"), fallback="unknown"),
        )
        future_ret20_failure_reason_counts_by_source_mode[source_mode] = dict(sorted(future_ret20_failure_reason_counts.items(), key=lambda item: (-item[1], item[0])))
    if scope_filter_applied_stage_values and len(scope_filter_applied_stage_values) == 1:
        scope_filter_applied_stage = next(iter(scope_filter_applied_stage_values))
    elif len(scope_filter_applied_stage_values) > 1:
        scope_filter_applied_stage = "mixed"
    else:
        scope_filter_applied_stage = "unknown"
    key_normalization_mode = "mixed" if len(key_normalization_mode_values) > 1 else (next(iter(key_normalization_mode_values)) if key_normalization_mode_values else "unknown")
    confirmed_universe_count = len([item for item in (manifest.get("universe") or []) if _text(item)])
    planned_probe_count = sum(
        len([candidate_id for candidate_id in (family_spec.get("candidate_order") or []) if _text(candidate_id)])
        for family_spec in (manifest.get("method_families") or [])
        if isinstance(family_spec, dict)
    )
    regime_window_count = len([item for item in (manifest.get("period_segments") or []) if isinstance(item, dict)])
    evaluation_row_count = max(sample_counts) if sample_counts else 0
    stage_counts = {
        "confirmed_universe": confirmed_universe_count,
        "probe_selection": planned_probe_count,
        "candidate_rows_built": candidate_row_count,
        "eligibility_passed": eligible_candidate_count,
        "future_ret20_computable": ret20_computable_count,
        "compare_rows_emitted": compare_row_count,
        "sample_rows_retained": sample_rows_retained_count,
        "candidate_rows_before_future_guard": candidate_rows_before_future_guard,
        "candidate_rows_after_future_guard": candidate_rows_after_future_guard,
        "ret20_joinable_rows": ret20_joinable_rows,
    }
    stage_order = [
        "confirmed_universe",
        "probe_selection",
        "candidate_rows_built",
        "eligibility_passed",
        "future_ret20_computable",
        "candidate_rows_before_future_guard",
        "candidate_rows_after_future_guard",
        "ret20_joinable_rows",
        "compare_rows_emitted",
        "sample_rows_retained",
    ]
    if confirmed_universe_count <= 0:
        first_zero_stage = "confirmed_universe"
    elif planned_probe_count <= 0:
        first_zero_stage = "probe_selection"
    elif candidate_row_count <= 0:
        first_zero_stage = "candidate_rows_built"
    elif eligible_candidate_count <= 0:
        first_zero_stage = "eligibility_passed"
    elif ret20_computable_count <= 0:
        first_zero_stage = "future_ret20_computable"
    elif compare_row_count <= 0:
        first_zero_stage = "compare_rows_emitted"
    elif sample_rows_retained_count <= 0:
        first_zero_stage = "sample_rows_retained"
    else:
        first_zero_stage = "passed"
    return {
        "confirmed_universe_count": confirmed_universe_count,
        "probe_candidate_count": candidate_row_count,
        "probe_selection_count": planned_probe_count,
        "candidate_rows_built_count": candidate_row_count,
        "eligible_candidate_count": eligible_candidate_count,
        "ret20_computable_count": ret20_computable_count,
        "compare_row_count": compare_row_count,
        "sample_rows_retained_count": sample_rows_retained_count,
        "future_ret20_candidate_day_count": future_ret20_candidate_day_count,
        "candidate_rows_before_future_guard": candidate_rows_before_future_guard,
        "candidate_rows_after_future_guard": candidate_rows_after_future_guard,
        "ret20_joinable_rows": ret20_joinable_rows,
        "compare_rows_emitted": compare_rows_emitted,
        "sample_rows_retained": sample_rows_retained,
        "future_ret20_passed_count": future_ret20_passed_count,
        "future_ret20_guarded_out_count": future_ret20_guarded_out_count,
        "future_ret20_failure_reason_counts": future_ret20_failure_reason_counts,
        "future_ret20_failure_reason_counts_by_source_mode": future_ret20_failure_reason_counts_by_source_mode,
        "future_ret20_failure_details": future_ret20_failure_details,
        "future_ret20_source_mode_counts": future_ret20_source_mode_counts,
        "future_ret20_source_coverage": {
            "ret20_source_mode": _text(session_state.get("ret20_source_mode"), fallback=_text(summary_future_ret20_source_coverage.get("ret20_source_mode"), fallback="unknown")),
            "missing_by_source_table": dict(sorted(future_ret20_missing_by_source_table.items(), key=lambda item: (-item[1], item[0]))),
            "missing_by_code": dict(sorted(future_ret20_missing_by_code.items(), key=lambda item: (-item[1], item[0]))),
            "missing_by_month": dict(sorted(future_ret20_missing_by_month.items(), key=lambda item: (-item[1], item[0]))),
            "missing_near_data_end_count": future_ret20_missing_near_data_end_count,
            "missing_join_miss_count": future_ret20_missing_join_miss_count,
            "missing_trade_sequence_shortage_count": future_ret20_missing_trade_sequence_shortage_count,
            "missing_examples": list(future_ret20_source_missing_details[:25]),
            "mixed_source_mode": sum(1 for count in future_ret20_source_mode_counts.values() if count) > 1,
        },
        "candidate_scope_gap_coverage": {
            "scope_filter_applied_stage": scope_filter_applied_stage,
            "candidate_in_scope_before_build_count": candidate_in_scope_before_build_count,
            "candidate_in_scope_after_build_count": candidate_in_scope_after_build_count,
            "candidate_removed_by_scope_boundary_count": candidate_removed_by_scope_boundary_count,
            "candidate_scope_key_mismatch_reason_counts": dict(sorted(candidate_scope_key_mismatch_reason_counts.items(), key=lambda item: (-item[1], item[0]))),
            "candidate_scope_gap_reason_counts": dict(sorted(candidate_scope_gap_reason_counts.items(), key=lambda item: (-item[1], item[0]))),
            "candidate_scope_gap_examples": list(candidate_scope_gap_examples[:25]),
            "candidate_scope_gap_count": sum(candidate_scope_gap_reason_counts.values()),
            "key_normalization_mode": key_normalization_mode,
        },
        "future_ret20_join_gap_coverage": {
            "after_scope_filter_count": future_ret20_join_gap_after_scope_filter_count,
            "reason_counts": dict(sorted(future_ret20_join_gap_reason_counts.items(), key=lambda item: (-item[1], item[0]))),
            "examples": list(future_ret20_join_gap_examples[:25]),
            "candidate_rows_before_scope_filter": future_ret20_candidate_rows_before_scope_filter,
            "candidate_rows_after_scope_filter": future_ret20_candidate_rows_after_scope_filter,
            "future_rows_before_scope_filter": future_ret20_future_rows_before_scope_filter,
            "future_rows_after_scope_filter": future_ret20_future_rows_after_scope_filter,
            "joinable_code_date_pairs_before_scope": future_ret20_joinable_code_date_pairs_before_scope,
            "joinable_code_date_pairs_after_scope": future_ret20_joinable_code_date_pairs_after_scope,
        },
        "future_ret20_code_coverage": {
            "candidate_guarded_by_last_valid_ret20_date_count": future_ret20_candidate_guarded_by_last_valid_ret20_date_count,
            "codes_with_any_candidate": future_ret20_codes_with_any_candidate,
            "codes_with_future_ret20_pass": future_ret20_codes_with_future_ret20_pass,
            "codes_all_failed_future_ret20": future_ret20_codes_all_failed_future_ret20,
            "top_failed_codes": future_ret20_top_failed_codes,
        },
        "future_ret20_candidate_guarded_by_last_valid_ret20_date_count": future_ret20_candidate_guarded_by_last_valid_ret20_date_count,
        "future_ret20_codes_with_any_candidate": future_ret20_codes_with_any_candidate,
        "future_ret20_codes_with_future_ret20_pass": future_ret20_codes_with_future_ret20_pass,
        "future_ret20_codes_all_failed_future_ret20": future_ret20_codes_all_failed_future_ret20,
        "future_ret20_top_failed_codes": future_ret20_top_failed_codes,
        "candidate_scope_gap_reason_counts": dict(sorted(candidate_scope_gap_reason_counts.items(), key=lambda item: (-item[1], item[0]))),
        "candidate_in_scope_before_build_count": candidate_in_scope_before_build_count,
        "candidate_in_scope_after_build_count": candidate_in_scope_after_build_count,
        "session_failure_reason_counts": dict(sorted(session_failure_reason_counts.items(), key=lambda item: (-item[1], item[0]))),
        "scope_filter_applied_stage": scope_filter_applied_stage,
        "regime_window_count": regime_window_count,
        "evaluation_row_count": evaluation_row_count,
        "sample_count": evaluation_row_count,
        "sample_count_min": min(sample_counts) if sample_counts else 0,
        "sample_count_max": max(sample_counts) if sample_counts else 0,
        "window_count_min": min(window_counts) if window_counts else 0,
        "window_count_max": max(window_counts) if window_counts else 0,
        "stage_counts": stage_counts,
        "stage_order": stage_order,
        "first_zero_stage": first_zero_stage,
        "failure_stage": first_zero_stage,
        "insufficient_samples": evaluation_row_count <= 0 or sum(1 for count in future_ret20_source_mode_counts.values() if count) > 1,
    }


def _format_family_leaderboard_markdown(leaderboard: dict[str, Any]) -> str:
    session_meta = leaderboard.get("session_meta") if isinstance(leaderboard.get("session_meta"), dict) else {}
    overview = leaderboard.get("overview") if isinstance(leaderboard.get("overview"), dict) else {}
    family_rows = leaderboard.get("family_summary") if isinstance(leaderboard.get("family_summary"), list) else []
    candidate_rows = leaderboard.get("candidate_rows") if isinstance(leaderboard.get("candidate_rows"), list) else []
    lines: list[str] = []
    lines.append("# TRADEX Family Leaderboard")
    lines.append("")
    lines.append(f"- session_id: `{_text(session_meta.get('session_id'))}`")
    lines.append(f"- random_seed: `{_text(session_meta.get('random_seed'))}`")
    lines.append(f"- generated_at: `{_text(session_meta.get('generated_at'))}`")
    lines.append(f"- eval_window_mode: `{_text(session_meta.get('eval_window_mode'), fallback='unknown')}`")
    lines.append(f"- eval_window_mode_reason: `{_text(session_meta.get('eval_window_mode_reason'), fallback='unknown')}`")
    lines.append(f"- ret20_source_mode: `{_text(session_meta.get('ret20_source_mode'), fallback='unknown')}`")
    lines.append(f"- ret20_source_mode_reason: `{_text(session_meta.get('ret20_source_mode_reason'), fallback='unknown')}`")
    lines.append(f"- scope_filter_applied_stage: `{_text(session_meta.get('scope_filter_applied_stage'), fallback='unknown')}`")
    lines.append(
        "- future_ret20 stage counts: before_guard=`{before}` / after_guard=`{after}` / joinable=`{joinable}` / compare_emitted=`{compare}` / retained=`{retained}`".format(
            before=int(session_meta.get("candidate_rows_before_future_guard") or 0),
            after=int(session_meta.get("candidate_rows_after_future_guard") or 0),
            joinable=int(session_meta.get("ret20_joinable_rows") or 0),
            compare=int(session_meta.get("compare_rows_emitted") or 0),
            retained=int(session_meta.get("sample_rows_retained") or 0),
        )
    )
    candidate_gap_counts = session_meta.get("candidate_scope_gap_reason_counts") if isinstance(session_meta.get("candidate_scope_gap_reason_counts"), dict) else {}
    if candidate_gap_counts:
        lines.append(f"- candidate_scope_gap_reason_counts: `{json.dumps(_json_ready(candidate_gap_counts), ensure_ascii=False, sort_keys=True)}`")
    session_failure_counts = session_meta.get("session_failure_reason_counts") if isinstance(session_meta.get("session_failure_reason_counts"), dict) else {}
    if session_failure_counts:
        lines.append(f"- session_failure_reason_counts: `{json.dumps(_json_ready(session_failure_counts), ensure_ascii=False, sort_keys=True)}`")
    future_ret20_counts_by_mode = session_meta.get("future_ret20_failure_reason_counts_by_source_mode") if isinstance(session_meta.get("future_ret20_failure_reason_counts_by_source_mode"), dict) else {}
    if future_ret20_counts_by_mode:
        lines.append(f"- future_ret20_failure_reason_counts_by_source_mode: `{json.dumps(_json_ready(future_ret20_counts_by_mode), ensure_ascii=False, sort_keys=True)}`")
    lines.append(f"- source_compare_path: `{_text(leaderboard.get('source_compare_path'))}`")
    lines.append(f"- source_report_path: `{_text(leaderboard.get('source_report_path'))}`")
    lines.append("")
    lines.append("## Overview")
    lines.append("")
    lines.append("| families | keep | hold | drop | candidates |")
    lines.append("| ---: | ---: | ---: | ---: | ---: |")
    lines.append(
        "| {family_count} | {keep_family_count} | {hold_family_count} | {drop_family_count} | {candidate_count} |".format(
            family_count=int(overview.get("family_count") or 0),
            keep_family_count=int(overview.get("keep_family_count") or 0),
            hold_family_count=int(overview.get("hold_family_count") or 0),
            drop_family_count=int(overview.get("drop_family_count") or 0),
            candidate_count=int(overview.get("candidate_count") or 0),
        )
    )
    if bool(overview.get("insufficient_samples")):
        lines.append("")
        lines.append("- validity: `invalid (insufficient_samples)`")
    lines.append("")
    lines.append("## Family Summary")
    lines.append("")
    lines.append("| family | decision | keep | hold | drop | best method |")
    lines.append("| --- | --- | ---: | ---: | ---: | --- |")
    for family in family_rows:
        if not isinstance(family, dict):
            continue
        lines.append(
            "| {family} | {decision} | {keep} | {hold} | {drop} | {method} |".format(
                family=_text(family.get("method_family")),
                decision=_text(family.get("decision")),
                keep=int(family.get("keep_count") or 0),
                hold=int(family.get("hold_count") or 0),
                drop=int(family.get("drop_count") or 0),
                method=_text(family.get("best_candidate_method_title"), fallback=_text(family.get("best_candidate_method_id"))),
            )
        )
        reasons = family.get("decision_reasons") if isinstance(family.get("decision_reasons"), list) else []
        if reasons:
            lines.append(f"- `{_text(family.get('method_family'))}` decision_reasons: `{json.dumps(_json_ready(reasons), ensure_ascii=False)}`")
    lines.append("")
    lines.append("## Candidate Rows")
    lines.append("")
    lines.append("| family | candidate | decision | ret20 mode | top5 | top10 | monthly capture | zero-pass | worst regime | dd | turnover | liquidity fail | reasons |")
    lines.append("| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |")
    for row in candidate_rows:
        if not isinstance(row, dict):
            continue
        comparison = row.get("comparison") if isinstance(row.get("comparison"), dict) else {}
        reasons = row.get("decision_reasons") if isinstance(row.get("decision_reasons"), list) else []
        reason_text = ", ".join(
            f"{_text(reason.get('code'))}:{_text(reason.get('status'))}"
            for reason in reasons
            if isinstance(reason, dict)
        )
        lines.append(
            "| {family} | {candidate} | {decision} | {ret20_mode} | {top5:.4f} | {top10:.4f} | {capture:.4f} | {zero_pass} | {worst:.4f} | {dd:.4f} | {turnover:.4f} | {liquidity:.4f} | {reasons} |".format(
                family=_text(row.get("method_family")),
                candidate=_text(row.get("method_title")),
                decision=_text(row.get("decision")),
                ret20_mode=_text(row.get("ret20_source_mode"), fallback="unknown"),
                top5=float(comparison.get("challenger_top5_ret20_mean") or 0.0),
                top10=float(comparison.get("challenger_top10_ret20_mean") or 0.0),
                capture=float(comparison.get("challenger_monthly_top5_capture_mean") or 0.0),
                zero_pass=int(comparison.get("challenger_zero_pass_months") or 0),
                worst=float(comparison.get("challenger_worst_regime_ret20_mean") or 0.0),
                dd=float(comparison.get("challenger_dd") or 0.0),
                turnover=float(comparison.get("challenger_turnover") or 0.0),
                liquidity=float(comparison.get("challenger_liquidity_fail_rate") or 0.0),
                reasons=reason_text or "none",
            )
        )
        candidate_scope_gap = row.get("candidate_scope_gap_coverage") if isinstance(row.get("candidate_scope_gap_coverage"), dict) else {}
        if candidate_scope_gap:
            lines.append(f"- scope_filter_applied_stage: `{_text(candidate_scope_gap.get('scope_filter_applied_stage'), fallback='unknown')}`")
            lines.append(f"- key_normalization_mode: `{_text(candidate_scope_gap.get('key_normalization_mode'), fallback='unknown')}`")
            lines.append(f"- candidate_in_scope_before_build_count: `{int(candidate_scope_gap.get('candidate_in_scope_before_build_count') or 0)}`")
            lines.append(f"- candidate_in_scope_after_build_count: `{int(candidate_scope_gap.get('candidate_in_scope_after_build_count') or 0)}`")
            lines.append(f"- candidate_removed_by_scope_boundary_count: `{int(candidate_scope_gap.get('candidate_removed_by_scope_boundary_count') or 0)}`")
            lines.append(
                f"- candidate_scope_key_mismatch_reason_counts: `{json.dumps(_json_ready(candidate_scope_gap.get('candidate_scope_key_mismatch_reason_counts') or {}), ensure_ascii=False, sort_keys=True)}`"
            )
            lines.append(f"- candidate_scope_gap_reason_counts: `{json.dumps(_json_ready(candidate_scope_gap.get('candidate_scope_gap_reason_counts') or {}), ensure_ascii=False, sort_keys=True)}`")
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- compare artifact が正本で、markdown report は派生物")
    lines.append("- decision は `keep / drop / hold` のみ")
    lines.append("- hold は追加 1 候補だけ試す余地を残す暫定状態")
    lines.append("- MeeMee にはまだ接続しない")
    lines.append(f"- legacy analysis env must be `0` for research runs (`{LEGACY_ANALYSIS_DISABLE_ENV}`)")
    return "\n".join(lines).rstrip() + "\n"


def _write_family_leaderboard_artifacts(session_state: dict[str, Any]) -> tuple[Path, Path, dict[str, Any]]:
    session_id = _text(session_state.get("session_id"))
    if not session_id:
        raise RuntimeError("session_id is required for leaderboard generation")
    leaderboard = _build_family_leaderboard(session_state)
    validate_family_leaderboard_artifact(leaderboard)
    leaderboard_path = _session_family_leaderboard_file(session_id)
    report_path = _session_family_leaderboard_report_file(session_id)
    _write_json(leaderboard_path, leaderboard)
    report_path.write_text(_format_family_leaderboard_markdown(leaderboard), encoding="utf-8")
    return leaderboard_path, report_path, leaderboard


def _ensure_session_report(session_state: dict[str, Any], *, force: bool = False) -> Path:
    session_id = _text(session_state.get("session_id"))
    if not session_id:
        raise RuntimeError("session_id is required for session report generation")
    report_path = _session_report_file(session_id)
    if not force and report_path.exists():
        return report_path
    report_path.write_text(_render_session_report(session_state), encoding="utf-8")
    return report_path


def _ensure_family_leaderboard_artifacts(
    session_state: dict[str, Any],
    *,
    force: bool = False,
) -> tuple[Path, Path, dict[str, Any]]:
    session_id = _text(session_state.get("session_id"))
    if not session_id:
        raise RuntimeError("session_id is required for leaderboard generation")
    leaderboard_path = _session_family_leaderboard_file(session_id)
    report_path = _session_family_leaderboard_report_file(session_id)
    if not force and leaderboard_path.exists() and report_path.exists():
        return leaderboard_path, report_path, {}
    existing_payload = _read_json_file(leaderboard_path) if leaderboard_path.exists() else None

    leaderboard = existing_payload if isinstance(existing_payload, dict) else _build_family_leaderboard(session_state)
    validate_family_leaderboard_artifact(leaderboard)
    if force or existing_payload is None:
        _write_json(leaderboard_path, leaderboard)
    if force or not report_path.exists():
        report_path.write_text(_format_family_leaderboard_markdown(leaderboard), encoding="utf-8")
    return leaderboard_path, report_path, leaderboard


def _leaderboard_average(total: float, count: int) -> float:
    if count <= 0:
        return 0.0
    return total / float(count)


def _leaderboard_family_decision(candidate_rows: list[dict[str, Any]]) -> tuple[str, list[dict[str, Any]]]:
    if candidate_rows and all(bool(row.get("insufficient_samples")) for row in candidate_rows):
        return "hold", [{"code": "insufficient_samples_only", "candidate_count": len(candidate_rows)}]
    keep_count = sum(1 for row in candidate_rows if _text(row.get("decision")) == "keep")
    hold_count = sum(1 for row in candidate_rows if _text(row.get("decision")) == "hold")
    drop_count = sum(1 for row in candidate_rows if _text(row.get("decision")) == "drop")
    if keep_count >= 1:
        return "keep", [{"code": "candidate_keep_present", "keep_count": keep_count}]
    if candidate_rows and drop_count == len(candidate_rows):
        return "drop", [{"code": "all_candidates_drop", "drop_count": drop_count}]
    if candidate_rows:
        return "hold", [{"code": "additional_candidate_worth_trying", "hold_count": hold_count or 1}]
    return "drop", [{"code": "no_candidates_available"}]


def _build_session_leaderboard_rollup() -> dict[str, Any]:
    session_payloads: list[dict[str, Any]] = []
    root = _session_root()
    if root.exists():
        for session_dir in sorted((entry for entry in root.iterdir() if entry.is_dir()), key=lambda item: item.name):
            payload = _read_json_file(session_dir / SESSION_FAMILY_LEADERBOARD_FILE)
            if not payload:
                continue
            session_state = _read_json_file(session_dir / "session.json") or {}
            session_meta = payload.get("session_meta") if isinstance(payload.get("session_meta"), dict) else {}
            artifact_consistency_errors: list[str] = []
            if isinstance(session_state, dict) and session_state:
                state_meta = {
                    "session_id": _text(session_state.get("session_id"), fallback=session_dir.name),
                    "random_seed": int(session_state.get("random_seed") or 0),
                    "eval_window_mode": _text(session_state.get("eval_window_mode"), fallback="unknown"),
                    "ret20_source_mode": _text(session_state.get("ret20_source_mode"), fallback="unknown"),
                    "sample_count": int((session_state.get("coverage_waterfall") or {}).get("sample_count") or 0),
                    "insufficient_samples": bool(session_state.get("insufficient_samples")),
                }
                payload_meta = {
                    "session_id": _text(session_meta.get("session_id"), fallback=session_dir.name),
                    "random_seed": int(session_meta.get("random_seed") or 0),
                    "eval_window_mode": _text(session_meta.get("eval_window_mode"), fallback="unknown"),
                    "ret20_source_mode": _text(session_meta.get("ret20_source_mode"), fallback="unknown"),
                    "sample_count": int(session_meta.get("sample_count") or 0),
                    "insufficient_samples": bool(session_meta.get("insufficient_samples")),
                }
                for key in ("session_id", "random_seed", "eval_window_mode", "ret20_source_mode", "sample_count", "insufficient_samples"):
                    if state_meta[key] != payload_meta[key]:
                        artifact_consistency_errors.append(f"{key}:{state_meta[key]}!={payload_meta[key]}")
            session_payloads.append(
                {
                    "session_id": _text(session_meta.get("session_id"), fallback=session_dir.name),
                    "random_seed": int(session_meta.get("random_seed") or 0),
                    "generated_at": _text(session_meta.get("generated_at")),
                    "eval_window_mode": _text(session_meta.get("eval_window_mode"), fallback=_text((payload.get("session_meta") or {}).get("eval_window_mode"), fallback="unknown")),
                    "eval_window_mode_reason": _text(session_meta.get("eval_window_mode_reason"), fallback=_text((payload.get("session_meta") or {}).get("eval_window_mode_reason"), fallback="unknown")),
                    "ret20_source_mode": _text(session_meta.get("ret20_source_mode"), fallback=_text((payload.get("session_meta") or {}).get("ret20_source_mode"), fallback="unknown")),
                    "ret20_source_mode_reason": _text(session_meta.get("ret20_source_mode_reason"), fallback=_text((payload.get("session_meta") or {}).get("ret20_source_mode_reason"), fallback="unknown")),
                    "insufficient_samples": bool(payload.get("insufficient_samples") or (payload.get("coverage_waterfall") or {}).get("insufficient_samples")),
                    "artifact_consistent": not artifact_consistency_errors,
                    "artifact_consistency_errors": artifact_consistency_errors,
                    "payload": payload,
                }
            )
    session_payloads = sorted(session_payloads, key=lambda item: (item["generated_at"], item["session_id"]))
    valid_session_payloads = [item for item in session_payloads if not bool(item.get("insufficient_samples")) and bool(item.get("artifact_consistent", True))]

    family_map: dict[str, dict[str, Any]] = {}
    candidate_map: dict[str, dict[str, Any]] = {}
    session_ids: list[str] = []

    for session_entry in valid_session_payloads:
        session_id = _text(session_entry.get("session_id"))
        if session_id:
            session_ids.append(session_id)
        payload = session_entry.get("payload") if isinstance(session_entry.get("payload"), dict) else {}
        for row in payload.get("candidate_rows") if isinstance(payload.get("candidate_rows"), list) else []:
            if not isinstance(row, dict):
                continue
            decision = _text(row.get("decision"))
            comparison = row.get("comparison") if isinstance(row.get("comparison"), dict) else {}
            family_id = _text(row.get("method_family"))
            signature_hash = _text(row.get("method_signature_hash"))
            if not signature_hash:
                signature_hash = tradex._stable_hash(
                    {
                        "method_family": family_id,
                        "method_title": _text(row.get("method_title")),
                        "method_thesis": _text(row.get("method_thesis")),
                    }
                )
            candidate_entry = candidate_map.setdefault(
                signature_hash,
                {
                    "method_signature_hash": signature_hash,
                    "method_family": family_id,
                    "method_title": _text(row.get("method_title")),
                    "method_thesis": _text(row.get("method_thesis")),
                    "feature_family": _text(row.get("feature_family")),
                    "artifact_detail_level": _text(row.get("artifact_detail_level"), fallback=TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE),
                    "fallback_status": _text(row.get("fallback_status"), fallback=TRADEX_FALLBACK_STATUS_AUTHORITATIVE),
                    "victory_metrics": row.get("victory_metrics") if isinstance(row.get("victory_metrics"), dict) else {},
                    "long_horizon_regime_score": tradex._float(row.get("long_horizon_regime_score")) or 0.0,
                    "recent_adaptation_score": tradex._float(row.get("recent_adaptation_score")) or 0.0,
                    "keep_count": 0,
                    "drop_count": 0,
                    "hold_count": 0,
                    "session_count": 0,
                    "session_ids": [],
                    "avg_top5_ret20_mean_delta_total": 0.0,
                    "avg_top10_ret20_mean_delta_total": 0.0,
                    "avg_monthly_capture_delta_total": 0.0,
                    "avg_zero_pass_delta_total": 0.0,
                    "avg_worst_regime_delta_total": 0.0,
                    "avg_dd_delta_total": 0.0,
                    "avg_turnover_delta_total": 0.0,
                    "avg_liquidity_fail_delta_total": 0.0,
                    "avg_changed_top5_members_count_total": 0.0,
                    "avg_changed_top10_members_count_total": 0.0,
                    "avg_changed_rank_count_total": 0.0,
                    "avg_top5_boundary_score_gap_total": 0.0,
                    "avg_top10_boundary_score_gap_total": 0.0,
                    "row_count": 0,
                    "latest_sort_key": ("", ""),
                    "latest_session_id": "",
                    "latest_generated_at": "",
                    "latest_eval_window_mode": "unknown",
                    "latest_eval_window_mode_reason": "unknown",
                    "latest_decision": "",
                    "latest_decision_reasons": [],
                    "latest_selection_divergence_reason": "",
                    "insufficient_samples": False,
                },
            )
            candidate_entry["method_family"] = family_id or candidate_entry["method_family"]
            candidate_entry["method_title"] = _text(row.get("method_title"), fallback=candidate_entry["method_title"])
            candidate_entry["method_thesis"] = _text(row.get("method_thesis"), fallback=candidate_entry["method_thesis"])
            candidate_entry["feature_family"] = _text(row.get("feature_family"), fallback=_text(candidate_entry.get("feature_family")))
            candidate_entry["session_count"] += 1
            if session_id and session_id not in candidate_entry["session_ids"]:
                candidate_entry["session_ids"].append(session_id)
            if decision == "keep":
                candidate_entry["keep_count"] += 1
            elif decision == "hold":
                candidate_entry["hold_count"] += 1
            else:
                candidate_entry["drop_count"] += 1
            candidate_entry["row_count"] += 1
            candidate_entry["avg_top5_ret20_mean_delta_total"] += _leaderboard_metric_delta(
                comparison.get("champion_top5_ret20_mean"),
                comparison.get("challenger_top5_ret20_mean"),
            ) or 0.0
            candidate_entry["avg_top10_ret20_mean_delta_total"] += _leaderboard_metric_delta(
                comparison.get("champion_top10_ret20_mean"),
                comparison.get("challenger_top10_ret20_mean"),
            ) or 0.0
            candidate_entry["avg_monthly_capture_delta_total"] += _leaderboard_metric_delta(
                comparison.get("champion_monthly_top5_capture_mean"),
                comparison.get("challenger_monthly_top5_capture_mean"),
            ) or 0.0
            candidate_entry["avg_zero_pass_delta_total"] += _leaderboard_metric_delta(
                comparison.get("champion_zero_pass_months"),
                comparison.get("challenger_zero_pass_months"),
            ) or 0.0
            candidate_entry["avg_worst_regime_delta_total"] += _leaderboard_metric_delta(
                comparison.get("champion_worst_regime_ret20_mean"),
                comparison.get("challenger_worst_regime_ret20_mean"),
            ) or 0.0
            candidate_entry["avg_dd_delta_total"] += _leaderboard_metric_delta(
                comparison.get("champion_dd"),
                comparison.get("challenger_dd"),
            ) or 0.0
            candidate_entry["avg_turnover_delta_total"] += _leaderboard_metric_delta(
                comparison.get("champion_turnover"),
                comparison.get("challenger_turnover"),
            ) or 0.0
            candidate_entry["avg_liquidity_fail_delta_total"] += _leaderboard_metric_delta(
                comparison.get("champion_liquidity_fail_rate"),
                comparison.get("challenger_liquidity_fail_rate"),
            ) or 0.0
            candidate_entry["avg_changed_top5_members_count_total"] += float(row.get("changed_top5_members_count") or 0.0)
            candidate_entry["avg_changed_top10_members_count_total"] += float(row.get("changed_top10_members_count") or 0.0)
            candidate_entry["avg_changed_rank_count_total"] += float(row.get("changed_rank_count") or 0.0)
            candidate_entry["avg_top5_boundary_score_gap_total"] += float(row.get("top5_boundary_score_gap") or 0.0)
            candidate_entry["avg_top10_boundary_score_gap_total"] += float(row.get("top10_boundary_score_gap") or 0.0)
            sort_key = (_text(session_entry.get("generated_at")), session_id)
            if sort_key >= candidate_entry["latest_sort_key"]:
                candidate_entry["latest_sort_key"] = sort_key
                candidate_entry["latest_session_id"] = session_id
                candidate_entry["latest_generated_at"] = _text(session_entry.get("generated_at"))
                candidate_entry["latest_eval_window_mode"] = _text(session_entry.get("eval_window_mode"), fallback="unknown")
                candidate_entry["latest_eval_window_mode_reason"] = _text(session_entry.get("eval_window_mode_reason"), fallback="unknown")
                candidate_entry["latest_decision"] = decision
                candidate_entry["latest_decision_reasons"] = row.get("decision_reasons") if isinstance(row.get("decision_reasons"), list) else []
                candidate_entry["latest_selection_divergence_reason"] = _text(row.get("selection_divergence_reason"), fallback="no_meaningful_branching")
            if bool(session_entry.get("insufficient_samples")):
                candidate_entry["insufficient_samples"] = True

    candidate_rows: list[dict[str, Any]] = []
    for entry in candidate_map.values():
        row_count = int(entry.get("row_count") or 0)
        candidate_rows.append(
            {
                "method_signature_hash": _text(entry.get("method_signature_hash")),
                "method_family": _text(entry.get("method_family")),
                "method_title": _text(entry.get("method_title")),
                "method_thesis": _text(entry.get("method_thesis")),
                "feature_family": _text(entry.get("feature_family")),
                "keep_count": int(entry.get("keep_count") or 0),
                "drop_count": int(entry.get("drop_count") or 0),
                "hold_count": int(entry.get("hold_count") or 0),
                "session_count": int(entry.get("session_count") or 0),
                "session_ids": [str(item) for item in entry.get("session_ids") or [] if str(item).strip()],
                "avg_top5_ret20_mean_delta": _leaderboard_average(float(entry.get("avg_top5_ret20_mean_delta_total") or 0.0), row_count),
                "avg_top10_ret20_mean_delta": _leaderboard_average(float(entry.get("avg_top10_ret20_mean_delta_total") or 0.0), row_count),
                "avg_monthly_capture_delta": _leaderboard_average(float(entry.get("avg_monthly_capture_delta_total") or 0.0), row_count),
                "avg_zero_pass_delta": _leaderboard_average(float(entry.get("avg_zero_pass_delta_total") or 0.0), row_count),
                "avg_worst_regime_delta": _leaderboard_average(float(entry.get("avg_worst_regime_delta_total") or 0.0), row_count),
                "avg_dd_delta": _leaderboard_average(float(entry.get("avg_dd_delta_total") or 0.0), row_count),
                "avg_turnover_delta": _leaderboard_average(float(entry.get("avg_turnover_delta_total") or 0.0), row_count),
                "avg_liquidity_fail_delta": _leaderboard_average(float(entry.get("avg_liquidity_fail_delta_total") or 0.0), row_count),
                "avg_changed_top5_members_count": _leaderboard_average(float(entry.get("avg_changed_top5_members_count_total") or 0.0), row_count),
                "avg_changed_top10_members_count": _leaderboard_average(float(entry.get("avg_changed_top10_members_count_total") or 0.0), row_count),
                "avg_changed_rank_count": _leaderboard_average(float(entry.get("avg_changed_rank_count_total") or 0.0), row_count),
                "avg_top5_boundary_score_gap": _leaderboard_average(float(entry.get("avg_top5_boundary_score_gap_total") or 0.0), row_count),
                "avg_top10_boundary_score_gap": _leaderboard_average(float(entry.get("avg_top10_boundary_score_gap_total") or 0.0), row_count),
                "latest_session_id": _text(entry.get("latest_session_id")),
                "latest_generated_at": _text(entry.get("latest_generated_at")),
                "latest_eval_window_mode": _text(entry.get("latest_eval_window_mode"), fallback="unknown"),
                "latest_eval_window_mode_reason": _text(entry.get("latest_eval_window_mode_reason"), fallback="unknown"),
                "latest_decision": _text(entry.get("latest_decision")),
                "candidate_local_decision": _text(entry.get("latest_decision")),
                "session_aggregate_decision": _text(entry.get("latest_decision")),
                "selection_divergence_reason": _text(entry.get("latest_selection_divergence_reason"), fallback="no_meaningful_branching"),
                "decision_reasons": _json_ready(entry.get("latest_decision_reasons") or []),
                "latest_decision_reasons": _json_ready(entry.get("latest_decision_reasons") or []),
                "artifact_detail_level": _text(entry.get("artifact_detail_level"), fallback=TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE),
                "fallback_status": _text(entry.get("fallback_status"), fallback=TRADEX_FALLBACK_STATUS_AUTHORITATIVE),
                "victory_metrics": _json_ready(entry.get("victory_metrics") or {}),
                "long_horizon_regime_score": tradex._float(entry.get("long_horizon_regime_score")) or 0.0,
                "recent_adaptation_score": tradex._float(entry.get("recent_adaptation_score")) or 0.0,
                "insufficient_samples": bool(entry.get("insufficient_samples")),
            }
        )

    family_map_rows: dict[str, dict[str, Any]] = {}
    for row in candidate_rows:
        family_id = _text(row.get("method_family"))
        family_entry = family_map_rows.setdefault(
            family_id,
            {
                "method_family": family_id,
                "method_title": _text(row.get("method_title")),
                "method_thesis": _text(row.get("method_thesis")),
                "feature_family": _text(row.get("feature_family")),
                "keep_count": 0,
                "drop_count": 0,
                "hold_count": 0,
                "candidate_count": 0,
                "candidate_rows": [],
                "latest_sort_key": ("", ""),
                "latest_session_id": "",
                "latest_generated_at": "",
                "latest_decision": "",
                "session_aggregate_decision": "",
                "authoritative_rollup_decision": "",
                "latest_decision_reasons": [],
                "insufficient_samples": False,
                "effective_universe_count": 0,
                "top_k": 0,
                "meaningful_topk_branching_possible": False,
                "topk_branching_block_reason": "",
            },
        )
        family_entry["candidate_count"] += 1
        family_entry["candidate_rows"].append(row)
        family_entry["method_title"] = _text(row.get("method_title"), fallback=family_entry["method_title"])
        family_entry["method_thesis"] = _text(row.get("method_thesis"), fallback=family_entry["method_thesis"])
        family_entry["keep_count"] += int(row.get("keep_count") or 0)
        family_entry["drop_count"] += int(row.get("drop_count") or 0)
        family_entry["hold_count"] += int(row.get("hold_count") or 0)
        sort_key = (_text(row.get("latest_generated_at")), _text(row.get("latest_session_id")))
        if sort_key >= family_entry["latest_sort_key"]:
            family_entry["latest_sort_key"] = sort_key
            family_entry["latest_session_id"] = _text(row.get("latest_session_id"))
            family_entry["latest_generated_at"] = _text(row.get("latest_generated_at"))
            family_entry["latest_decision"] = _text(row.get("latest_decision"))
            family_entry["session_aggregate_decision"] = _text(row.get("session_aggregate_decision"), fallback=_text(row.get("latest_decision")))
            family_entry["authoritative_rollup_decision"] = _text(row.get("authoritative_rollup_decision"), fallback=_text(row.get("latest_decision")))
            family_entry["latest_decision_reasons"] = row.get("latest_decision_reasons") if isinstance(row.get("latest_decision_reasons"), list) else []
            family_entry["effective_universe_count"] = int(row.get("effective_universe_count") or 0)
            family_entry["top_k"] = int(row.get("top_k") or 0)
            family_entry["meaningful_topk_branching_possible"] = bool(row.get("meaningful_topk_branching_possible"))
            family_entry["topk_branching_block_reason"] = _text(row.get("topk_branching_block_reason"))
        if bool(row.get("insufficient_samples")):
            family_entry["insufficient_samples"] = True

    family_summary: list[dict[str, Any]] = []
    for family_entry in family_map_rows.values():
        family_candidate_rows = family_entry.pop("candidate_rows")
        family_decision, family_decision_reasons = _leaderboard_family_decision(family_candidate_rows)
        family_summary.append(
            {
                "method_family": _text(family_entry.get("method_family")),
                "method_title": _text(family_entry.get("method_title")),
                "method_thesis": _text(family_entry.get("method_thesis")),
                "candidate_count": int(family_entry.get("candidate_count") or 0),
                "keep_count": int(family_entry.get("keep_count") or 0),
                "drop_count": int(family_entry.get("drop_count") or 0),
                "hold_count": int(family_entry.get("hold_count") or 0),
                "decision": family_decision,
                "session_aggregate_decision": family_decision,
                "authoritative_rollup_decision": family_decision,
                "decision_reasons": family_decision_reasons,
                "latest_session_id": _text(family_entry.get("latest_session_id")),
                "latest_generated_at": _text(family_entry.get("latest_generated_at")),
                "latest_eval_window_mode": _text(family_entry.get("latest_eval_window_mode"), fallback="unknown"),
                "latest_eval_window_mode_reason": _text(family_entry.get("latest_eval_window_mode_reason"), fallback="unknown"),
                "latest_decision": _text(family_entry.get("latest_decision")),
                "latest_decision_reasons": _json_ready(family_entry.get("latest_decision_reasons") or []),
                "insufficient_samples": bool(family_entry.get("insufficient_samples")),
                "effective_universe_count": int(family_entry.get("effective_universe_count") or 0),
                "top_k": int(family_entry.get("top_k") or 0),
                "meaningful_topk_branching_possible": bool(family_entry.get("meaningful_topk_branching_possible")),
                "topk_branching_block_reason": _text(family_entry.get("topk_branching_block_reason")),
            }
        )

    candidate_rows = sorted(
        candidate_rows,
        key=lambda row: (
            _text(row.get("method_family")),
            0 if _text(row.get("latest_decision")) == "keep" else 1 if _text(row.get("latest_decision")) == "hold" else 2,
            -int(row.get("session_count") or 0),
            _text(row.get("method_title")),
            _text(row.get("method_signature_hash")),
        ),
    )
    family_summary = sorted(family_summary, key=lambda row: (_text(row.get("method_family")), 0 if _text(row.get("decision")) == "keep" else 1 if _text(row.get("decision")) == "hold" else 2))

    overview = {
        "session_count": len(session_payloads),
        "valid_session_count": len(valid_session_payloads),
        "invalid_session_count": len(session_payloads) - len(valid_session_payloads),
        "artifact_consistency_error_count": sum(1 for item in session_payloads if not bool(item.get("artifact_consistent", True))),
        "family_count": len(family_summary),
        "candidate_count": len(candidate_rows),
        "keep_family_count": sum(1 for row in family_summary if row.get("decision") == "keep"),
        "hold_family_count": sum(1 for row in family_summary if row.get("decision") == "hold"),
        "drop_family_count": sum(1 for row in family_summary if row.get("decision") == "drop"),
        "keep_candidate_count": sum(1 for row in candidate_rows if row.get("latest_decision") == "keep"),
        "hold_candidate_count": sum(1 for row in candidate_rows if row.get("latest_decision") == "hold"),
        "drop_candidate_count": sum(1 for row in candidate_rows if row.get("latest_decision") == "drop"),
        "insufficient_samples": any(bool(item.get("insufficient_samples")) for item in session_payloads),
    }
    return {
        "schema_version": SESSION_LEADERBOARD_ROLLUP_SCHEMA_VERSION,
        "session_meta": {
            "generated_at": _utc_now_iso(),
            "session_count": len(session_payloads),
            "valid_session_count": len(valid_session_payloads),
            "invalid_session_count": len(session_payloads) - len(valid_session_payloads),
            "artifact_consistency_error_count": overview["artifact_consistency_error_count"],
            "session_ids": session_ids,
            "insufficient_samples": overview["insufficient_samples"],
            "eval_window_mode_counts": {
                "standard": sum(1 for item in valid_session_payloads if _text(item.get("eval_window_mode")) == "standard"),
                "fallback": sum(1 for item in valid_session_payloads if _text(item.get("eval_window_mode")) == "fallback"),
                "exploratory": sum(1 for item in valid_session_payloads if _text(item.get("eval_window_mode")) == "exploratory"),
                "unknown": sum(1 for item in valid_session_payloads if _text(item.get("eval_window_mode")) not in {"standard", "fallback", "exploratory"}),
            },
        },
        "source_family_leaderboard_paths": [str(_session_family_leaderboard_file(session_entry["session_id"])) for session_entry in session_payloads],
        "insufficient_samples": overview["insufficient_samples"] or overview["artifact_consistency_error_count"] > 0,
        "authoritative_rollup_decision": "keep" if overview["keep_family_count"] else "hold" if overview["hold_family_count"] else "drop",
        "overview": overview,
        "family_summary": family_summary,
        "candidate_rows": candidate_rows,
    }


def _format_session_leaderboard_rollup_markdown(rollup: dict[str, Any]) -> str:
    session_meta = rollup.get("session_meta") if isinstance(rollup.get("session_meta"), dict) else {}
    overview = rollup.get("overview") if isinstance(rollup.get("overview"), dict) else {}
    family_rows = rollup.get("family_summary") if isinstance(rollup.get("family_summary"), list) else []
    candidate_rows = rollup.get("candidate_rows") if isinstance(rollup.get("candidate_rows"), list) else []
    lines: list[str] = []
    lines.append("# TRADEX Session Leaderboard Rollup")
    lines.append("")
    lines.append(f"- generated_at: `{_text(session_meta.get('generated_at'))}`")
    lines.append(f"- session_count: `{int(session_meta.get('session_count') or 0)}`")
    if int(session_meta.get("valid_session_count") or 0) or int(session_meta.get("invalid_session_count") or 0):
        lines.append(
            f"- valid_session_count: `{int(session_meta.get('valid_session_count') or 0)}` / invalid_session_count: `{int(session_meta.get('invalid_session_count') or 0)}`"
        )
    if int(session_meta.get("artifact_consistency_error_count") or 0):
        lines.append(f"- artifact_consistency_error_count: `{int(session_meta.get('artifact_consistency_error_count') or 0)}`")
    session_ids = [str(item) for item in (session_meta.get("session_ids") or []) if str(item).strip()]
    lines.append(f"- session_ids: `{', '.join(session_ids) if session_ids else 'none'}`")
    if bool(session_meta.get("insufficient_samples")):
        lines.append("- validity: `invalid (insufficient_samples)`")
    if int(session_meta.get("artifact_consistency_error_count") or 0):
        lines.append("- validity: `invalid (artifact_consistency_error)`")
    if int(overview.get("invalid_session_count") or 0):
        lines.append("- note: invalid sessions are excluded from family / candidate aggregation.")
    lines.append("")
    lines.append("## Overview")
    lines.append("")
    lines.append("| sessions | families | candidates | keep families | hold families | drop families |")
    lines.append("| ---: | ---: | ---: | ---: | ---: | ---: |")
    lines.append(
        "| {sessions} | {families} | {candidates} | {keep} | {hold} | {drop} |".format(
            sessions=int(overview.get("session_count") or 0),
            families=int(overview.get("family_count") or 0),
            candidates=int(overview.get("candidate_count") or 0),
            keep=int(overview.get("keep_family_count") or 0),
            hold=int(overview.get("hold_family_count") or 0),
            drop=int(overview.get("drop_family_count") or 0),
        )
    )
    lines.append("")
    lines.append("## Family Summary")
    lines.append("")
    lines.append("| family | decision | keep | hold | drop | latest decision |")
    lines.append("| --- | --- | ---: | ---: | ---: | --- |")
    for family in family_rows:
        if not isinstance(family, dict):
            continue
        lines.append(
            "| {family} | {decision} | {keep} | {hold} | {drop} | {latest} |".format(
                family=_text(family.get("method_family")),
                decision=_text(family.get("decision")),
                keep=int(family.get("keep_count") or 0),
                hold=int(family.get("hold_count") or 0),
                drop=int(family.get("drop_count") or 0),
                latest=_text(family.get("latest_decision")),
            )
        )
    lines.append("")
    lines.append("## Candidate Rows")
    lines.append("")
    lines.append("| family | title | decision | sessions | top5Δ | top10Δ | monthlyΔ | zero-passΔ | worstΔ | ddΔ | turnoverΔ | liquidityΔ | latest reasons |")
    lines.append("| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |")
    for row in candidate_rows:
        if not isinstance(row, dict):
            continue
        latest_reasons = row.get("latest_decision_reasons") if isinstance(row.get("latest_decision_reasons"), list) else []
        reason_text = ", ".join(
            f"{_text(reason.get('code'))}:{_text(reason.get('status'))}"
            for reason in latest_reasons
            if isinstance(reason, dict)
        ) or "none"
        lines.append(
            "| {family} | {title} | {decision} | {sessions} | {top5:.4f} | {top10:.4f} | {monthly:.4f} | {zero:.4f} | {worst:.4f} | {dd:.4f} | {turnover:.4f} | {liquidity:.4f} | {reasons} |".format(
                family=_text(row.get("method_family")),
                title=_text(row.get("method_title")),
                decision=_text(row.get("latest_decision")),
                sessions=int(row.get("session_count") or 0),
                top5=float(row.get("avg_top5_ret20_mean_delta") or 0.0),
                top10=float(row.get("avg_top10_ret20_mean_delta") or 0.0),
                monthly=float(row.get("avg_monthly_capture_delta") or 0.0),
                zero=float(row.get("avg_zero_pass_delta") or 0.0),
                worst=float(row.get("avg_worst_regime_delta") or 0.0),
                dd=float(row.get("avg_dd_delta") or 0.0),
                turnover=float(row.get("avg_turnover_delta") or 0.0),
                liquidity=float(row.get("avg_liquidity_fail_delta") or 0.0),
                reasons=reason_text,
            )
        )
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- compare artifact と family_leaderboard を正本として集計した rollup です。")
    lines.append("- hold は追加 1 候補の余地を示す暫定状態です。")
    lines.append("- MeeMee にはまだ接続していません。")
    return "\n".join(lines).rstrip() + "\n"


def _write_session_leaderboard_rollup_artifacts() -> tuple[Path, Path, dict[str, Any]]:
    rollup = _build_session_leaderboard_rollup()
    rollup_path = _session_leaderboard_rollup_file()
    report_path = _session_leaderboard_rollup_report_file()
    rollup["rollup_path"] = str(rollup_path)
    rollup["report_path"] = str(report_path)
    validate_session_rollup_artifact(rollup)
    _write_json(rollup_path, rollup)
    _verify_json_roundtrip(rollup_path, rollup, artifact_name="session_leaderboard_rollup")
    report_path.write_text(_format_session_leaderboard_rollup_markdown(rollup), encoding="utf-8")
    return rollup_path, report_path, rollup


def _ensure_session_leaderboard_rollup_artifacts(*, force: bool = False) -> tuple[Path, Path, dict[str, Any]]:
    rollup_path = _session_leaderboard_rollup_file()
    report_path = _session_leaderboard_rollup_report_file()
    if not force and rollup_path.exists() and report_path.exists():
        return rollup_path, report_path, {}
    existing_payload = _read_json_file(rollup_path) if rollup_path.exists() else None

    rollup = existing_payload if isinstance(existing_payload, dict) else _build_session_leaderboard_rollup()
    if not isinstance(existing_payload, dict):
        rollup["rollup_path"] = str(rollup_path)
        rollup["report_path"] = str(report_path)
        validate_session_rollup_artifact(rollup)
    if force or existing_payload is None:
        _write_json(rollup_path, rollup)
        _verify_json_roundtrip(rollup_path, rollup, artifact_name="session_leaderboard_rollup")
    if force or not report_path.exists():
        report_path.write_text(_format_session_leaderboard_rollup_markdown(rollup), encoding="utf-8")
    return rollup_path, report_path, rollup


def _stability_session_row(session_state: dict[str, Any]) -> dict[str, Any]:
    session_id = _text(session_state.get("session_id"))
    session_scope_id = _text(session_state.get("session_scope_id"), fallback=session_id)
    family_leaderboard = _read_json_file(_session_family_leaderboard_file(session_id)) or {}
    coverage = session_state.get("coverage_waterfall") if isinstance(session_state.get("coverage_waterfall"), dict) else {}
    overview = family_leaderboard.get("overview") if isinstance(family_leaderboard.get("overview"), dict) else {}
    best_result = session_state.get("best_result") if isinstance(session_state.get("best_result"), dict) else {}
    comparison = best_result.get("selection_compare") if isinstance(best_result.get("selection_compare"), dict) else {}
    evaluation = best_result.get("evaluation_summary") if isinstance(best_result.get("evaluation_summary"), dict) else {}
    best_present = bool(best_result)
    family_summary_rows = family_leaderboard.get("family_summary") if isinstance(family_leaderboard.get("family_summary"), list) else []
    family_summary_row = family_summary_rows[0] if family_summary_rows and isinstance(family_summary_rows[0], dict) else {}
    session_decision = _text(
        family_leaderboard.get("authoritative_rollup_decision"),
        fallback=_text(family_summary_row.get("session_aggregate_decision"), fallback=_text(family_summary_row.get("decision"), fallback="")),
    )
    if not session_decision:
        session_decision = "keep" if int(overview.get("keep_family_count") or 0) else "hold" if int(overview.get("hold_family_count") or 0) else "drop"
    decision_reasons = _json_ready(family_summary_row.get("decision_reasons") or [])
    if not isinstance(decision_reasons, list) or not decision_reasons:
        decision_reasons = [{"code": f"{session_decision}_from_family_leaderboard"}]
    champion_top5 = _first_metric_value(
        (comparison, "champion_topk_ret20_mean"),
        (comparison, "champion_top5_ret20_mean"),
        (evaluation, "champion_topk_ret20_mean"),
        (evaluation, "champion_top5_ret20_mean"),
    )
    challenger_top5 = _first_metric_value(
        (comparison, "challenger_topk_ret20_mean"),
        (comparison, "challenger_top5_ret20_mean"),
        (evaluation, "challenger_topk_ret20_mean"),
        (evaluation, "challenger_top5_ret20_mean"),
    )
    champion_top10 = _first_metric_value(
        (comparison, "champion_topk10_ret20_mean"),
        (evaluation, "champion_top10_ret20_mean"),
        (evaluation, "champion_topk10_ret20_mean"),
    )
    challenger_top10 = _first_metric_value(
        (comparison, "challenger_topk10_ret20_mean"),
        (evaluation, "challenger_top10_ret20_mean"),
        (evaluation, "challenger_topk10_ret20_mean"),
    )
    champion_capture = _first_metric_value(
        (comparison, "champion_monthly_top5_capture_mean"),
        (evaluation, "champion_monthly_top5_capture_mean"),
    )
    challenger_capture = _first_metric_value(
        (comparison, "challenger_monthly_top5_capture_mean"),
        (evaluation, "challenger_monthly_top5_capture_mean"),
    )
    champion_zero_pass = _first_metric_value(
        (comparison, "champion_zero_pass_months"),
        (evaluation, "champion_zero_pass_months"),
    )
    challenger_zero_pass = _first_metric_value(
        (comparison, "challenger_zero_pass_months"),
        (evaluation, "challenger_zero_pass_months"),
    )
    champion_dd = _first_metric_value((comparison, "champion_dd"), (evaluation, "champion_dd"))
    challenger_dd = _first_metric_value((comparison, "challenger_dd"), (evaluation, "challenger_dd"))
    champion_turnover = _first_metric_value((comparison, "champion_turnover"), (evaluation, "champion_turnover"))
    challenger_turnover = _first_metric_value((comparison, "challenger_turnover"), (evaluation, "challenger_turnover"))
    champion_liquidity = _first_metric_value(
        (comparison, "champion_liquidity_fail_rate"),
        (evaluation, "champion_liquidity_fail_rate"),
    )
    challenger_liquidity = _first_metric_value(
        (comparison, "challenger_liquidity_fail_rate"),
        (evaluation, "challenger_liquidity_fail_rate"),
    )
    windows = evaluation.get("windows") if isinstance(evaluation.get("windows"), list) else []
    worst_regime_delta = None
    if windows:
        margins = []
        for window in windows:
            if not isinstance(window, dict):
                continue
            margins.append(_leaderboard_metric_delta(window.get("champion_top5_ret20_mean"), window.get("challenger_top5_ret20_mean")))
        margins = [float(value) for value in margins if value is not None]
        if margins:
            worst_regime_delta = min(margins)
    return {
        "session_id": session_id,
        "session_scope_id": session_scope_id,
        "random_seed": int(session_state.get("random_seed") or 0),
        "generated_at": _text(session_state.get("completed_at"), fallback=_text(session_state.get("updated_at"))),
        "eval_window_mode": _text(session_state.get("eval_window_mode"), fallback="unknown"),
        "eval_window_mode_reason": _text(session_state.get("eval_window_mode_reason"), fallback="unknown"),
        "ret20_source_mode": _text(session_state.get("ret20_source_mode"), fallback="unknown"),
        "ret20_source_mode_reason": _text(session_state.get("ret20_source_mode_reason"), fallback="unknown"),
        "decision": session_decision,
        "session_aggregate_decision": session_decision,
        "decision_reasons": decision_reasons,
        "first_zero_stage": _text((coverage or {}).get("first_zero_stage"), fallback=_text((coverage or {}).get("failure_stage"), fallback="passed")),
        "sample_count": int(coverage.get("sample_count") or 0),
        "confirmed_universe_count": int(coverage.get("confirmed_universe_count") or 0),
        "probe_selection_count": int(coverage.get("probe_selection_count") or 0),
        "candidate_rows_built_count": int(coverage.get("candidate_rows_built_count") or 0),
        "eligible_candidate_count": int(coverage.get("eligible_candidate_count") or 0),
        "ret20_computable_count": int(coverage.get("ret20_computable_count") or 0),
        "compare_row_count": int(coverage.get("compare_row_count") or 0),
        "sample_rows_retained_count": int(coverage.get("sample_rows_retained_count") or 0),
        "future_ret20_candidate_day_count": int(coverage.get("future_ret20_candidate_day_count") or 0),
        "candidate_rows_before_future_guard": int(coverage.get("candidate_rows_before_future_guard") or 0),
        "candidate_rows_after_future_guard": int(coverage.get("candidate_rows_after_future_guard") or 0),
        "ret20_joinable_rows": int(coverage.get("ret20_joinable_rows") or 0),
        "compare_rows_emitted": int(coverage.get("compare_rows_emitted") or 0),
        "sample_rows_retained": int(coverage.get("sample_rows_retained") or 0),
        "future_ret20_passed_count": int(coverage.get("future_ret20_passed_count") or 0),
        "future_ret20_guarded_out_count": int(coverage.get("future_ret20_guarded_out_count") or 0),
        "future_ret20_failure_reason_counts": coverage.get("future_ret20_failure_reason_counts") if isinstance(coverage.get("future_ret20_failure_reason_counts"), dict) else {},
        "future_ret20_failure_reason_counts_by_source_mode": coverage.get("future_ret20_failure_reason_counts_by_source_mode") if isinstance(coverage.get("future_ret20_failure_reason_counts_by_source_mode"), dict) else {},
        "future_ret20_source_coverage": coverage.get("future_ret20_source_coverage") if isinstance(coverage.get("future_ret20_source_coverage"), dict) else {},
        "future_ret20_join_gap_coverage": coverage.get("future_ret20_join_gap_coverage") if isinstance(coverage.get("future_ret20_join_gap_coverage"), dict) else {},
        "future_ret20_code_coverage": coverage.get("future_ret20_code_coverage") if isinstance(coverage.get("future_ret20_code_coverage"), dict) else {},
        "ret20_source_mode": _text(session_state.get("ret20_source_mode"), fallback="unknown"),
        "ret20_source_mode_reason": _text(session_state.get("ret20_source_mode_reason"), fallback="unknown"),
        "best_result_present": best_present,
        "keep_count": int(overview.get("keep_family_count") or 0),
        "drop_count": int(overview.get("drop_family_count") or 0),
        "hold_count": int(overview.get("hold_family_count") or 0),
        "top5_ret20_mean_delta": _leaderboard_metric_delta(champion_top5, challenger_top5) or 0.0,
        "top10_ret20_mean_delta": _leaderboard_metric_delta(champion_top10, challenger_top10) or 0.0,
        "monthly_capture_delta": _leaderboard_metric_delta(champion_capture, challenger_capture) or 0.0,
        "zero_pass_delta": _leaderboard_metric_delta(champion_zero_pass, challenger_zero_pass) or 0.0,
        "worst_regime_delta": worst_regime_delta if worst_regime_delta is not None else 0.0,
        "dd_delta": _leaderboard_metric_delta(champion_dd, challenger_dd) or 0.0,
        "turnover_delta": _leaderboard_metric_delta(champion_turnover, challenger_turnover) or 0.0,
        "liquidity_fail_delta": _leaderboard_metric_delta(champion_liquidity, challenger_liquidity) or 0.0,
        "insufficient_samples": bool(session_state.get("insufficient_samples")),
        "family_leaderboard_path": str(_session_family_leaderboard_file(session_id)),
        "compare_path": str(_session_compare_file(session_id)),
    }


def _build_stability_rollup(rows: list[dict[str, Any]]) -> dict[str, Any]:
    rows = sorted(rows, key=lambda row: (row.get("random_seed") or 0, row.get("session_id") or ""))
    sample_counts = [int(row.get("sample_count") or 0) for row in rows]
    first_zero_stage_counts: dict[str, int] = {}
    for row in rows:
        stage = _text(row.get("first_zero_stage"), fallback=_text(row.get("failure_stage"), fallback="passed"))
        first_zero_stage_counts[stage] = first_zero_stage_counts.get(stage, 0) + 1
    return {
        "schema_version": STABILITY_ROLLUP_SCHEMA_VERSION,
        "status": "invalid" if any(count <= 0 for count in sample_counts) else "complete",
        "session_meta": {
            "generated_at": _utc_now_iso(),
            "session_count": len(rows),
            "session_ids": [str(row.get("session_id")) for row in rows if _text(row.get("session_id"))],
            "random_seeds": [int(row.get("random_seed") or 0) for row in rows],
            "eval_window_mode_counts": {
                "standard": sum(1 for row in rows if _text(row.get("eval_window_mode")) == "standard"),
                "fallback": sum(1 for row in rows if _text(row.get("eval_window_mode")) == "fallback"),
                "exploratory": sum(1 for row in rows if _text(row.get("eval_window_mode")) == "exploratory"),
                "unknown": sum(1 for row in rows if _text(row.get("eval_window_mode")) not in {"standard", "fallback", "exploratory"}),
            },
        },
        "overview": {
            "session_count": len(rows),
            "sample_count_min": min(sample_counts) if sample_counts else 0,
            "sample_count_max": max(sample_counts) if sample_counts else 0,
            "sample_count_mean": (sum(sample_counts) / float(len(sample_counts))) if sample_counts else 0.0,
            "best_result_present_count": sum(1 for row in rows if bool(row.get("best_result_present"))),
            "insufficient_samples_count": sum(1 for row in rows if bool(row.get("insufficient_samples"))),
            "first_zero_stage_counts": first_zero_stage_counts,
        },
        "session_rows": rows,
    }


def _format_stability_rollup_markdown(rollup: dict[str, Any]) -> str:
    session_meta = rollup.get("session_meta") if isinstance(rollup.get("session_meta"), dict) else {}
    overview = rollup.get("overview") if isinstance(rollup.get("overview"), dict) else {}
    rows = rollup.get("session_rows") if isinstance(rollup.get("session_rows"), list) else []
    lines: list[str] = []
    lines.append("# TRADEX Stability Rollup")
    lines.append("")
    lines.append(f"- generated_at: `{_text(session_meta.get('generated_at'))}`")
    lines.append(f"- session_count: `{int(session_meta.get('session_count') or 0)}`")
    if int(session_meta.get("valid_session_count") or 0) or int(session_meta.get("invalid_session_count") or 0):
        lines.append(
            f"- valid_session_count: `{int(session_meta.get('valid_session_count') or 0)}` / invalid_session_count: `{int(session_meta.get('invalid_session_count') or 0)}`"
        )
    lines.append(f"- session_ids: `{', '.join(str(item) for item in session_meta.get('session_ids') or [] if str(item).strip()) or 'none'}`")
    if _text(rollup.get("status"), fallback="complete") != "complete":
        lines.append(f"- status: `{_text(rollup.get('status'), fallback='invalid')}`")
    counts = session_meta.get("eval_window_mode_counts") if isinstance(session_meta.get("eval_window_mode_counts"), dict) else {}
    lines.append(
        f"- eval_window_mode_counts: standard=`{int(counts.get('standard') or 0)}`, fallback=`{int(counts.get('fallback') or 0)}`, exploratory=`{int(counts.get('exploratory') or 0)}`, unknown=`{int(counts.get('unknown') or 0)}`"
    )
    first_zero_counts = overview.get("first_zero_stage_counts") if isinstance(overview.get("first_zero_stage_counts"), dict) else {}
    if first_zero_counts:
        lines.append(f"- first_zero_stage_counts: `{json.dumps(_json_ready(first_zero_counts), ensure_ascii=False, sort_keys=True)}`")
    lines.append("")
    lines.append("## Overview")
    lines.append("")
    lines.append("| sessions | sample_count_min | sample_count_max | sample_count_mean | best_result_present | insufficient_samples |")
    lines.append("| ---: | ---: | ---: | ---: | ---: | ---: |")
    lines.append(
        "| {sessions} | {sample_min} | {sample_max} | {sample_mean:.2f} | {best_present} | {insufficient} |".format(
            sessions=int(overview.get("session_count") or 0),
            sample_min=int(overview.get("sample_count_min") or 0),
            sample_max=int(overview.get("sample_count_max") or 0),
            sample_mean=float(overview.get("sample_count_mean") or 0.0),
            best_present=int(overview.get("best_result_present_count") or 0),
            insufficient=int(overview.get("insufficient_samples_count") or 0),
        )
    )
    lines.append("")
    lines.append("## Session Rows")
    lines.append("")
    lines.append("| session | seed | mode | sample_count | best_result | keep | drop | hold | top5Δ | worstΔ | ddΔ | turnoverΔ | liquidityΔ |")
    lines.append("| --- | ---: | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for row in rows:
        if not isinstance(row, dict):
            continue
        lines.append(
            "| {session} | {seed} | {mode} | {sample} | {best} | {keep} | {drop} | {hold} | {top5:.4f} | {worst:.4f} | {dd:.4f} | {turnover:.4f} | {liquidity:.4f} |".format(
                session=_text(row.get("session_id")),
                seed=int(row.get("random_seed") or 0),
                mode=_text(row.get("eval_window_mode"), fallback="unknown"),
                sample=int(row.get("sample_count") or 0),
                best="yes" if bool(row.get("best_result_present")) else "no",
                keep=int(row.get("keep_count") or 0),
                drop=int(row.get("drop_count") or 0),
                hold=int(row.get("hold_count") or 0),
                top5=float(row.get("top5_ret20_mean_delta") or 0.0),
                worst=float(row.get("worst_regime_delta") or 0.0),
                dd=float(row.get("dd_delta") or 0.0),
                turnover=float(row.get("turnover_delta") or 0.0),
                liquidity=float(row.get("liquidity_fail_delta") or 0.0),
            )
        )
        lines.append(f"- eval_window_mode_reason: `{_text(row.get('eval_window_mode_reason'), fallback='unknown')}`")
        lines.append(f"- ret20_source_mode: `{_text(row.get('ret20_source_mode'), fallback='unknown')}`")
        lines.append(f"- ret20_source_mode_reason: `{_text(row.get('ret20_source_mode_reason'), fallback='unknown')}`")
        lines.append(
            "- future_ret20: candidate_day_count=`{candidate}`, passed_count=`{passed}`, guarded_out_count=`{guarded}`".format(
                candidate=int(row.get("future_ret20_candidate_day_count") or 0),
                passed=int(row.get("future_ret20_passed_count") or 0),
                guarded=int(row.get("future_ret20_guarded_out_count") or 0),
            )
        )
        lines.append(f"- ret20_source_mode: `{_text(row.get('ret20_source_mode'), fallback='unknown')}`")
        lines.append(f"- ret20_source_mode_reason: `{_text(row.get('ret20_source_mode_reason'), fallback='unknown')}`")
        future_counts = row.get("future_ret20_failure_reason_counts") if isinstance(row.get("future_ret20_failure_reason_counts"), dict) else {}
        if future_counts:
            lines.append(f"- future_ret20_failure_reason_counts: `{json.dumps(_json_ready(future_counts), ensure_ascii=False, sort_keys=True)}`")
        join_gap_coverage = row.get("future_ret20_join_gap_coverage") if isinstance(row.get("future_ret20_join_gap_coverage"), dict) else {}
        if join_gap_coverage:
            lines.append(f"- future_ret20_join_gap_coverage: `{json.dumps(_json_ready(join_gap_coverage), ensure_ascii=False, sort_keys=True)}`")
        lines.append(f"- family_leaderboard_path: `{_text(row.get('family_leaderboard_path'))}`")
        lines.append(f"- compare_path: `{_text(row.get('compare_path'))}`")
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append(f"- legacy analysis env: `{LEGACY_ANALYSIS_DISABLE_ENV}` must be `0` for research runs.")
    lines.append(f"- standard window min days: `{tradex.TRADEX_STANDARD_EVAL_WINDOW_MIN_TRADING_DAYS}`")
    lines.append(f"- fallback window min days: `{tradex.TRADEX_RESEARCH_FALLBACK_EVAL_WINDOW_MIN_TRADING_DAYS}`")
    lines.append("- sample_count=0 sessions are invalid and must not be used for pruning.")
    return "\n".join(lines).rstrip() + "\n"


def _scope_decision_from_rows(session_rows: list[dict[str, Any]]) -> tuple[str, list[dict[str, Any]]]:
    sample_counts = [int(row.get("sample_count") or 0) for row in session_rows]
    positive_count = sum(1 for count in sample_counts if count > 0)
    if positive_count <= 0:
        return "unusable", [{"code": "no_sessions_have_samples", "sample_count_max": max(sample_counts) if sample_counts else 0}]
    if positive_count == len(sample_counts):
        return "usable", [{"code": "all_sessions_have_samples", "sample_count_min": min(sample_counts) if sample_counts else 0}]
    return "unstable", [
        {
            "code": "mixed_sample_presence",
            "sample_count_min": min(sample_counts) if sample_counts else 0,
            "sample_count_max": max(sample_counts) if sample_counts else 0,
            "sample_count_positive": positive_count,
            "sample_count_total": len(sample_counts),
        }
    ]


def _build_scope_stability_rollup(rows: list[dict[str, Any]]) -> dict[str, Any]:
    rows = sorted(rows, key=lambda row: (row.get("session_scope_id") or "", row.get("random_seed") or 0, row.get("session_id") or ""))
    for row in rows:
        if not isinstance(row, dict):
            continue
        decision = _text(row.get("decision"))
        if not decision:
            decision = "keep" if int(row.get("sample_count") or 0) > 0 else "drop"
        row["decision"] = decision
        row["session_aggregate_decision"] = _text(row.get("session_aggregate_decision"), fallback=decision)
        row["decision_reasons"] = _json_ready(row.get("decision_reasons") or [{"code": f"{decision}_from_scope_rollup"}])
    scope_map: dict[str, dict[str, Any]] = {}
    candidate_scope_gap_reason_counts: dict[str, int] = {}
    candidate_scope_key_mismatch_reason_counts: dict[str, int] = {}
    candidate_in_scope_before_build_count = 0
    candidate_in_scope_after_build_count = 0
    candidate_removed_by_scope_boundary_count = 0
    candidate_rows_before_future_guard = 0
    candidate_rows_after_future_guard = 0
    ret20_joinable_rows = 0
    compare_rows_emitted = 0
    sample_rows_retained = 0
    session_failure_reason_counts: dict[str, int] = {}
    scope_filter_applied_stage_values: set[str] = set()
    key_normalization_mode_values: set[str] = set()
    future_ret20_failure_reason_counts_by_source_mode: dict[str, dict[str, int]] = {}
    for row in rows:
        scope_id = _text(row.get("session_scope_id"), fallback=_text(row.get("session_id"), fallback="session"))
        scope_entry = scope_map.setdefault(
            scope_id,
            {
                "session_scope_id": scope_id,
                "session_rows": [],
                "session_ids": [],
                "random_seeds": [],
                "latest_sort_key": ("", ""),
                "latest_session_id": "",
                "latest_generated_at": "",
                "latest_eval_window_mode": "unknown",
                "latest_eval_window_mode_reason": "unknown",
                "latest_first_zero_stage": "passed",
            },
        )
        scope_entry["session_rows"].append(row)
        session_id = _text(row.get("session_id"))
        if session_id and session_id not in scope_entry["session_ids"]:
            scope_entry["session_ids"].append(session_id)
        random_seed = int(row.get("random_seed") or 0)
        if random_seed not in scope_entry["random_seeds"]:
            scope_entry["random_seeds"].append(random_seed)
        sort_key = (_text(row.get("generated_at")), session_id)
        if sort_key >= scope_entry["latest_sort_key"]:
            scope_entry["latest_sort_key"] = sort_key
            scope_entry["latest_session_id"] = session_id
            scope_entry["latest_generated_at"] = _text(row.get("generated_at"))
            scope_entry["latest_eval_window_mode"] = _text(row.get("eval_window_mode"), fallback="unknown")
            scope_entry["latest_eval_window_mode_reason"] = _text(row.get("eval_window_mode_reason"), fallback="unknown")
            scope_entry["latest_first_zero_stage"] = _text(row.get("first_zero_stage"), fallback=_text(row.get("failure_stage"), fallback="passed"))
        candidate_scope_gap = row.get("candidate_scope_gap_coverage") if isinstance(row.get("candidate_scope_gap_coverage"), dict) else {}
        if candidate_scope_gap:
            candidate_in_scope_before_build_count += int(candidate_scope_gap.get("candidate_in_scope_before_build_count") or 0)
            candidate_in_scope_after_build_count += int(candidate_scope_gap.get("candidate_in_scope_after_build_count") or 0)
            stage = _text(candidate_scope_gap.get("scope_filter_applied_stage"), fallback="")
            if stage:
                scope_filter_applied_stage_values.add(stage)
            reason_counts = candidate_scope_gap.get("candidate_scope_gap_reason_counts") if isinstance(candidate_scope_gap.get("candidate_scope_gap_reason_counts"), dict) else {}
            for reason, count in reason_counts.items():
                key = _text(reason)
                if not key:
                    continue
                candidate_scope_gap_reason_counts[key] = candidate_scope_gap_reason_counts.get(key, 0) + int(count or 0)
        row_future_coverage = row.get("future_ret20_coverage") if isinstance(row.get("future_ret20_coverage"), dict) else {}
        if row_future_coverage:
            candidate_rows_before_future_guard += int(row_future_coverage.get("candidate_rows_before_future_guard") or 0)
            candidate_rows_after_future_guard += int(row_future_coverage.get("candidate_rows_after_future_guard") or 0)
            ret20_joinable_rows += int(row_future_coverage.get("ret20_joinable_rows") or 0)
            compare_rows_emitted += int(row_future_coverage.get("compare_rows_emitted") or 0)
            sample_rows_retained += int(row_future_coverage.get("sample_rows_retained") or 0)
            reason_counts_by_mode = row_future_coverage.get("failure_reason_counts_by_source_mode")
            if isinstance(reason_counts_by_mode, dict):
                for source_mode, nested in reason_counts_by_mode.items():
                    source_mode_key = _text(source_mode, fallback="unknown")
                    if not isinstance(nested, dict):
                        continue
                    bucket = future_ret20_failure_reason_counts_by_source_mode.setdefault(source_mode_key, {})
                    for reason, count in nested.items():
                        key = _text(reason)
                        if not key:
                            continue
                        bucket[key] = bucket.get(key, 0) + int(count or 0)
        session_failure_reason = _text(row.get("session_failure_reason"), fallback="")
        if session_failure_reason:
            session_failure_reason_counts[session_failure_reason] = session_failure_reason_counts.get(session_failure_reason, 0) + 1

    scope_summary: list[dict[str, Any]] = []
    for scope_entry in scope_map.values():
        session_rows = scope_entry.pop("session_rows")
        scope_decision, scope_decision_reasons = _scope_decision_from_rows(session_rows)
        sample_counts = [int(row.get("sample_count") or 0) for row in session_rows]
        first_zero_stage_counts: dict[str, int] = {}
        eval_window_mode_counts: dict[str, int] = {"standard": 0, "fallback": 0, "exploratory": 0, "unknown": 0}
        ret20_source_mode_counts: dict[str, int] = {"precomputed": 0, "derived_from_daily_bars": 0, "unknown": 0}
        future_ret20_failure_reason_counts: dict[str, int] = {}
        future_ret20_join_gap_after_scope_filter_count = 0
        future_ret20_join_gap_reason_counts: dict[str, int] = {}
        scope_candidate_scope_gap_reason_counts: dict[str, int] = {}
        scope_candidate_in_scope_before_build_count = 0
        scope_candidate_in_scope_after_build_count = 0
        scope_candidate_removed_by_scope_boundary_count = 0
        scope_candidate_key_mismatch_reason_counts: dict[str, int] = {}
        scope_key_normalization_mode_values: set[str] = set()
        scope_filter_stage_values: set[str] = set()
        scope_session_failure_reason_counts: dict[str, int] = {}
        for row in session_rows:
            stage = _text(row.get("first_zero_stage"), fallback=_text(row.get("failure_stage"), fallback="passed"))
            first_zero_stage_counts[stage] = first_zero_stage_counts.get(stage, 0) + 1
            mode = _text(row.get("eval_window_mode"), fallback="unknown")
            eval_window_mode_counts[mode if mode in {"standard", "fallback", "exploratory"} else "unknown"] += 1
            ret20_mode = _text(row.get("ret20_source_mode"), fallback="unknown")
            ret20_source_mode_counts[ret20_mode if ret20_mode in {"precomputed", "derived_from_daily_bars"} else "unknown"] += 1
            session_failure_reason = _text(row.get("session_failure_reason"), fallback="")
            if session_failure_reason:
                scope_session_failure_reason_counts[session_failure_reason] = scope_session_failure_reason_counts.get(session_failure_reason, 0) + 1
            row_candidate_scope_gap = row.get("candidate_scope_gap_coverage") if isinstance(row.get("candidate_scope_gap_coverage"), dict) else {}
            if row_candidate_scope_gap:
                scope_candidate_in_scope_before_build_count += int(row_candidate_scope_gap.get("candidate_in_scope_before_build_count") or 0)
                scope_candidate_in_scope_after_build_count += int(row_candidate_scope_gap.get("candidate_in_scope_after_build_count") or 0)
                scope_candidate_removed_by_scope_boundary_count += int(row_candidate_scope_gap.get("candidate_removed_by_scope_boundary_count") or 0)
                stage = _text(row_candidate_scope_gap.get("scope_filter_applied_stage"), fallback="")
                if stage:
                    scope_filter_stage_values.add(stage)
                key_mode = _text(row_candidate_scope_gap.get("key_normalization_mode"), fallback="")
                if key_mode:
                    scope_key_normalization_mode_values.add(key_mode)
                row_candidate_gap_reason_counts = row_candidate_scope_gap.get("candidate_scope_key_mismatch_reason_counts")
                if not isinstance(row_candidate_gap_reason_counts, dict):
                    row_candidate_gap_reason_counts = row_candidate_scope_gap.get("candidate_scope_gap_reason_counts") if isinstance(row_candidate_scope_gap.get("candidate_scope_gap_reason_counts"), dict) else {}
                for reason, count in row_candidate_gap_reason_counts.items():
                    key = _text(reason)
                    if not key:
                        continue
                    scope_candidate_scope_gap_reason_counts[key] = scope_candidate_scope_gap_reason_counts.get(key, 0) + int(count or 0)
                    scope_candidate_key_mismatch_reason_counts[key] = scope_candidate_key_mismatch_reason_counts.get(key, 0) + int(count or 0)
            row_future_counts = row.get("future_ret20_failure_reason_counts") if isinstance(row.get("future_ret20_failure_reason_counts"), dict) else {}
            for reason, count in row_future_counts.items():
                future_ret20_failure_reason_counts[reason] = future_ret20_failure_reason_counts.get(reason, 0) + int(count or 0)
            row_join_gap = row.get("future_ret20_join_gap_coverage") if isinstance(row.get("future_ret20_join_gap_coverage"), dict) else {}
            future_ret20_join_gap_after_scope_filter_count += int(row_join_gap.get("after_scope_filter_count") or 0)
            row_join_gap_reason_counts = row_join_gap.get("reason_counts") if isinstance(row_join_gap.get("reason_counts"), dict) else {}
            for reason, count in row_join_gap_reason_counts.items():
                key = _text(reason)
                if not key:
                    continue
                future_ret20_join_gap_reason_counts[key] = future_ret20_join_gap_reason_counts.get(key, 0) + int(count or 0)
        scope_summary.append(
            {
                "session_scope_id": _text(scope_entry.get("session_scope_id")),
                "decision": scope_decision,
                "session_aggregate_decision": scope_decision,
                "decision_reasons": scope_decision_reasons,
                "session_count": len(session_rows),
                "seed_count": len({int(row.get("random_seed") or 0) for row in session_rows}),
                "session_ids": [str(item) for item in scope_entry.get("session_ids") or [] if str(item).strip()],
                "random_seeds": [int(item) for item in scope_entry.get("random_seeds") or []],
                "sample_count_min": min(sample_counts) if sample_counts else 0,
                "sample_count_max": max(sample_counts) if sample_counts else 0,
                "sample_count_mean": (sum(sample_counts) / float(len(sample_counts))) if sample_counts else 0.0,
                "best_result_present_count": sum(1 for row in session_rows if bool(row.get("best_result_present"))),
                "insufficient_samples_count": sum(1 for row in session_rows if bool(row.get("insufficient_samples"))),
                "keep_count": sum(int(row.get("keep_count") or 0) for row in session_rows),
                "drop_count": sum(int(row.get("drop_count") or 0) for row in session_rows),
                "hold_count": sum(int(row.get("hold_count") or 0) for row in session_rows),
                "eval_window_mode_counts": eval_window_mode_counts,
                "ret20_source_mode_counts": ret20_source_mode_counts,
                "first_zero_stage_counts": first_zero_stage_counts,
                "avg_top5_ret20_mean_delta": _leaderboard_average(sum(float(row.get("top5_ret20_mean_delta") or 0.0) for row in session_rows), len(session_rows)),
                "avg_top10_ret20_mean_delta": _leaderboard_average(sum(float(row.get("top10_ret20_mean_delta") or 0.0) for row in session_rows), len(session_rows)),
                "avg_monthly_capture_delta": _leaderboard_average(sum(float(row.get("monthly_capture_delta") or 0.0) for row in session_rows), len(session_rows)),
                "avg_zero_pass_delta": _leaderboard_average(sum(float(row.get("zero_pass_delta") or 0.0) for row in session_rows), len(session_rows)),
                "avg_worst_regime_delta": _leaderboard_average(sum(float(row.get("worst_regime_delta") or 0.0) for row in session_rows), len(session_rows)),
                "avg_dd_delta": _leaderboard_average(sum(float(row.get("dd_delta") or 0.0) for row in session_rows), len(session_rows)),
                "avg_turnover_delta": _leaderboard_average(sum(float(row.get("turnover_delta") or 0.0) for row in session_rows), len(session_rows)),
                "avg_liquidity_fail_delta": _leaderboard_average(sum(float(row.get("liquidity_fail_delta") or 0.0) for row in session_rows), len(session_rows)),
                "avg_future_ret20_candidate_day_count": _leaderboard_average(sum(float(row.get("future_ret20_candidate_day_count") or 0.0) for row in session_rows), len(session_rows)),
                "avg_candidate_rows_before_future_guard": _leaderboard_average(sum(float(row.get("candidate_rows_before_future_guard") or 0.0) for row in session_rows), len(session_rows)),
                "avg_candidate_rows_after_future_guard": _leaderboard_average(sum(float(row.get("candidate_rows_after_future_guard") or 0.0) for row in session_rows), len(session_rows)),
                "avg_ret20_joinable_rows": _leaderboard_average(sum(float(row.get("ret20_joinable_rows") or 0.0) for row in session_rows), len(session_rows)),
                "avg_compare_rows_emitted": _leaderboard_average(sum(float(row.get("compare_rows_emitted") or 0.0) for row in session_rows), len(session_rows)),
                "avg_sample_rows_retained": _leaderboard_average(sum(float(row.get("sample_rows_retained") or 0.0) for row in session_rows), len(session_rows)),
                "avg_future_ret20_passed_count": _leaderboard_average(sum(float(row.get("future_ret20_passed_count") or 0.0) for row in session_rows), len(session_rows)),
                "avg_future_ret20_guarded_out_count": _leaderboard_average(sum(float(row.get("future_ret20_guarded_out_count") or 0.0) for row in session_rows), len(session_rows)),
                "future_ret20_failure_reason_counts": dict(sorted(future_ret20_failure_reason_counts.items(), key=lambda item: (-item[1], item[0]))),
                "future_ret20_failure_reason_counts_by_source_mode": {
                    source_mode: dict(sorted(reason_counts.items(), key=lambda item: (-item[1], item[0])))
                    for source_mode, reason_counts in sorted(future_ret20_failure_reason_counts_by_source_mode.items())
                },
                "candidate_scope_gap_reason_counts": dict(sorted(scope_candidate_scope_gap_reason_counts.items(), key=lambda item: (-item[1], item[0]))),
                "candidate_scope_key_mismatch_reason_counts": dict(sorted(scope_candidate_key_mismatch_reason_counts.items(), key=lambda item: (-item[1], item[0]))),
                "candidate_in_scope_before_build_count": scope_candidate_in_scope_before_build_count,
                "candidate_in_scope_after_build_count": scope_candidate_in_scope_after_build_count,
                "candidate_removed_by_scope_boundary_count": scope_candidate_removed_by_scope_boundary_count,
                "candidate_rows_before_future_guard": candidate_rows_before_future_guard,
                "candidate_rows_after_future_guard": candidate_rows_after_future_guard,
                "ret20_joinable_rows": ret20_joinable_rows,
                "compare_rows_emitted": compare_rows_emitted,
                "sample_rows_retained": sample_rows_retained,
                "scope_filter_applied_stage": "mixed" if len(scope_filter_stage_values) > 1 else (next(iter(scope_filter_stage_values)) if scope_filter_stage_values else "unknown"),
                "key_normalization_mode": "mixed" if len(scope_key_normalization_mode_values) > 1 else (next(iter(scope_key_normalization_mode_values)) if scope_key_normalization_mode_values else "unknown"),
                "session_failure_reason_counts": dict(sorted(scope_session_failure_reason_counts.items(), key=lambda item: (-item[1], item[0]))),
                "future_ret20_join_gap_coverage": {
                    "after_scope_filter_count": future_ret20_join_gap_after_scope_filter_count,
                    "reason_counts": dict(sorted(future_ret20_join_gap_reason_counts.items(), key=lambda item: (-item[1], item[0]))),
                },
                "future_ret20_code_coverage": {
                    "candidate_guarded_by_last_valid_ret20_date_count": sum(
                        int((row.get("future_ret20_code_coverage") or {}).get("candidate_guarded_by_last_valid_ret20_date_count") or 0)
                        for row in session_rows
                        if isinstance(row.get("future_ret20_code_coverage"), dict)
                    ),
                    "codes_with_any_candidate": sum(
                        int((row.get("future_ret20_code_coverage") or {}).get("codes_with_any_candidate") or 0)
                        for row in session_rows
                        if isinstance(row.get("future_ret20_code_coverage"), dict)
                    ),
                    "codes_with_future_ret20_pass": sum(
                        int((row.get("future_ret20_code_coverage") or {}).get("codes_with_future_ret20_pass") or 0)
                        for row in session_rows
                        if isinstance(row.get("future_ret20_code_coverage"), dict)
                    ),
                    "codes_all_failed_future_ret20": sum(
                        int((row.get("future_ret20_code_coverage") or {}).get("codes_all_failed_future_ret20") or 0)
                        for row in session_rows
                        if isinstance(row.get("future_ret20_code_coverage"), dict)
                    ),
                    "top_failed_codes": [
                        item
                        for row in session_rows
                        if isinstance(row.get("future_ret20_code_coverage"), dict)
                        for item in (row.get("future_ret20_code_coverage") or {}).get("top_failed_codes", [])
                        if isinstance(item, dict)
                    ][:25],
                },
                "latest_session_id": _text(scope_entry.get("latest_session_id")),
                "latest_generated_at": _text(scope_entry.get("latest_generated_at")),
                "latest_eval_window_mode": _text(scope_entry.get("latest_eval_window_mode"), fallback="unknown"),
                "latest_eval_window_mode_reason": _text(scope_entry.get("latest_eval_window_mode_reason"), fallback="unknown"),
                "latest_first_zero_stage": _text(scope_entry.get("latest_first_zero_stage"), fallback="passed"),
            }
        )

    scope_summary = sorted(scope_summary, key=lambda row: (_text(row.get("session_scope_id")), 0 if _text(row.get("decision")) == "usable" else 1 if _text(row.get("decision")) == "unstable" else 2))
    sample_counts = [int(row.get("sample_count") or 0) for row in rows]
    first_zero_stage_counts: dict[str, int] = {}
    for row in rows:
        stage = _text(row.get("first_zero_stage"), fallback=_text(row.get("failure_stage"), fallback="passed"))
        first_zero_stage_counts[stage] = first_zero_stage_counts.get(stage, 0) + 1
    has_insufficient_samples = any(bool(row.get("insufficient_samples")) for row in rows)
    scope_filter_applied_stage = "mixed" if len(scope_filter_applied_stage_values) > 1 else (next(iter(scope_filter_applied_stage_values)) if scope_filter_applied_stage_values else "unknown")
    future_ret20_failure_reason_counts_by_source_mode_overview: dict[str, dict[str, int]] = {}
    for row in rows:
        row_future_by_mode = row.get("future_ret20_failure_reason_counts_by_source_mode") if isinstance(row.get("future_ret20_failure_reason_counts_by_source_mode"), dict) else {}
        for source_mode, nested in row_future_by_mode.items():
            source_mode_key = _text(source_mode, fallback="unknown")
            if not isinstance(nested, dict):
                continue
            bucket = future_ret20_failure_reason_counts_by_source_mode_overview.setdefault(source_mode_key, {})
            for reason, count in nested.items():
                key = _text(reason)
                if not key:
                    continue
                bucket[key] = bucket.get(key, 0) + int(count or 0)
    return {
        "schema_version": SCOPE_STABILITY_ROLLUP_SCHEMA_VERSION,
        "status": "complete" if any(_text(item.get("decision")) == "usable" for item in scope_summary) and not has_insufficient_samples else "invalid",
        "authoritative_rollup_decision": "keep" if any(_text(item.get("decision")) == "usable" for item in scope_summary) and not has_insufficient_samples else "drop",
        "session_meta": {
            "generated_at": _utc_now_iso(),
            "scope_count": len(scope_summary),
            "session_count": len(rows),
            "scope_ids": [str(row.get("session_scope_id")) for row in scope_summary if _text(row.get("session_scope_id"))],
            "random_seeds": [int(row.get("random_seed") or 0) for row in rows],
            "eval_window_mode_counts": {
                "standard": sum(1 for item in rows if _text(item.get("eval_window_mode")) == "standard"),
                "fallback": sum(1 for item in rows if _text(item.get("eval_window_mode")) == "fallback"),
                "unknown": sum(1 for item in rows if _text(item.get("eval_window_mode")) not in {"standard", "fallback"}),
            },
            "ret20_source_mode_counts": {
                "precomputed": sum(1 for item in rows if _text(item.get("ret20_source_mode")) == "precomputed"),
                "derived_from_daily_bars": sum(1 for item in rows if _text(item.get("ret20_source_mode")) == "derived_from_daily_bars"),
                "unknown": sum(1 for item in rows if _text(item.get("ret20_source_mode")) not in {"precomputed", "derived_from_daily_bars"}),
            },
        },
        "overview": {
            "scope_count": len(scope_summary),
            "usable_scope_count": sum(1 for row in scope_summary if _text(row.get("decision")) == "usable"),
            "unstable_scope_count": sum(1 for row in scope_summary if _text(row.get("decision")) == "unstable"),
            "unusable_scope_count": sum(1 for row in scope_summary if _text(row.get("decision")) == "unusable"),
            "session_count": len(rows),
            "sample_count_min": min(sample_counts) if sample_counts else 0,
            "sample_count_max": max(sample_counts) if sample_counts else 0,
            "sample_count_mean": (sum(sample_counts) / float(len(sample_counts))) if sample_counts else 0.0,
            "best_result_present_count": sum(1 for row in rows if bool(row.get("best_result_present"))),
            "insufficient_samples_count": sum(1 for row in rows if bool(row.get("insufficient_samples"))),
            "insufficient_samples": has_insufficient_samples,
            "first_zero_stage_counts": first_zero_stage_counts,
            "candidate_scope_gap_reason_counts": dict(sorted(candidate_scope_gap_reason_counts.items(), key=lambda item: (-item[1], item[0]))),
            "candidate_scope_key_mismatch_reason_counts": dict(sorted(candidate_scope_key_mismatch_reason_counts.items(), key=lambda item: (-item[1], item[0]))),
            "candidate_in_scope_before_build_count": candidate_in_scope_before_build_count,
            "candidate_in_scope_after_build_count": candidate_in_scope_after_build_count,
            "candidate_removed_by_scope_boundary_count": candidate_removed_by_scope_boundary_count,
            "future_ret20_failure_reason_counts_by_source_mode": dict(sorted(
                (
                    source_mode,
                    dict(sorted(reason_counts.items(), key=lambda item: (-item[1], item[0])))
                )
                for source_mode, reason_counts in future_ret20_failure_reason_counts_by_source_mode_overview.items()
            )),
            "session_failure_reason_counts": dict(sorted(session_failure_reason_counts.items(), key=lambda item: (-item[1], item[0]))),
            "scope_filter_applied_stage": scope_filter_applied_stage,
            "key_normalization_mode": "mixed" if len(key_normalization_mode_values) > 1 else (next(iter(key_normalization_mode_values)) if key_normalization_mode_values else "unknown"),
        },
        "scope_summary": scope_summary,
        "session_rows": rows,
    }


def _format_scope_stability_rollup_markdown(rollup: dict[str, Any]) -> str:
    session_meta = rollup.get("session_meta") if isinstance(rollup.get("session_meta"), dict) else {}
    overview = rollup.get("overview") if isinstance(rollup.get("overview"), dict) else {}
    scope_rows = rollup.get("scope_summary") if isinstance(rollup.get("scope_summary"), list) else []
    session_rows = rollup.get("session_rows") if isinstance(rollup.get("session_rows"), list) else []
    lines: list[str] = []
    lines.append("# TRADEX Scope Stability Rollup")
    lines.append("")
    lines.append(f"- generated_at: `{_text(session_meta.get('generated_at'))}`")
    lines.append(f"- scope_count: `{int(session_meta.get('scope_count') or 0)}`")
    lines.append(f"- session_count: `{int(session_meta.get('session_count') or 0)}`")
    scope_ids = [str(item) for item in (session_meta.get("scope_ids") or []) if str(item).strip()]
    lines.append(f"- scope_ids: `{', '.join(scope_ids) if scope_ids else 'none'}`")
    counts = session_meta.get("eval_window_mode_counts") if isinstance(session_meta.get("eval_window_mode_counts"), dict) else {}
    lines.append(
        f"- eval_window_mode_counts: standard=`{int(counts.get('standard') or 0)}`, fallback=`{int(counts.get('fallback') or 0)}`, exploratory=`{int(counts.get('exploratory') or 0)}`, unknown=`{int(counts.get('unknown') or 0)}`"
    )
    ret20_counts = session_meta.get("ret20_source_mode_counts") if isinstance(session_meta.get("ret20_source_mode_counts"), dict) else {}
    lines.append(
        f"- ret20_source_mode_counts: precomputed=`{int(ret20_counts.get('precomputed') or 0)}`, derived=`{int(ret20_counts.get('derived_from_daily_bars') or 0)}`, unknown=`{int(ret20_counts.get('unknown') or 0)}`"
    )
    lines.append(f"- scope_filter_applied_stage: `{_text(overview.get('scope_filter_applied_stage'), fallback='unknown')}`")
    lines.append(f"- key_normalization_mode: `{_text(overview.get('key_normalization_mode'), fallback='unknown')}`")
    lines.append(
        "- future_ret20 stage counts: before_guard=`{before}` / after_guard=`{after}` / joinable=`{joinable}` / compare_emitted=`{compare}` / retained=`{retained}`".format(
            before=int(overview.get("candidate_rows_before_future_guard") or 0),
            after=int(overview.get("candidate_rows_after_future_guard") or 0),
            joinable=int(overview.get("ret20_joinable_rows") or 0),
            compare=int(overview.get("compare_rows_emitted") or 0),
            retained=int(overview.get("sample_rows_retained") or 0),
        )
    )
    candidate_gap_counts = overview.get("candidate_scope_gap_reason_counts") if isinstance(overview.get("candidate_scope_gap_reason_counts"), dict) else {}
    if candidate_gap_counts:
        lines.append(f"- candidate_scope_gap_reason_counts: `{json.dumps(_json_ready(candidate_gap_counts), ensure_ascii=False, sort_keys=True)}`")
    candidate_key_counts = overview.get("candidate_scope_key_mismatch_reason_counts") if isinstance(overview.get("candidate_scope_key_mismatch_reason_counts"), dict) else {}
    if candidate_key_counts:
        lines.append(f"- candidate_scope_key_mismatch_reason_counts: `{json.dumps(_json_ready(candidate_key_counts), ensure_ascii=False, sort_keys=True)}`")
    future_ret20_counts_by_mode = overview.get("future_ret20_failure_reason_counts_by_source_mode") if isinstance(overview.get("future_ret20_failure_reason_counts_by_source_mode"), dict) else {}
    if future_ret20_counts_by_mode:
        lines.append(f"- future_ret20_failure_reason_counts_by_source_mode: `{json.dumps(_json_ready(future_ret20_counts_by_mode), ensure_ascii=False, sort_keys=True)}`")
    lines.append(
        f"- candidate_in_scope_before_build_count: `{int(overview.get('candidate_in_scope_before_build_count') or 0)}` / candidate_in_scope_after_build_count: `{int(overview.get('candidate_in_scope_after_build_count') or 0)}`"
    )
    lines.append(f"- candidate_removed_by_scope_boundary_count: `{int(overview.get('candidate_removed_by_scope_boundary_count') or 0)}`")
    session_failure_counts = overview.get("session_failure_reason_counts") if isinstance(overview.get("session_failure_reason_counts"), dict) else {}
    if session_failure_counts:
        lines.append(f"- session_failure_reason_counts: `{json.dumps(_json_ready(session_failure_counts), ensure_ascii=False, sort_keys=True)}`")
    lines.append("")
    lines.append("## Overview")
    lines.append("")
    lines.append("| usable | unstable | unusable | sessions | sample_min | sample_max | sample_mean |")
    lines.append("| ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    lines.append(
        "| {usable} | {unstable} | {unusable} | {sessions} | {sample_min} | {sample_max} | {sample_mean:.2f} |".format(
            usable=int(overview.get("usable_scope_count") or 0),
            unstable=int(overview.get("unstable_scope_count") or 0),
            unusable=int(overview.get("unusable_scope_count") or 0),
            sessions=int(overview.get("session_count") or 0),
            sample_min=int(overview.get("sample_count_min") or 0),
            sample_max=int(overview.get("sample_count_max") or 0),
            sample_mean=float(overview.get("sample_count_mean") or 0.0),
        )
    )
    lines.append("")
    lines.append("## Scope Summary")
    lines.append("")
    lines.append("| scope | decision | sessions | sample_min | sample_max | sample_mean | first_zero_stage |")
    lines.append("| --- | --- | ---: | ---: | ---: | ---: | --- |")
    for row in scope_rows:
        if not isinstance(row, dict):
            continue
        lines.append(
            "| {scope} | {decision} | {sessions} | {sample_min} | {sample_max} | {sample_mean:.2f} | {first_zero} |".format(
                scope=_text(row.get("session_scope_id")),
                decision=_text(row.get("decision")),
                sessions=int(row.get("session_count") or 0),
                sample_min=int(row.get("sample_count_min") or 0),
                sample_max=int(row.get("sample_count_max") or 0),
                sample_mean=float(row.get("sample_count_mean") or 0.0),
                first_zero=_text(row.get("latest_first_zero_stage"), fallback="passed"),
            )
        )
        lines.append(f"- decision_reasons: `{json.dumps(_json_ready(row.get('decision_reasons') or []), ensure_ascii=False)}`")
        lines.append(f"- first_zero_stage_counts: `{json.dumps(_json_ready(row.get('first_zero_stage_counts') or {}), ensure_ascii=False, sort_keys=True)}`")
        lines.append(f"- eval_window_mode_counts: `{json.dumps(_json_ready(row.get('eval_window_mode_counts') or {}), ensure_ascii=False, sort_keys=True)}`")
        lines.append(f"- ret20_source_mode_counts: `{json.dumps(_json_ready(row.get('ret20_source_mode_counts') or {}), ensure_ascii=False, sort_keys=True)}`")
        lines.append(f"- scope_filter_applied_stage: `{_text(row.get('scope_filter_applied_stage'), fallback='unknown')}`")
        lines.append(f"- key_normalization_mode: `{_text(row.get('key_normalization_mode'), fallback='unknown')}`")
        lines.append(
            "- future_ret20 stage counts: before_guard=`{before}` / after_guard=`{after}` / joinable=`{joinable}` / compare_emitted=`{compare}` / retained=`{retained}`".format(
                before=int(row.get("candidate_rows_before_future_guard") or 0),
                after=int(row.get("candidate_rows_after_future_guard") or 0),
                joinable=int(row.get("ret20_joinable_rows") or 0),
                compare=int(row.get("compare_rows_emitted") or 0),
                retained=int(row.get("sample_rows_retained") or 0),
            )
        )
        candidate_gap_counts = row.get("candidate_scope_gap_reason_counts") if isinstance(row.get("candidate_scope_gap_reason_counts"), dict) else {}
        if candidate_gap_counts:
            lines.append(f"- candidate_scope_gap_reason_counts: `{json.dumps(_json_ready(candidate_gap_counts), ensure_ascii=False, sort_keys=True)}`")
        candidate_key_counts = row.get("candidate_scope_key_mismatch_reason_counts") if isinstance(row.get("candidate_scope_key_mismatch_reason_counts"), dict) else {}
        if candidate_key_counts:
            lines.append(f"- candidate_scope_key_mismatch_reason_counts: `{json.dumps(_json_ready(candidate_key_counts), ensure_ascii=False, sort_keys=True)}`")
        future_ret20_counts_by_mode = row.get("future_ret20_failure_reason_counts_by_source_mode") if isinstance(row.get("future_ret20_failure_reason_counts_by_source_mode"), dict) else {}
        if future_ret20_counts_by_mode:
            lines.append(f"- future_ret20_failure_reason_counts_by_source_mode: `{json.dumps(_json_ready(future_ret20_counts_by_mode), ensure_ascii=False, sort_keys=True)}`")
        session_failure_counts = row.get("session_failure_reason_counts") if isinstance(row.get("session_failure_reason_counts"), dict) else {}
        if session_failure_counts:
            lines.append(f"- session_failure_reason_counts: `{json.dumps(_json_ready(session_failure_counts), ensure_ascii=False, sort_keys=True)}`")
        lines.append(
            "- candidate_in_scope_before_build_count=`{before}` / candidate_in_scope_after_build_count=`{after}`".format(
                before=int(row.get("candidate_in_scope_before_build_count") or 0),
                after=int(row.get("candidate_in_scope_after_build_count") or 0),
            )
        )
        lines.append(f"- candidate_removed_by_scope_boundary_count: `{int(row.get('candidate_removed_by_scope_boundary_count') or 0)}`")
        lines.append(
            "- future_ret20: candidate_day_count=`{candidate}`, passed_count=`{passed}`, guarded_out_count=`{guarded}`".format(
                candidate=int(row.get("future_ret20_candidate_day_count") or 0),
                passed=int(row.get("future_ret20_passed_count") or 0),
                guarded=int(row.get("future_ret20_guarded_out_count") or 0),
            )
        )
        future_counts = row.get("future_ret20_failure_reason_counts") if isinstance(row.get("future_ret20_failure_reason_counts"), dict) else {}
        if future_counts:
            lines.append(f"- future_ret20_failure_reason_counts: `{json.dumps(_json_ready(future_counts), ensure_ascii=False, sort_keys=True)}`")
        join_gap_coverage = row.get("future_ret20_join_gap_coverage") if isinstance(row.get("future_ret20_join_gap_coverage"), dict) else {}
        if join_gap_coverage:
            lines.append(f"- future_ret20_join_gap_coverage: `{json.dumps(_json_ready(join_gap_coverage), ensure_ascii=False, sort_keys=True)}`")
    lines.append("")
    lines.append("## Session Rows")
    lines.append("")
    lines.append("| scope | seed | mode | sample | best | first_zero | top5Δ | worstΔ | ddΔ | turnoverΔ | liquidityΔ |")
    lines.append("| --- | ---: | --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: |")
    for row in session_rows:
        if not isinstance(row, dict):
            continue
        lines.append(
            "| {scope} | {seed} | {mode} | {sample} | {best} | {first_zero} | {top5:.4f} | {worst:.4f} | {dd:.4f} | {turnover:.4f} | {liquidity:.4f} |".format(
                scope=_text(row.get("session_scope_id")),
                seed=int(row.get("random_seed") or 0),
                mode=_text(row.get("eval_window_mode"), fallback="unknown"),
                sample=int(row.get("sample_count") or 0),
                best="yes" if bool(row.get("best_result_present")) else "no",
                first_zero=_text(row.get("first_zero_stage"), fallback=_text(row.get("failure_stage"), fallback="passed")),
                top5=float(row.get("top5_ret20_mean_delta") or 0.0),
                worst=float(row.get("worst_regime_delta") or 0.0),
                dd=float(row.get("dd_delta") or 0.0),
                turnover=float(row.get("turnover_delta") or 0.0),
                liquidity=float(row.get("liquidity_fail_delta") or 0.0),
            )
        )
        if row.get("candidate_scope_gap_reason_counts"):
            lines.append(f"- candidate_scope_gap_reason_counts: `{json.dumps(_json_ready(row.get('candidate_scope_gap_reason_counts')), ensure_ascii=False, sort_keys=True)}`")
        lines.append(
            "- future_ret20: candidate_day_count=`{candidate}`, passed_count=`{passed}`, guarded_out_count=`{guarded}`".format(
                candidate=int(row.get("future_ret20_candidate_day_count") or 0),
                passed=int(row.get("future_ret20_passed_count") or 0),
                guarded=int(row.get("future_ret20_guarded_out_count") or 0),
            )
        )
        future_counts = row.get("future_ret20_failure_reason_counts") if isinstance(row.get("future_ret20_failure_reason_counts"), dict) else {}
        if future_counts:
            lines.append(f"- future_ret20_failure_reason_counts: `{json.dumps(_json_ready(future_counts), ensure_ascii=False, sort_keys=True)}`")
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append(f"- legacy analysis env must be `0` for research runs (`{LEGACY_ANALYSIS_DISABLE_ENV}`).")
    lines.append(f"- standard window min days: `{tradex.TRADEX_STANDARD_EVAL_WINDOW_MIN_TRADING_DAYS}`")
    lines.append(f"- fallback window min days: `{tradex.TRADEX_RESEARCH_FALLBACK_EVAL_WINDOW_MIN_TRADING_DAYS}`")
    lines.append("- sample_count=0 or scope_decision != usable sessions must not be used for pruning.")
    return "\n".join(lines).rstrip() + "\n"


def _write_scope_stability_rollup_artifacts(session_rows: list[dict[str, Any]]) -> tuple[Path, Path, dict[str, Any]]:
    rollup = _build_scope_stability_rollup(session_rows)
    rollup_path = _scope_stability_rollup_file()
    report_path = _scope_stability_rollup_report_file()
    rollup["rollup_path"] = str(rollup_path)
    rollup["report_path"] = str(report_path)
    validate_scope_rollup_artifact(rollup)
    _write_json(rollup_path, rollup)
    _verify_json_roundtrip(rollup_path, rollup, artifact_name="scope_stability_rollup")
    report_path.write_text(_format_scope_stability_rollup_markdown(rollup), encoding="utf-8")
    return rollup_path, report_path, rollup


def _ensure_scope_stability_rollup_artifacts(
    session_rows: list[dict[str, Any]],
    *,
    force: bool = False,
) -> tuple[Path, Path, dict[str, Any]]:
    rollup_path = _scope_stability_rollup_file()
    report_path = _scope_stability_rollup_report_file()
    existing_payload = _read_json_file(rollup_path) if rollup_path.exists() else None
    if not force and existing_payload is not None and report_path.exists():
        return rollup_path, report_path, existing_payload
    rollup = existing_payload if isinstance(existing_payload, dict) else _build_scope_stability_rollup(session_rows)
    if not isinstance(existing_payload, dict):
        rollup["rollup_path"] = str(rollup_path)
        rollup["report_path"] = str(report_path)
        validate_scope_rollup_artifact(rollup)
    if force or existing_payload is None:
        _write_json(rollup_path, rollup)
        _verify_json_roundtrip(rollup_path, rollup, artifact_name="scope_stability_rollup")
    if force or not report_path.exists():
        report_path.write_text(_format_scope_stability_rollup_markdown(rollup), encoding="utf-8")
    return rollup_path, report_path, rollup


def _write_stability_rollup_artifacts(session_rows: list[dict[str, Any]]) -> tuple[Path, Path, dict[str, Any]]:
    rollup = _build_stability_rollup(session_rows)
    rollup_path = _stability_rollup_file()
    report_path = _stability_rollup_report_file()
    rollup["rollup_path"] = str(rollup_path)
    rollup["report_path"] = str(report_path)
    _write_json(rollup_path, rollup)
    _verify_json_roundtrip(rollup_path, rollup, artifact_name="stability_rollup")
    report_path.write_text(_format_stability_rollup_markdown(rollup), encoding="utf-8")
    return rollup_path, report_path, rollup


def _ensure_stability_rollup_artifacts(
    session_rows: list[dict[str, Any]],
    *,
    force: bool = False,
) -> tuple[Path, Path, dict[str, Any]]:
    rollup_path = _stability_rollup_file()
    report_path = _stability_rollup_report_file()
    existing_payload = _read_json_file(rollup_path) if rollup_path.exists() else None
    if not force and existing_payload is not None and report_path.exists():
        return rollup_path, report_path, existing_payload
    rollup = existing_payload if isinstance(existing_payload, dict) else _build_stability_rollup(session_rows)
    if force or existing_payload is None:
        rollup["rollup_path"] = str(rollup_path)
        rollup["report_path"] = str(report_path)
        _write_json(rollup_path, rollup)
        _verify_json_roundtrip(rollup_path, rollup, artifact_name="stability_rollup")
    if force or not report_path.exists():
        report_path.write_text(_format_stability_rollup_markdown(rollup), encoding="utf-8")
    return rollup_path, report_path, rollup


def _classify_session_failure_reason(error: Exception | str, *, context: str) -> str:
    text = _text(error).lower()
    context_text = _text(context).lower()
    if not text:
        return "unknown_session_failure"
    if "confirmed universe is empty" in text or "get_all_codes" in text or "universe is empty" in text:
        return "scope_resolution_failed"
    if "ret20_source_mode" in text and ("invalid" in text or "contract" in text or "mixed" in text):
        return "ret20_mode_contract_failed"
    if "missing session state after completed run" in text or "missing session state" in text or "session state is unreadable" in text:
        return "compare_artifact_incomplete"
    if "artifact_consistency" in text or "artifact inconsistent" in text or "inconsistent session" in text or ("session_leaderboard_rollup" in text and "mismatch" in text):
        return "leaderboard_consistency_failed"
    if "regime" in text and ("window" in text or "segment" in text or "coverage" in text):
        return "regime_window_unavailable"
    if "compare" in text and ("incomplete" in text or "missing" in text or "unreadable" in text):
        return "compare_artifact_incomplete"
    if "duplicate candidate method prohibited" in text:
        return "leaderboard_consistency_failed"
    if "failed to create family" in text or "failed to materialize champion baseline" in text:
        return "compare_artifact_incomplete"
    if "manifest" in text and ("mismatch" in text or "different" in text):
        return "compare_artifact_incomplete"
    if context_text and "scope" in context_text and "window" in text:
        return "regime_window_unavailable"
    return "unknown_session_failure"


def _build_stability_failure_row(*, session_id: str, random_seed: int, error: Exception | str) -> dict[str, Any]:
    session_failure_reason = _classify_session_failure_reason(error, context="tradex stability sweep")
    return {
        "session_id": session_id,
        "random_seed": int(random_seed),
        "generated_at": _utc_now_iso(),
        "eval_window_mode": "unknown",
        "eval_window_mode_reason": session_failure_reason,
        "ret20_source_mode": "unknown",
        "ret20_source_mode_reason": session_failure_reason,
        "session_failure_reason": session_failure_reason,
        "session_failure_reason_detail": _text(error),
        "decision": "drop",
        "session_aggregate_decision": "drop",
        "decision_reasons": [{"code": session_failure_reason, "status": "fail"}],
        "first_zero_stage": session_failure_reason,
        "failure_stage": session_failure_reason,
        "sample_count": 0,
        "best_result_present": False,
        "keep_count": 0,
        "drop_count": 0,
        "hold_count": 0,
        "top5_ret20_mean_delta": 0.0,
        "top10_ret20_mean_delta": 0.0,
        "monthly_capture_delta": 0.0,
        "zero_pass_delta": 0.0,
        "worst_regime_delta": 0.0,
        "dd_delta": 0.0,
        "turnover_delta": 0.0,
        "liquidity_fail_delta": 0.0,
        "future_ret20_candidate_day_count": 0,
        "candidate_rows_before_future_guard": 0,
        "candidate_rows_after_future_guard": 0,
        "ret20_joinable_rows": 0,
        "compare_rows_emitted": 0,
        "sample_rows_retained": 0,
        "future_ret20_passed_count": 0,
        "future_ret20_guarded_out_count": 0,
        "future_ret20_failure_reason_counts": {},
        "future_ret20_failure_reason_counts_by_source_mode": {},
        "future_ret20_failure_details": [],
        "future_ret20_join_gap_coverage": {
            "after_scope_filter_count": 0,
            "reason_counts": {},
            "examples": [],
            "candidate_rows_before_scope_filter": 0,
            "candidate_rows_after_scope_filter": 0,
            "future_rows_before_scope_filter": 0,
            "future_rows_after_scope_filter": 0,
            "joinable_code_date_pairs_before_scope": 0,
            "joinable_code_date_pairs_after_scope": 0,
        },
        "candidate_scope_gap_reason_counts": {},
        "candidate_in_scope_before_build_count": 0,
        "candidate_in_scope_after_build_count": 0,
        "future_ret20_code_coverage": {
            "candidate_guarded_by_last_valid_ret20_date_count": 0,
            "codes_with_any_candidate": 0,
            "codes_with_future_ret20_pass": 0,
            "codes_all_failed_future_ret20": 0,
            "top_failed_codes": [],
        },
        "insufficient_samples": True,
        "family_leaderboard_path": str(_session_family_leaderboard_file(session_id)),
        "compare_path": str(_session_compare_file(session_id)),
        "error": str(error),
    }


def run_tradex_stability_sweep(
    *,
    session_id: str,
    random_seeds: list[int] | tuple[int, ...] = STABILITY_SWEEP_DEFAULT_SEEDS,
    universe_size: int = DEFAULT_UNIVERSE_SIZE,
    max_candidates_per_family: int = DEFAULT_MAX_CANDIDATES_PER_FAMILY,
    session_scope_id: str | None = None,
    ret20_source_mode: str = tradex.TRADEX_RET20_SOURCE_MODE_PRECOMPUTED,
) -> dict[str, Any]:
    _require_legacy_analysis_enabled(context="tradex stability sweep")
    seed_values = [int(seed) for seed in random_seeds]
    scope_id = _text(session_scope_id, fallback=session_id)
    session_rows: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    with tradex.use_research_execution_context(tradex.ResearchExecutionContext()):
        for seed in seed_values:
            seed_session_id = f"{_slug(session_id)}-seed-{int(seed)}"
            try:
                run_tradex_research_session(
                    session_id=seed_session_id,
                    random_seed=int(seed),
                    universe_size=int(universe_size),
                    max_candidates_per_family=int(max_candidates_per_family),
                    session_scope_id=scope_id,
                    ret20_source_mode=ret20_source_mode,
                    finalize_shared_rollups=False,
                )
                session_state = _load_session_state(seed_session_id)
                if not isinstance(session_state, dict):
                    raise RuntimeError(f"missing session state after completed run: {seed_session_id}")
                session_rows.append(_stability_session_row(session_state))
            except Exception as exc:
                failure_row = _build_stability_failure_row(session_id=seed_session_id, random_seed=int(seed), error=exc)
                session_rows.append(failure_row)
                failures.append(failure_row)
    _ensure_session_leaderboard_rollup_artifacts()
    rollup_path, report_path, rollup = _ensure_stability_rollup_artifacts(session_rows)
    rollup["rollup_path"] = str(rollup_path)
    rollup["report_path"] = str(report_path)
    _write_json(rollup_path, rollup)
    report_path.write_text(_format_stability_rollup_markdown(rollup), encoding="utf-8")
    if failures or any(int(row.get("sample_count") or 0) <= 0 for row in session_rows):
        rollup["status"] = "invalid"
        rollup["failure_count"] = len(failures)
        _write_json(rollup_path, rollup)
        _verify_json_roundtrip(rollup_path, rollup, artifact_name="stability_rollup")
        report_path.write_text(_format_stability_rollup_markdown(rollup), encoding="utf-8")
        raise RuntimeError(
            "stability sweep incomplete: "
            + json.dumps(
                {
                    "session_id": session_id,
                    "failure_count": len(failures),
                    "session_rows": [
                        {
                            "session_id": row.get("session_id"),
                            "random_seed": row.get("random_seed"),
                            "sample_count": row.get("sample_count"),
                            "error": row.get("error"),
                        }
                        for row in session_rows
                        if int(row.get("sample_count") or 0) <= 0 or row.get("error")
                    ],
                },
                ensure_ascii=False,
                sort_keys=True,
            )
    )
    return rollup


def _build_scope_stability_failure_row(*, session_id: str, session_scope_id: str, random_seed: int, error: Exception | str) -> dict[str, Any]:
    session_failure_reason = _classify_session_failure_reason(error, context="tradex scope stability sweep")
    return {
        "session_id": session_id,
        "session_scope_id": _text(session_scope_id, fallback="session"),
        "random_seed": int(random_seed),
        "generated_at": _utc_now_iso(),
        "eval_window_mode": "unknown",
        "eval_window_mode_reason": session_failure_reason,
        "ret20_source_mode": "unknown",
        "ret20_source_mode_reason": session_failure_reason,
        "session_failure_reason": session_failure_reason,
        "session_failure_reason_detail": _text(error),
        "first_zero_stage": session_failure_reason,
        "failure_stage": session_failure_reason,
        "confirmed_universe_count": 0,
        "probe_selection_count": 0,
        "candidate_rows_built_count": 0,
        "eligible_candidate_count": 0,
        "ret20_computable_count": 0,
        "compare_row_count": 0,
        "sample_rows_retained_count": 0,
        "candidate_scope_gap_reason_counts": {},
        "candidate_in_scope_before_build_count": 0,
        "candidate_in_scope_after_build_count": 0,
        "future_ret20_candidate_day_count": 0,
        "candidate_rows_before_future_guard": 0,
        "candidate_rows_after_future_guard": 0,
        "ret20_joinable_rows": 0,
        "compare_rows_emitted": 0,
        "sample_rows_retained": 0,
        "future_ret20_passed_count": 0,
        "future_ret20_guarded_out_count": 0,
        "future_ret20_failure_reason_counts": {},
        "future_ret20_failure_reason_counts_by_source_mode": {},
        "future_ret20_failure_details": [],
        "future_ret20_join_gap_coverage": {
            "after_scope_filter_count": 0,
            "reason_counts": {},
            "examples": [],
            "candidate_rows_before_scope_filter": 0,
            "candidate_rows_after_scope_filter": 0,
            "future_rows_before_scope_filter": 0,
            "future_rows_after_scope_filter": 0,
            "joinable_code_date_pairs_before_scope": 0,
            "joinable_code_date_pairs_after_scope": 0,
        },
        "sample_count": 0,
        "best_result_present": False,
        "keep_count": 0,
        "drop_count": 0,
        "hold_count": 0,
        "top5_ret20_mean_delta": 0.0,
        "top10_ret20_mean_delta": 0.0,
        "monthly_capture_delta": 0.0,
        "zero_pass_delta": 0.0,
        "worst_regime_delta": 0.0,
        "dd_delta": 0.0,
        "turnover_delta": 0.0,
        "liquidity_fail_delta": 0.0,
        "insufficient_samples": True,
        "family_leaderboard_path": str(_session_family_leaderboard_file(session_id)),
        "compare_path": str(_session_compare_file(session_id)),
        "error": str(error),
    }


def run_tradex_scope_stability_sweep(
    *,
    session_id: str,
    session_scope_ids: list[str] | tuple[str, ...],
    random_seeds: list[int] | tuple[int, ...] = STABILITY_SWEEP_DEFAULT_SEEDS,
    universe_size: int = DEFAULT_UNIVERSE_SIZE,
    max_candidates_per_family: int = DEFAULT_MAX_CANDIDATES_PER_FAMILY,
    ret20_source_mode: str = tradex.TRADEX_RET20_SOURCE_MODE_PRECOMPUTED,
) -> dict[str, Any]:
    _require_legacy_analysis_enabled(context="tradex scope stability sweep")
    seed_values = [int(seed) for seed in random_seeds]
    scope_values = [_text(item) for item in session_scope_ids if _text(item)]
    if not scope_values:
        scope_values = [_text(session_id, fallback="session")]
    session_rows: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    with tradex.use_research_execution_context(tradex.ResearchExecutionContext()):
        for scope_id in scope_values:
            for seed in seed_values:
                seed_session_id = _scope_session_id(session_id, scope_id, int(seed))
                try:
                    run_tradex_research_session(
                        session_id=seed_session_id,
                        random_seed=int(seed),
                        universe_size=int(universe_size),
                        max_candidates_per_family=int(max_candidates_per_family),
                        session_scope_id=scope_id,
                        ret20_source_mode=ret20_source_mode,
                        finalize_shared_rollups=False,
                    )
                    session_state = _load_session_state(seed_session_id)
                    if not isinstance(session_state, dict):
                        raise RuntimeError(f"missing session state after completed run: {seed_session_id}")
                    session_rows.append(_stability_session_row(session_state))
                except Exception as exc:
                    failure_row = _build_scope_stability_failure_row(
                        session_id=seed_session_id,
                        session_scope_id=scope_id,
                        random_seed=int(seed),
                        error=exc,
                    )
                    session_rows.append(failure_row)
                    failures.append(failure_row)
    _ensure_session_leaderboard_rollup_artifacts()
    rollup_path, report_path, rollup = _ensure_scope_stability_rollup_artifacts(session_rows)
    rollup["rollup_path"] = str(rollup_path)
    rollup["report_path"] = str(report_path)
    if failures:
        rollup["status"] = "invalid"
        rollup["failure_count"] = len(failures)
        _write_json(rollup_path, rollup)
        _verify_json_roundtrip(rollup_path, rollup, artifact_name="scope_stability_rollup")
        report_path.write_text(_format_scope_stability_rollup_markdown(rollup), encoding="utf-8")
    return rollup


def _render_session_report(session_state: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# TRADEX Research Session")
    lines.append("")
    lines.append(f"- session_id: `{session_state.get('session_id')}`")
    lines.append(f"- session_scope_id: `{_text(session_state.get('session_scope_id'), fallback=_text(session_state.get('session_id')))}`")
    lines.append(f"- random_seed: `{session_state.get('random_seed')}`")
    lines.append(f"- manifest_hash: `{session_state.get('manifest_hash')}`")
    lines.append(f"- eval_window_mode: `{_text(session_state.get('eval_window_mode'), fallback='unknown')}`")
    lines.append(f"- eval_window_mode_reason: `{_text(session_state.get('eval_window_mode_reason'), fallback='unknown')}`")
    lines.append(f"- ret20_source_mode: `{_text(session_state.get('ret20_source_mode'), fallback='unknown')}`")
    lines.append(f"- ret20_source_mode_reason: `{_text(session_state.get('ret20_source_mode_reason'), fallback='unknown')}`")
    lines.append(f"- eval_window_mode_standard_windows: `{int(session_state.get('eval_window_mode_standard_window_count') or 0)}`")
    lines.append(f"- eval_window_mode_fallback_windows: `{int(session_state.get('eval_window_mode_fallback_window_count') or 0)}`")
    lines.append(f"- evaluation_window_min_days_standard: `{tradex.TRADEX_STANDARD_EVAL_WINDOW_MIN_TRADING_DAYS}`")
    lines.append(f"- evaluation_window_min_days_used: `{tradex.TRADEX_RESEARCH_FALLBACK_EVAL_WINDOW_MIN_TRADING_DAYS}`")
    coverage = session_state.get("coverage_waterfall") if isinstance(session_state.get("coverage_waterfall"), dict) else {}
    if coverage:
        lines.append("")
        lines.append("## Coverage")
        lines.append("")
        lines.append("| confirmed universe | probe selection | candidate rows | eligible | ret20 computable | compare rows | sample rows | sample count | insufficient |")
        lines.append("| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |")
        lines.append(
            "| {confirmed} | {probe_selection} | {candidate_rows} | {eligible} | {ret20} | {compare_rows} | {sample_rows} | {sample} | {insufficient} |".format(
                confirmed=int(coverage.get("confirmed_universe_count") or 0),
                probe_selection=int(coverage.get("probe_selection_count") or coverage.get("probe_candidate_count") or 0),
                candidate_rows=int(coverage.get("candidate_rows_built_count") or coverage.get("probe_candidate_count") or 0),
                eligible=int(coverage.get("eligible_candidate_count") or 0),
                ret20=int(coverage.get("ret20_computable_count") or 0),
                compare_rows=int(coverage.get("compare_row_count") or 0),
                sample_rows=int(coverage.get("sample_rows_retained_count") or 0),
                sample=int(coverage.get("sample_count") or 0),
                insufficient="true" if bool(coverage.get("insufficient_samples")) else "false",
            )
        )
        lines.append(
            "- future_ret20 stage counts: before_guard=`{before}` / after_guard=`{after}` / joinable=`{joinable}` / compare_emitted=`{compare}` / retained=`{retained}`".format(
                before=int(coverage.get("candidate_rows_before_future_guard") or 0),
                after=int(coverage.get("candidate_rows_after_future_guard") or 0),
                joinable=int(coverage.get("ret20_joinable_rows") or 0),
                compare=int(coverage.get("compare_rows_emitted") or 0),
                retained=int(coverage.get("sample_rows_retained") or 0),
            )
        )
        lines.append(f"- first_zero_stage: `{_text(coverage.get('first_zero_stage'), fallback=_text(coverage.get('failure_stage'), fallback='passed'))}`")
        lines.append(f"- failure_stage: `{_text(coverage.get('failure_stage'), fallback='passed')}`")
        future_ret20_counts = coverage.get("future_ret20_failure_reason_counts") if isinstance(coverage.get("future_ret20_failure_reason_counts"), dict) else {}
        future_ret20_counts_by_mode = coverage.get("future_ret20_failure_reason_counts_by_source_mode") if isinstance(coverage.get("future_ret20_failure_reason_counts_by_source_mode"), dict) else {}
        source_coverage = coverage.get("future_ret20_source_coverage") if isinstance(coverage.get("future_ret20_source_coverage"), dict) else {}
        join_gap_coverage = coverage.get("future_ret20_join_gap_coverage") if isinstance(coverage.get("future_ret20_join_gap_coverage"), dict) else {}
        lines.append(
            "- future_ret20: candidate_day_count=`{candidate}`, passed_count=`{passed}`, guarded_out_count=`{guarded}`".format(
                candidate=int(coverage.get("future_ret20_candidate_day_count") or 0),
                passed=int(coverage.get("future_ret20_passed_count") or 0),
                guarded=int(coverage.get("future_ret20_guarded_out_count") or 0),
            )
        )
        if future_ret20_counts:
            lines.append(f"- future_ret20_failure_reason_counts: `{json.dumps(_json_ready(future_ret20_counts), ensure_ascii=False, sort_keys=True)}`")
        if future_ret20_counts_by_mode:
            lines.append(f"- future_ret20_failure_reason_counts_by_source_mode: `{json.dumps(_json_ready(future_ret20_counts_by_mode), ensure_ascii=False, sort_keys=True)}`")
        if source_coverage:
            lines.append(f"- ret20_source_mode: `{_text(source_coverage.get('ret20_source_mode'), fallback=_text(session_state.get('ret20_source_mode'), fallback='unknown'))}`")
            lines.append(f"- future_ret20_source_coverage: `{json.dumps(_json_ready(source_coverage), ensure_ascii=False, sort_keys=True)}`")
        if join_gap_coverage:
            lines.append(f"- future_ret20_join_gap_coverage: `{json.dumps(_json_ready(join_gap_coverage), ensure_ascii=False, sort_keys=True)}`")
        candidate_scope_gap_coverage = coverage.get("candidate_scope_gap_coverage") if isinstance(coverage.get("candidate_scope_gap_coverage"), dict) else {}
        if candidate_scope_gap_coverage:
            lines.append(f"- candidate_scope_gap_coverage: `{json.dumps(_json_ready(candidate_scope_gap_coverage), ensure_ascii=False, sort_keys=True)}`")
    if bool(session_state.get("insufficient_samples")):
        lines.append("")
        lines.append("## Validity")
        lines.append("")
        lines.append("- status: `invalid`")
        lines.append("- reason: `insufficient_samples`")
    lines.append("")
    lines.append("## Champion")
    lines.append("")
    champion = session_state.get("champion") if isinstance(session_state.get("champion"), dict) else {}
    champion_method = champion.get("method") if isinstance(champion.get("method"), dict) else {}
    lines.append(f"- method_title: `{_text(champion_method.get('method_title'))}`")
    lines.append(f"- method_thesis: `{_text(champion_method.get('method_thesis'))}`")
    lines.append(f"- run_id: `{_text(champion.get('run_id'))}`")
    lines.append("")
    lines.append("## Families")
    lines.append("")
    lines.append("| family | best method | top5 mean | median | monthly capture | promote |")
    lines.append("| --- | --- | ---: | ---: | ---: | --- |")
    for family in session_state.get("family_results") or []:
        if not isinstance(family, dict):
            continue
        best = family.get("best_candidate") if isinstance(family.get("best_candidate"), dict) else {}
        evaluation = best.get("evaluation_summary") if isinstance(best.get("evaluation_summary"), dict) else {}
        method = best.get("candidate_method") if isinstance(best.get("candidate_method"), dict) else {}
        lines.append(
            "| {family} | {method} | {top5:.4f} | {median:.4f} | {capture:.4f} | {promote} |".format(
                family=_text(family.get("method_family")),
                method=_text(method.get("method_title"), fallback=_text(best.get("plan_id"))),
                top5=float(evaluation.get("challenger_topk_ret20_mean") or 0.0),
                median=float(evaluation.get("challenger_topk_ret20_median") or 0.0),
                capture=float(evaluation.get("challenger_monthly_top5_capture_mean") or 0.0),
                promote="true" if bool(best.get("promote_ready")) else "false",
            )
        )
        lines.append("- 名前: `{}`".format(_text(method.get('method_title'), fallback=_text(best.get('plan_id')))))
        lines.append(f"  - 仮説: `{_text(method.get('method_thesis'))}`")
        lines.append(f"  - 強い局面: `evaluation_summary.windows` を参照")
        lines.append(f"  - 弱い局面: `{', '.join(best.get('promote_reasons') or []) or 'none'}`")
        lines.append(
            f"  - champion との差分: `{float(evaluation.get('challenger_topk_ret20_mean') or 0.0) - float(evaluation.get('champion_topk_ret20_mean') or 0.0):.4f}`"
        )
    lines.append("")
    lines.append("## Best Result")
    lines.append("")
    best_result = session_state.get("best_result") if isinstance(session_state.get("best_result"), dict) else {}
    best_method = best_result.get("candidate_method") if isinstance(best_result.get("candidate_method"), dict) else {}
    lines.append("- method_title: `{}`".format(_text(best_method.get('method_title'), fallback=_text(best_result.get('plan_id')))))
    lines.append("- method_id: `{}`".format(_text(best_method.get('method_id'), fallback=_text(best_result.get('plan_id')))))
    lines.append(f"- promote_ready: `{bool(best_result.get('promote_ready'))}`")
    lines.append(f"- promote_reasons: `{', '.join(best_result.get('promote_reasons') or []) or 'none'}`")
    lines.append("")
    lines.append("## Phase 4")
    lines.append("")
    phase4 = session_state.get("phase4") if isinstance(session_state.get("phase4"), dict) else {}
    lines.append(f"- status: `{_text(phase4.get('status'), fallback='not_run')}`")
    if phase4.get("reason"):
        lines.append(f"- reason: `{phase4.get('reason')}`")
    ranked_candidates = phase4.get("ranked_candidates") if isinstance(phase4.get("ranked_candidates"), list) else []
    if ranked_candidates:
        lines.append("- ranked_candidates:")
        for item in ranked_candidates[:5]:
            if not isinstance(item, dict):
                continue
            lines.append(
                f"  - `{_text(item.get('method_title'))}` score=`{float(item.get('score') or 0.0):.4f}` promote=`{bool(item.get('promote_ready'))}`"
            )
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- compare artifact が正本で、markdown report は派生物")
    lines.append("- MeeMee にはまだ接続しない")
    lines.append("- best-result は top-K=5 を主評価にし、同点時は worst regime -> DD -> turnover で選んだ")
    return "\n".join(lines).rstrip() + "\n"


def _build_session_state(
    *,
    session_id: str,
    random_seed: int,
    universe: list[str],
    period_segments: list[dict[str, Any]],
    family_specs: tuple[FamilySpec, ...],
    max_candidates_per_family: int,
    session_scope_id: str | None = None,
    runtime_meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    manifest = _build_manifest(
        session_id,
        random_seed,
        universe,
        period_segments,
        family_specs,
        max_candidates_per_family,
        session_scope_id=session_scope_id,
        runtime_meta=runtime_meta,
    )
    scope_id = _text(session_scope_id, fallback=session_id)
    now = _utc_now_iso()
    return {
        "schema_version": SESSION_SCHEMA_VERSION,
        "session_id": session_id,
        "session_scope_id": scope_id,
        "random_seed": int(random_seed),
        "seed_int": manifest["seed_int"],
        "manifest_hash": tradex._stable_hash(manifest),
        "manifest": manifest,
        "created_at": now,
        "updated_at": now,
        "status": "created",
        "phase": "phase1",
        "champion": {},
        "family_results": [],
        "best_result": {},
        "phase4": {},
        "eval_window_mode": _text((runtime_meta or {}).get("eval_window_mode"), fallback="unknown"),
        "eval_window_mode_reason": _text((runtime_meta or {}).get("eval_window_mode_reason"), fallback="unknown"),
        "eval_window_mode_standard_window_count": int((runtime_meta or {}).get("standard_window_count") or 0),
        "eval_window_mode_fallback_window_count": int((runtime_meta or {}).get("fallback_window_count") or 0),
        "eval_window_mode_standard_issues": list((runtime_meta or {}).get("standard_issues") or []),
        "eval_window_mode_fallback_issues": list((runtime_meta or {}).get("fallback_issues") or []),
        "fallback_status": TRADEX_FALLBACK_STATUS_RESEARCH if _text((runtime_meta or {}).get("eval_window_mode"), fallback="standard") != "standard" else TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
        "artifact_detail_level": TRADEX_ARTIFACT_DETAIL_LEVEL_RESEARCH_FALLBACK if _text((runtime_meta or {}).get("eval_window_mode"), fallback="standard") != "standard" else TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        "cost_model": dict(TRADEX_DEFAULT_COST_MODEL),
    }


def _build_run_manifest(
    *,
    session_id: str,
    random_seed: int,
    universe: list[str],
    period_segments: list[dict[str, Any]],
    runtime_meta: dict[str, Any],
    session_scope_id: str,
    ret20_source_mode: str,
) -> dict[str, Any]:
    eval_window_mode = _text(runtime_meta.get("eval_window_mode"), fallback="standard")
    meaningful_topk_branching_possible = bool(runtime_meta.get("meaningful_topk_branching_possible", True))
    topk_branching_block_reason = _text(runtime_meta.get("topk_branching_block_reason"))
    fallback_status = (
        TRADEX_FALLBACK_STATUS_RESEARCH
        if eval_window_mode != "standard"
        or ret20_source_mode != tradex.TRADEX_RET20_SOURCE_MODE_PRECOMPUTED
        or not meaningful_topk_branching_possible
        else TRADEX_FALLBACK_STATUS_AUTHORITATIVE
    )
    artifact_detail_level = (
        TRADEX_ARTIFACT_DETAIL_LEVEL_RESEARCH_FALLBACK if fallback_status == TRADEX_FALLBACK_STATUS_RESEARCH else TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE
    )
    config = {
        "session_scope_id": session_scope_id,
        "random_seed": int(random_seed),
        "max_candidates_per_family": DEFAULT_MAX_CANDIDATES_PER_FAMILY,
        "ret20_source_mode": ret20_source_mode,
        "eval_window_mode": eval_window_mode,
        "eval_window_mode_reason": _text(runtime_meta.get("eval_window_mode_reason"), fallback="unknown"),
        "cost_model": dict(TRADEX_DEFAULT_COST_MODEL),
    }
    input_artifacts = [
        {
            "kind": "confirmed_universe",
            "path": _text(runtime_meta.get("confirmed_universe_source"), fallback="daily_bars distinct codes"),
        },
        {"kind": "evaluation_regime_rows", "path": "tradex evaluation windows"},
        {"kind": "ret20_source_mode", "path": ret20_source_mode},
    ]
    asof = period_segments[-1]["end_date"] if period_segments else ""
    manifest = build_run_manifest(
        session_id=session_id,
        seed=int(random_seed),
        random_seed=int(random_seed),
        input_artifacts=input_artifacts,
        asof=asof,
        config=config,
        universe=universe,
        period={"segments": list(period_segments)},
        horizon="20d",
        artifact_detail_level=artifact_detail_level,
        fallback_status=fallback_status,
        cost_model=TRADEX_DEFAULT_COST_MODEL,
    )
    manifest["session_scope_id"] = session_scope_id
    manifest["ret20_source_mode"] = ret20_source_mode
    manifest["fallback_reasons"] = []
    if fallback_status == TRADEX_FALLBACK_STATUS_RESEARCH:
        manifest["fallback_reasons"].append(_text(runtime_meta.get("eval_window_mode_reason"), fallback="unknown"))
        if not meaningful_topk_branching_possible and topk_branching_block_reason:
            manifest["fallback_reasons"].append(f"topk_branching:{topk_branching_block_reason}")
    return manifest


def run_tradex_research_session(
    *,
    session_id: str,
    random_seed: int,
    universe_size: int = DEFAULT_UNIVERSE_SIZE,
    max_candidates_per_family: int = DEFAULT_MAX_CANDIDATES_PER_FAMILY,
    session_scope_id: str | None = None,
    ret20_source_mode: str = tradex.TRADEX_RET20_SOURCE_MODE_PRECOMPUTED,
    finalize_shared_rollups: bool = True,
    family_specs: tuple[FamilySpec, ...] | None = None,
    exploratory_min_trading_days: int | None = TRADEX_EXPLORATORY_EVAL_WINDOW_MIN_TRADING_DAYS,
) -> dict[str, Any]:
    if tradex.get_research_execution_context() is None:
        with tradex.use_research_execution_context(tradex.ResearchExecutionContext()):
            return run_tradex_research_session(
                session_id=session_id,
                random_seed=random_seed,
                universe_size=universe_size,
                max_candidates_per_family=max_candidates_per_family,
                session_scope_id=session_scope_id,
                ret20_source_mode=ret20_source_mode,
                finalize_shared_rollups=finalize_shared_rollups,
                family_specs=family_specs,
                exploratory_min_trading_days=exploratory_min_trading_days,
            )
    family_specs = tuple(family_specs) if family_specs is not None else _build_family_specs()
    max_candidates_per_family = max(1, min(int(max_candidates_per_family), 3))
    _require_legacy_analysis_enabled(context="tradex research session")
    scope_id = _text(session_scope_id, fallback=session_id)
    ret20_mode = _text(ret20_source_mode, fallback=tradex.TRADEX_RET20_SOURCE_MODE_PRECOMPUTED)
    if ret20_mode not in {tradex.TRADEX_RET20_SOURCE_MODE_PRECOMPUTED, tradex.TRADEX_RET20_SOURCE_MODE_DERIVED}:
        raise ValueError(f"invalid ret20_source_mode: {ret20_mode}")
    repo = get_stock_repo()
    codes = [code for code in repo.get_all_codes() if _text(code)]
    if not codes:
        repo_db_path = _text(getattr(repo, "_db_path", ""))
        data_dir = str(app_config.DATA_DIR)
        db_diagnostics: dict[str, Any] = {
            "data_dir": data_dir,
            "db_path": repo_db_path,
            "codes_count": 0,
            "source": "daily_bars distinct code",
        }
        if repo_db_path and Path(repo_db_path).exists():
            try:
                with duckdb.connect(repo_db_path, read_only=True) as conn:
                    tables = [row[0] for row in conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main' ORDER BY table_name").fetchall()]
                    db_diagnostics["daily_bars_exists"] = "daily_bars" in tables
                    if "daily_bars" in tables:
                        db_diagnostics["daily_bars_count"] = int(conn.execute("SELECT COUNT(*) FROM daily_bars").fetchone()[0])
                        db_diagnostics["daily_bars_code_count"] = int(conn.execute("SELECT COUNT(DISTINCT code) FROM daily_bars").fetchone()[0])
                    else:
                        db_diagnostics["daily_bars_count"] = 0
                        db_diagnostics["daily_bars_code_count"] = 0
            except Exception as exc:
                db_diagnostics["db_probe_error"] = f"{exc.__class__.__name__}:{exc}"
        raise RuntimeError(f"confirmed universe is empty: {json.dumps(_json_ready(db_diagnostics), ensure_ascii=False, sort_keys=True)}")
    period_segments, period_mode_meta = _build_period_segments_with_mode(
        exploratory_min_trading_days=exploratory_min_trading_days,
    )
    _configure_research_execution_context(
        repo=repo,
        codes=codes,
        period_segments=period_segments,
        preload_timelines=scope_id != FEATURE_SNAPSHOT_SCOPE_ID,
    )
    if scope_id == FEATURE_SNAPSHOT_SCOPE_ID:
        eligible_codes, universe_meta = _scope_feature_snapshot_eligible_codes(
            repo,
            codes=codes,
            period_segments=period_segments,
        )
    else:
        eligible_codes, universe_meta = _scope_analysis_eligible_codes(
            repo,
            codes=codes,
            period_segments=period_segments,
        )
    if not eligible_codes:
        raise RuntimeError(
            "confirmed universe is empty after scope analysis coverage filter: "
            + json.dumps(_json_ready(universe_meta), ensure_ascii=False, sort_keys=True)
        )
    selected_universe_size = min(int(universe_size), len(eligible_codes))
    universe = _choose_universe(
        eligible_codes,
        session_id=scope_id,
        random_seed=random_seed,
        universe_size=selected_universe_size,
    )
    _preload_research_universe_context(repo=repo, universe=universe)
    session_top_k = max(
        (
            max(1, tradex._int(candidate.plan_overrides.get("top_k")) or 0)
            for family_spec in family_specs
            for candidate in family_spec.candidates
        ),
        default=0,
    )
    meaningful_topk_branching_possible = len(universe) > session_top_k
    runtime_meta = {
        "eval_window_mode": _text(period_mode_meta.get("mode"), fallback="fallback"),
        "eval_window_mode_reason": _text(period_mode_meta.get("mode_reason"), fallback="unknown"),
        "standard_window_count": int(period_mode_meta.get("standard_window_count") or 0),
        "fallback_window_count": int(period_mode_meta.get("fallback_window_count") or 0),
        "standard_issues": [str(item) for item in (period_mode_meta.get("standard_issues") or []) if str(item).strip()],
        "fallback_issues": [str(item) for item in (period_mode_meta.get("fallback_issues") or []) if str(item).strip()],
        "legacy_analysis_env": os.getenv(LEGACY_ANALYSIS_DISABLE_ENV, "1"),
        "session_scope_id": scope_id,
        "ret20_source_mode": ret20_mode,
        "ret20_source_mode_reason": "explicit_session_mode",
        "confirmed_universe_source": _text(universe_meta.get("source_path"), fallback="daily_bars distinct codes"),
        "confirmed_universe_selection_mode": _text(universe_meta.get("selection_mode"), fallback="unfiltered_daily_bars"),
        "confirmed_universe_requested_count": int(universe_meta.get("requested_code_count") or len(codes)),
        "confirmed_universe_eligible_count": int(universe_meta.get("eligible_code_count") or 0),
        "confirmed_universe_selected_count": len(universe),
        "confirmed_universe_clamped": len(universe) < int(universe_size),
        "confirmed_universe_segment_coverage_counts": dict(universe_meta.get("segment_coverage_counts") or {}),
        "effective_universe_count": len(universe),
        "top_k": session_top_k,
        "meaningful_topk_branching_possible": meaningful_topk_branching_possible,
        "topk_branching_block_reason": "" if meaningful_topk_branching_possible else "effective_universe_too_small_for_topk",
    }
    manifest = _build_manifest(
        session_id,
        random_seed,
        universe,
        period_segments,
        family_specs,
        max_candidates_per_family,
        session_scope_id=scope_id,
        runtime_meta=runtime_meta,
    )
    manifest_hash = tradex._stable_hash(manifest)

    state = _load_session_state(session_id)
    if state:
        stored_hash = _text(state.get("manifest_hash"))
        if stored_hash and stored_hash != manifest_hash:
            raise ValueError("session_id already exists with different manifest; choose a new session_id")
        state = dict(state)
    else:
        state = _build_session_state(
            session_id=session_id,
            random_seed=random_seed,
            universe=universe,
            period_segments=period_segments,
            family_specs=family_specs,
            max_candidates_per_family=max_candidates_per_family,
            session_scope_id=scope_id,
            runtime_meta=runtime_meta,
        )
    state["runtime_meta"] = _json_ready(runtime_meta)
    state["session_scope_id"] = scope_id
    state["ret20_source_mode"] = ret20_mode
    state["ret20_source_mode_reason"] = "explicit_session_mode"
    state["eval_window_mode"] = _text(runtime_meta.get("eval_window_mode"), fallback="unknown")
    state["eval_window_mode_reason"] = _text(runtime_meta.get("eval_window_mode_reason"), fallback="unknown")
    state["eval_window_mode_standard_window_count"] = int(runtime_meta.get("standard_window_count") or 0)
    state["eval_window_mode_fallback_window_count"] = int(runtime_meta.get("fallback_window_count") or 0)
    state["eval_window_mode_standard_issues"] = list(runtime_meta.get("standard_issues") or [])
    state["eval_window_mode_fallback_issues"] = list(runtime_meta.get("fallback_issues") or [])
    run_manifest = _build_run_manifest(
        session_id=session_id,
        random_seed=random_seed,
        universe=universe,
        period_segments=period_segments,
        runtime_meta=runtime_meta,
        session_scope_id=scope_id,
        ret20_source_mode=ret20_mode,
    )
    state["fallback_status"] = _text(run_manifest.get("fallback_status"), fallback=TRADEX_FALLBACK_STATUS_AUTHORITATIVE)
    state["artifact_detail_level"] = _text(run_manifest.get("artifact_detail_level"), fallback=TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE)
    state["cost_model"] = dict(run_manifest.get("cost_model") or TRADEX_DEFAULT_COST_MODEL)
    state["run_manifest_hash"] = _text(run_manifest.get("run_manifest_hash"))
    state["run_manifest"] = run_manifest

    if _text(state.get("status")) == "complete" and _text(state.get("manifest_hash")) == manifest_hash:
        _ensure_session_report(state)
        _ensure_family_leaderboard_artifacts(state)
        if finalize_shared_rollups:
            _ensure_session_leaderboard_rollup_artifacts()
        _ensure_run_manifest(session_id, run_manifest)
        return state

    completed_family_results: dict[str, dict[str, Any]] = {}
    seen_method_pairs: set[tuple[str, str]] = set()
    seen_method_signatures: set[str] = set()
    for item in state.get("family_results") or []:
        if not isinstance(item, dict):
            continue
        family_id = _text(item.get("family_id"))
        if family_id:
            completed_family_results[family_id] = item
        compare = item.get("compare") if isinstance(item.get("compare"), dict) else {}
        for candidate_item in compare.get("candidate_results") if isinstance(compare.get("candidate_results"), list) else []:
            if not isinstance(candidate_item, dict):
                continue
            candidate_method = candidate_item.get("candidate_method") if isinstance(candidate_item.get("candidate_method"), dict) else {}
            method_family = _text(candidate_method.get("method_family"))
            method_thesis = _text(candidate_method.get("method_thesis"))
            if method_family and method_thesis:
                seen_method_pairs.add((method_family, method_thesis))
            seen_method_signatures.add(_leaderboard_candidate_signature_hash(candidate_item))

    state["manifest"] = manifest
    state["manifest_hash"] = manifest_hash
    state["updated_at"] = _utc_now_iso()
    state["status"] = "running"
    state["phase"] = "phase1"
    _append_jsonl(
        _session_events_file(session_id),
        {
            "event": "session_started",
            "session_id": session_id,
            "random_seed": int(random_seed),
            "manifest_hash": manifest_hash,
            "at": _utc_now_iso(),
        },
    )
    _write_session_state(session_id, state)
    _write_run_manifest(session_id, run_manifest)

    champion_family_id = _champion_family_id(session_id)
    champion_family = load_family(champion_family_id)
    champion_body = {
        "family_id": champion_family_id,
        "family_name": f"現行ランキング / champion / {session_id}",
        "session_scope_id": scope_id,
        "universe": universe,
        "period": {"segments": period_segments},
        "probes": [],
        "baseline_plan": _build_champion_plan(ret20_source_mode=ret20_mode, top_k=session_top_k or SCENARIO_SEARCH_TOP_K),
        "candidate_plans": [],
        "confirmed_only": True,
        "input_dataset_version": f"session:{session_id}:champion",
        "code_revision": tradex._git_commit(),
        "timezone": "Asia/Tokyo",
        "price_source": "daily_bars",
        "data_cutoff_at": period_segments[-1]["end_date"],
        "random_seed": int(random_seed),
        "notes": "session champion baseline",
    }
    if not champion_family:
        tradex.create_family(champion_body)
        champion_family = load_family(champion_family_id)
    if not champion_family:
        raise RuntimeError("failed to create champion family")
    champion_run = load_run(champion_family_id, f"{champion_family_id}-baseline")
    if not isinstance(champion_run, dict) or _text(champion_run.get("status")) not in {"succeeded", "compared"}:
        champion_run = tradex.create_run(family_id=champion_family_id, run_kind="baseline", notes="research champion baseline")
    if not isinstance(champion_run, dict):
        raise RuntimeError("failed to materialize champion baseline")
    state["champion"] = {
        "family_id": champion_family_id,
        "run_id": champion_run.get("run_id"),
        "method": {
            "method_id": champion_run.get("method_id"),
            "method_title": champion_run.get("method_title"),
            "method_thesis": champion_run.get("method_thesis"),
            "method_family": champion_run.get("method_family"),
        },
        "selection_summary": champion_run.get("selection_summary") if isinstance(champion_run.get("selection_summary"), dict) else {},
        "readiness_summary": champion_run.get("readiness_summary") if isinstance(champion_run.get("readiness_summary"), dict) else {},
        "waterfall_summary": champion_run.get("waterfall_summary") if isinstance(champion_run.get("waterfall_summary"), dict) else {},
        "diagnostics_schema_version": champion_run.get("diagnostics_schema_version"),
    }
    _write_session_state(session_id, state)

    family_results: list[dict[str, Any]] = []
    for family_spec in family_specs:
        candidate_specs = list(family_spec.candidates[:max_candidates_per_family])
        family_id = _session_family_id(session_id, family_spec.method_family)
        family_body = _build_family_body(
            session_id=session_id,
            random_seed=random_seed,
            session_scope_id=scope_id,
            universe=universe,
            period_segments=period_segments,
            family_spec=family_spec,
            candidate_specs=candidate_specs,
            ret20_source_mode=ret20_mode,
            top_k=session_top_k or SCENARIO_SEARCH_TOP_K,
        )
        family = load_family(family_id)
        if not family:
            tradex.create_family(family_body)
            family = load_family(family_id)
        if not family:
            raise RuntimeError(f"failed to create family: {family_id}")
        existing_family_result = completed_family_results.get(family_id)
        if isinstance(existing_family_result, dict):
            family_results.append(existing_family_result)
            state["family_results"] = family_results
            state["updated_at"] = _utc_now_iso()
            state["phase"] = "phase3"
            _write_session_state(session_id, state)
            _append_jsonl(
                _session_events_file(session_id),
                {
                    "event": "family_resumed",
                    "session_id": session_id,
                    "family_id": family_id,
                    "method_family": family_spec.method_family,
                    "best_method_title": existing_family_result.get("best_method_title"),
                    "promote_ready": existing_family_result.get("promote_ready"),
                    "at": _utc_now_iso(),
                },
            )
            continue
        baseline_run = load_run(family_id, f"{family_id}-baseline")
        if not isinstance(baseline_run, dict) or _text(baseline_run.get("status")) not in {"succeeded", "compared"}:
            baseline_run = _seed_family_baseline_from_reference(reference_run=champion_run, family_id=family_id, family=family)
        if not isinstance(baseline_run, dict):
            raise RuntimeError(f"failed to seed baseline for family: {family_id}")

        for candidate_spec in candidate_specs:
            candidate_pair = (candidate_spec.method_family, candidate_spec.method_thesis)
            candidate_signature_payload = {
                "method_family": candidate_spec.method_family,
                "method_id": candidate_spec.method_id,
                "minimum_confidence": tradex._float(candidate_spec.plan_overrides.get("minimum_confidence")),
                "minimum_ready_rate": tradex._float(candidate_spec.plan_overrides.get("minimum_ready_rate")),
                "signal_bias": _text(candidate_spec.plan_overrides.get("signal_bias"), fallback="balanced"),
                "top_k": max(1, tradex._int(candidate_spec.plan_overrides.get("top_k")) or 0),
                "bad_pick_penalty_scale": tradex._float(candidate_spec.plan_overrides.get("bad_pick_penalty_scale")) or 0.0,
                "bad_pick_penalty_profile": _text(candidate_spec.plan_overrides.get("bad_pick_penalty_profile"), fallback="shared"),
                "playbook_up_score_bonus": tradex._float(candidate_spec.plan_overrides.get("playbook_up_score_bonus")) or 0.0,
                "playbook_down_score_bonus": tradex._float(candidate_spec.plan_overrides.get("playbook_down_score_bonus")) or 0.0,
            }
            candidate_signature = tradex._stable_hash(candidate_signature_payload)
            candidate_run_id = f"{family_id}-{candidate_spec.method_id}"
            candidate_run_path = run_file(family_id, candidate_run_id)
            if (candidate_pair in seen_method_pairs or candidate_signature in seen_method_signatures) and not candidate_run_path.exists():
                raise RuntimeError(
                    "duplicate candidate method prohibited: "
                    f"{candidate_spec.method_family} / {candidate_spec.method_thesis}"
                )
            candidate_run_id = f"{family_id}-{candidate_spec.method_id}"
            candidate_run_path = run_file(family_id, candidate_run_id)
            candidate_run = load_run(family_id, candidate_run_id)
            if candidate_run_path.exists():
                if not isinstance(candidate_run, dict):
                    raise RuntimeError(f"existing candidate run is unreadable: {candidate_run_path}")
            elif not isinstance(candidate_run, dict) or _text(candidate_run.get("status")) not in {"succeeded", "compared", "adopt_candidate", "rejected"}:
                candidate_run = tradex.create_run(
                    family_id=family_id,
                    run_kind="candidate",
                    plan_id=candidate_spec.method_id,
                    notes=candidate_spec.method_title,
                )
            if not isinstance(candidate_run, dict):
                raise RuntimeError(f"failed to run candidate {candidate_spec.method_id}")
            seen_method_pairs.add(candidate_pair)
            seen_method_signatures.add(candidate_signature)

        compare = tradex.get_family_compare(family_id)
        if not isinstance(compare, dict):
            compare = {}
        family_result = _family_result_summary(family_spec=family_spec, family=family, compare=compare)
        family_results.append(family_result)
        state["family_results"] = family_results
        state["updated_at"] = _utc_now_iso()
        state["phase"] = "phase3"
        _write_session_state(session_id, state)
        _append_jsonl(
            _session_events_file(session_id),
            {
                "event": "family_complete",
                "session_id": session_id,
                "family_id": family_id,
                "method_family": family_spec.method_family,
                "best_method_title": family_result.get("best_method_title"),
                "promote_ready": family_result.get("promote_ready"),
                "at": _utc_now_iso(),
            },
        )

    coverage_waterfall = _session_coverage_summary(state)
    state["coverage_waterfall"] = coverage_waterfall
    state["candidate_scope_gap_coverage"] = coverage_waterfall.get("candidate_scope_gap_coverage") if isinstance(coverage_waterfall.get("candidate_scope_gap_coverage"), dict) else {}
    state["session_failure_reason_counts"] = coverage_waterfall.get("session_failure_reason_counts") if isinstance(coverage_waterfall.get("session_failure_reason_counts"), dict) else {}
    state["scope_filter_applied_stage"] = _text(coverage_waterfall.get("scope_filter_applied_stage"), fallback="unknown")
    state["insufficient_samples"] = bool(coverage_waterfall.get("insufficient_samples"))

    best_candidates = [item.get("best_candidate") for item in family_results if isinstance(item.get("best_candidate"), dict)]
    best_result = sorted(best_candidates, key=_family_best_key)[0] if best_candidates else {}
    if bool(state["insufficient_samples"]):
        state["best_result"] = {}
        state["phase"] = "sample_rows"
    else:
        state["best_result"] = best_result if isinstance(best_result, dict) else {}
        state["phase"] = "phase4" if bool(state["best_result"]) and bool(state["best_result"].get("promote_ready")) else "complete"

    if not bool(state["insufficient_samples"]) and bool(state["best_result"]) and bool(state["best_result"].get("promote_ready")):
        state["phase4"] = _train_phase4_ranker(family_results=family_results, random_seed=random_seed)
    elif bool(state["insufficient_samples"]):
        state["phase4"] = {"status": "skipped", "reason": "insufficient_samples"}
    else:
        state["phase4"] = {"status": "skipped", "reason": "no_promote_ready_winner"}
    state["status"] = "invalid" if bool(state["insufficient_samples"]) else "complete"
    state["completed_at"] = _utc_now_iso()
    state["updated_at"] = _utc_now_iso()
    state["compare_schema_version"] = SESSION_COMPARE_SCHEMA_VERSION
    _write_session_state(session_id, state)
    _append_jsonl(
        _session_events_file(session_id),
        {
            "event": "session_complete",
            "session_id": session_id,
            "best_method_title": _text((state.get("best_result") or {}).get("candidate_method", {}).get("method_title")),
            "phase4_status": _text((state.get("phase4") or {}).get("status"), fallback="skipped"),
            "at": _utc_now_iso(),
        },
    )

    report_path = _session_report_file(session_id)
    report_path.write_text(_render_session_report(state), encoding="utf-8")
    state["report_path"] = str(report_path)
    _write_session_state(session_id, state)
    _write_family_leaderboard_artifacts(state)
    if finalize_shared_rollups:
        _write_session_leaderboard_rollup_artifacts()
    return state


def _logic_search_float(value: Any, default: float = 0.0) -> float:
    parsed = tradex._float(value)
    return float(parsed) if parsed is not None else float(default)


def _logic_search_mutation_hints(candidate_effective_config: dict[str, Any]) -> list[dict[str, Any]]:
    if not isinstance(candidate_effective_config, dict):
        return []
    hints: list[dict[str, Any]] = []
    numeric_steps = {
        "minimum_confidence": 0.02,
        "minimum_ready_rate": 0.05,
        "playbook_up_score_bonus": 0.005,
        "playbook_down_score_bonus": 0.005,
        "bad_pick_penalty_scale": 1.0,
    }
    for key, step in numeric_steps.items():
        if key not in candidate_effective_config:
            continue
        value = tradex._float(candidate_effective_config.get(key))
        if value is None:
            continue
        if key == "bad_pick_penalty_scale":
            lower = max(0.0, float(value) - step)
            upper = float(value) + step
            candidates = [round(lower, 4), round(float(value), 4), round(upper, 4)]
        else:
            lower = max(0.0, float(value) - step)
            upper = min(1.0, float(value) + step)
            candidates = [round(lower, 4), round(float(value), 4), round(upper, 4)]
        hints.append({"parameter": key, "candidates": sorted(set(candidates))})
    if "top_k" in candidate_effective_config:
        value = tradex._int(candidate_effective_config.get("top_k"))
        if value is not None:
            candidates = sorted({max(1, int(value) - 1), max(1, int(value)), max(1, int(value) + 1)})
            hints.append({"parameter": "top_k", "candidates": candidates})
    signal_bias = _text(candidate_effective_config.get("signal_bias"), fallback="")
    if signal_bias:
        hints.append({"parameter": "signal_bias", "candidates": sorted({signal_bias, "balanced", "buy", "sell"})})
    return hints


def _logic_search_candidate_score(evaluation: dict[str, Any]) -> float:
    promote_ready = bool(evaluation.get("promote_ready"))
    insufficient_samples = bool(evaluation.get("insufficient_samples"))
    meaningful_topk_branching_possible = bool(evaluation.get("meaningful_topk_branching_possible"))
    top5 = _logic_search_float(evaluation.get("challenger_topk_ret20_mean"))
    top10 = _logic_search_float(evaluation.get("challenger_topk10_ret20_mean"))
    monthly_capture = _logic_search_float((evaluation.get("challenger_monthly_top5_capture") or {}).get("mean"))
    worst_regime = _logic_search_float(evaluation.get("challenger_worst_regime_ret20_mean"))
    dd = max(0.0, _logic_search_float(evaluation.get("challenger_dd")))
    turnover = max(0.0, _logic_search_float(evaluation.get("challenger_turnover")))
    liquidity_fail_rate = max(0.0, _logic_search_float(evaluation.get("challenger_liquidity_fail_rate")))
    zero_pass_months = max(0.0, _logic_search_float(evaluation.get("challenger_zero_pass_months")))
    score = (
        (1000.0 if promote_ready else 0.0)
        + (500.0 * top5)
        + (300.0 * top10)
        + (200.0 * monthly_capture)
        + (250.0 * worst_regime)
        - (250.0 * dd)
        - (120.0 * turnover)
        - (300.0 * liquidity_fail_rate)
        - (60.0 * zero_pass_months)
    )
    if insufficient_samples:
        score -= 2000.0
    if not meaningful_topk_branching_possible:
        score -= 500.0
    return float(score)


def _logic_search_candidate_row(
    *,
    search_id: str,
    session_state: dict[str, Any],
    family_result: dict[str, Any],
) -> dict[str, Any] | None:
    best_candidate = family_result.get("best_candidate") if isinstance(family_result.get("best_candidate"), dict) else None
    if not isinstance(best_candidate, dict):
        return None
    candidate_method = best_candidate.get("candidate_method") if isinstance(best_candidate.get("candidate_method"), dict) else {}
    diagnostics = best_candidate.get("diagnostics") if isinstance(best_candidate.get("diagnostics"), dict) else {}
    candidate_effective_config = diagnostics.get("candidate_effective_config") if isinstance(diagnostics.get("candidate_effective_config"), dict) else {}
    evaluation_summary = best_candidate.get("evaluation_summary") if isinstance(best_candidate.get("evaluation_summary"), dict) else {}
    if not evaluation_summary:
        evaluation_summary = best_candidate if isinstance(best_candidate, dict) else {}
    score = _logic_search_candidate_score(evaluation_summary)
    candidate_signature_hash = _leaderboard_candidate_signature_hash(best_candidate)
    method_id = _text(candidate_method.get("method_id"), fallback=_text(best_candidate.get("plan_id"), fallback=candidate_signature_hash))
    method_title = _text(candidate_method.get("method_title"), fallback=method_id)
    method_family = _text(candidate_method.get("method_family"), fallback=_text(family_result.get("method_family"), fallback="unknown"))
    family_title = _text(family_result.get("family_title"), fallback=method_family)
    session_id = _text(session_state.get("session_id"), fallback=search_id)
    session_scope_id = _text(session_state.get("session_scope_id"), fallback=session_id)
    random_seed = int(session_state.get("random_seed") or 0)
    return {
        "search_id": search_id,
        "session_id": session_id,
        "session_scope_id": session_scope_id,
        "random_seed": random_seed,
        "family_id": _text(family_result.get("family_id"), fallback="unknown"),
        "family_title": family_title,
        "method_family": method_family,
        "method_id": method_id,
        "method_title": method_title,
        "candidate_signature_hash": candidate_signature_hash,
        "score": score,
        "promote_ready": bool(evaluation_summary.get("promote_ready")),
        "sample_count": int(evaluation_summary.get("sample_count") or 0),
        "top5_ret20_mean": _logic_search_float(evaluation_summary.get("challenger_topk_ret20_mean")),
        "top10_ret20_mean": _logic_search_float(evaluation_summary.get("challenger_topk10_ret20_mean")),
        "monthly_capture_mean": _logic_search_float((evaluation_summary.get("challenger_monthly_top5_capture") or {}).get("mean")),
        "worst_regime_ret20_mean": _logic_search_float(evaluation_summary.get("challenger_worst_regime_ret20_mean")),
        "dd": max(0.0, _logic_search_float(evaluation_summary.get("challenger_dd"))),
        "turnover": max(0.0, _logic_search_float(evaluation_summary.get("challenger_turnover"))),
        "liquidity_fail_rate": max(0.0, _logic_search_float(evaluation_summary.get("challenger_liquidity_fail_rate"))),
        "zero_pass_months": max(0.0, _logic_search_float(evaluation_summary.get("challenger_zero_pass_months"))),
        "candidate_effective_config": _json_ready(candidate_effective_config),
        "mutation_hints": _logic_search_mutation_hints(candidate_effective_config),
        "decision": _text(best_candidate.get("decision"), fallback="hold"),
        "promote_reasons": _json_ready(best_candidate.get("promote_reasons") or []),
        "evaluation_summary": _json_ready(evaluation_summary),
    }


def _logic_search_mutated_family_specs(
    candidate_rows: list[dict[str, Any]],
    *,
    round_index: int,
    max_families: int = 4,
    max_mutations_per_family: int = 3,
) -> tuple[FamilySpec, ...]:
    if not candidate_rows:
        return ()
    seen_families: set[str] = set()
    ranked_rows = sorted(
        candidate_rows,
        key=lambda row: (
            -float(row.get("score") or 0.0),
            -int(row.get("sample_count") or 0),
            _text(row.get("method_family")),
            _text(row.get("method_id")),
        ),
    )
    families: list[FamilySpec] = []
    for row in ranked_rows:
        method_family = _text(row.get("method_family"), fallback="")
        if not method_family or method_family in seen_families:
            continue
        seen_families.add(method_family)
        candidate_effective_config = row.get("candidate_effective_config") if isinstance(row.get("candidate_effective_config"), dict) else {}
        mutation_hints = row.get("mutation_hints") if isinstance(row.get("mutation_hints"), list) else []
        if not mutation_hints:
            mutation_hints = _logic_search_mutation_hints(candidate_effective_config)
        base_method_id = _text(row.get("method_id"), fallback=method_family)
        base_method_title = _text(row.get("method_title"), fallback=base_method_id)
        base_method_thesis = f"Auto mutation source from {base_method_title}"
        candidates: list[CandidateMethodSpec] = []
        for hint in mutation_hints:
            parameter = _text(hint.get("parameter"), fallback="")
            if not parameter:
                continue
            values = hint.get("candidates") if isinstance(hint.get("candidates"), list) else []
            for value in values:
                if len(candidates) >= max_mutations_per_family:
                    break
                if value is None:
                    continue
                mutated_config = dict(candidate_effective_config)
                mutated_config[parameter] = value
                mutation_method_id = f"{base_method_id}__r{int(round_index)}__{_slug(parameter)}_{_slug(value)}"
                mutation_method_title = f"{base_method_title} [{parameter}={value}]"
                mutation_method_thesis = f"Auto mutation around {base_method_title}: {parameter}={value}"
                candidates.append(
                    CandidateMethodSpec(
                        method_family=method_family,
                        method_id=mutation_method_id,
                        method_title=mutation_method_title,
                        method_thesis=mutation_method_thesis,
                        plan_overrides=mutated_config,
                    )
                )
            if len(candidates) >= max_mutations_per_family:
                break
        if not candidates:
            mutated_config = dict(candidate_effective_config)
            candidates.append(
                CandidateMethodSpec(
                    method_family=method_family,
                    method_id=f"{base_method_id}__r{int(round_index)}__fallback",
                    method_title=f"{base_method_title} [auto-fallback]",
                    method_thesis=base_method_thesis,
                    plan_overrides=mutated_config,
                )
            )
        families.append(
            FamilySpec(
                method_family=method_family,
                family_title=f"Auto search / {method_family}",
                family_thesis=base_method_thesis,
                candidates=tuple(candidates[:max_mutations_per_family]),
            )
        )
        if len(families) >= max_families:
            break
    return tuple(families)


def _scenario_feature_family_for_population(population_kind: str, filter_family: str) -> str:
    filter_text = _text(filter_family)
    if filter_text in TRADEX_FEATURE_FAMILIES:
        return filter_text
    if population_kind == "regime_selector_population":
        return "regime_adjustment"
    if population_kind == "short_expert_population":
        return "bad_pick_removal"
    return "boundary_feature"


def _scenario_candidate_spec(search_id: str, spec: ScenarioSpec) -> CandidateMethodSpec:
    feature_family = _scenario_feature_family_for_population(spec.population_kind, spec.filter_family)
    method_id = _scenario_method_id(search_id, spec.population_kind, spec)
    return CandidateMethodSpec(
        method_family=spec.population_kind,
        method_id=method_id,
        method_title=_scenario_method_title(spec),
        method_thesis=_scenario_method_thesis(spec),
        plan_overrides=_scenario_plan_overrides(spec, search_id=search_id, parent_candidate_ids=list(spec.mutation_parent_ids)),
        feature_family=feature_family,
        population_kind=spec.population_kind,
        scenario_definition=_scenario_definition_to_payload(spec),
        generation_index=int(spec.generation_index),
        mutation_parent_ids=tuple(spec.mutation_parent_ids),
    )


def _scenario_candidate_spec_with_top_k(
    search_id: str,
    spec: ScenarioSpec,
    *,
    top_k: int,
    variant_label: str | None = None,
) -> CandidateMethodSpec:
    candidate = _scenario_candidate_spec(search_id, spec)
    plan_overrides = dict(candidate.plan_overrides)
    plan_overrides["top_k"] = max(1, int(top_k))
    variant_suffix = f"-{_slug(variant_label)}" if _text(variant_label) else ""
    title_suffix = f" [{variant_label}]" if _text(variant_label) else ""
    return CandidateMethodSpec(
        method_family=candidate.method_family,
        method_id=f"{candidate.method_id}{variant_suffix}",
        method_title=f"{candidate.method_title}{title_suffix}",
        method_thesis=f"{candidate.method_thesis}{title_suffix}",
        plan_overrides=plan_overrides,
        feature_family=candidate.feature_family,
        population_kind=candidate.population_kind,
        scenario_definition=candidate.scenario_definition,
        generation_index=candidate.generation_index,
        mutation_parent_ids=candidate.mutation_parent_ids,
    )


def _scenario_mutate_spec(parent_record: dict[str, Any], *, generation_index: int, rng: random.Random) -> ScenarioSpec:
    parent_spec = _scenario_spec_from_record(parent_record, generation_index=generation_index)
    population = parent_spec.population_kind
    defaults = _scenario_defaults_for_population(population)
    direction = parent_spec.direction
    mutate_axes = [
        "entry_family",
        "filter_family",
        "ranking_target",
        "holding_days",
        "exit_family",
        "stop_type",
        "regime_gate",
        "liquidity_gate",
        "weight_profile",
        "signal_bias",
        "minimum_confidence",
        "minimum_ready_rate",
        "bad_pick_penalty_scale",
        "playbook_up_score_bonus",
        "playbook_down_score_bonus",
    ]
    rng.shuffle(mutate_axes)
    chosen = mutate_axes[: 3 if population != "regime_selector_population" else 4]
    values = parent_spec.__dict__.copy()
    for axis in chosen:
        if axis == "entry_family":
            options = {
                "long_expert_population": ["long_breakout", "long_reversal", "long_pullback"],
                "short_expert_population": ["short_downtrend", "short_failed_high", "short_crash_top"],
                "regime_selector_population": ["regime_selector", "regime_router"],
            }.get(population, [defaults["entry_family"]])
            values[axis] = rng.choice(options)
        elif axis == "filter_family":
            options = {
                "long_expert_population": ["boundary_feature", "regime_adjustment", "symbol_specific_adjustment"],
                "short_expert_population": ["bad_pick_removal", "symbol_specific_adjustment", "liquidity_first"],
                "regime_selector_population": ["regime_adjustment", "boundary_feature"],
            }.get(population, ["boundary_feature", "bad_pick_removal", "regime_adjustment", "symbol_specific_adjustment", "common_pattern"])
            values[axis] = rng.choice(options)
        elif axis == "ranking_target":
            options = {
                "long_expert_population": ["topk_uplift", "regime_balance", "stability"],
                "short_expert_population": ["bad_pick_removal", "drawdown_control", "stability"],
                "regime_selector_population": ["regime_balance", "stability"],
            }.get(population, ["topk_uplift", "bad_pick_removal", "regime_balance", "stability", "drawdown_control"])
            values[axis] = rng.choice(options)
        elif axis == "holding_days":
            options = {
                "long_expert_population": [10, 20, 40],
                "short_expert_population": [5, 10, 15],
                "regime_selector_population": [10, 20, 30],
            }.get(population, [5, 10, 15, 20, 40])
            values[axis] = int(rng.choice(options))
        elif axis == "exit_family":
            options = {
                "long_expert_population": ["time_stop", "signal_exit", "regime_switch"],
                "short_expert_population": ["risk_exit", "signal_exit", "regime_switch"],
                "regime_selector_population": ["regime_switch", "adaptive", "signal_exit"],
            }.get(population, ["time_stop", "signal_exit", "regime_switch", "risk_exit"])
            values[axis] = rng.choice(options)
        elif axis == "stop_type":
            options = {
                "long_expert_population": ["trailing", "adaptive"],
                "short_expert_population": ["hard", "adaptive"],
                "regime_selector_population": ["adaptive", "trailing"],
            }.get(population, ["hard", "trailing", "adaptive"])
            values[axis] = rng.choice(options)
        elif axis == "regime_gate":
            options = {
                "long_expert_population": ["trend_long", "bottom_building", "top_warning"],
                "short_expert_population": ["trend_short", "top_warning", "range_sell"],
                "regime_selector_population": ["multi_regime", "trend_long", "trend_short"],
            }.get(population, ["trend_long", "trend_short", "range_buy", "range_sell", "bottom_building", "top_warning"])
            values[axis] = rng.choice(options)
        elif axis == "liquidity_gate":
            options = {
                "long_expert_population": ["balanced", "liquidity_first"],
                "short_expert_population": ["tight", "liquidity_first", "balanced"],
                "regime_selector_population": ["balanced", "liquidity_first"],
            }.get(population, ["tight", "balanced", "liquidity_first"])
            values[axis] = rng.choice(options)
        elif axis == "weight_profile":
            options = {
                "long_expert_population": ["boundary_heavy", "regime_heavy", "balanced"],
                "short_expert_population": ["bad_pick_heavy", "liquidity_heavy", "balanced"],
                "regime_selector_population": ["regime_heavy", "balanced"],
            }.get(population, ["balanced", "boundary_heavy", "bad_pick_heavy", "regime_heavy", "liquidity_heavy"])
            values[axis] = rng.choice(options)
        elif axis == "signal_bias":
            options = {
                "long_expert_population": ["buy", "balanced", "sell"],
                "short_expert_population": ["sell", "balanced", "buy"],
                "regime_selector_population": ["balanced", "buy", "sell"],
            }.get(population, ["balanced", "buy", "sell"])
            values[axis] = rng.choice(options)
        elif axis == "minimum_confidence":
            options = {
                "long_expert_population": [0.20, 0.35, 0.56, 0.82],
                "short_expert_population": [0.18, 0.30, 0.52, 0.78],
                "regime_selector_population": [0.25, 0.40, 0.55, 0.75],
            }.get(population, [0.20, 0.35, 0.55, 0.75])
            values[axis] = float(rng.choice(options))
        elif axis == "minimum_ready_rate":
            options = {
                "long_expert_population": [0.10, 0.25, 0.45, 0.70],
                "short_expert_population": [0.08, 0.20, 0.40, 0.65],
                "regime_selector_population": [0.15, 0.30, 0.45, 0.65],
            }.get(population, [0.10, 0.25, 0.45, 0.65])
            values[axis] = float(rng.choice(options))
        elif axis == "bad_pick_penalty_scale":
            options = {
                "long_expert_population": [0.0, 0.5, 1.0, 1.5],
                "short_expert_population": [1.0, 2.5, 4.0, 6.0],
                "regime_selector_population": [0.0, 0.8, 1.2, 2.0],
            }.get(population, [0.0, 0.5, 1.0, 2.5, 4.0, 6.0])
            values[axis] = float(rng.choice(options))
        elif axis == "playbook_up_score_bonus":
            options = {
                "long_expert_population": [0.0, 0.02, 0.05, 0.08],
                "short_expert_population": [0.0, 0.01, 0.03],
                "regime_selector_population": [0.0, 0.01, 0.03, 0.05],
            }.get(population, [0.0, 0.01, 0.02, 0.05, 0.08])
            values[axis] = float(rng.choice(options))
        elif axis == "playbook_down_score_bonus":
            options = {
                "long_expert_population": [0.0, 0.01, 0.03],
                "short_expert_population": [0.0, 0.03, 0.06, 0.10],
                "regime_selector_population": [0.0, 0.01, 0.03, 0.05],
            }.get(population, [0.0, 0.01, 0.03, 0.06, 0.10])
            values[axis] = float(rng.choice(options))
    values["direction"] = direction
    parent_ids = tuple(dict.fromkeys([*tuple(parent_spec.mutation_parent_ids or ()), _text(parent_record.get("candidate_id"), fallback=_text(parent_record.get("method_signature_hash"), fallback=_text(parent_record.get("method_id"))))]))
    values["mutation_parent_ids"] = parent_ids
    values["generation_index"] = generation_index
    values["population_kind"] = population
    return ScenarioSpec(**values)


def _scenario_crossover_spec(
    left_record: dict[str, Any],
    right_record: dict[str, Any],
    *,
    generation_index: int,
    rng: random.Random,
) -> ScenarioSpec:
    left = _scenario_spec_from_record(left_record, generation_index=generation_index)
    right = _scenario_spec_from_record(right_record, generation_index=generation_index)
    population = left.population_kind
    values = {
        "direction": left.direction if rng.random() < 0.5 else right.direction,
        "entry_family": left.entry_family if rng.random() < 0.5 else right.entry_family,
        "filter_family": left.filter_family if rng.random() < 0.5 else right.filter_family,
        "ranking_target": left.ranking_target if rng.random() < 0.5 else right.ranking_target,
        "holding_days": int(left.holding_days if rng.random() < 0.5 else right.holding_days),
        "exit_family": left.exit_family if rng.random() < 0.5 else right.exit_family,
        "stop_type": left.stop_type if rng.random() < 0.5 else right.stop_type,
        "regime_gate": left.regime_gate if rng.random() < 0.5 else right.regime_gate,
        "liquidity_gate": left.liquidity_gate if rng.random() < 0.5 else right.liquidity_gate,
        "weight_profile": left.weight_profile if rng.random() < 0.5 else right.weight_profile,
        "signal_bias": left.signal_bias if rng.random() < 0.5 else right.signal_bias,
        "minimum_confidence": float(left.minimum_confidence if rng.random() < 0.5 else right.minimum_confidence),
        "minimum_ready_rate": float(left.minimum_ready_rate if rng.random() < 0.5 else right.minimum_ready_rate),
        "bad_pick_penalty_scale": float(left.bad_pick_penalty_scale if rng.random() < 0.5 else right.bad_pick_penalty_scale),
        "playbook_up_score_bonus": float(left.playbook_up_score_bonus if rng.random() < 0.5 else right.playbook_up_score_bonus),
        "playbook_down_score_bonus": float(left.playbook_down_score_bonus if rng.random() < 0.5 else right.playbook_down_score_bonus),
        "mutation_parent_ids": tuple(
            dict.fromkeys(
                [
                    *tuple(left.mutation_parent_ids or ()),
                    *tuple(right.mutation_parent_ids or ()),
                    _text(left_record.get("candidate_id"), fallback=_text(left_record.get("method_signature_hash"), fallback=_text(left_record.get("method_id")))),
                    _text(right_record.get("candidate_id"), fallback=_text(right_record.get("method_signature_hash"), fallback=_text(right_record.get("method_id")))),
                ]
            )
        ),
        "generation_index": generation_index,
        "population_kind": population,
    }
    if population == "regime_selector_population" and rng.random() < 0.25:
        values["direction"] = "short" if values["direction"] == "long" else "long"
    return ScenarioSpec(**values)


def _scenario_boundary_probe_spec(
    parent_record: dict[str, Any],
    *,
    generation_index: int,
    rng: random.Random,
    variant: str = "permissive",
) -> ScenarioSpec:
    parent_spec = _scenario_spec_from_record(parent_record, generation_index=generation_index)
    population = parent_spec.population_kind
    parent_ids = tuple(
        dict.fromkeys(
            [
                *tuple(parent_spec.mutation_parent_ids or ()),
                _text(parent_record.get("candidate_id"), fallback=_text(parent_record.get("method_signature_hash"), fallback=_text(parent_record.get("method_id")))),
            ]
        )
    )
    if population == "short_expert_population":
        if variant == "restrictive":
            return ScenarioSpec(
                direction="short",
                entry_family="short_crash_top",
                filter_family="bad_pick_removal",
                ranking_target="bad_pick_removal",
                holding_days=15,
                exit_family="risk_exit",
                stop_type="hard",
                regime_gate="trend_short",
                liquidity_gate="tight",
                weight_profile="bad_pick_heavy",
                signal_bias="sell",
                minimum_confidence=0.90,
                minimum_ready_rate=0.70,
                bad_pick_penalty_scale=7.5,
                playbook_up_score_bonus=0.0,
                playbook_down_score_bonus=0.0,
                mutation_parent_ids=parent_ids,
                generation_index=generation_index,
                population_kind=population,
            )
        return ScenarioSpec(
            direction="short",
            entry_family="short_downtrend",
            filter_family="symbol_specific_adjustment",
            ranking_target="drawdown_control",
            holding_days=10,
            exit_family="signal_exit",
            stop_type="adaptive",
            regime_gate="multi_regime",
            liquidity_gate="balanced",
            weight_profile="balanced",
            signal_bias="buy",
            minimum_confidence=0.08,
            minimum_ready_rate=0.0,
            bad_pick_penalty_scale=1.5,
            playbook_up_score_bonus=0.0,
            playbook_down_score_bonus=0.15,
            mutation_parent_ids=parent_ids,
            generation_index=generation_index,
            population_kind=population,
        )
    if population == "regime_selector_population":
        if variant == "restrictive":
            return ScenarioSpec(
                direction="long",
                entry_family="regime_router",
                filter_family="regime_adjustment",
                ranking_target="regime_balance",
                holding_days=30,
                exit_family="regime_switch",
                stop_type="adaptive",
                regime_gate="multi_regime",
                liquidity_gate="balanced",
                weight_profile="regime_heavy",
                signal_bias="balanced",
                minimum_confidence=0.85,
                minimum_ready_rate=0.60,
                bad_pick_penalty_scale=2.4,
                playbook_up_score_bonus=0.0,
                playbook_down_score_bonus=0.0,
                mutation_parent_ids=parent_ids,
                generation_index=generation_index,
                population_kind=population,
            )
        return ScenarioSpec(
            direction="long",
            entry_family="regime_router",
            filter_family="boundary_feature",
            ranking_target="regime_balance",
            holding_days=10,
            exit_family="regime_switch",
            stop_type="trailing",
            regime_gate="multi_regime",
            liquidity_gate="balanced",
            weight_profile="balanced",
            signal_bias="balanced",
            minimum_confidence=0.08,
            minimum_ready_rate=0.0,
            bad_pick_penalty_scale=0.8,
            playbook_up_score_bonus=0.08,
            playbook_down_score_bonus=0.08,
            mutation_parent_ids=parent_ids,
            generation_index=generation_index,
            population_kind=population,
        )
    if variant == "restrictive":
        return ScenarioSpec(
            direction="long",
            entry_family="long_reversal",
            filter_family="boundary_feature",
            ranking_target="stability",
            holding_days=15,
            exit_family="time_stop",
            stop_type="trailing",
            regime_gate="top_warning",
            liquidity_gate="balanced",
            weight_profile="boundary_heavy",
            signal_bias="buy",
            minimum_confidence=0.88,
            minimum_ready_rate=0.68,
            bad_pick_penalty_scale=0.0,
            playbook_up_score_bonus=0.0,
            playbook_down_score_bonus=0.0,
            mutation_parent_ids=parent_ids,
            generation_index=generation_index,
            population_kind=population,
        )
    return ScenarioSpec(
        direction="long",
        entry_family="long_pullback",
        filter_family="symbol_specific_adjustment",
        ranking_target="topk_uplift",
        holding_days=20,
        exit_family="signal_exit",
        stop_type="adaptive",
        regime_gate="multi_regime",
        liquidity_gate="balanced",
        weight_profile="balanced",
        signal_bias="buy",
        minimum_confidence=0.08,
        minimum_ready_rate=0.0,
        bad_pick_penalty_scale=0.5,
        playbook_up_score_bonus=0.12,
        playbook_down_score_bonus=0.0,
        mutation_parent_ids=parent_ids,
        generation_index=generation_index,
        population_kind=population,
    )


def _scenario_records_from_session(
    *,
    search_id: str,
    session_id: str,
    session_state: dict[str, Any],
    family_leaderboard: dict[str, Any],
    generation_index: int,
    source_kind: str,
    mutation_parent_ids_by_method_id: dict[str, tuple[str, ...]] | None = None,
) -> list[dict[str, Any]]:
    family_lookup = {
        _text(row.get("method_family")): row
        for row in (family_leaderboard.get("family_summary") or [])
        if isinstance(row, dict)
    }
    records: list[dict[str, Any]] = []
    for family_result in session_state.get("family_results") or []:
        if not isinstance(family_result, dict):
            continue
        compare = family_result.get("compare") if isinstance(family_result.get("compare"), dict) else {}
        candidate_results = [item for item in (compare.get("candidate_results") or []) if isinstance(item, dict)]
        family_row = family_lookup.get(_text(family_result.get("method_family")))
        if family_row is None and family_lookup:
            family_row = next(iter(family_lookup.values()))
        for candidate_result in candidate_results:
            try:
                candidate_method = candidate_result.get("candidate_method") if isinstance(candidate_result.get("candidate_method"), dict) else {}
                method_id = _text(candidate_method.get("method_id"), fallback=_text(candidate_result.get("plan_id")))
                records.append(
                    _scenario_record_from_candidate_result(
                        session_id=session_id,
                        family_result=family_result,
                        family_row=family_row,
                        candidate_result=candidate_result,
                        generation_index=generation_index,
                        source_kind=source_kind,
                        mutation_parent_ids=tuple(mutation_parent_ids_by_method_id.get(method_id) or ()) if isinstance(mutation_parent_ids_by_method_id, dict) else None,
                    )
                )
            except Exception:
                continue
    return records


def _scenario_seed_memory_from_sessions(*, max_sessions: int = 120) -> dict[str, Any]:
    root = tradex_research_sessions_root()
    session_dirs = [entry for entry in root.iterdir() if entry.is_dir()] if root.exists() else []
    session_dirs = sorted(
        session_dirs,
        key=lambda path: (path.stat().st_mtime if path.exists() else 0.0, path.name),
        reverse=True,
    )[: max(1, int(max_sessions))]
    records: list[dict[str, Any]] = []
    source_refs: list[dict[str, Any]] = []
    for session_dir in session_dirs:
        session_id = session_dir.name
        if "wide-topk" in session_id:
            continue
        session_state_path = session_dir / "session.json"
        family_leaderboard_path = session_dir / "family_leaderboard.json"
        compare_path = session_dir / "compare.json"
        if not session_state_path.exists() or not family_leaderboard_path.exists() or not compare_path.exists():
            continue
        session_state = _read_json_file(session_state_path)
        family_leaderboard = _read_json_file(family_leaderboard_path)
        if not session_state or not family_leaderboard:
            continue
        source_refs.append(
            {
                "session_id": session_id,
                "session_state_path": str(session_state_path),
                "family_leaderboard_path": str(family_leaderboard_path),
                "compare_path": str(compare_path),
            }
        )
        records.extend(
            _scenario_records_from_session(
                search_id=session_id,
                session_id=session_id,
                session_state=session_state,
                family_leaderboard=family_leaderboard,
                generation_index=0,
                source_kind="legacy_session_artifact",
            )
        )
    source_record_count = len(records)
    records = [record for record in records if _scenario_is_seed_eligible(record)]
    records = sorted(records, key=_scenario_parent_priority)
    population_summary: dict[str, dict[str, int]] = {}
    for record in records:
        bucket = population_summary.setdefault(
            _text(record.get("population_kind"), fallback="long_expert_population"),
            {"count": 0, "keep": 0, "hold": 0, "drop": 0},
        )
        bucket["count"] += 1
        bucket[_text(record.get("decision"), fallback="hold")] = bucket.get(_text(record.get("decision"), fallback="hold"), 0) + 1
    return {
        "schema_version": SCENARIO_SEARCH_MEMORY_SCHEMA_VERSION,
        "generated_at": _utc_now_iso(),
        "source_artifact_refs": source_refs,
        "excluded_seed_record_count": max(0, source_record_count - len(records)),
        "records": records,
        "population_summary": population_summary,
        "record_count": len(records),
    }


def _scenario_family_specs_from_records(
    search_id: str,
    population_records: dict[str, list[dict[str, Any]]],
    *,
    generation_index: int,
    max_candidates_per_family: int,
    rng: random.Random,
) -> tuple[FamilySpec, ...]:
    families: list[FamilySpec] = []
    for population_kind in ("long_expert_population", "short_expert_population", "regime_selector_population"):
        parents = population_records.get(population_kind) or []
        if not parents:
            continue
        parent_count = max(1, min(len(parents), max_candidates_per_family + 1))
        selected_parents = parents[:parent_count]
        candidate_specs: list[CandidateMethodSpec] = []
        seen_signatures: set[str] = set()
        seen_method_pairs: set[tuple[str, str]] = set()

        def _add_candidate(spec: ScenarioSpec) -> None:
            signature = _scenario_candidate_signature(spec)
            method_pair = (spec.population_kind, _scenario_method_thesis(spec))
            if signature in seen_signatures or method_pair in seen_method_pairs:
                return
            seen_signatures.add(signature)
            seen_method_pairs.add(method_pair)
            candidate_specs.append(_scenario_candidate_spec(search_id, spec))

        if selected_parents:
            _add_candidate(_scenario_boundary_probe_spec(selected_parents[0], generation_index=generation_index, rng=rng))
        for parent in selected_parents[: max_candidates_per_family]:
            _add_candidate(_scenario_mutate_spec(parent, generation_index=generation_index, rng=rng))
        if len(selected_parents) >= 2 and len(candidate_specs) < max_candidates_per_family:
            _add_candidate(
                _scenario_crossover_spec(
                    selected_parents[0],
                    selected_parents[1],
                    generation_index=generation_index,
                    rng=rng,
                )
            )
        attempts = 0
        max_attempts = max(6, max_candidates_per_family * 8)
        while len(candidate_specs) < max_candidates_per_family and attempts < max_attempts:
            _add_candidate(_scenario_mutate_spec(rng.choice(selected_parents), generation_index=generation_index, rng=rng))
            attempts += 1
        families.append(
            FamilySpec(
                method_family=population_kind,
                family_title=f"{_scenario_population_label(population_kind)} / g{generation_index + 1}",
                family_thesis=f"Scenario search generation {generation_index + 1} for {population_kind}",
                candidates=tuple(candidate_specs[: max(1, max_candidates_per_family)]),
            )
        )
    return tuple(families)


def _scenario_wide_topk_family_specs(
    search_id: str,
    champion_record: dict[str, Any],
    challenger_record: dict[str, Any],
    *,
    top_k: int,
) -> tuple[FamilySpec, ...]:
    families: list[FamilySpec] = []
    def _wide_family_method_family(variant_token: str) -> str:
        return f"w{variant_token}"

    for variant_token, record in (("0", champion_record), ("1", challenger_record)):
        population_kind = _text(record.get("population_kind"), fallback="unknown")
        candidate_specs: list[CandidateMethodSpec] = []
        seen_signatures: set[str] = set()
        seen_method_pairs: set[tuple[str, str]] = set()

        def _add_candidate(spec: ScenarioSpec, *, label: str) -> None:
            candidate_spec = _scenario_candidate_spec_with_top_k(search_id, spec, top_k=top_k, variant_label=label)
            signature = _scenario_hash(
                {
                    "method_family": candidate_spec.method_family,
                    "method_id": candidate_spec.method_id,
                    "method_thesis": candidate_spec.method_thesis,
                    "top_k": int(candidate_spec.plan_overrides.get("top_k") or 0),
                }
            )
            method_pair = (candidate_spec.method_family, candidate_spec.method_thesis)
            if signature in seen_signatures or method_pair in seen_method_pairs:
                return
            seen_signatures.add(signature)
            seen_method_pairs.add(method_pair)
            candidate_specs.append(candidate_spec)

        _add_candidate(_scenario_spec_from_record(record, generation_index=int(record.get("generation_index") or 0)), label=f"{variant_token}-seed")
        _add_candidate(
            _scenario_boundary_probe_spec(
                record,
                generation_index=int(record.get("generation_index") or 0),
                rng=random.Random(_seed_int(f"{search_id}:{_text(record.get('candidate_id'))}:restrictive", 17)),
                variant="restrictive",
            ),
            label=f"{variant_token}-restrictive",
        )
        _add_candidate(
            _scenario_boundary_probe_spec(
                record,
                generation_index=int(record.get("generation_index") or 0),
                rng=random.Random(_seed_int(f"{search_id}:{_text(record.get('candidate_id'))}:permissive", 17)),
                variant="permissive",
            ),
            label=f"{variant_token}-permissive",
        )
        families.append(
            FamilySpec(
                method_family=_wide_family_method_family(variant_token),
                family_title=f"{_scenario_population_label(population_kind)} / wide top-k",
                family_thesis=f"Wide top-k reevaluation for {_scenario_population_label(population_kind)}",
                candidates=tuple(candidate_specs[:3]),
            )
        )
    return tuple(families)


def _scenario_group_records_by_population(records: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        population_kind = _text(record.get("population_kind"), fallback="long_expert_population")
        grouped.setdefault(population_kind, []).append(record)
    for population_kind in grouped:
        grouped[population_kind] = sorted(grouped[population_kind], key=_scenario_parent_priority)
    return grouped


def _scenario_select_survivors(
    records: list[dict[str, Any]],
    *,
    max_per_population: int,
) -> dict[str, list[dict[str, Any]]]:
    grouped = _scenario_group_records_by_population(records)
    survivors: dict[str, list[dict[str, Any]]] = {}
    for population_kind, bucket in grouped.items():
        filtered = [
            record
            for record in bucket
            if _text(record.get("decision")) in {"keep", "hold"} and bool(record.get("sample_validity"))
        ]
        filtered.sort(key=_scenario_parent_priority)
        survivors[population_kind] = filtered[: max(1, int(max_per_population))]
    return survivors


def _scenario_rollup(records: list[dict[str, Any]]) -> dict[str, Any]:
    grouped = _scenario_group_records_by_population(records)
    population_rows: list[dict[str, Any]] = []
    for population_kind, bucket in grouped.items():
        population_rows.append(
            {
                "population_kind": population_kind,
                "count": len(bucket),
                "keep_count": sum(1 for row in bucket if _text(row.get("decision")) == "keep"),
                "hold_count": sum(1 for row in bucket if _text(row.get("decision")) == "hold"),
                "drop_count": sum(1 for row in bucket if _text(row.get("decision")) == "drop"),
                "sample_valid_count": sum(1 for row in bucket if bool(row.get("sample_validity"))),
                "best_candidate_id": _text(bucket[0].get("candidate_id")) if bucket else "",
                "best_score": float(bucket[0].get("score") or 0.0) if bucket else 0.0,
            }
        )
    population_rows.sort(key=lambda row: (_text(row.get("population_kind")),))
    return {
        "schema_version": SCENARIO_SEARCH_ROLLUP_SCHEMA_VERSION,
        "generated_at": _utc_now_iso(),
        "overview": {
            "candidate_count": len(records),
            "population_count": len(population_rows),
            "keep_count": sum(1 for row in records if _text(row.get("decision")) == "keep"),
            "hold_count": sum(1 for row in records if _text(row.get("decision")) == "hold"),
            "drop_count": sum(1 for row in records if _text(row.get("decision")) == "drop"),
            "sample_valid_count": sum(1 for row in records if bool(row.get("sample_validity"))),
        },
        "population_rows": population_rows,
        "candidate_rows": records,
    }


def _scenario_lineage_rows(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for record in records:
        rows.append(
            {
                "candidate_id": _text(record.get("candidate_id")),
                "population_kind": _text(record.get("population_kind")),
                "generation_index": int(record.get("generation_index") or 0),
                "parent_candidate_ids": list(_text(item) for item in (record.get("parent_candidate_ids") or []) if _text(item)),
                "mutation_parent_ids": list(_text(item) for item in (record.get("parent_candidate_ids") or []) if _text(item)),
                "decision": _text(record.get("decision"), fallback="hold"),
                "source_kind": _text(record.get("source_kind"), fallback="unknown"),
                "scenario_definition": dict(record.get("scenario_definition") or {}),
                "score": float(record.get("score") or 0.0),
                "failure_reason": _text(record.get("failure_reason"), fallback=""),
                "source_artifact_refs": dict(record.get("source_artifact_refs") or {}),
            }
        )
    return rows


def _scenario_best_candidate(records: list[dict[str, Any]], *, population_kind: str | None = None) -> dict[str, Any]:
    bucket = [record for record in records if population_kind is None or _text(record.get("population_kind")) == population_kind]
    if not bucket:
        return {}
    def _branch_priority(row: dict[str, Any]) -> tuple[float, float, float, float, float, str]:
        evaluation_metrics = row.get("evaluation_metrics") if isinstance(row.get("evaluation_metrics"), dict) else {}
        branching_metrics = row.get("branching_metrics") if isinstance(row.get("branching_metrics"), dict) else {}
        branching_priority = (
            float(evaluation_metrics.get("changed_rank_count") or 0.0) * 10.0
            + float(evaluation_metrics.get("changed_top5_members_count") or 0.0) * 5.0
            + float(evaluation_metrics.get("changed_top10_members_count") or 0.0) * 3.0
            + float(evaluation_metrics.get("top5_boundary_score_gap") or 0.0) * 100.0
            + float(evaluation_metrics.get("top10_boundary_score_gap") or 0.0) * 80.0
            + (2.0 if bool(branching_metrics.get("meaningful_topk_branching_possible")) else 0.0)
        )
        return (
            -branching_priority,
            -float(row.get("score") or 0.0),
            -float((row.get("comparison") or {}).get("challenger_top5_ret20_mean") or 0.0),
            -float((row.get("comparison") or {}).get("challenger_top10_ret20_mean") or 0.0),
            float((row.get("comparison") or {}).get("challenger_dd") or 0.0),
            _text(row.get("candidate_id"), fallback=_text(row.get("method_id"))),
        )

    return sorted(bucket, key=_branch_priority)[0]


def _scenario_wide_topk_branch_candidate(candidate_results: list[dict[str, Any]]) -> dict[str, Any]:
    if not candidate_results:
        return {}

    def _wide_branch_priority(candidate_result: dict[str, Any]) -> tuple[float, float, float, float, float, str]:
        selection_compare = candidate_result.get("selection_compare") if isinstance(candidate_result.get("selection_compare"), dict) else {}
        return (
            -float(selection_compare.get("changed_top5_members_count") or 0.0),
            -abs(float(selection_compare.get("top5_boundary_score_gap") or 0.0)),
            -float(selection_compare.get("changed_top10_members_count") or 0.0),
            -float(selection_compare.get("changed_rank_count") or 0.0),
            -float(selection_compare.get("challenger_topk_ret20_mean") or 0.0),
            _text(candidate_result.get("candidate_method", {}).get("method_id"), fallback=_text(candidate_result.get("plan_id"), fallback=_text(candidate_result.get("method_signature_hash")))),
        )

    return sorted(candidate_results, key=_wide_branch_priority)[0]


def run_tradex_scenario_search(
    *,
    search_id: str,
    generations: int = 3,
    random_seed: int = 7,
    universe_size: int = DEFAULT_UNIVERSE_SIZE,
    max_candidates_per_family: int = DEFAULT_MAX_CANDIDATES_PER_FAMILY,
    max_parents_per_population: int = 3,
    session_scope_id: str | None = None,
    ret20_source_mode: str = tradex.TRADEX_RET20_SOURCE_MODE_PRECOMPUTED,
    finalize_shared_rollups: bool = False,
    exploratory_min_trading_days: int | None = TRADEX_EXPLORATORY_EVAL_WINDOW_MIN_TRADING_DAYS,
) -> dict[str, Any]:
    if generations < 1:
        raise ValueError("generations must be >= 1")
    if max_candidates_per_family < 1:
        raise ValueError("max_candidates_per_family must be >= 1")
    if max_parents_per_population < 1:
        raise ValueError("max_parents_per_population must be >= 1")

    search_id = _text(search_id, fallback="scenario-search")
    started_at = _utc_now_iso()
    tradex_reports_root().mkdir(parents=True, exist_ok=True)
    tradex_research_keep_root().mkdir(parents=True, exist_ok=True)
    scope_id = _text(session_scope_id, fallback=FEATURE_SNAPSHOT_SCOPE_ID)
    seed_memory = _scenario_seed_memory_from_sessions()
    memory_records = list(seed_memory.get("records") or [])
    population_history = _scenario_group_records_by_population(memory_records)
    generation_summaries: list[dict[str, Any]] = []
    lineage_rows: list[dict[str, Any]] = []
    session_results: list[dict[str, Any]] = []
    stop_reason = "max_generations_reached"
    best_overall_score: float | None = None
    no_improvement_streak = 0
    no_keep_streak = 0
    final_session_state: dict[str, Any] = {}
    final_family_leaderboard: dict[str, Any] = {}

    for generation_index in range(generations):
        parent_groups = _scenario_select_survivors(
            list(population_history.get("long_expert_population", []))
            + list(population_history.get("short_expert_population", []))
            + list(population_history.get("regime_selector_population", [])),
            max_per_population=max_parents_per_population,
        )
        family_specs = _scenario_family_specs_from_records(
            search_id,
            parent_groups,
            generation_index=generation_index,
            max_candidates_per_family=max_candidates_per_family,
            rng=random.Random(_seed_int(f"{search_id}:g{generation_index}", random_seed)),
        )
        generation_parent_map: dict[str, tuple[str, ...]] = {}
        for family_spec in family_specs:
            for candidate_spec in family_spec.candidates:
                generation_parent_map[_text(candidate_spec.method_id)] = tuple(
                    _text(item) for item in (candidate_spec.mutation_parent_ids or ()) if _text(item)
                )
        if not family_specs:
            stop_reason = "no_family_specs"
            break

        session_id = f"{_slug(search_id)}-g{generation_index + 1:02d}"
        session_scope = scope_id
        session_state = run_tradex_research_session(
            session_id=session_id,
            random_seed=int(random_seed) + generation_index,
            universe_size=int(universe_size),
            max_candidates_per_family=int(max_candidates_per_family),
            session_scope_id=session_scope,
            ret20_source_mode=ret20_source_mode,
            finalize_shared_rollups=bool(finalize_shared_rollups and generation_index == generations - 1),
            family_specs=family_specs,
            exploratory_min_trading_days=exploratory_min_trading_days,
        )
        session_results.append(session_state)

        family_leaderboard = _read_json_file(_session_family_leaderboard_file(session_id))
        generation_records = _scenario_records_from_session(
            search_id=search_id,
            session_id=session_id,
            session_state=session_state,
            family_leaderboard=family_leaderboard,
            generation_index=generation_index,
            source_kind="evaluated_generation",
            mutation_parent_ids_by_method_id=generation_parent_map,
        )
        if not generation_records:
            stop_reason = "no_generation_records"
            final_session_state = session_state
            final_family_leaderboard = family_leaderboard
            break

        memory_records.extend(generation_records)
        lineage_rows.extend(_scenario_lineage_rows(generation_records))
        final_session_state = session_state
        final_family_leaderboard = family_leaderboard

        best_record = _scenario_best_candidate(generation_records)
        best_score = float(best_record.get("score") or 0.0) if best_record else 0.0
        keep_count = sum(1 for row in generation_records if _text(row.get("decision")) == "keep")
        hold_count = sum(1 for row in generation_records if _text(row.get("decision")) == "hold")
        drop_count = sum(1 for row in generation_records if _text(row.get("decision")) == "drop")
        sample_valid_count = sum(1 for row in generation_records if bool(row.get("sample_validity")))
        generation_summaries.append(
            {
                "generation_index": generation_index,
                "session_id": session_id,
                "session_scope_id": session_scope,
                "candidate_count": len(generation_records),
                "population_count": len(_scenario_group_records_by_population(generation_records)),
                "keep_count": keep_count,
                "hold_count": hold_count,
                "drop_count": drop_count,
                "sample_valid_count": sample_valid_count,
                "best_candidate_id": _text(best_record.get("candidate_id"), fallback=""),
                "best_population_kind": _text(best_record.get("population_kind"), fallback=""),
                "best_score": best_score,
                "survivor_count": sum(
                    1
                    for row in generation_records
                    if _text(row.get("decision")) in {"keep", "hold"} and bool(row.get("sample_validity"))
                ),
                "stop_reason": "",
            }
        )

        if best_overall_score is None or best_score > best_overall_score + 0.5:
            best_overall_score = best_score
            no_improvement_streak = 0
        else:
            no_improvement_streak += 1
        if keep_count <= 0:
            no_keep_streak += 1
        else:
            no_keep_streak = 0

        population_history = _scenario_select_survivors(
            generation_records,
            max_per_population=max_parents_per_population,
        )
        if not any(population_history.values()):
            stop_reason = "no_survivors"
            generation_summaries[-1]["stop_reason"] = stop_reason
            break
        if no_keep_streak >= 2:
            stop_reason = "no_new_keep_candidates"
            generation_summaries[-1]["stop_reason"] = stop_reason
            break
        if no_improvement_streak >= 2:
            stop_reason = "no_meaningful_improvement"
            generation_summaries[-1]["stop_reason"] = stop_reason
            break

    seed_records = list(seed_memory.get("records") or [])
    final_records = list(memory_records)
    final_population = _scenario_select_survivors(final_records, max_per_population=max_parents_per_population)
    final_population_records = list(final_population.get("long_expert_population", [])) + list(final_population.get("short_expert_population", [])) + list(final_population.get("regime_selector_population", []))
    rollup = _scenario_rollup(final_records)
    memory_payload = {
        "schema_version": SCENARIO_SEARCH_MEMORY_SCHEMA_VERSION,
        "search_id": search_id,
        "generated_at": _utc_now_iso(),
        "started_at": started_at,
        "finished_at": _utc_now_iso(),
        "seed_artifact_refs": seed_memory.get("source_artifact_refs") if isinstance(seed_memory.get("source_artifact_refs"), list) else [],
        "record_count": len(final_records),
        "seed_record_count": len(seed_records),
        "records": final_records,
        "population_summary": rollup.get("population_rows") or [],
        "stop_reason": stop_reason,
    }
    population_payload = {
        "schema_version": SCENARIO_SEARCH_POPULATION_SCHEMA_VERSION,
        "search_id": search_id,
        "generated_at": _utc_now_iso(),
        "generation_index": generation_summaries[-1]["generation_index"] if generation_summaries else 0,
        "populations": [
            {
                "population_kind": population_kind,
                "candidates": records,
            }
            for population_kind, records in sorted(final_population.items(), key=lambda item: item[0])
        ],
    }
    generation_payload = {
        "schema_version": SCENARIO_SEARCH_GENERATION_SUMMARY_SCHEMA_VERSION,
        "search_id": search_id,
        "generated_at": _utc_now_iso(),
        "started_at": started_at,
        "finished_at": _utc_now_iso(),
        "generation_count": len(generation_summaries),
        "stop_reason": stop_reason,
        "generation_summaries": generation_summaries,
    }
    lineage_payload = {
        "schema_version": SCENARIO_SEARCH_LINEAGE_SCHEMA_VERSION,
        "search_id": search_id,
        "generated_at": _utc_now_iso(),
        "lineage_rows": lineage_rows,
        "lineage_count": len(lineage_rows),
    }
    keep_drop_hold_payload = {
        "schema_version": SCENARIO_SEARCH_ROLLUP_SCHEMA_VERSION,
        "search_id": search_id,
        "generated_at": _utc_now_iso(),
        "overview": {
            "candidate_count": len(final_records),
            "seed_record_count": len(seed_records),
            "generation_count": len(generation_summaries),
            "keep_count": sum(1 for row in final_records if _text(row.get("decision")) == "keep"),
            "hold_count": sum(1 for row in final_records if _text(row.get("decision")) == "hold"),
            "drop_count": sum(1 for row in final_records if _text(row.get("decision")) == "drop"),
            "sample_valid_count": sum(1 for row in final_records if bool(row.get("sample_validity"))),
            "population_count": len(final_population),
            "stop_reason": stop_reason,
        },
        "population_rows": rollup.get("population_rows") or [],
        "candidate_rows": final_records,
    }
    regime_selector_records = [row for row in final_records if _text(row.get("population_kind")) == "regime_selector_population"]
    regime_selector_payload = {
        "schema_version": SCENARIO_SEARCH_ROLLUP_SCHEMA_VERSION,
        "search_id": search_id,
        "generated_at": _utc_now_iso(),
        "overview": {
            "candidate_count": len(regime_selector_records),
            "keep_count": sum(1 for row in regime_selector_records if _text(row.get("decision")) == "keep"),
            "hold_count": sum(1 for row in regime_selector_records if _text(row.get("decision")) == "hold"),
            "drop_count": sum(1 for row in regime_selector_records if _text(row.get("decision")) == "drop"),
            "sample_valid_count": sum(1 for row in regime_selector_records if bool(row.get("sample_validity"))),
        },
        "candidate_rows": regime_selector_records,
    }
    seed_best = _scenario_best_candidate(seed_records)
    challenger_best = _scenario_best_candidate(final_records)
    champion_challenger_payload = {
        "schema_version": SCENARIO_SEARCH_ROLLUP_SCHEMA_VERSION,
        "search_id": search_id,
        "generated_at": _utc_now_iso(),
        "champion": seed_best,
        "challenger": challenger_best,
        "comparison": {
            "score_delta": float(challenger_best.get("score") or 0.0) - float(seed_best.get("score") or 0.0),
            "population_kind_delta": _text(challenger_best.get("population_kind"), fallback="") != _text(seed_best.get("population_kind"), fallback=""),
            "generation_index_delta": int(challenger_best.get("generation_index") or 0) - int(seed_best.get("generation_index") or 0),
            "decision_delta": {
                "champion": _text(seed_best.get("decision"), fallback=""),
                "challenger": _text(challenger_best.get("decision"), fallback=""),
            },
        },
    }

    wide_topk_payload: dict[str, Any] = {
        "schema_version": SCENARIO_SEARCH_WIDE_TOPK_SCHEMA_VERSION,
        "search_id": search_id,
        "generated_at": _utc_now_iso(),
        "status": "skipped",
        "wide_top_k": 5,
        "reason": "missing_seed_or_challenger",
    }
    if seed_best and challenger_best:
        wide_session_id = f"{_slug(search_id)}-wide-topk-{_seed_int(f'{search_id}:{started_at}', random_seed)}"
        try:
            wide_family_specs = _scenario_wide_topk_family_specs(
                search_id,
                seed_best,
                challenger_best,
                top_k=5,
            )
            wide_session_state = run_tradex_research_session(
                session_id=wide_session_id,
                random_seed=int(random_seed) + 1000,
                universe_size=int(universe_size),
                max_candidates_per_family=3,
                session_scope_id=scope_id,
                ret20_source_mode=ret20_source_mode,
                finalize_shared_rollups=False,
                family_specs=wide_family_specs,
                exploratory_min_trading_days=exploratory_min_trading_days,
            )
            wide_family_results = [
                family_result
                for family_result in (wide_session_state.get("family_results") or [])
                if isinstance(family_result, dict)
            ]
            wide_candidate_results = [
                candidate_result
                for family_result in wide_family_results
                for candidate_result in (family_result.get("candidate_results") or [])
                if isinstance(candidate_result, dict)
            ]
            wide_seed_champion_id = _text(seed_best.get("candidate_id"), fallback=_text(seed_best.get("method_signature_hash"), fallback=_text(seed_best.get("method_id"))))
            wide_champion_result = next(
                (
                    candidate_result
                    for candidate_result in wide_candidate_results
                    if wide_seed_champion_id
                    and wide_seed_champion_id
                    == _text(
                        candidate_result.get("method_signature_hash"),
                        fallback=_text(candidate_result.get("plan_id"), fallback=_text(candidate_result.get("candidate_method", {}).get("method_id"))),
                    )
                ),
                wide_candidate_results[0] if wide_candidate_results else {},
            )
            wide_branch_result = _scenario_wide_topk_branch_candidate(wide_candidate_results)
            if not wide_branch_result:
                wide_branch_result = wide_champion_result
            wide_champion_contract = wide_champion_result.get("same_condition_contract") if isinstance(wide_champion_result.get("same_condition_contract"), dict) else {}
            wide_branch_contract = wide_branch_result.get("same_condition_contract") if isinstance(wide_branch_result.get("same_condition_contract"), dict) else {}
            wide_champion_compare = wide_champion_result.get("selection_compare") if isinstance(wide_champion_result.get("selection_compare"), dict) else {}
            wide_branch_compare = wide_branch_result.get("selection_compare") if isinstance(wide_branch_result.get("selection_compare"), dict) else {}
            wide_candidate_summaries = [
                {
                    "candidate_id": _text(
                        candidate_result.get("method_signature_hash"),
                        fallback=_text(candidate_result.get("plan_id"), fallback=_text(candidate_result.get("candidate_method", {}).get("method_id"))),
                    ),
                    "population_kind": _text(candidate_result.get("population_kind"), fallback=_text(candidate_result.get("candidate_method", {}).get("method_family"))),
                    "decision": _text(candidate_result.get("candidate_local_decision"), fallback=""),
                    "selection_compare": candidate_result.get("selection_compare") if isinstance(candidate_result.get("selection_compare"), dict) else {},
                }
                for candidate_result in wide_candidate_results
            ]
            wide_topk_payload = {
                "schema_version": SCENARIO_SEARCH_WIDE_TOPK_SCHEMA_VERSION,
                "search_id": search_id,
                "generated_at": _utc_now_iso(),
                "status": "complete",
                "wide_top_k": 5,
                "session_id": wide_session_id,
                "champion": {
                    "candidate_id": _text(wide_champion_result.get("plan_id"), fallback=_text(wide_champion_result.get("method_signature_hash"))),
                    "decision": _text(wide_champion_result.get("candidate_local_decision"), fallback=""),
                    "selection_compare": wide_champion_compare,
                    "same_condition_contract_hash": _text(wide_champion_contract.get("contract_hash")),
                },
                "challenger": {
                    "candidate_id": _text(wide_branch_result.get("plan_id"), fallback=_text(wide_branch_result.get("method_signature_hash"))),
                    "decision": _text(wide_branch_result.get("candidate_local_decision"), fallback=""),
                    "selection_compare": wide_branch_compare,
                    "same_condition_contract_hash": _text(wide_branch_contract.get("contract_hash")),
                },
                "comparison": {
                    "score_delta": float(wide_branch_compare.get("challenger_topk_ret20_mean") or 0.0) - float(wide_champion_compare.get("challenger_topk_ret20_mean") or 0.0),
                    "decision_delta": {
                        "champion": _text(wide_champion_result.get("candidate_local_decision"), fallback=""),
                        "challenger": _text(wide_branch_result.get("candidate_local_decision"), fallback=""),
                    },
                    "changed_rank_count": int(wide_branch_compare.get("changed_rank_count") or 0),
                    "changed_top5_members_count": int(wide_branch_compare.get("changed_top5_members_count") or 0),
                    "changed_top10_members_count": int(wide_branch_compare.get("changed_top10_members_count") or 0),
                    "top5_boundary_score_gap": float(wide_branch_compare.get("top5_boundary_score_gap") or 0.0),
                    "top10_boundary_score_gap": float(wide_branch_compare.get("top10_boundary_score_gap") or 0.0),
                },
                "candidates": wide_candidate_summaries,
            }
        except Exception as exc:
            wide_topk_payload = {
                "schema_version": SCENARIO_SEARCH_WIDE_TOPK_SCHEMA_VERSION,
                "search_id": search_id,
                "generated_at": _utc_now_iso(),
                "status": "failed",
                "wide_top_k": 5,
                "reason": _text(exc),
                "champion": seed_best,
                "challenger": challenger_best,
            }

    memory_path = _scenario_search_file(search_id, SCENARIO_SEARCH_FILE)
    population_path = _scenario_search_file(search_id, SCENARIO_POPULATION_FILE)
    generation_path = _scenario_search_file(search_id, SCENARIO_GENERATION_SUMMARY_FILE)
    lineage_path = _scenario_search_file(search_id, SCENARIO_LINEAGE_FILE)
    keep_drop_path = _scenario_search_file(search_id, SCENARIO_KEEP_DROP_HOLD_FILE)
    selector_eval_path = _scenario_search_file(search_id, SCENARIO_SELECTOR_EVAL_FILE)
    champion_path = _scenario_search_file(search_id, SCENARIO_CHAMPION_FILE)
    wide_topk_path = _scenario_search_file(search_id, SCENARIO_WIDE_TOPK_FILE)
    for path, payload, artifact_name in (
        (memory_path, memory_payload, "research_memory"),
        (population_path, population_payload, "candidate_population"),
        (generation_path, generation_payload, "generation_summary"),
        (lineage_path, lineage_payload, "evolution_lineage"),
        (keep_drop_path, keep_drop_hold_payload, "keep_drop_hold_rollup"),
        (selector_eval_path, regime_selector_payload, "regime_selector_eval"),
        (champion_path, champion_challenger_payload, "champion_vs_challenger_summary"),
        (wide_topk_path, wide_topk_payload, "practical_topk_eval"),
    ):
        _write_json(path, payload)
        _verify_json_roundtrip(path, payload, artifact_name=artifact_name)

    return {
        "status": "complete",
        "search_id": search_id,
        "scenario_search_scope_mode": "explicit" if session_scope_id else "feature_snapshot_default",
        "scenario_search_scope_id": scope_id,
        "schema_version": SCENARIO_SEARCH_ROLLUP_SCHEMA_VERSION,
        "generation_count": len(generation_summaries),
        "stop_reason": stop_reason,
        "memory_path": str(memory_path),
        "candidate_population_path": str(population_path),
        "generation_summary_path": str(generation_path),
        "lineage_path": str(lineage_path),
        "keep_drop_hold_rollup_path": str(keep_drop_path),
        "regime_selector_eval_path": str(selector_eval_path),
        "champion_vs_challenger_summary_path": str(champion_path),
        "practical_topk_eval_path": str(wide_topk_path),
        "memory": memory_payload,
        "candidate_population": population_payload,
        "generation_summary": generation_payload,
        "lineage": lineage_payload,
        "keep_drop_hold_rollup": keep_drop_hold_payload,
        "regime_selector_eval": regime_selector_payload,
        "champion_vs_challenger_summary": champion_challenger_payload,
        "practical_topk_eval": wide_topk_payload,
        "session_results": session_results,
        "final_session_state": final_session_state,
        "final_family_leaderboard": final_family_leaderboard,
        "seed_memory": seed_memory,
    }


def _aggregate_logic_search_rows(rows: list[dict[str, Any]], *, group_key: str) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        key = _text(row.get(group_key), fallback="unknown")
        grouped.setdefault(key, []).append(row)
    aggregated: list[dict[str, Any]] = []
    for key, bucket in grouped.items():
        scores = [float(item.get("score") or 0.0) for item in bucket]
        promote_ready_count = sum(1 for item in bucket if bool(item.get("promote_ready")))
        best_item = max(bucket, key=lambda item: (float(item.get("score") or 0.0), int(item.get("sample_count") or 0), _text(item.get("method_id")), _text(item.get("session_id"))))
        aggregated.append(
            {
                group_key: key,
                "session_count": len(bucket),
                "promote_ready_count": promote_ready_count,
                "promote_ready_rate": float(promote_ready_count / len(bucket)) if bucket else 0.0,
                "score_mean": float(sum(scores) / len(scores)) if scores else 0.0,
                "score_min": float(min(scores)) if scores else 0.0,
                "score_max": float(max(scores)) if scores else 0.0,
                "sample_count_mean": float(sum(float(item.get("sample_count") or 0.0) for item in bucket) / len(bucket)) if bucket else 0.0,
                "best_session_id": _text(best_item.get("session_id")),
                "best_session_scope_id": _text(best_item.get("session_scope_id")),
                "best_random_seed": int(best_item.get("random_seed") or 0),
                "best_method_family": _text(best_item.get("method_family")),
                "best_method_id": _text(best_item.get("method_id")),
                "best_method_title": _text(best_item.get("method_title")),
                "best_candidate_signature_hash": _text(best_item.get("candidate_signature_hash")),
                "best_score": float(best_item.get("score") or 0.0),
                "best_promote_ready": bool(best_item.get("promote_ready")),
                "best_top5_ret20_mean": float(best_item.get("top5_ret20_mean") or 0.0),
                "best_top10_ret20_mean": float(best_item.get("top10_ret20_mean") or 0.0),
                "best_monthly_capture_mean": float(best_item.get("monthly_capture_mean") or 0.0),
                "best_worst_regime_ret20_mean": float(best_item.get("worst_regime_ret20_mean") or 0.0),
                "best_dd": float(best_item.get("dd") or 0.0),
                "best_turnover": float(best_item.get("turnover") or 0.0),
                "best_liquidity_fail_rate": float(best_item.get("liquidity_fail_rate") or 0.0),
                "best_zero_pass_months": float(best_item.get("zero_pass_months") or 0.0),
                "mutation_hints": _json_ready(best_item.get("mutation_hints") or []),
            }
        )
    return sorted(aggregated, key=lambda item: (-float(item.get("score_mean") or 0.0), -float(item.get("promote_ready_rate") or 0.0), _text(item.get(group_key))))


def _build_logic_search_rollup(
    *,
    search_id: str,
    session_rows: list[dict[str, Any]],
    candidate_rows: list[dict[str, Any]],
    session_scope_ids: list[str],
    random_seeds: list[int],
) -> dict[str, Any]:
    session_rows = sorted(
        session_rows,
        key=lambda row: (-float(row.get("score") or 0.0), -int(row.get("sample_count") or 0), _text(row.get("session_id"))),
    )
    candidate_rows = sorted(
        candidate_rows,
        key=lambda row: (-float(row.get("score") or 0.0), -int(row.get("sample_count") or 0), _text(row.get("method_family")), _text(row.get("method_id"))),
    )
    family_rows = _aggregate_logic_search_rows(candidate_rows, group_key="method_family")
    candidate_method_rows = _aggregate_logic_search_rows(candidate_rows, group_key="candidate_signature_hash")
    best_session = session_rows[0] if session_rows else {}
    best_candidate = candidate_rows[0] if candidate_rows else {}
    best_family = family_rows[0] if family_rows else {}
    return {
        "schema_version": LOGIC_SEARCH_ROLLUP_SCHEMA_VERSION,
        "search_id": search_id,
        "generated_at": _utc_now_iso(),
        "session_scope_ids": list(session_scope_ids),
        "random_seeds": list(random_seeds),
        "overview": {
            "session_count": len(session_rows),
            "candidate_count": len(candidate_rows),
            "candidate_method_count": len(candidate_method_rows),
            "family_count": len(family_rows),
            "promote_ready_session_count": sum(1 for row in session_rows if bool(row.get("promote_ready"))),
            "promote_ready_candidate_count": sum(1 for row in candidate_rows if bool(row.get("promote_ready"))),
            "promote_ready_family_count": sum(1 for row in family_rows if bool(row.get("promote_ready_count") or 0)),
        },
        "best_session": _json_ready(best_session),
        "best_candidate": _json_ready(best_candidate),
        "best_family": _json_ready(best_family),
        "session_rows": _json_ready(session_rows),
        "candidate_rows": _json_ready(candidate_rows),
        "candidate_method_rows": _json_ready(candidate_method_rows),
        "family_rows": _json_ready(family_rows),
        "next_round_proposals": _json_ready(
            [
                {
                    "method_family": row.get("method_family"),
                    "method_id": row.get("method_id"),
                    "method_title": row.get("method_title"),
                    "score": row.get("score"),
                    "promote_ready": row.get("promote_ready"),
                    "mutation_hints": row.get("mutation_hints"),
                }
                for row in candidate_rows[: min(5, len(candidate_rows))]
            ]
        ),
    }


def _format_logic_search_rollup_markdown(rollup: dict[str, Any]) -> str:
    overview = rollup.get("overview") if isinstance(rollup.get("overview"), dict) else {}
    best_session = rollup.get("best_session") if isinstance(rollup.get("best_session"), dict) else {}
    best_candidate = rollup.get("best_candidate") if isinstance(rollup.get("best_candidate"), dict) else {}
    best_family = rollup.get("best_family") if isinstance(rollup.get("best_family"), dict) else {}
    session_rows = rollup.get("session_rows") if isinstance(rollup.get("session_rows"), list) else []
    candidate_rows = rollup.get("candidate_rows") if isinstance(rollup.get("candidate_rows"), list) else []
    family_rows = rollup.get("family_rows") if isinstance(rollup.get("family_rows"), list) else []
    lines: list[str] = []
    lines.append("# TRADEX Logic Search Rollup")
    lines.append("")
    lines.append(f"- generated_at: `{_text(rollup.get('generated_at'))}`")
    lines.append(f"- search_id: `{_text(rollup.get('search_id'))}`")
    lines.append(f"- session_scope_ids: `{', '.join(str(item) for item in rollup.get('session_scope_ids') or [] if str(item).strip()) or 'none'}`")
    lines.append(f"- random_seeds: `{', '.join(str(item) for item in rollup.get('random_seeds') or [] if str(item).strip()) or 'none'}`")
    lines.append(
        "- overview: sessions=`{sessions}`, candidates=`{candidates}`, candidate_methods=`{candidate_methods}`, families=`{families}`, ready_sessions=`{ready_sessions}`, ready_candidates=`{ready_candidates}`, ready_families=`{ready_families}`".format(
            sessions=int(overview.get("session_count") or 0),
            candidates=int(overview.get("candidate_count") or 0),
            candidate_methods=int(overview.get("candidate_method_count") or 0),
            families=int(overview.get("family_count") or 0),
            ready_sessions=int(overview.get("promote_ready_session_count") or 0),
            ready_candidates=int(overview.get("promote_ready_candidate_count") or 0),
            ready_families=int(overview.get("promote_ready_family_count") or 0),
        )
    )
    lines.append("")
    if best_candidate:
        lines.append("## Best Candidate")
        lines.append("")
        lines.append(
            "- {family} / {method} / score=`{score:.4f}` / promote_ready=`{ready}` / sample_count=`{sample}`".format(
                family=_text(best_candidate.get("method_family"), fallback="unknown"),
                method=_text(best_candidate.get("method_title"), fallback=_text(best_candidate.get("method_id"), fallback="unknown")),
                score=float(best_candidate.get("score") or 0.0),
                ready="yes" if bool(best_candidate.get("promote_ready")) else "no",
                sample=int(best_candidate.get("sample_count") or 0),
            )
        )
        lines.append(f"- mutation_hints: `{json.dumps(_json_ready(best_candidate.get('mutation_hints') or []), ensure_ascii=False, sort_keys=True)}`")
        lines.append("")
    if best_family:
        lines.append("## Best Family")
        lines.append("")
        lines.append(
            "- {family} / best_method=`{method}` / score_mean=`{score:.4f}` / promote_ready_rate=`{rate:.2%}`".format(
                family=_text(best_family.get("method_family"), fallback="unknown"),
                method=_text(best_family.get("best_method_id"), fallback="unknown"),
                score=float(best_family.get("score_mean") or 0.0),
                rate=float(best_family.get("promote_ready_rate") or 0.0),
            )
        )
        lines.append("")
    if best_session:
        lines.append("## Best Session")
        lines.append("")
        lines.append(
            "- {session} / seed=`{seed}` / scope=`{scope}` / score=`{score:.4f}`".format(
                session=_text(best_session.get("session_id"), fallback="unknown"),
                seed=int(best_session.get("random_seed") or 0),
                scope=_text(best_session.get("session_scope_id"), fallback="unknown"),
                score=float(best_session.get("score") or 0.0),
            )
        )
        lines.append("")
    lines.append("## Session Rows")
    lines.append("")
    lines.append("| session | seed | scope | best_family | score | promote_ready |")
    lines.append("| --- | ---: | --- | --- | ---: | --- |")
    for row in session_rows:
        lines.append(
            "| {session} | {seed} | {scope} | {family} | {score:.4f} | {ready} |".format(
                session=_text(row.get("session_id")),
                seed=int(row.get("random_seed") or 0),
                scope=_text(row.get("session_scope_id"), fallback="unknown"),
                family=_text(row.get("method_family"), fallback=_text(row.get("best_method_family"), fallback="unknown")),
                score=float(row.get("score") or 0.0),
                ready="yes" if bool(row.get("promote_ready")) else "no",
            )
        )
    lines.append("")
    lines.append("## Candidate Leaderboard")
    lines.append("")
    lines.append("| method_family | method_id | score_mean | ready_rate | sessions | best_score |")
    lines.append("| --- | --- | ---: | ---: | ---: | ---: |")
    for row in candidate_rows:
        lines.append(
            "| {family} | {method} | {score:.4f} | {rate:.2%} | {sessions} | {best:.4f} |".format(
                family=_text(row.get("method_family"), fallback="unknown"),
                method=_text(row.get("method_id"), fallback="unknown"),
                score=float(row.get("score_mean") or 0.0),
                rate=float(row.get("promote_ready_rate") or 0.0),
                sessions=int(row.get("session_count") or 0),
                best=float(row.get("best_score") or row.get("score_max") or 0.0),
            )
        )
    lines.append("")
    lines.append("## Family Leaderboard")
    lines.append("")
    lines.append("| family | best_method | score_mean | ready_rate | sessions | best_score |")
    lines.append("| --- | --- | ---: | ---: | ---: | ---: |")
    for row in family_rows:
        lines.append(
            "| {family} | {method} | {score:.4f} | {rate:.2%} | {sessions} | {best:.4f} |".format(
                family=_text(row.get("method_family"), fallback="unknown"),
                method=_text(row.get("best_method_id"), fallback="unknown"),
                score=float(row.get("score_mean") or 0.0),
                rate=float(row.get("promote_ready_rate") or 0.0),
                sessions=int(row.get("session_count") or 0),
                best=float(row.get("best_score") or 0.0),
            )
        )
    lines.append("")
    lines.append("## Next Round Proposals")
    lines.append("")
    proposals = rollup.get("next_round_proposals") if isinstance(rollup.get("next_round_proposals"), list) else []
    if proposals:
        for proposal in proposals:
            lines.append(
                "- {family} / {method} / score=`{score:.4f}` / mutation_hints=`{hints}`".format(
                    family=_text(proposal.get("method_family"), fallback="unknown"),
                    method=_text(proposal.get("method_title"), fallback=_text(proposal.get("method_id"), fallback="unknown")),
                    score=float(proposal.get("score") or 0.0),
                    hints=json.dumps(_json_ready(proposal.get("mutation_hints") or []), ensure_ascii=False, sort_keys=True),
                )
            )
    else:
        lines.append("- none")
    return "\n".join(lines).rstrip() + "\n"


def _write_logic_search_artifacts(search_id: str, rollup: dict[str, Any]) -> tuple[Path, Path]:
    rollup_path = _logic_search_rollup_file(search_id)
    report_path = _logic_search_rollup_report_file(search_id)
    rollup["rollup_path"] = str(rollup_path)
    rollup["report_path"] = str(report_path)
    _write_json(rollup_path, rollup)
    _verify_json_roundtrip(rollup_path, rollup, artifact_name="logic_search_rollup")
    report_path.write_text(_format_logic_search_rollup_markdown(rollup), encoding="utf-8")
    return rollup_path, report_path


def run_tradex_logic_search(
    *,
    search_id: str,
    random_seeds: list[int],
    universe_size: int = DEFAULT_UNIVERSE_SIZE,
    max_candidates_per_family: int = DEFAULT_MAX_CANDIDATES_PER_FAMILY,
    session_scope_ids: list[str] | None = None,
    ret20_source_mode: str = tradex.TRADEX_RET20_SOURCE_MODE_PRECOMPUTED,
    rounds: int = 2,
    max_mutated_families: int = 4,
    max_mutations_per_family: int = 3,
    finalize_shared_rollups: bool = False,
    exploratory_min_trading_days: int | None = TRADEX_EXPLORATORY_EVAL_WINDOW_MIN_TRADING_DAYS,
) -> dict[str, Any]:
    search_id = _text(search_id, fallback="logic-search")
    normalized_seeds = [int(seed) for seed in random_seeds if str(seed).strip()]
    if not normalized_seeds:
        raise ValueError("random_seeds must not be empty")
    logic_rounds = max(1, int(rounds))
    max_mutated_families = max(1, int(max_mutated_families))
    max_mutations_per_family = max(1, int(max_mutations_per_family))
    normalized_scope_ids = [_text(scope_id) for scope_id in (session_scope_ids or []) if _text(scope_id)]
    if not normalized_scope_ids:
        normalized_scope_ids = [search_id]
    session_rows: list[dict[str, Any]] = []
    candidate_rows: list[dict[str, Any]] = []
    session_results: list[dict[str, Any]] = []
    round_family_specs: tuple[FamilySpec, ...] | None = None
    round_summaries: list[dict[str, Any]] = []
    for round_index in range(logic_rounds):
        if round_index == 0:
            round_family_specs = None
        elif candidate_rows:
            round_family_specs = _logic_search_mutated_family_specs(
                candidate_rows,
                round_index=round_index,
                max_families=max_mutated_families,
                max_mutations_per_family=max_mutations_per_family,
            )
        else:
            round_family_specs = None

        round_session_rows: list[dict[str, Any]] = []
        round_candidate_rows: list[dict[str, Any]] = []
        round_session_results: list[dict[str, Any]] = []
        for scope_id in normalized_scope_ids:
            for seed in normalized_seeds:
                session_id = _scope_session_id(f"{search_id}:r{round_index}", scope_id, seed)
                session_state = run_tradex_research_session(
                    session_id=session_id,
                    random_seed=int(seed),
                    universe_size=int(universe_size),
                    max_candidates_per_family=int(max_candidates_per_family),
                    session_scope_id=scope_id,
                ret20_source_mode=ret20_source_mode,
                finalize_shared_rollups=finalize_shared_rollups,
                family_specs=round_family_specs,
                exploratory_min_trading_days=exploratory_min_trading_days,
            )
                session_results.append(session_state)
                round_session_results.append(session_state)
                family_results = session_state.get("family_results") if isinstance(session_state.get("family_results"), list) else []
                extracted_rows: list[dict[str, Any]] = []
                for family_result in family_results:
                    if not isinstance(family_result, dict):
                        continue
                    candidate_row = _logic_search_candidate_row(search_id=search_id, session_state=session_state, family_result=family_result)
                    if candidate_row is None:
                        continue
                    candidate_row["round_index"] = round_index
                    extracted_rows.append(candidate_row)
                    round_candidate_rows.append(candidate_row)
                    candidate_rows.append(candidate_row)
                if extracted_rows:
                    session_best = max(
                        extracted_rows,
                        key=lambda row: (float(row.get("score") or 0.0), int(row.get("sample_count") or 0), _text(row.get("method_id"))),
                    )
                else:
                    session_best = {
                        "session_id": session_state.get("session_id"),
                        "session_scope_id": session_state.get("session_scope_id"),
                        "random_seed": session_state.get("random_seed"),
                        "method_family": None,
                        "method_id": None,
                        "method_title": None,
                        "score": 0.0,
                        "promote_ready": False,
                        "sample_count": 0,
                        "candidate_signature_hash": None,
                        "mutation_hints": [],
                        "round_index": round_index,
                    }
                session_best = dict(session_best)
                session_best["family_count"] = len(family_results)
                session_best["candidate_count"] = len(extracted_rows)
                session_best["round_index"] = round_index
                round_session_rows.append(session_best)
                session_rows.append(session_best)
        round_best_session = max(round_session_rows, key=lambda row: (float(row.get("score") or 0.0), int(row.get("sample_count") or 0), _text(row.get("method_id")))) if round_session_rows else {}
        round_best_candidate = max(round_candidate_rows, key=lambda row: (float(row.get("score") or 0.0), int(row.get("sample_count") or 0), _text(row.get("method_id")))) if round_candidate_rows else {}
        round_family_specs_payload = [
            {
                "method_family": spec.method_family,
                "family_title": spec.family_title,
                "family_thesis": spec.family_thesis,
                "candidate_ids": [candidate.method_id for candidate in spec.candidates],
                "candidate_titles": [candidate.method_title for candidate in spec.candidates],
                "candidate_count": len(spec.candidates),
            }
            for spec in (round_family_specs or ())
        ]
        round_summaries.append(
            {
                "round_index": round_index,
                "session_count": len(round_session_rows),
                "candidate_count": len(round_candidate_rows),
                "family_count": len(round_family_specs or ()),
                "best_session": _json_ready(round_best_session),
                "best_candidate": _json_ready(round_best_candidate),
                "family_specs": round_family_specs_payload,
            }
        )
    rollup = _build_logic_search_rollup(
        search_id=search_id,
        session_rows=session_rows,
        candidate_rows=candidate_rows,
        session_scope_ids=normalized_scope_ids,
        random_seeds=normalized_seeds,
    )
    rollup["round_count"] = logic_rounds
    rollup["round_summaries"] = round_summaries
    rollup_path, report_path = _write_logic_search_artifacts(search_id, rollup)
    rollup["rollup_path"] = str(rollup_path)
    rollup["report_path"] = str(report_path)
    return {
        "status": "complete",
        "schema_version": LOGIC_SEARCH_ROLLUP_SCHEMA_VERSION,
        "search_id": search_id,
        "session_count": len(session_rows),
        "candidate_count": len(candidate_rows),
        "family_count": len(rollup.get("family_rows") or []),
        "best_session": rollup.get("best_session") or {},
        "best_candidate": rollup.get("best_candidate") or {},
        "best_family": rollup.get("best_family") or {},
        "next_round_proposals": rollup.get("next_round_proposals") or [],
        "round_count": logic_rounds,
        "round_summaries": round_summaries,
        "rollup_path": str(rollup_path),
        "report_path": str(report_path),
        "rollup": rollup,
        "session_results": session_results,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a single-machine TRADEX research session.")
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--random-seed", required=True, type=int)
    parser.add_argument("--universe-size", type=int, default=DEFAULT_UNIVERSE_SIZE)
    parser.add_argument("--max-candidates-per-family", type=int, default=DEFAULT_MAX_CANDIDATES_PER_FAMILY)
    parser.add_argument(
        "--ret20-source-mode",
        default=tradex.TRADEX_RET20_SOURCE_MODE_PRECOMPUTED,
        choices=[tradex.TRADEX_RET20_SOURCE_MODE_PRECOMPUTED, tradex.TRADEX_RET20_SOURCE_MODE_DERIVED],
        help="ret_20 source path for research sessions.",
    )
    parser.add_argument("--stability-sweep", action="store_true", help="Run the multi-seed stability sweep instead of a single session.")
    parser.add_argument(
        "--stability-seeds",
        default=",".join(str(seed) for seed in STABILITY_SWEEP_DEFAULT_SEEDS),
        help="Comma-separated random seeds for --stability-sweep.",
    )
    parser.add_argument(
        "--stability-scope-id",
        default="",
        help="Optional fixed scope id for stability sweeps; keeps the evaluation universe stable across seed runs.",
    )
    parser.add_argument(
        "--scope-stability-sweep",
        action="store_true",
        help="Run a scope x seed sweep to compare multiple session_scope_id candidates.",
    )
    parser.add_argument(
        "--scope-stability-seeds",
        default=",".join(str(seed) for seed in STABILITY_SWEEP_DEFAULT_SEEDS),
        help="Comma-separated random seeds for --scope-stability-sweep.",
    )
    parser.add_argument(
        "--scope-stability-scope-ids",
        default="",
        help="Comma-separated session_scope_id candidates for --scope-stability-sweep.",
    )
    parser.add_argument("--logic-search", action="store_true", help="Run the repeated logic search loop instead of a single session.")
    parser.add_argument(
        "--logic-search-seeds",
        default="",
        help="Comma-separated random seeds for --logic-search. Falls back to --random-seed when omitted.",
    )
    parser.add_argument(
        "--logic-search-scope-ids",
        default="",
        help="Comma-separated session_scope_id candidates for --logic-search. Falls back to the session id when omitted.",
    )
    parser.add_argument(
        "--logic-search-universe-size",
        type=int,
        default=None,
        help="Optional universe size override for --logic-search.",
    )
    parser.add_argument(
        "--logic-search-max-candidates-per-family",
        type=int,
        default=None,
        help="Optional max-candidates-per-family override for --logic-search.",
    )
    parser.add_argument(
        "--logic-search-rounds",
        type=int,
        default=2,
        help="Number of exploration rounds for --logic-search.",
    )
    parser.add_argument(
        "--logic-search-max-mutated-families",
        type=int,
        default=4,
        help="Upper bound on auto-generated mutated families per exploration round.",
    )
    parser.add_argument(
        "--logic-search-max-mutations-per-family",
        type=int,
        default=3,
        help="Upper bound on mutated candidates generated per family.",
    )
    parser.add_argument(
        "--logic-search-exploratory-min-trading-days",
        type=int,
        default=TRADEX_EXPLORATORY_EVAL_WINDOW_MIN_TRADING_DAYS,
        help="Fallback minimum trading days for exploratory eval windows when standard/fallback windows are insufficient. Set to 0 to disable.",
    )
    parser.add_argument("--scenario-search", action="store_true", help="Run the self-evolving scenario search loop instead of a single session.")
    parser.add_argument(
        "--scenario-search-generations",
        type=int,
        default=3,
        help="Number of generations for --scenario-search.",
    )
    parser.add_argument(
        "--scenario-search-max-parents-per-population",
        type=int,
        default=3,
        help="Upper bound on parent candidates retained per population for --scenario-search.",
    )
    parser.add_argument(
        "--scenario-search-scope-id",
        default="",
        help="Optional fixed session_scope_id for --scenario-search; falls back to the feature snapshot scope when omitted.",
    )
    parser.add_argument(
        "--scenario-search-exploratory-min-trading-days",
        type=int,
        default=TRADEX_EXPLORATORY_EVAL_WINDOW_MIN_TRADING_DAYS,
        help="Fallback minimum trading days for exploratory eval windows when scenario-search stages need a reduced window. Set to 0 to disable.",
    )
    return parser


def _validate_runtime_db_contract() -> dict[str, Any]:
    readiness_report = tradex_environment_readiness.evaluate_environment_readiness()
    readiness_summary = readiness_report.get("readiness_summary") if isinstance(readiness_report.get("readiness_summary"), dict) else {}
    contract = {
        "schema_version": "tradex_runtime_db_contract_validation_v1",
        "ready": bool(readiness_report.get("ready")),
        "cause_class": _text(readiness_report.get("cause_class"), fallback="unknown"),
        "cause_source": _text(readiness_report.get("cause_source"), fallback="unknown"),
        "database_path": _text(readiness_summary.get("database_path")),
        "required_table": _text(readiness_summary.get("required_table"), fallback="market_regime_daily"),
        "table_exists": bool(readiness_summary.get("table_exists")),
        "table_row_count": int(readiness_summary.get("table_row_count") or 0),
        "label_version_row_count": int(readiness_summary.get("label_version_row_count") or 0),
        "selected_window_count": int(readiness_summary.get("selected_window_count") or 0),
        "selected_window_issues": [str(item) for item in (readiness_summary.get("selected_window_issues") or []) if str(item).strip()],
        "checked_at": _text(readiness_report.get("checked_at")),
        "remediation_hint": _text(readiness_report.get("remediation_hint")),
    }
    if not contract["ready"]:
        raise RuntimeError(
            "TRADEX runtime DB contract failed: "
            + json.dumps(_json_ready(contract), ensure_ascii=False, sort_keys=True)
        )
    return contract


def main() -> int:
    args = _build_parser().parse_args()
    _validate_runtime_db_contract()
    if bool(args.scenario_search):
        result = run_tradex_scenario_search(
            search_id=str(args.session_id),
            generations=int(args.scenario_search_generations),
            random_seed=int(args.random_seed),
            universe_size=int(args.universe_size),
            max_candidates_per_family=int(args.max_candidates_per_family),
            max_parents_per_population=int(args.scenario_search_max_parents_per_population),
            session_scope_id=str(args.scenario_search_scope_id).strip() or None,
            ret20_source_mode=str(args.ret20_source_mode),
            exploratory_min_trading_days=(
                int(args.scenario_search_exploratory_min_trading_days)
                if int(args.scenario_search_exploratory_min_trading_days) > 0
                else None
            ),
        )
        print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2))
        return 0
    if bool(args.logic_search):
        seeds = [int(item.strip()) for item in str(args.logic_search_seeds).split(",") if item.strip()]
        if not seeds:
            seeds = [int(args.random_seed)]
        scope_ids = [item.strip() for item in str(args.logic_search_scope_ids).split(",") if item.strip()]
        result = run_tradex_logic_search(
            search_id=str(args.session_id),
            random_seeds=seeds,
            universe_size=int(args.logic_search_universe_size or args.universe_size),
            max_candidates_per_family=int(args.logic_search_max_candidates_per_family or args.max_candidates_per_family),
            session_scope_ids=scope_ids,
            ret20_source_mode=str(args.ret20_source_mode),
            rounds=int(args.logic_search_rounds),
            max_mutated_families=int(args.logic_search_max_mutated_families),
            max_mutations_per_family=int(args.logic_search_max_mutations_per_family),
            exploratory_min_trading_days=(
                int(args.logic_search_exploratory_min_trading_days)
                if int(args.logic_search_exploratory_min_trading_days) > 0
                else None
            ),
        )
        print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2))
        return 0
    if bool(args.scope_stability_sweep):
        seeds = [int(item.strip()) for item in str(args.scope_stability_seeds).split(",") if item.strip()]
        scope_ids = [item.strip() for item in str(args.scope_stability_scope_ids).split(",") if item.strip()]
        result = run_tradex_scope_stability_sweep(
            session_id=str(args.session_id),
            session_scope_ids=scope_ids,
            random_seeds=seeds,
            universe_size=int(args.universe_size),
            max_candidates_per_family=int(args.max_candidates_per_family),
            ret20_source_mode=str(args.ret20_source_mode),
        )
    elif bool(args.stability_sweep):
        seeds = [int(item.strip()) for item in str(args.stability_seeds).split(",") if item.strip()]
        result = run_tradex_stability_sweep(
            session_id=str(args.session_id),
            random_seeds=seeds,
            universe_size=int(args.universe_size),
            max_candidates_per_family=int(args.max_candidates_per_family),
            session_scope_id=str(args.stability_scope_id).strip() or None,
            ret20_source_mode=str(args.ret20_source_mode),
        )
    else:
        result = run_tradex_research_session(
            session_id=str(args.session_id),
            random_seed=int(args.random_seed),
            universe_size=int(args.universe_size),
            max_candidates_per_family=int(args.max_candidates_per_family),
            ret20_source_mode=str(args.ret20_source_mode),
        )
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
