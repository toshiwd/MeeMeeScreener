from __future__ import annotations

import json
import math
import shutil
import subprocess
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

TOP_K_VALUES = (5, 10, 20)
CHAMPION_TOPK_VALUES = (5, 10, 20)
DEFAULT_SCAN_ROOT_NAMES = (
    "compare.json",
    "family.json",
    "session.json",
    "run_manifest.json",
    "summary.json",
    "decision.json",
    "family_leaderboard.json",
    "session_leaderboard_rollup.json",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if pd.isna(value):
        return None
    return value


def _json_text(payload: Any) -> str:
    return json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, default=str)


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_json_text(payload) + "\n", encoding="utf-8")
    return path


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _safe_path(value: str | Path | None, default: Path) -> Path:
    if value and str(value).strip():
        return Path(str(value)).expanduser().resolve()
    return default.resolve()


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None or pd.isna(value):
        return float(default)
    try:
        out = float(value)
    except Exception:
        return float(default)
    return out if math.isfinite(out) else float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    if value is None or pd.isna(value):
        return int(default)
    try:
        return int(value)
    except Exception:
        return int(default)


def _normalize_text(value: Any, default: str = "") -> str:
    text = str(value or "").strip()
    return text or default


def _current_score(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    try:
        out = float(value)
    except Exception:
        return None
    if not math.isfinite(out):
        return None
    return out


def _mean_or_none(values: Iterable[Any]) -> float | None:
    series = [float(value) for value in values if value is not None and not pd.isna(value)]
    if not series:
        return None
    return float(sum(series) / len(series))


def _median_or_none(values: Iterable[Any]) -> float | None:
    series = sorted(float(value) for value in values if value is not None and not pd.isna(value))
    if not series:
        return None
    mid = len(series) // 2
    if len(series) % 2:
        return float(series[mid])
    return float((series[mid - 1] + series[mid]) / 2.0)


def _value_or_none(value: Any) -> Any:
    if value is None or pd.isna(value):
        return None
    return value


def _ensure_columns(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    for column in ("anchor_date", "symbol", "side"):
        if column in frame.columns:
            frame[column] = frame[column].astype(str)
    for column in (
        "champion_rank",
        "candidate_rank",
        "rank",
        "adjusted_rank",
        "original_rank",
    ):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("Int64")
    for column in (
        "score",
        "candidate_score",
        "champion_score",
        "original_score",
        "adjusted_score",
        "forward_ret_20d",
        "path_value_score_v1",
        "mfe_20d",
        "mae_20d",
        "gap_pct",
        "vol_ratio5_20",
        "candle_body_ratio",
        "candle_upper_wick_ratio",
        "candle_lower_wick_ratio",
    ):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    for column in (
        "champion_selected_top5",
        "champion_selected_top10",
        "champion_selected_top20",
        "top15_label",
        "bottom15_label",
        "monthly_context_no_lookahead",
        "weekly_context_no_lookahead",
    ):
        if column in frame.columns:
            frame[column] = frame[column].fillna(False).astype(bool)
    for column in (
        "monthly_context",
        "weekly_context",
        "shape_classification",
        "shape_joined",
        "prefilter_bucket",
        "prefilter_reason",
        "market_regime_bucket",
        "family_regime_context",
        "dominant_regime_context",
    ):
        if column in frame.columns:
            frame[column] = frame[column].astype(object)
    return frame


def _is_json_artifact(path: Path) -> bool:
    return path.is_file() and path.suffix.lower() == ".json"


def _scan_candidate_artifacts(roots: Iterable[Path]) -> list[Path]:
    artifacts: list[Path] = []
    globs = [
        "compare.json",
        "family.json",
        "session.json",
        "run_manifest.json",
        "summary.json",
        "decision.json",
        "family_leaderboard.json",
        "session_leaderboard_rollup.json",
        "*_compare.json",
        "*_summary.json",
        "*_decision.json",
        "_ARTIFACT_COMPLETE.json",
    ]
    if shutil.which("rg"):
        for root in roots:
            if not root.exists():
                continue
            if root.is_file() and _is_json_artifact(root):
                artifacts.append(root)
                continue
            scan_roots = [root]
            if str(root).lower() == str(Path(r"G:\Tradex").resolve()).lower():
                allowed_names = {
                    "artifacts",
                    "candidate_generation_pre_filter_context_shape_v1",
                    "candidate_generation_pre_filter_context_shape_v1_accumulated",
                    "candidate_generation_pre_filter_context_shape_v1_accumulated_v2",
                    "candidate_generation_pre_filter_context_shape_v1_larger",
                    "candidate_generation_two_stage_admission_context_shape_v1",
                    "families",
                    "keep",
                    "reports",
                    "research_sessions",
                    "reflectability_gap_audit_v1",
                    "runs",
                    "scratch",
                    "champion_topk_bad_pick_veto_v1",
                }
                keyword_fragments = (
                    "audit",
                    "candidate",
                    "family",
                    "keep",
                    "report",
                    "research",
                    "reflectability",
                    "run",
                    "session",
                )
                scan_roots = [
                    child
                    for child in root.iterdir()
                    if child.is_dir()
                    and (
                        child.name in allowed_names
                        or any(fragment in child.name.lower() for fragment in keyword_fragments)
                    )
                ]
            for scan_root in scan_roots:
                cmd = ["rg", "--files", str(scan_root), "--hidden"]
                for glob in globs:
                    cmd.extend(["-g", glob])
                try:
                    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
                except Exception:
                    result = None
                if result and result.stdout:
                    for line in result.stdout.splitlines():
                        path = Path(line.strip())
                        if path.is_file():
                            artifacts.append(path)
        unique: list[Path] = []
        seen: set[str] = set()
        for path in artifacts:
            key = str(path.resolve())
            if key not in seen:
                seen.add(key)
                unique.append(path)
        return sorted(unique, key=lambda value: str(value).lower())
    for root in roots:
        if not root.exists():
            continue
        if root.is_file() and _is_json_artifact(root):
            artifacts.append(root)
            continue
        for path in root.rglob("*.json"):
            if path.name in DEFAULT_SCAN_ROOT_NAMES:
                artifacts.append(path)
                continue
            if path.name.endswith("_compare.json") or path.name.endswith("_summary.json") or path.name.endswith("_decision.json"):
                artifacts.append(path)
                continue
            if path.name == "_ARTIFACT_COMPLETE.json":
                artifacts.append(path)
    unique: list[Path] = []
    seen: set[str] = set()
    for path in artifacts:
        key = str(path.resolve())
        if key not in seen:
            seen.add(key)
            unique.append(path)
    return sorted(unique, key=lambda value: str(value).lower())


def _artifact_root_for(path: Path) -> str:
    return str(path.parent.resolve())


def _extract_first(payload: dict[str, Any], keys: Iterable[str]) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _extract_topk_metrics(payload: dict[str, Any]) -> dict[str, Any]:
    compare_topk = payload.get("compare_topk")
    if isinstance(compare_topk, dict):
        topk_rows: dict[str, Any] = {}
        for top_k in TOP_K_VALUES:
            topk_rows[str(top_k)] = compare_topk.get(str(top_k)) or compare_topk.get(top_k) or {}
        return topk_rows
    candidate_vs_champion = payload.get("champion_vs_challenger")
    if isinstance(candidate_vs_champion, dict):
        topk = candidate_vs_champion.get("selection_only") or candidate_vs_champion.get("policy_trade") or {}
        if isinstance(topk, dict):
            return {str(key): value for key, value in topk.items() if str(key) in {"5", "10", "20"}}
    if isinstance(payload.get("topk_brief"), dict):
        return {str(key): value for key, value in payload["topk_brief"].items() if str(key) in {"5", "10", "20"}}
    return {}


def _extract_decision_fields(payload: dict[str, Any]) -> dict[str, Any]:
    best_result = payload.get("best_result") if isinstance(payload.get("best_result"), dict) else {}
    return {
        "candidate_local_decision": payload.get("candidate_local_decision") or payload.get("local_decision") or payload.get("decision"),
        "session_aggregate_decision": payload.get("session_aggregate_decision") or payload.get("session_decision") or best_result.get("candidate_local_decision"),
        "authoritative_rollup_decision": payload.get("authoritative_rollup_decision") or payload.get("decision") or best_result.get("authoritative_rollup_decision"),
        "typed_reason": payload.get("decision_reason")
        or payload.get("reason")
        or payload.get("authoritative_gate_reason")
        or payload.get("compare_engine_local_reason"),
    }


def _extract_cost_mode(payload: dict[str, Any]) -> str | None:
    same_condition = payload.get("same_condition_contract")
    if isinstance(same_condition, dict):
        cost_model = same_condition.get("cost_model")
        if isinstance(cost_model, dict):
            return cost_model.get("mode") or cost_model.get("cost_model")
    cost_model = payload.get("cost_model")
    if isinstance(cost_model, dict):
        return cost_model.get("mode")
    return None


def _extract_artifact_detail_level(payload: dict[str, Any]) -> str | None:
    value = payload.get("artifact_detail_level")
    if isinstance(value, str):
        return value
    best_result = payload.get("best_result")
    if isinstance(best_result, dict):
        detail = best_result.get("artifact_detail_level")
        if isinstance(detail, str):
            return detail
    return None


def _extract_same_condition_contract(payload: dict[str, Any]) -> dict[str, Any] | None:
    same = payload.get("same_condition_contract")
    if isinstance(same, dict):
        return same
    best_result = payload.get("best_result")
    if isinstance(best_result, dict):
        maybe = best_result.get("same_condition_contract")
        if isinstance(maybe, dict):
            return maybe
    return None


def _extract_regime_split_summary(payload: dict[str, Any]) -> dict[str, Any]:
    candidates = [
        payload.get("regime_summary"),
        payload.get("by_regime"),
        payload.get("regime_split"),
        payload.get("regime_bucket_breakdown"),
        payload.get("family_regime_breakdown"),
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            return candidate
    if isinstance(payload.get("regime_summary"), list):
        return {str(index): value for index, value in enumerate(payload["regime_summary"])}
    if isinstance(payload.get("by_regime"), list):
        return {str(index): value for index, value in enumerate(payload["by_regime"])}
    return {}


def _classify_publishability(record: dict[str, Any]) -> str:
    decision = str(record.get("authoritative_rollup_decision") or record.get("session_aggregate_decision") or record.get("candidate_local_decision") or "").lower()
    detail = str(record.get("artifact_detail_level") or "").lower()
    fallback_status = str(record.get("fallback_status") or record.get("same_condition_fallback_status") or "").lower()
    artifact_complete = bool(record.get("artifact_complete"))
    if not artifact_complete:
        return "blocked"
    if "analysis_only" in detail or "research_fallback" in detail or "research-fallback" in fallback_status:
        return "analysis_only"
    if decision in {"keep", "hold"}:
        return "publish_review_only"
    if decision in {"drop", "research_failure"}:
        return "blocked"
    return "analysis_only"


def build_candidate_record(path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    decision_fields = _extract_decision_fields(payload)
    same_condition = _extract_same_condition_contract(payload)
    core_payload_fields = [
        payload.get("candidate_local_decision"),
        payload.get("authoritative_rollup_decision"),
        payload.get("decision"),
        payload.get("branching_metrics"),
        payload.get("compare_topk"),
        payload.get("champion_vs_challenger"),
        payload.get("best_result"),
    ]
    artifact_complete = bool(
        payload.get("artifact_complete")
        or payload.get("complete")
        or any(value is not None for value in core_payload_fields)
    )
    candidate_id = (
        payload.get("candidate_name")
        or payload.get("candidate_id")
        or payload.get("candidate_run_id")
        or payload.get("method_id")
        or path.stem
    )
    family_id = payload.get("family_id") or payload.get("family_name") or payload.get("method_family") or payload.get("family")
    branch = payload.get("branching_metrics")
    if not isinstance(branch, dict):
        branch = payload.get("branching")
        if not isinstance(branch, dict):
            branch = {}
    compare_topk = _extract_topk_metrics(payload)
    record = {
        "candidate_id": str(candidate_id),
        "family_id": None if family_id is None else str(family_id),
        "artifact_root": _artifact_root_for(path),
        "artifact_path": str(path.resolve()),
        "artifact_name": path.name,
        "schema_version": payload.get("schema_version"),
        "candidate_local_decision": decision_fields["candidate_local_decision"],
        "session_aggregate_decision": decision_fields["session_aggregate_decision"],
        "authoritative_rollup_decision": decision_fields["authoritative_rollup_decision"],
        "typed_reason": decision_fields["typed_reason"],
        "topk": compare_topk,
        "top5_mean_ret20": _extract_first(payload, ("compare_topk", "5", "delta", "mean_forward_ret_20d"))
        or _extract_first(payload, ("champion_vs_challenger", "selection_only", "5", "delta", "mean_forward_ret_20d")),
        "top5_median_ret20": _extract_first(payload, ("compare_topk", "5", "delta", "median_forward_ret_20d"))
        or _extract_first(payload, ("champion_vs_challenger", "selection_only", "5", "delta", "median_forward_ret_20d")),
        "top10_mean_ret20": _extract_first(payload, ("compare_topk", "10", "delta", "mean_forward_ret_20d"))
        or _extract_first(payload, ("champion_vs_challenger", "selection_only", "10", "delta", "mean_forward_ret_20d")),
        "top10_median_ret20": _extract_first(payload, ("compare_topk", "10", "delta", "median_forward_ret_20d"))
        or _extract_first(payload, ("champion_vs_challenger", "selection_only", "10", "delta", "median_forward_ret_20d")),
        "top20_mean_ret20": _extract_first(payload, ("compare_topk", "20", "delta", "mean_forward_ret_20d"))
        or _extract_first(payload, ("champion_vs_challenger", "selection_only", "20", "delta", "mean_forward_ret_20d")),
        "top20_median_ret20": _extract_first(payload, ("compare_topk", "20", "delta", "median_forward_ret_20d"))
        or _extract_first(payload, ("champion_vs_challenger", "selection_only", "20", "delta", "median_forward_ret_20d")),
        "monthly_top5_capture": payload.get("monthly_top5_capture")
        or payload.get("champion_monthly_top5_capture_mean")
        or payload.get("challenger_monthly_top5_capture_mean")
        or _extract_first(payload, ("comparison_summary", "top5_top15_capture_delta")),
        "bad_pick_removal": payload.get("bad_pick_removal")
        or _extract_first(payload, ("evidence", "bad_pick_removal"))
        or _extract_first(payload, ("comparison_summary", "bad_pick_removal"))
        or _extract_first(payload, ("compare_topk", "10", "delta", "bad_pick_family_contamination_rate")),
        "changed_top5_members_count": payload.get("changed_top5_members_count")
        or _extract_first(payload, ("branching_metrics", "changed_top5_members_count")),
        "changed_top10_members_count": payload.get("changed_top10_members_count")
        or _extract_first(payload, ("branching_metrics", "changed_top10_members_count")),
        "changed_rank_count": payload.get("changed_rank_count")
        or _extract_first(payload, ("branching_metrics", "changed_rank_count")),
        "champion_overlap": payload.get("overlap_vs_champion")
        or _extract_first(payload, ("branching_metrics", "top10_overlap_ratio"))
        or _extract_first(payload, ("comparison_summary", "top10_overlap_ratio")),
        "turnover": payload.get("turnover")
        or _extract_first(payload, ("comparison_summary", "turnover"))
        or _extract_first(payload, ("comparison", "turnover"))
        or _extract_first(payload, ("exposure_normalization", "challenger", "unused_capital_rate")),
        "cost_slippage_mode": _extract_cost_mode(payload),
        "artifact_detail_level": _extract_artifact_detail_level(payload),
        "regime_split_summary": _extract_regime_split_summary(payload),
        "sample_count": payload.get("sample_count")
        or payload.get("anchor_count")
        or _extract_first(payload, ("comparison_summary", "groups"))
        or _extract_first(payload, ("comparison", "groups")),
        "fallback_status": payload.get("fallback_status") or _extract_first(payload, ("same_condition_contract", "fallback_status")),
        "artifact_complete": artifact_complete,
        "publishability": _classify_publishability(
            {
                "candidate_local_decision": decision_fields["candidate_local_decision"],
                "session_aggregate_decision": decision_fields["session_aggregate_decision"],
                "authoritative_rollup_decision": decision_fields["authoritative_rollup_decision"],
                "artifact_detail_level": _extract_artifact_detail_level(payload),
                "fallback_status": payload.get("fallback_status") or _extract_first(payload, ("same_condition_contract", "fallback_status")),
                "artifact_complete": artifact_complete,
            }
        ),
        "same_condition_contract": same_condition,
    }
    return record


def summarize_branching(record: dict[str, Any]) -> dict[str, Any]:
    topk = record.get("topk") if isinstance(record.get("topk"), dict) else {}
    top5 = topk.get("5") if isinstance(topk, dict) else {}
    top10 = topk.get("10") if isinstance(topk, dict) else {}
    top20 = topk.get("20") if isinstance(topk, dict) else {}
    return {
        "changed_top5_members_count": _safe_int(record.get("changed_top5_members_count"), 0),
        "changed_top10_members_count": _safe_int(record.get("changed_top10_members_count"), 0),
        "changed_rank_count": _safe_int(record.get("changed_rank_count"), 0),
        "selection_divergence_reason": record.get("typed_reason")
        or top10.get("selection_divergence_reason")
        or top5.get("selection_divergence_reason")
        or "unknown",
        "top5_boundary_score_gap": _safe_float(top5.get("boundary_score_gap")) if isinstance(top5, dict) and top5.get("boundary_score_gap") is not None else None,
        "top10_boundary_score_gap": _safe_float(top10.get("boundary_score_gap")) if isinstance(top10, dict) and top10.get("boundary_score_gap") is not None else None,
        "top20_boundary_score_gap": _safe_float(top20.get("boundary_score_gap")) if isinstance(top20, dict) and top20.get("boundary_score_gap") is not None else None,
    }


def build_artifact_complete(payload: dict[str, Any], artifact_names: list[str], *, schema_version: str) -> dict[str, Any]:
    return {
        "schema_version": schema_version,
        "generated_at": _utc_now(),
        "artifact_count": len(artifact_names),
        "artifacts": artifact_names,
        "complete": True,
        "source_schema_version": payload.get("schema_version"),
    }
