from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import tradex_iizuka_signal_expectancy_v1 as base

SCRIPT_NAME = "tradex_iizuka_signal_expectancy_phase1d_v1"
SCHEMA_VERSION = "tradex_iizuka_signal_expectancy_phase1d_v1"
DEFAULT_PHASE1C_ROOT = Path(r"G:\Tradex\iizuka_signal_expectancy_v1_phase1c\20260509T050355Z-080636")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\iizuka_signal_expectancy_v1_phase1d")
DEFAULT_RANKING_SURFACE = Path(
    r"G:\Tradex\candidate_generation_pre_filter_context_shape_v1_accumulated_v2\20260502T082333Z-2cd00bf5\candidate_prefilter_rows.parquet"
)
CANDIDATE_ID = "monthly_C_mixed_pullback_end_reclaim7_v1"
REQUIRED_ARTIFACTS = [
    "run_manifest.json",
    "input_resolution.json",
    "phase1d_candidate_snapshot.json",
    "phase1d_severe_loser_audit.json",
    "phase1d_liquidity_concentration_audit.json",
    "phase1d_ranking_exposure_preflight.json",
    "phase1d_internal_combination_viability.json",
    "phase1d_decision.json",
    "_ARTIFACT_COMPLETE.json",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    base._write_json(path, payload)


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _safe_path(value: str | Path | None, default: Path) -> Path:
    return base._safe_path(value, default)


def _normalize_date_series(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series.astype(str), errors="coerce")
    return parsed.dt.strftime("%Y-%m-%d")


def _price_bucket(value: Any) -> str:
    try:
        price = float(value)
    except Exception:
        return "unknown"
    if price < 500:
        return "price_lt_500"
    if price < 1000:
        return "price_500_999"
    if price < 3000:
        return "price_1000_2999"
    if price < 10000:
        return "price_3000_9999"
    return "price_ge_10000"


def _dist(frame: pd.DataFrame, column: str, *, top: int | None = None) -> dict[str, int]:
    if column not in frame.columns:
        return {}
    counts = frame[column].fillna("unknown").astype(str).value_counts(dropna=False)
    if top is not None:
        counts = counts.head(top)
    return {str(k): int(v) for k, v in counts.items()}


def _share(frame: pd.DataFrame, column: str) -> float | None:
    return base._largest_share(frame, column)


def _loss_masks(rows: pd.DataFrame) -> dict[str, pd.Series]:
    ret5 = pd.to_numeric(rows.get("ret5"), errors="coerce")
    ret20 = pd.to_numeric(rows.get("ret20"), errors="coerce")
    mae20 = pd.to_numeric(rows.get("mae20"), errors="coerce")
    return {
        "ret20_loser": ret20 < 0,
        "severe_loser": (ret20 < 0) & (mae20 <= -0.08),
        "failed_followthrough": (ret5 <= 0) & (ret20 <= 0),
        "adverse_first": mae20 <= -0.05,
    }


def _cohort_summary(rows: pd.DataFrame) -> dict[str, Any]:
    return {
        "count": int(len(rows)),
        "internal_combination_distribution": _dist(rows, "mixed_internal_combination"),
        "year_distribution": _dist(rows, "year"),
        "sector_distribution": _dist(rows, "sector", top=30),
        "liquidity_or_volume_bucket_distribution": _dist(rows, "liquidity_bucket"),
        "price_bucket_distribution": _dist(rows, "price_bucket"),
        "monthly_regime_detail_distribution": _dist(rows, "monthly_regime_detail"),
        "largest_internal_combination_share": _share(rows, "mixed_internal_combination"),
        "largest_year_share": _share(rows, "year"),
        "largest_sector_share": _share(rows, "sector"),
        "largest_liquidity_or_volume_bucket_share": _share(rows, "liquidity_bucket"),
        "largest_price_bucket_share": _share(rows, "price_bucket"),
        "largest_monthly_regime_detail_share": _share(rows, "monthly_regime_detail"),
    }


def _prepare_mixed(rows: pd.DataFrame) -> pd.DataFrame:
    out = rows.copy()
    out["symbol"] = out["symbol"].astype(str)
    out["decision_date"] = _normalize_date_series(out["decision_date"])
    out["year"] = out["decision_date"].astype(str).str.slice(0, 4)
    price_source = "execution_price" if "execution_price" in out.columns else "close"
    out["price_bucket"] = out[price_source].map(_price_bucket)
    if "monthly_context" in out.columns:
        out["monthly_regime_detail"] = out["monthly_context"].fillna("unknown").astype(str)
    elif "monthly_C_regime" in out.columns:
        out["monthly_regime_detail"] = out["monthly_C_regime"].map(lambda value: "monthly_C_regime" if bool(value) else "not_monthly_C_regime")
    else:
        out["monthly_regime_detail"] = "unknown"
    return out


def _baseline2_distribution(source_path: Path) -> dict[str, Any]:
    if not source_path.exists():
        return {"status": "blocked", "reason": f"source parquet not found: {source_path}"}
    source = pd.read_parquet(source_path)
    normalized = base._normalize_source_frame(source)
    eligible = base._eligible_rows(base._attach_forward_outcomes(base._attach_signal_flags(normalized)), "baseline_2")
    eligible = eligible.copy()
    eligible["symbol"] = eligible["symbol"].astype(str)
    eligible["date"] = _normalize_date_series(eligible["date"])
    eligible["year"] = eligible["date"].astype(str).str.slice(0, 4)
    eligible["price_bucket"] = eligible["close"].map(_price_bucket)
    eligible["monthly_regime_detail"] = "monthly_C_regime"
    return {"status": "confirmed", "row_count": int(len(eligible)), "summary": _cohort_summary(eligible)}


def _severe_loser_audit(mixed: pd.DataFrame) -> dict[str, Any]:
    masks = _loss_masks(mixed)
    cohorts = {}
    for name, mask in masks.items():
        subset = mixed.loc[mask.fillna(False)].copy()
        summary = _cohort_summary(subset)
        summary["share_of_mixed_rows"] = float(len(subset) / len(mixed)) if len(mixed) else None
        cohorts[name] = summary
    return {
        "schema_version": f"{SCHEMA_VERSION}_severe_loser_audit_v1",
        "generated_at_utc": _utc_now(),
        "candidate_id": CANDIDATE_ID,
        "diagnostic_only": True,
        "definitions": {
            "ret20_loser": "ret20 < 0",
            "severe_loser": "ret20 < 0 and MAE_through_20_sessions <= -0.08",
            "failed_followthrough": "ret5 <= 0 and ret20 <= 0",
            "adverse_first": "MAE_through_20_sessions <= -0.05 before ret20_horizon_date",
        },
        "cohorts": cohorts,
    }


def _liquidity_audit(mixed: pd.DataFrame, baseline2: dict[str, Any]) -> dict[str, Any]:
    masks = _loss_masks(mixed)
    severe = mixed.loc[masks["severe_loser"].fillna(False)].copy()
    non_loser = mixed.loc[~masks["ret20_loser"].fillna(False)].copy()
    all_share = _share(mixed, "liquidity_bucket")
    severe_share = _share(severe, "liquidity_bucket")
    non_loser_share = _share(non_loser, "liquidity_bucket")
    baseline_share = None
    if baseline2.get("status") == "confirmed":
        baseline_share = ((baseline2.get("summary") or {}).get("largest_liquidity_or_volume_bucket_share"))
    reference = max(v for v in [all_share, non_loser_share, baseline_share] if v is not None)
    excess = float(severe_share - reference) if severe_share is not None else None
    acceptable = bool(severe_share is not None and severe_share <= 0.60 and (excess is None or excess <= 0.10))
    return {
        "schema_version": f"{SCHEMA_VERSION}_liquidity_concentration_audit_v1",
        "generated_at_utc": _utc_now(),
        "candidate_id": CANDIDATE_ID,
        "status": "confirmed" if baseline2.get("status") == "confirmed" else "provisional",
        "all_mixed_rows": _cohort_summary(mixed),
        "severe_losers": _cohort_summary(severe),
        "non_loser_mixed_rows": _cohort_summary(non_loser),
        "baseline_2_eligible_rows": baseline2,
        "severe_loser_largest_liquidity_share": severe_share,
        "reference_largest_liquidity_share": reference,
        "excess_vs_reference": excess,
        "acceptable_or_controllable": acceptable,
        "assessment": "acceptable_or_controllable" if acceptable else "liquidity_risk_needs_filter_design",
    }


def _ranking_preflight(mixed: pd.DataFrame, ranking_surface_path: Path | None) -> dict[str, Any]:
    if ranking_surface_path is None or not ranking_surface_path.exists():
        return {
            "schema_version": f"{SCHEMA_VERSION}_ranking_exposure_preflight_v1",
            "generated_at_utc": _utc_now(),
            "candidate_id": CANDIDATE_ID,
            "status": "blocked",
            "reason": "ranking snapshot surface unavailable",
            "ranking_challenger_created": False,
        }
    ranking = pd.read_parquet(ranking_surface_path)
    required = {"symbol", "side"}
    if "anchor_date" not in ranking.columns and "trade_date" not in ranking.columns:
        return {
            "schema_version": f"{SCHEMA_VERSION}_ranking_exposure_preflight_v1",
            "generated_at_utc": _utc_now(),
            "candidate_id": CANDIDATE_ID,
            "status": "blocked",
            "reason": "ranking snapshot lacks anchor_date/trade_date",
            "ranking_surface_path": str(ranking_surface_path),
            "ranking_challenger_created": False,
        }
    if missing := sorted(required - set(ranking.columns)):
        return {
            "schema_version": f"{SCHEMA_VERSION}_ranking_exposure_preflight_v1",
            "generated_at_utc": _utc_now(),
            "candidate_id": CANDIDATE_ID,
            "status": "blocked",
            "reason": f"ranking snapshot missing columns: {missing}",
            "ranking_surface_path": str(ranking_surface_path),
            "ranking_challenger_created": False,
        }
    rank_date_col = "anchor_date" if "anchor_date" in ranking.columns else "trade_date"
    work = ranking.copy()
    work["symbol"] = work["symbol"].astype(str)
    work["rank_date"] = _normalize_date_series(work[rank_date_col])
    work = work.loc[work["side"].astype(str).str.lower() == "long"].copy()
    signal_keys = mixed[["symbol", "decision_date", "mixed_internal_combination"]].rename(columns={"decision_date": "rank_date"})
    joined = work.merge(signal_keys, on=["symbol", "rank_date"], how="inner")

    top_counts = {}
    for k in (5, 10, 20, 50):
        flag = f"champion_selected_top{k}"
        if flag in joined.columns:
            top_counts[f"top{k}"] = int(joined[flag].fillna(False).astype(bool).sum())
        elif "champion_rank" in joined.columns:
            top_counts[f"top{k}"] = int((pd.to_numeric(joined["champion_rank"], errors="coerce") <= k).sum())
        elif "rank" in joined.columns:
            top_counts[f"top{k}"] = int((pd.to_numeric(joined["rank"], errors="coerce") <= k).sum())
        else:
            top_counts[f"top{k}"] = None

    overlap_count = int(len(joined))
    top20_count = top_counts.get("top20") or 0
    if top20_count > 0:
        natural_use = "boost"
    elif overlap_count > 0:
        natural_use = "explain"
    else:
        natural_use = "blocked"
    return {
        "schema_version": f"{SCHEMA_VERSION}_ranking_exposure_preflight_v1",
        "generated_at_utc": _utc_now(),
        "candidate_id": CANDIDATE_ID,
        "status": "confirmed",
        "ranking_surface_path": str(ranking_surface_path),
        "ranking_surface_row_count": int(len(ranking)),
        "long_ranking_surface_row_count": int(len(work)),
        "matched_signal_rows_in_ranking_surface": overlap_count,
        "topk_counts": top_counts,
        "internal_combination_distribution_in_overlap": _dist(joined, "mixed_internal_combination"),
        "overlap_with_existing_champion_candidates": bool(top20_count > 0),
        "natural_connection_mode": natural_use,
        "ranking_challenger_created": False,
    }


def _internal_viability(phase1c_root: Path) -> dict[str, Any]:
    source = _load_json(phase1c_root / "phase1c_internal_combination_compare.json")
    combinations = {}
    strongest_keep = None
    for name, payload in (source.get("combinations") or {}).items():
        item = {
            "decision": (payload.get("decision") or {}).get("decision"),
            "decision_reason": (payload.get("decision") or {}).get("decision_reason"),
            "signal_count": (payload.get("signal") or {}).get("count"),
            "ret20_median": (payload.get("signal") or {}).get("ret20_median"),
            "win_rate_20": (payload.get("signal") or {}).get("win_rate_20"),
            "keep_gates": (payload.get("decision") or {}).get("keep_gates"),
            "primary_baseline_beats": (payload.get("decision") or {}).get("primary_baseline_beats"),
        }
        combinations[name] = item
        if item["decision"] == "keep" and (strongest_keep is None or (item["ret20_median"] or -999) > (strongest_keep["ret20_median"] or -999)):
            strongest_keep = {"combination": name, **item}
    koma = _load_json(phase1c_root / "phase1c_reference_koma_comparison.json")
    return {
        "schema_version": f"{SCHEMA_VERSION}_internal_combination_viability_v1",
        "generated_at_utc": _utc_now(),
        "candidate_id": CANDIDATE_ID,
        "source_artifact": str(phase1c_root / "phase1c_internal_combination_compare.json"),
        "combinations": combinations,
        "strongest_keep_grade_internal_combination": strongest_keep,
        "koma_reclaim7_reference_only": {
            "decision": (koma.get("decision") or {}).get("decision"),
            "signal_count": (koma.get("signal") or {}).get("count"),
            "promotion_allowed": koma.get("promotion_allowed"),
            "promotion_block_reason": koma.get("promotion_block_reason"),
        },
    }


def run_phase1d(
    *,
    phase1c_root: Path,
    output_root: Path,
    ranking_surface_path: Path | None = DEFAULT_RANKING_SURFACE,
) -> dict[str, Any]:
    started = time.perf_counter()
    session_root = output_root / _session_id()
    session_root.mkdir(parents=True, exist_ok=True)
    mixed_path = phase1c_root / "phase1c_mixed_signal_rows.parquet"
    phase1c_decision_path = phase1c_root / "phase1c_signal_decision.json"
    input_resolution_path = phase1c_root / "input_resolution.json"
    mixed = _prepare_mixed(pd.read_parquet(mixed_path))
    phase1c_decision = _load_json(phase1c_decision_path)
    phase1c_input = _load_json(input_resolution_path)
    source_path = Path(str(phase1c_input.get("source_rows_parquet") or ""))
    baseline2 = _baseline2_distribution(source_path)

    severe_loser = _severe_loser_audit(mixed)
    liquidity = _liquidity_audit(mixed, baseline2)
    ranking = _ranking_preflight(mixed, ranking_surface_path)
    internal = _internal_viability(phase1c_root)
    phase1c_keep = phase1c_decision.get("authoritative_rollup_decision") == "keep"
    risk_ok = bool(liquidity.get("acceptable_or_controllable"))
    ranking_available = ranking.get("status") == "confirmed"
    ranking_affects = bool((ranking.get("topk_counts") or {}).get("top20") or 0)

    if not phase1c_keep:
        decision = "drop"
        reason = "phase1c_candidate_not_keep_grade"
    elif not risk_ok:
        decision = "hold_for_risk_filter_design"
        reason = "expectancy_keep_but_liquidity_or_severe_loser_risk_needs_fixed_filter_design"
    elif not ranking_available:
        decision = "blocked"
        reason = "ranking_exposure_snapshot_unavailable"
    elif ranking_affects:
        decision = "proceed_to_ranking_challenger_pretest"
        reason = "expectancy_keep_risk_acceptable_and_ranking_topk_exposure_confirmed"
    elif int(ranking.get("matched_signal_rows_in_ranking_surface") or 0) > 0:
        decision = "analysis_only"
        reason = "ranking_surface_overlap_exists_but_no_top20_champion_exposure"
    else:
        decision = "analysis_only"
        reason = "no_confirmed_overlap_with_current_ranking_candidate_surface"

    manifest = {
        "schema_version": f"{SCHEMA_VERSION}_manifest_v1",
        "generated_at_utc": _utc_now(),
        "script_name": SCRIPT_NAME,
        "session_root": str(session_root),
        "phase1c_root": str(phase1c_root),
        "boundary": "TRADEX-only",
        "ranking_challenger_created": False,
        "meemee_changed": False,
        "production_ranking_changed": False,
        "publish_changed": False,
        "runtime_seconds": float(time.perf_counter() - started),
    }
    input_resolution = {
        "schema_version": f"{SCHEMA_VERSION}_input_resolution_v1",
        "generated_at_utc": _utc_now(),
        "phase1c_root": str(phase1c_root),
        "phase1c_mixed_signal_rows": str(mixed_path),
        "phase1c_signal_decision": str(phase1c_decision_path),
        "phase1c_source_rows_parquet": str(source_path),
        "ranking_surface_path": str(ranking_surface_path) if ranking_surface_path else None,
        "mixed_signal_row_count": int(len(mixed)),
        "phase1c_authoritative_decision": phase1c_decision.get("authoritative_rollup_decision"),
        "baseline_2_resolution_status": baseline2.get("status"),
        "ranking_exposure_status": ranking.get("status"),
    }
    snapshot = {
        "schema_version": f"{SCHEMA_VERSION}_candidate_snapshot_v1",
        "generated_at_utc": _utc_now(),
        "candidate_id": CANDIDATE_ID,
        "source_phase1c_decision": phase1c_decision,
        "signal_definition_changed": False,
        "thresholds_changed": False,
        "phase1d_is_ranking_challenger": False,
        "non_goals": [
            "No MeeMee UI changes",
            "No MeeMee ranking changes",
            "No production ranking changes",
            "No publish changes",
            "No sell-side research",
            "No unknown discovery",
            "No image AI",
            "No similar-chart work",
            "No threshold changes",
            "No signal definition changes",
        ],
    }
    decision_payload = {
        "schema_version": f"{SCHEMA_VERSION}_decision_v1",
        "generated_at_utc": _utc_now(),
        "candidate_id": CANDIDATE_ID,
        "authoritative_decision": decision,
        "decision_reason": reason,
        "ranking_challenger_pretest_allowed_next": decision == "proceed_to_ranking_challenger_pretest",
        "phase1c_candidate_remains_keep_grade": phase1c_keep,
        "severe_loser_concentration_result": {
            "severe_loser_count": severe_loser["cohorts"]["severe_loser"]["count"],
            "severe_loser_share": severe_loser["cohorts"]["severe_loser"]["share_of_mixed_rows"],
            "largest_liquidity_share": severe_loser["cohorts"]["severe_loser"]["largest_liquidity_or_volume_bucket_share"],
        },
        "liquidity_concentration_result": {
            "assessment": liquidity.get("assessment"),
            "acceptable_or_controllable": liquidity.get("acceptable_or_controllable"),
            "excess_vs_reference": liquidity.get("excess_vs_reference"),
        },
        "ranking_exposure_availability": ranking.get("status"),
        "ranking_exposure": {
            "matched_signal_rows_in_ranking_surface": ranking.get("matched_signal_rows_in_ranking_surface"),
            "topk_counts": ranking.get("topk_counts"),
            "natural_connection_mode": ranking.get("natural_connection_mode"),
        },
        "strongest_internal_combination": internal.get("strongest_keep_grade_internal_combination"),
        "koma_reclaim7_reference_only": internal.get("koma_reclaim7_reference_only"),
        "meemee_reflection": "blocked",
        "ranking_challenger": "blocked_until_pretest",
        "publish": "blocked",
    }

    _write_json(session_root / "run_manifest.json", manifest)
    _write_json(session_root / "input_resolution.json", input_resolution)
    _write_json(session_root / "phase1d_candidate_snapshot.json", snapshot)
    _write_json(session_root / "phase1d_severe_loser_audit.json", severe_loser)
    _write_json(session_root / "phase1d_liquidity_concentration_audit.json", liquidity)
    _write_json(session_root / "phase1d_ranking_exposure_preflight.json", ranking)
    _write_json(session_root / "phase1d_internal_combination_viability.json", internal)
    _write_json(session_root / "phase1d_decision.json", decision_payload)
    complete = {
        "schema_version": f"{SCHEMA_VERSION}_artifact_complete_v1",
        "generated_at_utc": _utc_now(),
        "session_root": str(session_root),
        "required_artifacts": REQUIRED_ARTIFACTS,
        "all_present": all((session_root / artifact).exists() for artifact in REQUIRED_ARTIFACTS if artifact != "_ARTIFACT_COMPLETE.json"),
    }
    _write_json(session_root / "_ARTIFACT_COMPLETE.json", complete)
    return {
        "session_root": str(session_root),
        "decision": decision,
        "ranking_challenger_pretest_allowed_next": decision == "proceed_to_ranking_challenger_pretest",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="TRADEX Iizuka Phase 1d pre-ranking risk audit")
    parser.add_argument("--phase1c-root", default=str(DEFAULT_PHASE1C_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--ranking-surface-path", default=str(DEFAULT_RANKING_SURFACE))
    args = parser.parse_args()
    ranking_path = _safe_path(args.ranking_surface_path, DEFAULT_RANKING_SURFACE) if args.ranking_surface_path else None
    result = run_phase1d(
        phase1c_root=_safe_path(args.phase1c_root, DEFAULT_PHASE1C_ROOT),
        output_root=_safe_path(args.output_root, DEFAULT_OUTPUT_ROOT),
        ranking_surface_path=ranking_path,
    )
    print(json.dumps(base._json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
