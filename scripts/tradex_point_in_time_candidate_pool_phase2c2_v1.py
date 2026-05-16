from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import tradex_iizuka_signal_expectancy_v1 as base
from scripts import tradex_point_in_time_candidate_pool_phase2c_v1 as phase2c

SCRIPT_NAME = "tradex_point_in_time_candidate_pool_phase2c2_v1"
SCHEMA_VERSION = "tradex_point_in_time_candidate_pool_phase2c2_v1"
DEFAULT_PHASE2C_ROOT = Path(r"G:\Tradex\point_in_time_candidate_pool_contract_v1_phase2c\20260509T064219Z-989246")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\point_in_time_candidate_pool_contract_v1_phase2c2")
CONTRACT_PATH = REPO_ROOT / "docs" / "contracts" / "tradex_point_in_time_candidate_pool_contract_v1.json"
GENERATION_PATH = "scripts/tradex_side_aware_min_pool_feasibility_v1.py::build_artifacts"
REQUIRED_ARTIFACTS = [
    "run_manifest.json",
    "input_resolution.json",
    "context_enrichment_source_audit.json",
    "full_pool_context_enrichment_plan.json",
    "full_candidate_pool_rows_context_verified.parquet",
    "full_pool_no_lookahead_coverage.json",
    "point_in_time_contract_validation.json",
    "phase2c2_decision.json",
    "_ARTIFACT_COMPLETE.json",
]
PRESERVE_COLUMNS = [
    "candidate_pool_membership",
    "champion_score",
    "champion_rank",
    "top5_membership",
    "top10_membership",
    "top20_membership",
    "top50_membership",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    base._write_json(path, payload)


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    base._write_parquet(path, frame)


def _safe_path(value: str | Path | None, default: Path) -> Path:
    return base._safe_path(value, default)


def _load_contract() -> dict[str, Any]:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def _bool_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(False, index=frame.index)
    return frame[column].fillna(False).astype(bool)


def _date_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(pd.NaT, index=frame.index)
    return pd.to_datetime(frame[column], errors="coerce")


def _choose_context(frame: pd.DataFrame, *, grain: str) -> tuple[pd.Series, pd.Series, pd.Series]:
    acc_flag = _bool_series(frame, f"{grain}_context_no_lookahead_acc")
    acc_date = _date_series(frame, f"{grain}_context_date_acc")
    acc_source = frame.get(f"{grain}_context_source_acc", pd.Series(pd.NA, index=frame.index))
    broad_flag = _bool_series(frame, f"{grain}_context_no_lookahead")
    broad_date = _date_series(frame, f"{grain}_context_date")
    broad_source = frame.get(f"{grain}_context_source", pd.Series(pd.NA, index=frame.index))

    use_acc = acc_flag & acc_date.notna()
    use_broad = ~use_acc & broad_flag & broad_date.notna()
    source_date = pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns]")
    source_name = pd.Series(pd.NA, index=frame.index, dtype="object")
    source_flag = pd.Series(False, index=frame.index)
    source_date.loc[use_acc] = acc_date.loc[use_acc]
    source_name.loc[use_acc] = acc_source.loc[use_acc].astype(str)
    source_flag.loc[use_acc] = True
    source_date.loc[use_broad] = broad_date.loc[use_broad]
    source_name.loc[use_broad] = broad_source.loc[use_broad].astype(str)
    source_flag.loc[use_broad] = True
    return source_date, source_name, source_flag


def enrich_context_lineage(rows: pd.DataFrame) -> pd.DataFrame:
    out = rows.copy()
    as_of = _date_series(out, "as_of_date")
    candidate = _date_series(out, "candidate_date")
    feature_cutoff = _date_series(out, "feature_cutoff_date")

    out["daily_context_source_date"] = candidate
    out["daily_feature_cutoff_date"] = feature_cutoff.where(feature_cutoff.notna(), candidate)
    daily_cutoff = pd.to_datetime(out["daily_feature_cutoff_date"], errors="coerce")
    out["daily_no_lookahead_valid"] = (
        daily_cutoff.notna()
        & as_of.notna()
        & candidate.notna()
        & (daily_cutoff <= as_of)
        & (daily_cutoff <= candidate)
    )

    context_sources: dict[str, pd.Series] = {}
    for grain in ("weekly", "monthly"):
        source_date, source_name, source_flag = _choose_context(out, grain=grain)
        out[f"{grain}_context_source_date"] = source_date
        out[f"{grain}_feature_cutoff_date"] = source_date
        cutoff = pd.to_datetime(out[f"{grain}_feature_cutoff_date"], errors="coerce")
        out[f"{grain}_no_lookahead_valid"] = (
            source_flag
            & cutoff.notna()
            & as_of.notna()
            & candidate.notna()
            & (cutoff <= as_of)
            & (cutoff <= candidate)
        )
        context_sources[grain] = source_name

    out["context_no_lookahead_valid"] = (
        out["daily_no_lookahead_valid"].fillna(False).astype(bool)
        & out["weekly_no_lookahead_valid"].fillna(False).astype(bool)
        & out["monthly_no_lookahead_valid"].fillna(False).astype(bool)
    )

    missing_weekly = out["weekly_context_source_date"].isna()
    missing_monthly = out["monthly_context_source_date"].isna()
    future_weekly = out["weekly_context_source_date"].notna() & ~out["weekly_no_lookahead_valid"].fillna(False).astype(bool)
    future_monthly = out["monthly_context_source_date"].notna() & ~out["monthly_no_lookahead_valid"].fillna(False).astype(bool)
    invalid_daily = ~out["daily_no_lookahead_valid"].fillna(False).astype(bool)

    reasons: list[str] = []
    statuses: list[str] = []
    lineage: list[str] = []
    for idx in out.index:
        row_reasons: list[str] = []
        if bool(invalid_daily.loc[idx]):
            row_reasons.append("daily_feature_cutoff_unverifiable_or_after_candidate_date")
        if bool(missing_weekly.loc[idx]):
            row_reasons.append("missing_weekly_context_source_date")
        if bool(missing_monthly.loc[idx]):
            row_reasons.append("missing_monthly_context_source_date")
        if bool(future_weekly.loc[idx]):
            row_reasons.append("weekly_context_source_date_after_candidate_or_as_of")
        if bool(future_monthly.loc[idx]):
            row_reasons.append("monthly_context_source_date_after_candidate_or_as_of")
        if bool(out.loc[idx, "context_no_lookahead_valid"]):
            statuses.append("valid")
            reasons.append("")
        elif any(reason.endswith("after_candidate_or_as_of") for reason in row_reasons):
            statuses.append("invalid")
            reasons.append(";".join(row_reasons))
        else:
            statuses.append("unverifiable")
            reasons.append(";".join(row_reasons))
        weekly_source = context_sources["weekly"].loc[idx]
        monthly_source = context_sources["monthly"].loc[idx]
        lineage.append(
            "daily=feature_cutoff_date;"
            f"weekly={weekly_source if pd.notna(weekly_source) else 'missing'};"
            f"monthly={monthly_source if pd.notna(monthly_source) else 'missing'}"
        )

    out["context_no_lookahead_status"] = statuses
    out["context_no_lookahead_failure_reason"] = reasons
    out["context_lineage_source"] = lineage
    return out


def _preservation_audit(original: pd.DataFrame, enriched: pd.DataFrame) -> dict[str, Any]:
    changed: dict[str, int] = {}
    for column in PRESERVE_COLUMNS:
        if column in original.columns and column in enriched.columns:
            left = original[column].fillna("__NA__").astype(str)
            right = enriched[column].fillna("__NA__").astype(str)
            changed[column] = int((left != right).sum())
    return {
        "row_count_preserved": int(len(original)) == int(len(enriched)),
        "original_row_count": int(len(original)),
        "enriched_row_count": int(len(enriched)),
        "preserved_column_changed_counts": changed,
        "candidate_membership_changed": bool(changed.get("candidate_pool_membership", 0) != 0),
        "champion_score_changed": bool(changed.get("champion_score", 0) != 0),
        "champion_rank_changed": bool(changed.get("champion_rank", 0) != 0),
        "topk_flags_changed": bool(any(changed.get(c, 0) != 0 for c in ["top5_membership", "top10_membership", "top20_membership", "top50_membership"])),
    }


def _coverage(enriched: pd.DataFrame, preservation: dict[str, Any]) -> dict[str, Any]:
    valid = _bool_series(enriched, "context_no_lookahead_valid")
    invalid = enriched["context_no_lookahead_status"].astype(str).eq("invalid")
    unverifiable = enriched["context_no_lookahead_status"].astype(str).eq("unverifiable")
    reason_counts = enriched.loc[~valid, "context_no_lookahead_failure_reason"].fillna("").replace("", "none").value_counts().to_dict()
    return {
        "schema_version": f"{SCHEMA_VERSION}_coverage_v1",
        "generated_at_utc": _utc_now(),
        "total_rows": int(len(enriched)),
        "context_enriched_rows": int(
            enriched["daily_context_source_date"].notna().sum()
        ),
        "complete_context_source_rows": int(
            (
                enriched["daily_context_source_date"].notna()
                & enriched["weekly_context_source_date"].notna()
                & enriched["monthly_context_source_date"].notna()
            ).sum()
        ),
        "verified_no_lookahead_rows": int(valid.sum()),
        "unverifiable_rows": int(unverifiable.sum()),
        "invalid_rows": int(invalid.sum()),
        "daily_valid_count": int(_bool_series(enriched, "daily_no_lookahead_valid").sum()),
        "weekly_valid_count": int(_bool_series(enriched, "weekly_no_lookahead_valid").sum()),
        "monthly_valid_count": int(_bool_series(enriched, "monthly_no_lookahead_valid").sum()),
        "context_valid_share": float(valid.mean()) if len(enriched) else 0.0,
        "invalid_reason_counts": enriched.loc[invalid, "context_no_lookahead_failure_reason"].fillna("none").value_counts().to_dict(),
        "unverifiable_reason_counts": enriched.loc[unverifiable, "context_no_lookahead_failure_reason"].fillna("none").value_counts().to_dict(),
        "all_failure_reason_counts": reason_counts,
        "full_no_lookahead_verified": bool(len(enriched) > 0 and valid.all()),
        "preservation_audit": preservation,
    }


def _validate_contract(enriched: pd.DataFrame, coverage: dict[str, Any]) -> dict[str, Any]:
    base_validation = phase2c._validate_contract(enriched)
    context_fields = [
        "daily_context_source_date",
        "weekly_context_source_date",
        "monthly_context_source_date",
        "daily_feature_cutoff_date",
        "weekly_feature_cutoff_date",
        "monthly_feature_cutoff_date",
        "daily_no_lookahead_valid",
        "weekly_no_lookahead_valid",
        "monthly_no_lookahead_valid",
        "context_no_lookahead_valid",
        "context_no_lookahead_status",
        "context_no_lookahead_failure_reason",
        "context_lineage_source",
    ]
    missing_context = [field for field in context_fields if field not in enriched.columns]
    status_explicit = bool(enriched["context_no_lookahead_status"].notna().all()) if "context_no_lookahead_status" in enriched.columns else False
    return {
        **base_validation,
        "schema_version": f"{SCHEMA_VERSION}_contract_validation_v1",
        "context_lineage_required_fields": context_fields,
        "missing_context_lineage_fields": missing_context,
        "context_no_lookahead_status_explicit_all_rows": status_explicit,
        "full_context_no_lookahead_verified": coverage["full_no_lookahead_verified"],
        "validation_pass": bool(base_validation["validation_pass"] and len(missing_context) == 0 and status_explicit),
    }


def _source_audit(rows: pd.DataFrame, phase2c_root: Path) -> dict[str, Any]:
    counts = {
        column: int(rows[column].notna().sum())
        for column in [
            "monthly_context_no_lookahead",
            "weekly_context_no_lookahead",
            "monthly_context_date",
            "weekly_context_date",
            "monthly_context_source",
            "weekly_context_source",
            "monthly_context_no_lookahead_acc",
            "weekly_context_no_lookahead_acc",
            "monthly_context_date_acc",
            "weekly_context_date_acc",
            "monthly_context_source_acc",
            "weekly_context_source_acc",
        ]
        if column in rows.columns
    }
    return {
        "schema_version": f"{SCHEMA_VERSION}_source_audit_v1",
        "generated_at_utc": _utc_now(),
        "phase2c_root": str(phase2c_root),
        "function_script_creating_overlap_enrichment": GENERATION_PATH,
        "overlap_enrichment_functions": [
            "scripts/tradex_side_aware_min_pool_feasibility_v1.py::_attach_broad_context",
            "scripts/tradex_side_aware_min_pool_feasibility_v1.py::_attach_accumulated",
        ],
        "context_source_columns": [
            "monthly_context_no_lookahead",
            "weekly_context_no_lookahead",
            "monthly_context_date",
            "weekly_context_date",
            "monthly_context_source",
            "weekly_context_source",
            "monthly_context_no_lookahead_acc",
            "weekly_context_no_lookahead_acc",
            "monthly_context_date_acc",
            "weekly_context_date_acc",
            "monthly_context_source_acc",
            "weekly_context_source_acc",
        ],
        "join_key": "__key__ = anchor_date|side|symbol",
        "why_partial": "raw selection ledger does not carry monthly/weekly context source dates; context flags are joined only for rows overlapping broad prefilter or accumulated forward prediction artifacts",
        "non_null_context_column_counts": counts,
    }


def run_phase2c2(*, phase2c_root: Path, output_root: Path) -> dict[str, Any]:
    session_root = output_root / _session_id()
    session_root.mkdir(parents=True, exist_ok=True)
    contract = _load_contract()
    input_path = phase2c_root / "full_candidate_pool_rows.parquet"
    rows = pd.read_parquet(input_path)
    enriched = enrich_context_lineage(rows)
    preservation = _preservation_audit(rows, enriched)
    coverage = _coverage(enriched, preservation)
    validation = _validate_contract(enriched, coverage)

    if (
        validation["validation_pass"]
        and coverage["full_no_lookahead_verified"]
        and not preservation["candidate_membership_changed"]
        and not preservation["champion_score_changed"]
        and not preservation["champion_rank_changed"]
        and not preservation["topk_flags_changed"]
    ):
        decision = "ready"
        reason = "full_pool_context_no_lookahead_verified"
    elif preservation["candidate_membership_changed"] or preservation["champion_score_changed"] or preservation["champion_rank_changed"]:
        decision = "blocked_requires_scoring_change"
        reason = "enrichment_changed_candidate_membership_or_score_rank"
    elif coverage["invalid_rows"] > 0:
        decision = "blocked_future_label_risk"
        reason = "context_source_dates_after_candidate_or_as_of_detected"
    elif coverage["unverifiable_rows"] > 0:
        decision = "blocked_unverifiable_source_dates"
        reason = "weekly_or_monthly_context_source_dates_missing_for_full_pool"
    else:
        decision = "blocked_partial_context_coverage"
        reason = "context_coverage_not_complete"

    decision_payload = {
        "schema_version": f"{SCHEMA_VERSION}_decision_v1",
        "generated_at_utc": _utc_now(),
        "authoritative_decision": decision,
        "decision_reason": reason,
        "phase2c_root": str(phase2c_root),
        "source_input_path": str(input_path),
        "original_row_count": int(len(rows)),
        "enriched_row_count": int(len(enriched)),
        "context_enriched_rows": coverage["context_enriched_rows"],
        "complete_context_source_rows": coverage["complete_context_source_rows"],
        "verified_no_lookahead_rows": coverage["verified_no_lookahead_rows"],
        "invalid_rows": coverage["invalid_rows"],
        "unverifiable_rows": coverage["unverifiable_rows"],
        "full_no_lookahead_verified": coverage["full_no_lookahead_verified"],
        "contract_validation_pass": validation["validation_pass"],
        "scoring_behavior_changed": False,
        "candidate_membership_changed": preservation["candidate_membership_changed"],
        "champion_score_changed": preservation["champion_score_changed"],
        "champion_rank_changed": preservation["champion_rank_changed"],
        "topk_flags_changed": preservation["topk_flags_changed"],
        "future_phase2d_ranking_exposure_retest_allowed": decision == "ready",
        "meemee_changed": False,
        "production_ranking_changed": False,
        "publish_changed": False,
        "ranking_challenger_created": False,
        "ranking_pretest_ran": False,
    }

    artifacts = {
        "run_manifest.json": {
            "schema_version": f"{SCHEMA_VERSION}_manifest_v1",
            "generated_at_utc": _utc_now(),
            "script_name": SCRIPT_NAME,
            "session_root": str(session_root),
            "boundary": "TRADEX-only",
            "phase2c_root": str(phase2c_root),
            "meemee_changed": False,
            "production_ranking_changed": False,
            "publish_changed": False,
            "ranking_challenger_created": False,
            "ranking_pretest_ran": False,
        },
        "input_resolution.json": {
            "schema_version": f"{SCHEMA_VERSION}_input_resolution_v1",
            "generated_at_utc": _utc_now(),
            "contract_path": str(CONTRACT_PATH),
            "phase2c_root": str(phase2c_root),
            "primary_input": str(input_path),
            "primary_input_exists": input_path.exists(),
            "original_row_count": int(len(rows)),
        },
        "context_enrichment_source_audit.json": _source_audit(rows, phase2c_root),
        "full_pool_context_enrichment_plan.json": {
            "schema_version": f"{SCHEMA_VERSION}_enrichment_plan_v1",
            "generated_at_utc": _utc_now(),
            "method": "attach explicit daily lineage from candidate_date/feature_cutoff_date and use only existing broad/accumulated weekly/monthly context source dates when present",
            "no_silent_inference": True,
            "score_rank_candidate_membership_preserved": True,
            "daily_lineage_rule": "daily_feature_cutoff_date = feature_cutoff_date when present, otherwise candidate_date",
            "weekly_monthly_lineage_rule": "prefer accumulated context columns with source dates, else broad prefilter context columns with source dates; missing rows remain unverifiable",
        },
        "full_pool_no_lookahead_coverage.json": coverage,
        "point_in_time_contract_validation.json": validation,
        "phase2c2_decision.json": decision_payload,
    }
    for name, body in artifacts.items():
        _write_json(session_root / name, body)
    _write_json(session_root / "point_in_time_candidate_pool_contract_snapshot.json", contract)
    _write_parquet(session_root / "full_candidate_pool_rows_context_verified.parquet", enriched)
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
        "original_row_count": int(len(rows)),
        "enriched_row_count": int(len(enriched)),
        "full_no_lookahead_verified": coverage["full_no_lookahead_verified"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="TRADEX Phase 2c2 full-pool context no-lookahead coverage")
    parser.add_argument("--phase2c-root", default=str(DEFAULT_PHASE2C_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args()
    result = run_phase2c2(
        phase2c_root=_safe_path(args.phase2c_root, DEFAULT_PHASE2C_ROOT),
        output_root=_safe_path(args.output_root, DEFAULT_OUTPUT_ROOT),
    )
    print(json.dumps(base._json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
