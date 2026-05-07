from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent

SCRIPT_NAME = "tradex_observable_regime_false_positive_bottom15_summary_rebuild_v1"
SCHEMA_VERSION = "tradex_observable_regime_false_positive_bottom15_summary_rebuild_v1"
MANIFEST_SCHEMA_VERSION = "tradex_observable_regime_false_positive_bottom15_summary_rebuild_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_observable_regime_false_positive_bottom15_summary_rebuild_v1_input_resolution_v1"
SUMMARY_SCHEMA_VERSION = "tradex_observable_regime_false_positive_bottom15_summary_rebuild_v1_rebuilt_summary_v1"
DIFF_SCHEMA_VERSION = "tradex_observable_regime_false_positive_bottom15_summary_rebuild_v1_diff_v1"
RECONCILIATION_SCHEMA_VERSION = "tradex_observable_regime_false_positive_bottom15_summary_rebuild_v1_variant_pool_reconciliation_v1"
FREEZE_PRECHECK_SCHEMA_VERSION = "tradex_observable_regime_false_positive_bottom15_summary_rebuild_v1_freeze_precheck_v1"

TOP_K_VALUES = (5, 10, 20)

DEFAULT_CHALLENGER_SESSION = Path(r"G:\Tradex\observable_regime_false_positive_require_confirmation_v1\20260501T081501Z-999791")
DEFAULT_BOTTOM15_AUDIT_SESSION = Path(r"G:\Tradex\observable_regime_false_positive_bottom15_audit_v1\20260501T082434Z-859316")
DEFAULT_RECONCILIATION_SESSION = Path(
    r"G:\Tradex\research_freeze_summaries\observable_regime_false_positive_require_confirmation_metric_reconciliation\20260501T083905Z-242626"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\observable_regime_false_positive_bottom15_summary_rebuild_v1")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, set):
        return [_json_ready(item) for item in sorted(value, key=str)]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(path)
    return json.loads(path.read_text(encoding="utf-8"))


def _load_frame(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    frame = pd.read_parquet(path).copy()
    for column in ("anchor_date", "symbol", "side"):
        if column in frame.columns:
            frame[column] = frame[column].astype("string")
    return frame


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
        if math.isnan(out):
            return None
        return out
    except Exception:
        return None


def _bool_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(False, index=frame.index)
    return frame[column].fillna(False).astype(bool)


def _count_mask(frame: pd.DataFrame, mask: pd.Series) -> int:
    return int(mask.fillna(False).astype(bool).sum())


def _topk_summary(frame: pd.DataFrame, topk: int) -> dict[str, Any]:
    baseline = _bool_series(frame, f"champion_selected_top{topk}")
    variant = _bool_series(frame, f"variant_selected_top{topk}")
    top15 = _bool_series(frame, "top15_label")
    bottom15 = _bool_series(frame, "bottom15_label")
    selected_total = int(baseline.sum())
    variant_total = int(variant.sum())
    original_top15 = int((baseline & top15).sum())
    variant_top15 = int((variant & top15).sum())
    original_bottom15 = int((baseline & bottom15).sum())
    variant_bottom15 = int((variant & bottom15).sum())
    selected_or = baseline | variant
    original_top15_capture = _safe_float(original_top15 / max(int(top15.sum()), 1))
    variant_top15_capture = _safe_float(variant_top15 / max(int(top15.sum()), 1))
    return {
        "topk": topk,
        "original_selected_count": selected_total,
        "variant_selected_count": variant_total,
        "original_top15_count": original_top15,
        "variant_top15_count": variant_top15,
        "original_bottom15_count": original_bottom15,
        "variant_bottom15_count": variant_bottom15,
        "newly_added_top15_count": int((variant & top15 & ~baseline).sum()),
        "newly_added_bottom15_count": int((variant & bottom15 & ~baseline).sum()),
        "removed_top15_count": int((baseline & top15 & ~variant).sum()),
        "removed_bottom15_count": int((baseline & bottom15 & ~variant).sum()),
        "unchanged_top15_count": int((baseline & variant & top15).sum()),
        "unchanged_bottom15_count": int((baseline & variant & bottom15).sum()),
        "top15_delta_count": int(variant_top15 - original_top15),
        "bottom15_delta_count": int(variant_bottom15 - original_bottom15),
        "top15_rate": _safe_float(variant_top15 / max(variant_total, 1)),
        "bottom15_rate": _safe_float(variant_bottom15 / max(variant_total, 1)),
        "original_top15_rate": _safe_float(original_top15 / max(selected_total, 1)),
        "variant_top15_rate": _safe_float(variant_top15 / max(variant_total, 1)),
        "original_bottom15_rate": _safe_float(original_bottom15 / max(selected_total, 1)),
        "variant_bottom15_rate": _safe_float(variant_bottom15 / max(variant_total, 1)),
        "top15_capture_rate": variant_top15_capture,
        "original_top15_capture_rate": original_top15_capture,
        "bottom15_contamination_rate": _safe_float(variant_bottom15 / max(variant_total, 1)),
        "membership_changed_count": int((baseline ^ variant).sum()),
        "changed_members_count": int((baseline ^ variant).sum()),
        "overlap_ratio": _safe_float((baseline & variant).sum() / max(selected_total, 1)),
        "selected_or_count": int(selected_or.sum()),
    }


def _recompute_delta_rows(frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    top15 = _bool_series(frame, "top15_label")
    bottom15 = _bool_series(frame, "bottom15_label")
    for topk in TOP_K_VALUES:
        baseline = _bool_series(frame, f"champion_selected_top{topk}")
        variant = _bool_series(frame, f"variant_selected_top{topk}")
        common = baseline & variant
        rows.extend(
            [
                frame.loc[variant & bottom15 & ~baseline].copy().assign(audit_topk=topk, delta_type="newly_added_bottom15"),
                frame.loc[baseline & bottom15 & ~variant].copy().assign(audit_topk=topk, delta_type="removed_bottom15"),
                frame.loc[common & bottom15].copy().assign(audit_topk=topk, delta_type="unchanged_bottom15"),
                frame.loc[variant & top15 & ~baseline].copy().assign(audit_topk=topk, delta_type="newly_added_top15"),
                frame.loc[baseline & top15 & ~variant].copy().assign(audit_topk=topk, delta_type="removed_top15"),
                frame.loc[common & top15].copy().assign(audit_topk=topk, delta_type="unchanged_top15"),
            ]
        )
    if not rows:
        return pd.DataFrame()
    out = pd.concat(rows, ignore_index=True)
    if "effective_rank_score" in out.columns and "score" in out.columns:
        out["effective_rank_score"] = out["effective_rank_score"].fillna(out["score"])
    return out


def _build_rebuilt_summary(
    frame: pd.DataFrame,
    *,
    candidate_surface_path: Path,
    challenger_session: Path,
    bottom15_audit_session: Path,
    no_lookahead_path: Path,
) -> dict[str, Any]:
    topk_summary = {f"top{k}": _topk_summary(frame, k) for k in TOP_K_VALUES}
    row_counts = {
        "candidate_rows": int(len(frame)),
        "challenger_rows": int(len(frame)),
        "top5_added_bottom15": int(((_bool_series(frame, "variant_selected_top5")) & _bool_series(frame, "bottom15_label") & ~_bool_series(frame, "champion_selected_top5")).sum()),
        "top10_added_bottom15": int(((_bool_series(frame, "variant_selected_top10")) & _bool_series(frame, "bottom15_label") & ~_bool_series(frame, "champion_selected_top10")).sum()),
        "top5_added_top15": int(((_bool_series(frame, "variant_selected_top5")) & _bool_series(frame, "top15_label") & ~_bool_series(frame, "champion_selected_top5")).sum()),
        "top10_added_top15": int(((_bool_series(frame, "variant_selected_top10")) & _bool_series(frame, "top15_label") & ~_bool_series(frame, "champion_selected_top10")).sum()),
        "top5_removed_bottom15": int(((_bool_series(frame, "champion_selected_top5")) & _bool_series(frame, "bottom15_label") & ~_bool_series(frame, "variant_selected_top5")).sum()),
        "top10_removed_bottom15": int(((_bool_series(frame, "champion_selected_top10")) & _bool_series(frame, "bottom15_label") & ~_bool_series(frame, "variant_selected_top10")).sum()),
    }
    return {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "source_of_truth": str(candidate_surface_path),
        "authoritative_challenger_session": str(challenger_session),
        "reference_bottom15_audit_session": str(bottom15_audit_session),
        "no_lookahead_context_audit_path": str(no_lookahead_path),
        "definitions": {
            "selected_count": "absolute count of rows selected into the topK slice",
            "top15_count": "absolute count of top15-labeled rows inside the topK slice",
            "bottom15_count": "absolute count of bottom15-labeled rows inside the topK slice",
            "newly_added_*": "rows present in variant topK but not original topK",
            "removed_*": "rows present in original topK but not variant topK",
            "unchanged_*": "rows present in both original and variant topK",
            "rate": "count divided by selected_count",
            "top15_capture_rate": "variant top15_count divided by all top15 labels in the candidate surface",
            "bottom15_contamination_rate": "variant bottom15_count divided by selected_count",
            "overlap_ratio": "intersection of original and variant selected rows divided by original selected_count",
        },
        "row_counts": row_counts,
        "topk": topk_summary,
        "notes": [
            "rebuilt from authoritative candidate_confirmation_rows.parquet",
            "variant_pool_comparison.json is the cross-check source for topK counts and selection deltas",
            "the old bottom15_delta_summary.json and bottom15_delta_rows.parquet are superseded because they do not recompute from the authoritative parquet",
        ],
    }


def _build_diff(old_summary: dict[str, Any], rebuilt_summary: dict[str, Any], frame: pd.DataFrame) -> dict[str, Any]:
    diff: dict[str, Any] = {
        "schema_version": DIFF_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "status": "reconciled_with_authoritative_parquet",
        "mismatches": {},
        "summary": {
            "old_schema_version": old_summary.get("schema_version"),
            "rebuilt_schema_version": rebuilt_summary.get("schema_version"),
        },
    }
    for topk in TOP_K_VALUES:
        key = f"top{topk}"
        old = old_summary.get("topk", {}).get(key, {})
        rebuilt = rebuilt_summary.get("topk", {}).get(key, {})
        direct = _topk_summary(frame, topk)
        for field in [
            "original_selected_count",
            "variant_selected_count",
            "original_top15_count",
            "variant_top15_count",
            "original_bottom15_count",
            "variant_bottom15_count",
            "newly_added_top15_count",
            "newly_added_bottom15_count",
            "removed_top15_count",
            "removed_bottom15_count",
            "unchanged_top15_count",
            "unchanged_bottom15_count",
            "top15_delta_count",
            "bottom15_delta_count",
            "membership_changed_count",
            "overlap_ratio",
        ]:
            old_value = old.get(field)
            rebuilt_value = rebuilt.get(field)
            direct_value = direct.get(field)
            if old_value != rebuilt_value or rebuilt_value != direct_value:
                diff["mismatches"][f"{key}.{field}"] = {
                    "old_value": old_value,
                    "rebuilt_value": rebuilt_value,
                    "direct_parquet_value": direct_value,
                    "cause": "unknown_summary_generation_bug",
                    "old_value_invalidated": True,
                }
    return diff


def _build_variant_pool_reconciliation(variant_pool: dict[str, Any], rebuilt_summary: dict[str, Any], frame: pd.DataFrame) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    pass_all = True
    for topk in TOP_K_VALUES:
        key = f"top{topk}"
        vp = variant_pool.get("variant", {}).get(key, {})
        rebuilt = rebuilt_summary.get("topk", {}).get(key, {})
        direct = _topk_summary(frame, topk)
        for field, vp_field, rb_field in [
            ("count", "variant_selected_count", "variant_selected_count"),
            ("top15_count", "variant_top15_count", "variant_top15_count"),
            ("bottom15_count", "variant_bottom15_count", "variant_bottom15_count"),
            ("top15_precision", "variant_top15_rate", "top15_rate"),
            ("bottom15_precision", "variant_bottom15_rate", "bottom15_rate"),
        ]:
            vp_value = vp.get(field)
            rebuilt_value = rebuilt.get(rb_field)
            direct_value = direct.get(rb_field)
            status = "pass" if vp_value == rebuilt_value == direct_value else "mismatch"
            pass_all = pass_all and status == "pass"
            checks.append(
                {
                    "topk": topk,
                    "field": field,
                    "variant_pool_value": vp_value,
                    "rebuilt_value": rebuilt_value,
                    "direct_parquet_value": direct_value,
                    "status": status,
                }
            )
    return {
        "schema_version": RECONCILIATION_SCHEMA_VERSION,
        "status": "pass" if pass_all else "fail",
        "checks": checks,
        "summary": {
            "all_checks_passed": pass_all,
            "comparison_source": "variant_pool_comparison.json",
            "authoritative_source": "candidate_confirmation_rows.parquet",
        },
    }


def _build_freeze_precheck(rebuilt_summary: dict[str, Any], reconciliation: dict[str, Any], diff: dict[str, Any]) -> dict[str, Any]:
    if reconciliation.get("status") != "pass":
        decision = "needs_more_metric_reconciliation"
        reason = "rebuilt_summary_does_not_match_variant_pool_or_direct_parquet"
    else:
        decision = "ready_to_freeze"
        reason = "rebuilt_summary_matches_authoritative_parquet_and_variant_pool_and_supersedes_the_old_summary"
    return {
        "schema_version": FREEZE_PRECHECK_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "decision": decision,
        "status": decision,
        "reason": reason,
        "rebuilt_summary_schema_version": rebuilt_summary.get("schema_version"),
        "variant_pool_reconciliation_status": reconciliation.get("status"),
        "old_summary_mismatch_count": len(diff.get("mismatches", {})),
        "rebuild_ready": decision == "ready_to_freeze",
        "freeze_ready": decision == "ready_to_freeze",
    }


def _build_metric_contract() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "topk_values": list(TOP_K_VALUES),
        "definitions": {
            "selected_count": "number of rows selected into the topK slice",
            "original_top15_count": "count of top15-labeled rows selected by the original champion topK",
            "variant_top15_count": "count of top15-labeled rows selected by the challenger topK",
            "original_bottom15_count": "count of bottom15-labeled rows selected by the original champion topK",
            "variant_bottom15_count": "count of bottom15-labeled rows selected by the challenger topK",
            "newly_added_*": "rows selected by variant but not by original",
            "removed_*": "rows selected by original but not by variant",
            "unchanged_*": "rows selected by both original and variant",
            "rate": "count divided by selected_count",
            "top15_capture_rate": "variant top15_count divided by all top15 labels in the full candidate surface",
            "bottom15_contamination_rate": "variant bottom15_count divided by selected_count",
            "overlap_ratio": "intersection(original, variant) divided by original_selected_count",
        },
    }


def _build_manifest(*, output_root: Path, challenger_session: Path, bottom15_audit_session: Path, reconciliation_session: Path, jobs_requested: int, jobs_supported: int, baseline_rows: int, variant_rows: int, decision: str) -> dict[str, Any]:
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "script": SCRIPT_NAME,
        "session_id": "",
        "created_utc": _utc_now(),
        "output_root": str(output_root),
        "challenger_session": str(challenger_session),
        "bottom15_audit_session": str(bottom15_audit_session),
        "reconciliation_session": str(reconciliation_session),
        "jobs_requested": int(jobs_requested),
        "jobs_supported": int(jobs_supported),
        "baseline_rows": int(baseline_rows),
        "variant_rows": int(variant_rows),
        "decision": decision,
        "production_ranking_changed": False,
        "meemee_reflectable": False,
    }


def _build_input_resolution(
    *,
    challenger_session: Path,
    bottom15_audit_session: Path,
    reconciliation_session: Path,
    candidate_path: Path,
    variant_pool_path: Path,
    topk_membership_diff_path: Path,
    old_summary_path: Path,
    delta_rows_path: Path,
) -> dict[str, Any]:
    return {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "resolved_status": "ok",
        "challenger_session": str(challenger_session),
        "bottom15_audit_session": str(bottom15_audit_session),
        "reconciliation_session": str(reconciliation_session),
        "candidate_confirmation_rows_path": str(candidate_path),
        "variant_pool_comparison_path": str(variant_pool_path),
        "topk_membership_diff_path": str(topk_membership_diff_path),
        "old_bottom15_delta_summary_path": str(old_summary_path),
        "old_bottom15_delta_rows_path": str(delta_rows_path),
    }


def run_bottom15_summary_rebuild(
    *,
    challenger_session: str | Path | None = None,
    bottom15_audit_session: str | Path | None = None,
    reconciliation_session: str | Path | None = None,
    output_root: str | Path | None = None,
    jobs: int = 1,
) -> dict[str, Any]:
    challenger_session_path = Path(challenger_session) if challenger_session else DEFAULT_CHALLENGER_SESSION
    bottom15_audit_session_path = Path(bottom15_audit_session) if bottom15_audit_session else DEFAULT_BOTTOM15_AUDIT_SESSION
    reconciliation_session_path = Path(reconciliation_session) if reconciliation_session else DEFAULT_RECONCILIATION_SESSION
    output_root_path = Path(output_root) if output_root else DEFAULT_OUTPUT_ROOT
    output_root_path = output_root_path.expanduser().resolve()

    candidate_path = challenger_session_path / "candidate_confirmation_rows.parquet"
    variant_pool_path = challenger_session_path / "variant_pool_comparison.json"
    topk_membership_diff_path = challenger_session_path / "topk_membership_diff.parquet"
    old_summary_path = bottom15_audit_session_path / "bottom15_delta_summary.json"
    delta_rows_path = bottom15_audit_session_path / "bottom15_delta_rows.parquet"
    no_lookahead_path = Path(r"G:\Tradex\audit_surface_context_backfill_v1\20260501T051248Z-eba42646\no_lookahead_context_audit.json")

    candidate = _load_frame(candidate_path)
    variant_pool = _load_json(variant_pool_path)
    old_summary = _load_json(old_summary_path)
    _ = _load_frame(delta_rows_path)  # validate source artifact exists and is readable
    if topk_membership_diff_path.exists():
        _ = _load_frame(topk_membership_diff_path)
    no_lookahead = _load_json(no_lookahead_path)
    if no_lookahead.get("status") != "pass":
        raise RuntimeError(f"no-lookahead audit did not pass: {no_lookahead.get('status')}")

    rebuilt_summary = _build_rebuilt_summary(
        candidate,
        candidate_surface_path=candidate_path,
        challenger_session=challenger_session_path,
        bottom15_audit_session=bottom15_audit_session_path,
        no_lookahead_path=no_lookahead_path,
    )
    delta_rows = _recompute_delta_rows(candidate)
    diff = _build_diff(old_summary, rebuilt_summary, candidate)
    reconciliation = _build_variant_pool_reconciliation(variant_pool, rebuilt_summary, candidate)
    freeze_precheck = _build_freeze_precheck(rebuilt_summary, reconciliation, diff)

    session_id = _make_session_id()
    session_dir = output_root_path / session_id
    session_dir.mkdir(parents=True, exist_ok=True)

    run_manifest = _build_manifest(
        output_root=output_root_path,
        challenger_session=challenger_session_path,
        bottom15_audit_session=bottom15_audit_session_path,
        reconciliation_session=reconciliation_session_path,
        jobs_requested=jobs,
        jobs_supported=1,
        baseline_rows=len(candidate),
        variant_rows=len(candidate),
        decision=freeze_precheck["decision"],
    )
    run_manifest["session_id"] = session_id

    input_resolution = _build_input_resolution(
        challenger_session=challenger_session_path,
        bottom15_audit_session=bottom15_audit_session_path,
        reconciliation_session=reconciliation_session_path,
        candidate_path=candidate_path,
        variant_pool_path=variant_pool_path,
        topk_membership_diff_path=topk_membership_diff_path,
        old_summary_path=old_summary_path,
        delta_rows_path=delta_rows_path,
    )

    supersedes = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "supersedes": {
            "bottom15_delta_summary_json": str(old_summary_path),
            "bottom15_delta_rows_parquet": str(delta_rows_path),
        },
        "superseded_by": {
            "bottom15_delta_summary_rebuilt_json": str(session_dir / "bottom15_delta_summary_rebuilt.json"),
            "recomputed_bottom15_delta_rows_parquet": str(session_dir / "recomputed_bottom15_delta_rows.parquet"),
        },
        "reason": "the authoritative candidate_confirmation_rows parquet recomputes different top15/bottom15 baseline and delta counts than the old summary artifacts",
    }

    _write_json(session_dir / "run_manifest.json", run_manifest)
    _write_json(session_dir / "input_resolution.json", input_resolution)
    _write_json(session_dir / "bottom15_delta_summary_rebuilt.json", rebuilt_summary)
    _write_json(session_dir / "bottom15_delta_summary_diff.json", diff)
    _write_json(session_dir / "variant_pool_reconciliation.json", reconciliation)
    _write_json(session_dir / "supersedes_bottom15_delta_summary.json", supersedes)
    _write_json(session_dir / "freeze_precheck_after_rebuild.json", freeze_precheck)
    _write_json(session_dir / "metric_definition_contract.json", _build_metric_contract())
    _write_parquet(session_dir / "recomputed_bottom15_delta_rows.parquet", delta_rows)
    _write_json(
        session_dir / "_ARTIFACT_COMPLETE.json",
        {
            "schema_version": SCHEMA_VERSION,
            "session_id": session_id,
            "artifact_count": 8,
            "parse_status": {
                "run_manifest": True,
                "input_resolution": True,
                "bottom15_delta_summary_rebuilt": True,
                "bottom15_delta_summary_diff": True,
                "variant_pool_reconciliation": True,
                "supersedes_bottom15_delta_summary": True,
                "freeze_precheck_after_rebuild": True,
                "metric_definition_contract": True,
            },
            "verification_status": "generated",
        },
    )
    return {
        "output_dir": str(session_dir),
        "decision": freeze_precheck["decision"],
        "session_id": session_id,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--challenger-session", default=None)
    parser.add_argument("--bottom15-audit-session", default=None)
    parser.add_argument("--reconciliation-session", default=None)
    parser.add_argument("--output-root", default=None)
    parser.add_argument("--jobs", type=int, default=1)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    result = run_bottom15_summary_rebuild(
        challenger_session=args.challenger_session,
        bottom15_audit_session=args.bottom15_audit_session,
        reconciliation_session=args.reconciliation_session,
        output_root=args.output_root,
        jobs=args.jobs,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
