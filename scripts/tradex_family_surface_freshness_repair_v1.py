from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts import tradex_starter_candidate_review_pack_v2 as review_v2
from scripts.tradex_starter_entry_family_source_split_design_v1 import FAMILY_TO_SOURCE
from scripts.tradex_starter_entry_family_split_v1 import assign_families


AXIS_ID = "family_surface_freshness_repair_v1"
DEFAULT_FAMILY_SOURCE_ROOT = Path(r"G:\Tradex\starter_entry_family_source_split_design_v1\20260525T041110Z-starter-entry-family-source-split-design-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\family_surface_freshness_repair_v1")

REQUIRED_ARTIFACTS = (
    "input_artifact_report.json",
    "family_surface_freshness_report.json",
    "current_family_surface_rows.csv",
    "review_pack_freshness_check.json",
    "rerun_review_pack_summary.json",
    "_ARTIFACT_COMPLETE.json",
)


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "item"):
        return _json_ready(value.item())
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _max_decision_date(path: Path) -> int | None:
    if not path.exists():
        return None
    latest: int | None = None
    for chunk in pd.read_csv(path, usecols=["decision_date"], chunksize=500_000, low_memory=False):
        value = pd.to_numeric(chunk["decision_date"], errors="coerce").max()
        if pd.notna(value):
            latest = int(value) if latest is None else max(latest, int(value))
    return latest


def _read_date_rows(path: Path, decision_date: int) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for chunk in pd.read_csv(path, chunksize=500_000, low_memory=False):
        dates = pd.to_numeric(chunk["decision_date"], errors="coerce")
        part = chunk[dates.eq(decision_date)].copy()
        if not part.empty:
            parts.append(part)
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True)


def _surface_latest_dates(root: Path) -> dict[str, int | None]:
    return {
        family: _max_decision_date(root / filename)
        for family, filename in review_v2.SURFACE_FILES.items()
    }


def build_current_surface_rows(source_rows: pd.DataFrame) -> pd.DataFrame:
    rows = source_rows.copy()
    existing_label_cols = {"ret20", "mae20", "mfe20", "starter_good", "starter_bad", "selected_loser", "selected_winner"}
    rows = assign_families(rows)
    rows["research_candidate_source_family"] = rows["primary_family"].map(FAMILY_TO_SOURCE).fillna("uncategorized_source")
    rows["research_family_surface"] = rows["research_candidate_source_family"]
    rows["research_family_source_schema_version"] = "research_family_source_schema_v1_current_review"
    rows["feature_availability_json"] = rows.get("family_feature_availability_json", "{}")
    rows["research_family_assignment_reason_json"] = rows.get("family_assignment_reason_json", "{}")
    rows["labels_required_for_current_review"] = False
    rows["future_label_fields_available"] = all(c in rows.columns and rows[c].notna().any() for c in existing_label_cols)
    rows["current_review_no_lookahead_mode"] = True
    rows["within_family_baseline_rank"] = (
        rows.sort_values(["decision_date", "research_candidate_source_family", "baseline_score", "code"], ascending=[True, True, False, True])
        .groupby(["decision_date", "research_candidate_source_family"])
        .cumcount()
        + 1
    )
    return rows


def _latest_production_daily_date() -> int | None:
    candidates = [
        Path("production_data/production_daily.csv"),
        Path("production_data/daily_bars.csv"),
        Path("data/daily_bars.csv"),
        Path("dummy_daily.csv"),
    ]
    for path in candidates:
        if path.exists():
            try:
                cols = pd.read_csv(path, nrows=0).columns
                date_col = next((c for c in ["date", "Date", "decision_date", "ymd"] if c in cols), None)
                if date_col:
                    latest = None
                    for chunk in pd.read_csv(path, usecols=[date_col], chunksize=500_000, low_memory=False):
                        nums = pd.to_datetime(chunk[date_col], errors="coerce").dt.strftime("%Y%m%d")
                        value = pd.to_numeric(nums, errors="coerce").max()
                        if pd.notna(value):
                            latest = int(value) if latest is None else max(latest, int(value))
                    return latest
            except Exception:
                continue
    return None


def run(family_source_root: Path, output_root: Path) -> Path:
    out = output_root / f"{_now_tag()}-family-surface-freshness-repair-v1"
    out.mkdir(parents=True, exist_ok=True)
    source_path = family_source_root / "candidate_family_source_rows.csv"
    latest_global = _max_decision_date(source_path)
    if latest_global is None:
        raise RuntimeError(f"no decision_date found in {source_path}")
    before_surface_dates = _surface_latest_dates(family_source_root)
    latest_label_safe = max(v for v in before_surface_dates.values() if v is not None)
    source_rows = _read_date_rows(source_path, latest_global)
    if source_rows.empty:
        raise RuntimeError(f"no source rows found for latest global date {latest_global}")
    repaired = build_current_surface_rows(source_rows)
    repaired.to_csv(out / "current_family_surface_rows.csv", index=False)

    after_surface_dates = {
        str(family): int(pd.to_numeric(g["decision_date"], errors="coerce").max())
        for family, g in repaired.groupby("research_candidate_source_family")
    }
    input_report = {
        "source_root": family_source_root,
        "source_rows_path": source_path,
        "latest_global_candidate_date": latest_global,
        "source_rows_at_latest_global_date": int(len(source_rows)),
        "latest_production_daily_date": _latest_production_daily_date(),
        "latest_label_safe_date": latest_label_safe,
    }
    requested_families = {
        "pullback_reclaim_source",
        "breakout_retest_source",
        "mature_trend_continuation_source",
        "early_trend_source",
        "range_reversal_source",
        "overextension_risk_source",
    }
    covered_requested = sorted(requested_families.intersection(after_surface_dates))
    exact_blocker = None
    if input_report["latest_production_daily_date"] is not None and input_report["latest_production_daily_date"] < latest_global:
        exact_blocker = "production_daily is older than latest global candidate date; point-in-time chart features for latest global rows are unavailable"
    elif not covered_requested:
        exact_blocker = "latest global rows lack enough point-in-time feature coverage for requested family assignment"
    _write_json(out / "input_artifact_report.json", input_report)
    _write_json(
        out / "family_surface_freshness_report.json",
        {
            "latest_global_candidate_date": latest_global,
            "latest_family_surface_date_before_repair": before_surface_dates,
            "latest_family_surface_date_after_repair": after_surface_dates,
            "requested_family_coverage_after_repair": covered_requested,
            "current_review_family_surface_complete": len(covered_requested) > 0,
            "exact_blocker": exact_blocker,
            "repair_mode": "current_review_point_in_time_family_assignment",
            "historical_validation_labels_required": True,
            "current_review_labels_required": False,
            "family_assignment_uses_only_candidate_date_features": True,
            "runtime_db_write": False,
            "meemee_changed": False,
            "production_ranking_changed": False,
        },
    )
    rerun_root = review_v2.build_pack(out, Path(r"G:\Tradex\starter_candidate_review_pack_v2"))
    summary = json.loads((rerun_root / "review_pack_summary.json").read_text(encoding="utf-8"))
    decision = json.loads((rerun_root / "review_pack_decision.json").read_text(encoding="utf-8"))
    _write_json(
        out / "review_pack_freshness_check.json",
        {
            "rerun_review_pack_root": rerun_root,
            "latest_global_candidate_date": latest_global,
            "review_date": summary.get("review_date"),
            "review_date_equals_latest_global_date": summary.get("review_date") == latest_global,
            "stale_review_pack": summary.get("stale_review_pack"),
            "manual_review_available": summary.get("manual_review_available"),
        },
    )
    _write_json(
        out / "rerun_review_pack_summary.json",
        {
            "rerun_review_pack_root": rerun_root,
            "summary": summary,
            "decision": decision,
        },
    )
    existing = [name for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json" and (out / name).exists()]
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "required_artifacts": list(REQUIRED_ARTIFACTS), "complete": len(existing) == len(REQUIRED_ARTIFACTS) - 1})
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--family-source-root", type=Path, default=DEFAULT_FAMILY_SOURCE_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(args.family_source_root, args.output_root))


if __name__ == "__main__":
    main()
