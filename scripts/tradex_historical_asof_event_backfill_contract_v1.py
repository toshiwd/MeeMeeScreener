from __future__ import annotations

import argparse
import json
import math
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "historical_asof_event_backfill_contract_v1"
REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ASOF_ROOT = Path(r"G:\Tradex\asof_positive_selection_score_v1\20260525T134008Z-asof-positive-selection-score-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\historical_asof_event_backfill_contract_v1")
DEFAULT_EARNINGS_ARCHIVE = REPO_ROOT / "data_store" / "raw" / "jpx_financial_announcement"
DEFAULT_RIGHTS_ARCHIVE = REPO_ROOT / "data_store" / "raw" / "jpx_ex_rights"
EVENT_COLUMNS = [
    "earnings_nearby_flag",
    "days_to_next_earnings",
    "earnings_window_bucket",
    "ex_rights_nearby_flag",
    "days_to_next_ex_rights",
    "rights_window_bucket",
    "selected_event_snapshot_date",
    "event_feature_status",
]
OFFLINE_OUTCOME_COLUMNS = ["ret5", "ret20", "winner_ret20_gt_10pct", "bad_ret20_lt_minus_5pct", "severe_ret20_lt_minus_10pct"]
REQUIRED_ARTIFACTS = (
    "event_backfill_summary.json",
    "event_backfill_rows.parquet",
    "event_backfill_rows_sample.csv",
    "event_source_contract.json",
    "source_feasibility_audit.json",
    "feature_contract.json",
    "event_coverage_metrics.json",
    "event_bucket_metrics.json",
    "offline_outcome_audit.json",
    "no_lookahead_audit.json",
    "source_coverage.json",
    "research_decision.json",
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
    if isinstance(value, (datetime, date)):
        return value.isoformat()
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
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _normalize_code(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    digits = re.sub(r"\D", "", text)
    return digits[:4] if len(digits) >= 4 else None


def _snapshot_folders(root: Path) -> list[Path]:
    return sorted([p for p in root.iterdir() if p.is_dir() and re.fullmatch(r"\d{8}", p.name)])


def _folder_date(path: Path) -> date:
    return datetime.strptime(path.name, "%Y%m%d").date()


def archive_inventory(root: Path) -> dict[str, Any]:
    folders = _snapshot_folders(root) if root.exists() else []
    return {
        "root": str(root),
        "folder_count": len(folders),
        "file_count": sum(len([f for f in folder.iterdir() if f.is_file()]) for folder in folders),
        "snapshot_dates": [folder.name for folder in folders],
        "min_snapshot_date": folders[0].name if folders else None,
        "max_snapshot_date": folders[-1].name if folders else None,
    }


def _parse_earnings_file(path: Path, snapshot_date: date) -> pd.DataFrame:
    df = pd.read_excel(path, header=4)
    if df.empty or df.shape[1] < 8:
        return pd.DataFrame(columns=["code", "planned_date", "snapshot_date", "source_file"])
    out = pd.DataFrame(
        {
            "code": df.iloc[:, 1].map(_normalize_code),
            "planned_date": pd.to_datetime(df.iloc[:, 0], errors="coerce").dt.date,
            "snapshot_date": snapshot_date,
            "source_file": path.name,
        }
    )
    return out[out["code"].notna() & out["planned_date"].notna()].copy()


def _previous_business_day(target: date) -> date:
    cursor = target - timedelta(days=1)
    while cursor.weekday() >= 5:
        cursor -= timedelta(days=1)
    return cursor


def _parse_rights_file(path: Path, snapshot_date: date) -> pd.DataFrame:
    df = pd.read_excel(path, header=3, engine="xlrd")
    if df.empty or df.shape[1] < 5:
        return pd.DataFrame(columns=["code", "ex_date", "last_rights_date", "snapshot_date", "source_file"])
    ex_date = pd.to_datetime(df.iloc[:, 2], errors="coerce").dt.date
    out = pd.DataFrame(
        {
            "code": df.iloc[:, 4].map(_normalize_code),
            "ex_date": ex_date,
            "last_rights_date": ex_date.map(lambda d: _previous_business_day(d) if d is not None and not pd.isna(d) else None),
            "snapshot_date": snapshot_date,
            "source_file": path.name,
        }
    )
    return out[out["code"].notna() & out["ex_date"].notna()].copy()


def parse_archives(earnings_root: Path, rights_root: Path) -> dict[date, dict[str, pd.DataFrame]]:
    dates = sorted({_folder_date(p) for p in _snapshot_folders(earnings_root)} | {_folder_date(p) for p in _snapshot_folders(rights_root)})
    out: dict[date, dict[str, pd.DataFrame]] = {}
    for snap in dates:
        e_folder = earnings_root / snap.strftime("%Y%m%d")
        r_folder = rights_root / snap.strftime("%Y%m%d")
        e_frames = [_parse_earnings_file(p, snap) for p in sorted(e_folder.glob("*.xlsx"))] if e_folder.exists() else []
        r_frames = [_parse_rights_file(p, snap) for p in sorted(r_folder.glob("*.xls"))] if r_folder.exists() else []
        earnings = pd.concat(e_frames, ignore_index=True) if e_frames else pd.DataFrame(columns=["code", "planned_date", "snapshot_date", "source_file"])
        rights = pd.concat(r_frames, ignore_index=True) if r_frames else pd.DataFrame(columns=["code", "ex_date", "last_rights_date", "snapshot_date", "source_file"])
        if not earnings.empty:
            earnings = earnings.sort_values(["code", "planned_date"]).drop_duplicates(["code"], keep="first")
        if not rights.empty:
            rights = rights.sort_values(["code", "last_rights_date"]).drop_duplicates(["code"], keep="first")
        out[snap] = {"earnings": earnings, "rights": rights}
    return out


def selected_snapshot(anchor: date, snapshots: list[date]) -> date | None:
    eligible = [snap for snap in snapshots if snap <= anchor]
    return max(eligible) if eligible else None


def _bucket_earnings(days: Any) -> str:
    if pd.isna(days):
        return "earnings_missing"
    delta = int(days)
    if 0 <= delta <= 3:
        return "earnings_before_0_3d"
    if 4 <= delta <= 10:
        return "earnings_before_4_10d"
    if -3 <= delta <= -1:
        return "earnings_after_0_3d"
    if -10 <= delta <= -4:
        return "earnings_after_4_10d"
    return "earnings_not_nearby"


def _bucket_rights(days: Any) -> str:
    if pd.isna(days):
        return "rights_missing"
    delta = int(days)
    if 0 <= delta <= 3:
        return "rights_before_0_3d"
    if 4 <= delta <= 5:
        return "rights_before_4_5d"
    if -3 <= delta <= -1:
        return "rights_after_0_3d"
    if -5 <= delta <= -4:
        return "rights_after_4_5d"
    return "rights_not_nearby"


def build_event_rows(source: pd.DataFrame, snapshots: dict[date, dict[str, pd.DataFrame]]) -> pd.DataFrame:
    rows = source.copy()
    rows["anchor_dt"] = pd.to_datetime(rows["as_of_date"].astype(str), errors="coerce").dt.date
    snap_dates = sorted(snapshots)
    rows["selected_event_snapshot_date"] = rows["anchor_dt"].map(lambda d: selected_snapshot(d, snap_dates) if d else None)
    rows["event_feature_status"] = rows["selected_event_snapshot_date"].map(lambda d: "snapshot_selected" if d else "missing_no_prior_snapshot")
    rows["days_to_next_earnings"] = pd.Series([pd.NA] * len(rows), dtype="Int64")
    rows["days_to_next_ex_rights"] = pd.Series([pd.NA] * len(rows), dtype="Int64")
    rows["earnings_nearby_flag"] = pd.Series([pd.NA] * len(rows), dtype="boolean")
    rows["ex_rights_nearby_flag"] = pd.Series([pd.NA] * len(rows), dtype="boolean")
    rows["earnings_window_bucket"] = "earnings_missing"
    rows["rights_window_bucket"] = "rights_missing"

    for snap, idx in rows[rows["selected_event_snapshot_date"].notna()].groupby("selected_event_snapshot_date").groups.items():
        snap_date = snap if isinstance(snap, date) else pd.to_datetime(snap).date()
        subset = rows.loc[list(idx), ["anchor_dt", "code"]].copy()
        subset["_idx"] = subset.index
        pack = snapshots.get(snap_date, {})
        earnings = pack.get("earnings", pd.DataFrame())
        if not earnings.empty:
            merged = subset.merge(earnings[["code", "planned_date"]], on="code", how="left")
            days = (pd.to_datetime(merged["planned_date"], errors="coerce") - pd.to_datetime(merged["anchor_dt"], errors="coerce")).dt.days
            rows.loc[merged["_idx"], "days_to_next_earnings"] = days.astype("Int64").values
            rows.loc[merged["_idx"], "earnings_nearby_flag"] = days.between(-3, 10, inclusive="both").astype("boolean").values
            rows.loc[merged["_idx"], "earnings_window_bucket"] = days.map(_bucket_earnings).values
        rights = pack.get("rights", pd.DataFrame())
        if not rights.empty:
            merged = subset.merge(rights[["code", "last_rights_date"]], on="code", how="left")
            days = (pd.to_datetime(merged["last_rights_date"], errors="coerce") - pd.to_datetime(merged["anchor_dt"], errors="coerce")).dt.days
            rows.loc[merged["_idx"], "days_to_next_ex_rights"] = days.astype("Int64").values
            rows.loc[merged["_idx"], "ex_rights_nearby_flag"] = days.between(-3, 5, inclusive="both").astype("boolean").values
            rows.loc[merged["_idx"], "rights_window_bucket"] = days.map(_bucket_rights).values
    rows = rows.drop(columns=["anchor_dt"])
    return rows


def _rate(series: pd.Series) -> float | None:
    values = series.dropna()
    return None if values.empty else float(values.astype(bool).mean())


def _mean(frame: pd.DataFrame, col: str) -> float | None:
    values = pd.to_numeric(frame[col], errors="coerce").dropna() if col in frame else pd.Series(dtype=float)
    return None if values.empty else float(values.mean())


def _metric(frame: pd.DataFrame) -> dict[str, Any]:
    return {
        "sample_count": int(len(frame)),
        "date_count": int(frame["as_of_date"].nunique()) if not frame.empty else 0,
        "code_count": int(frame["code"].nunique()) if not frame.empty else 0,
        "mean_ret20": _mean(frame, "ret20"),
        "winner_rate_ret20_gt_10pct": _rate(frame["winner_ret20_gt_10pct"]) if "winner_ret20_gt_10pct" in frame else None,
        "bad_rate_ret20_lt_minus_5pct": _rate(frame["bad_ret20_lt_minus_5pct"]) if "bad_ret20_lt_minus_5pct" in frame else None,
        "severe_rate_ret20_lt_minus_10pct": _rate(frame["severe_ret20_lt_minus_10pct"]) if "severe_ret20_lt_minus_10pct" in frame else None,
        "outcome_coverage_rate": float(frame["ret20"].notna().mean()) if "ret20" in frame and not frame.empty else None,
    }


def event_bucket_metrics(rows: pd.DataFrame) -> dict[str, Any]:
    out = {
        "snapshot_selected": _metric(rows[rows["selected_event_snapshot_date"].notna()]),
        "no_prior_snapshot": _metric(rows[rows["selected_event_snapshot_date"].isna()]),
        "earnings_nearby_true": _metric(rows[rows["earnings_nearby_flag"].fillna(False)]),
        "earnings_nearby_false": _metric(rows[rows["earnings_nearby_flag"].fillna(False) == False]),
        "ex_rights_nearby_true": _metric(rows[rows["ex_rights_nearby_flag"].fillna(False)]),
        "ex_rights_nearby_false": _metric(rows[rows["ex_rights_nearby_flag"].fillna(False) == False]),
    }
    return out


def coverage_metrics(rows: pd.DataFrame, snapshots: dict[date, dict[str, pd.DataFrame]]) -> dict[str, Any]:
    selected = rows["selected_event_snapshot_date"].notna()
    earnings_known = rows["days_to_next_earnings"].notna()
    rights_known = rows["days_to_next_ex_rights"].notna()
    return {
        "row_count": int(len(rows)),
        "date_count": int(rows["as_of_date"].nunique()),
        "code_count": int(rows["code"].nunique()),
        "snapshot_count": int(len(snapshots)),
        "snapshot_selected_row_count": int(selected.sum()),
        "snapshot_selected_rate": float(selected.mean()) if len(rows) else 0.0,
        "earnings_feature_available_rate": float(earnings_known.mean()) if len(rows) else 0.0,
        "rights_feature_available_rate": float(rights_known.mean()) if len(rows) else 0.0,
        "first_snapshot_date": min(snapshots).isoformat() if snapshots else None,
        "last_snapshot_date": max(snapshots).isoformat() if snapshots else None,
    }


def decide(coverage: dict[str, Any], no_lookahead_pass: bool) -> tuple[str, str, list[str]]:
    if not no_lookahead_pass:
        return "blocked_no_lookahead_violation", "BLOCKED", ["future_snapshot_selected"]
    if coverage["snapshot_selected_rate"] < 0.20:
        return "historical_event_backfill_created_but_undercovered", "HOLD_UNDERPOWERED", ["jpx_snapshot_archives_are_point_in_time_but_cover_only_recent_fraction_of_all_bars_surface"]
    if coverage["earnings_feature_available_rate"] > 0.50:
        return "historical_event_backfill_ready_for_event_risk_pretest", "KEEP", ["jpx_earnings_rights_backfill_has_sufficient_asof_coverage"]
    return "historical_event_backfill_insufficient_coverage", "BLOCKED", ["event_snapshot_coverage_too_sparse_for_buyable_selection"]


def feature_contract() -> dict[str, Any]:
    fields = {"as_of_date": {"classification": "identifier"}, "code": {"classification": "identifier"}}
    for col in EVENT_COLUMNS:
        fields[col] = {"classification": "point_in_time_feature"}
    for col in OFFLINE_OUTCOME_COLUMNS:
        fields[col] = {"classification": "offline_outcome_only"}
    fields["actual_earnings_announcement_date"] = {"classification": "unavailable"}
    fields["tdnet_material_disclosure_flag"] = {"classification": "unavailable"}
    fields["ret20_derived_tags"] = {"classification": "forbidden_future_leak"}
    return {"axis_id": AXIS_ID, "fields": fields}


def run(asof_root: Path = DEFAULT_ASOF_ROOT, output_root: Path = DEFAULT_OUTPUT_ROOT, earnings_archive: Path = DEFAULT_EARNINGS_ARCHIVE, rights_archive: Path = DEFAULT_RIGHTS_ARCHIVE) -> Path:
    source_path = asof_root / "asof_positive_selection_score_rows.parquet"
    source = pd.read_parquet(source_path)
    snapshots = parse_archives(earnings_archive, rights_archive)
    rows = build_event_rows(source, snapshots)
    coverage = coverage_metrics(rows, snapshots)
    no_lookahead_pass = bool((pd.to_datetime(rows["selected_event_snapshot_date"], errors="coerce").dt.strftime("%Y%m%d").astype("float").fillna(0) <= rows["as_of_date"]).all())
    decision, decision_class, reasons = decide(coverage, no_lookahead_pass)
    metrics = event_bucket_metrics(rows)
    out = output_root / f"{_now_tag()}-historical-asof-event-backfill-contract-v1"
    out.mkdir(parents=True, exist_ok=True)
    output_cols = ["as_of_date", "code", *EVENT_COLUMNS, *[c for c in OFFLINE_OUTCOME_COLUMNS if c in rows.columns]]
    rows[output_cols].to_parquet(out / "event_backfill_rows.parquet", index=False)
    rows[output_cols].head(25000).to_csv(out / "event_backfill_rows_sample.csv", index=False)
    source_feasibility = {
        "axis_id": AXIS_ID,
        "earnings_archive": archive_inventory(earnings_archive),
        "rights_archive": archive_inventory(rights_archive),
        "classification": "available_actionable_point_in_time_undercovered",
        "coverage_metrics": coverage,
        "research_fallback_used": False,
    }
    contract = {
        "axis_id": AXIS_ID,
        "contract_id": "historical_asof_event_backfill_contract_v1",
        "diagnostic_only": True,
        "actionable_contract_complete": decision_class == "KEEP",
        "snapshot_selection_rule": "use latest JPX archive snapshot folder with snapshot_date <= as_of_date",
        "available_features": ["earnings_nearby_flag", "days_to_next_earnings", "earnings_window_bucket", "ex_rights_nearby_flag", "days_to_next_ex_rights", "rights_window_bucket"],
        "missing_features": ["actual_earnings_announcement_date", "tdnet_material_disclosure_flag", "dilution_event_flag", "ex_dividend_or_shareholder_benefit_flag"],
    }
    no_lookahead = {
        "audit_result": "pass" if no_lookahead_pass else "blocked",
        "no_lookahead_pass": no_lookahead_pass,
        "future_snapshot_selected": not no_lookahead_pass,
        "offline_outcomes_used_in_event_flags": False,
        "runtime_db_write": False,
        "research_fallback_used": False,
    }
    source_coverage = {"axis_id": AXIS_ID, **coverage, "asof_input_root": str(asof_root), "research_fallback_used": False}
    offline_audit = {"outcomes_are_offline_only": True, "outcome_columns": OFFLINE_OUTCOME_COLUMNS, "outcome_coverage_rate": float(rows["ret20"].notna().mean()) if "ret20" in rows else 0.0}
    summary = {"axis_id": AXIS_ID, "decision": decision, "decision_class": decision_class, "reason_typed": reasons, "coverage_metrics": coverage, "event_bucket_metrics": metrics}
    research_decision = {"axis_id": AXIS_ID, "research_decision": decision, "decision_class": decision_class, "reason_typed": reasons, "meemee_reflectable_candidate": False, "runtime_db_write": False, "production_ranking_changed": False, "production_candidate_generator_changed": False, "publish_allowed": False, "validated_buy_count": 0, "active_gate_created": False, "research_fallback_used": False}
    _write_json(out / "event_backfill_summary.json", summary)
    _write_json(out / "event_source_contract.json", contract)
    _write_json(out / "source_feasibility_audit.json", source_feasibility)
    _write_json(out / "feature_contract.json", feature_contract())
    _write_json(out / "event_coverage_metrics.json", coverage)
    _write_json(out / "event_bucket_metrics.json", metrics)
    _write_json(out / "offline_outcome_audit.json", offline_audit)
    _write_json(out / "no_lookahead_audit.json", no_lookahead)
    _write_json(out / "source_coverage.json", source_coverage)
    _write_json(out / "research_decision.json", research_decision)
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--asof-root", type=Path, default=DEFAULT_ASOF_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--earnings-archive", type=Path, default=DEFAULT_EARNINGS_ARCHIVE)
    parser.add_argument("--rights-archive", type=Path, default=DEFAULT_RIGHTS_ARCHIVE)
    args = parser.parse_args(argv)
    out = run(args.asof_root, args.output_root, args.earnings_archive, args.rights_archive)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
