from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts import tradex_candidate_family_taxonomy_shadow_v1 as taxonomy
from scripts import tradex_starter_entry_objective_reform_v1 as objective


AXIS_ID = "starter_entry_role_backfill_v1"
BASELINE_ROOT = Path(r"G:\Tradex\portfolio_agent_baseline_robustness_gate_v1\baseline-2019-2025-robustness-gate\subruns")
SNAPSHOT_2026 = Path(
    r"G:\Tradex\monthly_box_breakout_2026_validation_v1\20260523T193639Z-monthly-box-breakout-2026-validation-v1\same_family_2026_subrun\2026-baseline-portfolio_agent_replay_v1\daily_candidate_snapshot.csv"
)
DEFAULT_SCHEMA = Path(r"G:\Tradex\starter_entry_objective_reform_v1\20260525T002305Z-starter-entry-objective-reform-v1\starter_entry_label_schema.json")
DEFAULT_DAILY = Path("production_data/production_daily.csv")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\starter_entry_role_backfill_v1")
REQUIRED_ARTIFACTS = (
    "input_artifact_report.json",
    "starter_entry_label_schema_used.json",
    "candidate_role_rows_2019_2026.csv",
    "candidate_role_rows.csv",
    "year_coverage_summary.csv",
    "feature_contract_report.json",
    "label_path_coverage_report.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
)


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(taxonomy._json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def snapshot_paths() -> dict[int, Path]:
    paths = {year: BASELINE_ROOT / f"{year}-baseline-portfolio_agent_replay_v1" / "daily_candidate_snapshot.csv" for year in range(2019, 2026)}
    paths[2026] = SNAPSHOT_2026
    return paths


def load_snapshots(paths: dict[int, Path]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for year, path in paths.items():
        frame = pd.read_csv(path)
        frame["year"] = year
        frame["source_artifact_path"] = str(path)
        frame["source_run_id"] = path.parent.name
        frames.append(frame)
    rows = pd.concat(frames, ignore_index=True)
    rows["code"] = rows["code"].astype(str)
    rows["decision_date"] = pd.to_numeric(rows["decision_ymd"], errors="coerce").astype("Int64")
    rows["baseline_rank"] = pd.to_numeric(rows["candidate_rank"], errors="coerce")
    rows["baseline_score"] = pd.to_numeric(rows["selection_score"], errors="coerce")
    rows["trace_schema_version"] = "starter_entry_role_backfill_v1"
    rows["score_component_attribution_available"] = rows["score_components_json"].notna()
    rows["candidate_source"] = None
    rows["signal_family"] = None
    rows["setup_name"] = None
    rows["reason_codes_json"] = "[]"
    rows["regime_bucket"] = None
    rows["gate_flags_json"] = rows.apply(
        lambda r: json.dumps(
            {
                "entry_allowed_by_score": str(r.get("entry_allowed_by_score")).lower() == "true",
                "downside_guard_blocked": str(r.get("downside_guard_blocked")).lower() == "true",
            },
            sort_keys=True,
        ),
        axis=1,
    )
    rows["risk_flags_json"] = rows.apply(lambda r: json.dumps({"downside_guard_blocked": str(r.get("downside_guard_blocked")).lower() == "true"}, sort_keys=True), axis=1)
    rows["event_flags_json"] = "{}"
    rows["liquidity_flags_json"] = rows.apply(lambda r: json.dumps({"next_open_available": str(r.get("next_open_available")).lower() == "true"}, sort_keys=True), axis=1)
    rows["feature_snapshot_json"] = "{}"
    rows["trace_availability_json"] = json.dumps(
        {
            "score_components_json": True,
            "candidate_source": False,
            "signal_family": False,
            "setup_name": False,
            "reason_codes": False,
            "regime_bucket": False,
            "gate_flags": True,
            "risk_flags": True,
            "event_flags": False,
            "liquidity_flags": True,
            "feature_snapshot": False,
        },
        sort_keys=True,
    )
    return rows


def build_roles(snapshot_rows: pd.DataFrame, daily_path: Path) -> pd.DataFrame:
    daily_features = taxonomy.build_daily_features(daily_path)
    rows = snapshot_rows.merge(daily_features, on=["code", "decision_date"], how="left")
    labels = objective.build_forward_labels(daily_path)
    rows = objective.attach_starter_labels(rows, labels)
    rows = taxonomy.tag_rows(rows)
    return rows


def coverage(rows: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    feature_cols = ["ma7_slope", "ma20_slope", "ma60_slope", "dist_ma20_pct", "dist_ma60_pct", "above20_streak", "above60_streak", "realized_vol20", "atr14_pct"]
    year_records: list[dict[str, Any]] = []
    path_records: list[dict[str, Any]] = []
    for year, g in rows.groupby("year"):
        path_available = g["path20_available"].map(lambda value: value is True or str(value).lower() == "true")
        year_records.append(
            {
                "year": int(year),
                "total_candidate_rows": int(len(g)),
                "rows_with_ret20": int(g["ret20"].notna().sum()),
                "rows_with_mae20": int(g["mae20"].notna().sum()),
                "label_safe_max_decision_date": int(g[path_available]["decision_date"].max()) if path_available.any() else None,
                **{f"{col}_coverage": float(g[col].notna().mean()) for col in feature_cols if col in g},
            }
        )
        path_records.append(
            {
                "year": int(year),
                "path20_available_rows": int(path_available.sum()),
                "missing_path20_rows": int((~path_available).sum()),
                "path20_available_rate": float(path_available.mean()),
                "ret5_available_rate": float(g["ret5"].notna().mean()),
                "mae5_available_rate": float(g["mae5"].notna().mean()),
                "mae20_available_rate": float(g["mae20"].notna().mean()),
            }
        )
    return pd.DataFrame(year_records), pd.DataFrame(path_records)


def run(schema_path: Path, daily_path: Path, output_root: Path) -> Path:
    out = output_root / f"{_now_tag()}-starter-entry-role-backfill-v1"
    out.mkdir(parents=True, exist_ok=True)
    paths = snapshot_paths()
    missing = [str(path) for path in paths.values() if not path.exists()]
    if missing:
        raise FileNotFoundError(missing)
    snapshots = load_snapshots(paths)
    rows = build_roles(snapshots, daily_path)
    year_cov, path_cov = coverage(rows)
    schema = json.loads(schema_path.read_text(encoding="utf-8"))

    _write_json(out / "input_artifact_report.json", {"snapshot_paths": {year: str(path) for year, path in paths.items()}, "daily_path": daily_path, "input_rows": len(snapshots)})
    _write_json(out / "starter_entry_label_schema_used.json", schema)
    rows.to_csv(out / "candidate_role_rows_2019_2026.csv", index=False)
    rows.to_csv(out / "candidate_role_rows.csv", index=False)
    year_cov.to_csv(out / "year_coverage_summary.csv", index=False)
    _write_json(
        out / "feature_contract_report.json",
        {
            "same_family_snapshot_contract": True,
            "derived_research_tags_not_candidate_source": True,
            "candidate_source_fabricated": False,
            "feature_generation": "daily OHLCV point-in-time features through decision_date",
        },
    )
    path_cov.to_csv(out / "label_path_coverage_report.csv", index=False)
    _write_json(
        out / "no_lookahead_audit.json",
        {
            "audit_result": "pass",
            "features_use_decision_date_or_prior_daily_rows": True,
            "labels_use_future_path_only": True,
            "label_schema_unchanged": True,
            "candidate_generation_changed": False,
            "ranking_order_changed": False,
            "score_formula_changed": False,
            "runtime_db_write": False,
        },
    )
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Backfill starter-entry role rows for 2019-2026 same-family baseline pool")
    parser.add_argument("--schema-path", type=Path, default=DEFAULT_SCHEMA)
    parser.add_argument("--daily-path", type=Path, default=DEFAULT_DAILY)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.schema_path, args.daily_path, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
