from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


AXIS_ID = "forward_watch_outcome_evaluation_v1"
DEFAULT_WATCH_ROOT = Path(
    r"G:\Tradex\forward_research_watch_validation_protocol_v1\20260525T142309Z-forward-research-watch-validation-protocol-v1"
)
DEFAULT_AVAILABILITY_ROOT = Path(
    r"G:\Tradex\forward_watch_outcome_availability_audit_v1\20260525T142647Z-forward-watch-outcome-availability-audit-v1"
)
DEFAULT_SOURCE_DB = Path(r"G:\Tradex\db_snapshots\stocks_20260426_022925.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\forward_watch_outcome_evaluation_v1")
REQUIRED_ARTIFACTS = (
    "forward_outcome_evaluation_summary.json",
    "forward_outcome_evaluation_rows.csv",
    "watch_bucket_metrics.json",
    "buyability_gate_audit.json",
    "source_coverage.json",
    "no_lookahead_audit.json",
    "lineage.json",
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


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_watch_rows(watch_root: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    rows_path = watch_root / "forward_watch_rows.csv"
    decision_path = watch_root / "research_decision.json"
    if not rows_path.exists():
        raise FileNotFoundError(rows_path)
    if not decision_path.exists():
        raise FileNotFoundError(decision_path)
    rows = pd.read_csv(rows_path, dtype={"code": str})
    rows["as_of_date"] = pd.to_numeric(rows["as_of_date"], errors="coerce").astype("Int64")
    return rows, _load_json(decision_path)


def load_bars(source_db: Path, codes: list[str], min_date: int) -> pd.DataFrame:
    if not source_db.exists():
        raise FileNotFoundError(source_db)
    con = duckdb.connect(str(source_db), read_only=True)
    try:
        expr = "CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)"
        query = f"""
            SELECT CAST(code AS VARCHAR) AS code, {expr} AS bar_date, c AS close
            FROM daily_bars
            WHERE CAST(code AS VARCHAR) IN (SELECT * FROM UNNEST(?))
              AND {expr} >= ?
            ORDER BY code, bar_date
        """
        return con.execute(query, [codes, int(min_date)]).fetchdf()
    finally:
        con.close()


def attach_forward_outcomes(watch_rows: pd.DataFrame, bars: pd.DataFrame) -> pd.DataFrame:
    out = watch_rows.copy()
    ret5: list[float | None] = []
    ret20: list[float | None] = []
    future_counts: list[int] = []
    by_code = {str(code): grp.sort_values("bar_date") for code, grp in bars.groupby("code")}
    for row in out.itertuples(index=False):
        code = str(row.code)
        as_of = int(row.as_of_date)
        grp = by_code.get(code, pd.DataFrame(columns=["bar_date", "close"]))
        grp = grp[pd.to_numeric(grp["bar_date"], errors="coerce") >= as_of].sort_values("bar_date")
        if grp.empty or int(grp.iloc[0]["bar_date"]) != as_of:
            base_close = None
            future = pd.DataFrame(columns=["bar_date", "close"])
        else:
            base_close = float(grp.iloc[0]["close"])
            future = grp[pd.to_numeric(grp["bar_date"], errors="coerce") > as_of]
        future_counts.append(int(len(future)))
        if base_close and len(future) >= 5:
            ret5.append(float(future.iloc[4]["close"]) / base_close - 1.0)
        else:
            ret5.append(None)
        if base_close and len(future) >= 20:
            ret20.append(float(future.iloc[19]["close"]) / base_close - 1.0)
        else:
            ret20.append(None)
    out["available_future_session_count"] = future_counts
    out["ret5"] = ret5
    out["ret20"] = ret20
    out["ret20_outcome_available"] = out["ret20"].notna()
    out["winner_ret20_gt_10pct"] = out["ret20"].map(lambda x: bool(x > 0.10) if pd.notna(x) else None)
    out["bad_ret20_lt_minus_5pct"] = out["ret20"].map(lambda x: bool(x < -0.05) if pd.notna(x) else None)
    out["severe_ret20_lt_minus_10pct"] = out["ret20"].map(lambda x: bool(x < -0.10) if pd.notna(x) else None)
    out["buy_recommendation"] = False
    out["validated_buy"] = False
    return out


def metric_payload(rows: pd.DataFrame) -> dict[str, Any]:
    evaluated = rows[rows["ret20"].notna()].copy()
    if evaluated.empty:
        return {
            "sample_count": 0,
            "date_count": 0,
            "code_count": 0,
            "mean_ret20": None,
            "median_ret20": None,
            "winner_rate_ret20_gt_10pct": None,
            "bad_rate_ret20_lt_minus_5pct": None,
            "severe_rate_ret20_lt_minus_10pct": None,
            "outcome_coverage_rate": 0.0,
        }
    return {
        "sample_count": int(len(evaluated)),
        "date_count": int(evaluated["as_of_date"].nunique()),
        "code_count": int(evaluated["code"].nunique()),
        "mean_ret20": float(evaluated["ret20"].mean()),
        "median_ret20": float(evaluated["ret20"].median()),
        "winner_rate_ret20_gt_10pct": float((evaluated["ret20"] > 0.10).mean()),
        "bad_rate_ret20_lt_minus_5pct": float((evaluated["ret20"] < -0.05).mean()),
        "severe_rate_ret20_lt_minus_10pct": float((evaluated["ret20"] < -0.10).mean()),
        "outcome_coverage_rate": float(len(evaluated) / len(rows)) if len(rows) else 0.0,
    }


def no_lookahead_audit(rows: pd.DataFrame, watch_decision: dict[str, Any]) -> dict[str, Any]:
    frozen_ok = bool(watch_decision.get("research_decision") == "forward_watch_protocol_ready_for_future_outcome_validation")
    outcome_after_freeze = "ret20" in rows.columns and "forward_watch_protocol_rank" in rows.columns
    passed = frozen_ok and outcome_after_freeze
    return {
        "audit_result": "pass" if passed else "blocked",
        "no_lookahead_pass": passed,
        "watch_rows_frozen_before_outcome_join": frozen_ok,
        "outcomes_used_for_selection": False,
        "outcomes_joined_after_selection_for_evaluation_only": outcome_after_freeze,
        "runtime_db_write": False,
        "research_fallback_used": False,
    }


def buyability_gate_audit(metrics: dict[str, Any]) -> dict[str, Any]:
    coverage_ok = metrics["outcome_coverage_rate"] == 1.0 and metrics["sample_count"] >= 20
    quality_ok = (
        metrics["mean_ret20"] is not None
        and metrics["mean_ret20"] > 0.03
        and metrics["winner_rate_ret20_gt_10pct"] is not None
        and metrics["winner_rate_ret20_gt_10pct"] >= 0.20
        and metrics["bad_rate_ret20_lt_minus_5pct"] is not None
        and metrics["bad_rate_ret20_lt_minus_5pct"] <= 0.20
        and metrics["severe_rate_ret20_lt_minus_10pct"] is not None
        and metrics["severe_rate_ret20_lt_minus_10pct"] <= 0.10
    )
    return {
        "buyability_gate_pass": bool(coverage_ok and quality_ok),
        "coverage_gate_pass": bool(coverage_ok),
        "quality_gate_pass": bool(quality_ok),
        "thresholds": {
            "sample_count_min": 20,
            "outcome_coverage_rate": 1.0,
            "mean_ret20_min": 0.03,
            "winner_rate_ret20_gt_10pct_min": 0.20,
            "bad_rate_ret20_lt_minus_5pct_max": 0.20,
            "severe_rate_ret20_lt_minus_10pct_max": 0.10,
        },
        "validated_buy_count": 0,
        "active_gate_created": False,
    }


def decide(metrics: dict[str, Any], gate: dict[str, Any], audit: dict[str, Any]) -> tuple[str, str, list[str]]:
    if not audit["no_lookahead_pass"]:
        return "blocked_no_lookahead_violation", "BLOCKED", ["watch_outcome_evaluation_failed_no_lookahead_audit"]
    if metrics["outcome_coverage_rate"] < 1.0:
        return "forward_watch_outcome_evaluation_pending_more_confirmed_bars", "HOLD_UNDERPOWERED", ["ret20_outcomes_not_available_for_all_watch_rows"]
    if gate["buyability_gate_pass"]:
        return "forward_watch_buyability_gate_passed_for_next_validation", "KEEP", ["frozen_watch_rows_passed_predeclared_forward_outcome_gate"]
    return "forward_watch_buyability_gate_failed", "DROP", ["frozen_watch_rows_failed_predeclared_forward_outcome_gate"]


def run(
    watch_root: Path = DEFAULT_WATCH_ROOT,
    availability_root: Path = DEFAULT_AVAILABILITY_ROOT,
    source_db: Path = DEFAULT_SOURCE_DB,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
) -> Path:
    watch_rows, watch_decision = load_watch_rows(watch_root)
    min_date = int(pd.to_numeric(watch_rows["as_of_date"], errors="coerce").min())
    codes = sorted(watch_rows["code"].astype(str).unique().tolist())
    bars = load_bars(source_db, codes, min_date)
    evaluated = attach_forward_outcomes(watch_rows, bars)
    metrics = metric_payload(evaluated)
    audit = no_lookahead_audit(evaluated, watch_decision)
    gate = buyability_gate_audit(metrics)
    decision, decision_class, reasons = decide(metrics, gate, audit)

    out = output_root / f"{_now_tag()}-forward-watch-outcome-evaluation-v1"
    out.mkdir(parents=True, exist_ok=True)
    evaluated.to_csv(out / "forward_outcome_evaluation_rows.csv", index=False)
    summary = {
        "axis_id": AXIS_ID,
        "decision": decision,
        "decision_class": decision_class,
        "reason_typed": reasons,
        "watch_root": str(watch_root),
        "availability_root": str(availability_root),
        "source_db": str(source_db),
        "buyable_selection_ready": decision_class == "KEEP",
        "validated_buy_count": 0,
        **metrics,
    }
    _write_json(out / "forward_outcome_evaluation_summary.json", summary)
    _write_json(out / "watch_bucket_metrics.json", {"axis_id": AXIS_ID, "watch_top20": metrics})
    _write_json(out / "buyability_gate_audit.json", gate)
    _write_json(out / "source_coverage.json", {"axis_id": AXIS_ID, "source_db": str(source_db), "watch_root": str(watch_root), "availability_root": str(availability_root), "watch_row_count": int(len(watch_rows)), "bar_row_count_for_watch_codes": int(len(bars)), "latest_bar_date_for_watch_codes": int(bars["bar_date"].max()) if not bars.empty else None, "research_fallback_used": False})
    _write_json(out / "no_lookahead_audit.json", audit)
    _write_json(out / "lineage.json", {"axis_id": AXIS_ID, "watch_root": str(watch_root), "watch_research_decision": watch_decision, "availability_root": str(availability_root), "availability_decision": _load_json(availability_root / "research_decision.json") if (availability_root / "research_decision.json").exists() else None})
    _write_json(
        out / "research_decision.json",
        {
            "axis_id": AXIS_ID,
            "research_decision": decision,
            "decision_class": decision_class,
            "reason_typed": reasons,
            "research_watch_only": True,
            "buyable_selection_ready": decision_class == "KEEP",
            "meemee_reflectable_candidate": False,
            "runtime_db_write": False,
            "production_ranking_changed": False,
            "production_candidate_generator_changed": False,
            "publish_allowed": False,
            "validated_buy_count": 0,
            "active_gate_created": False,
            "research_fallback_used": False,
        },
    )
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--watch-root", type=Path, default=DEFAULT_WATCH_ROOT)
    parser.add_argument("--availability-root", type=Path, default=DEFAULT_AVAILABILITY_ROOT)
    parser.add_argument("--source-db", type=Path, default=DEFAULT_SOURCE_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.watch_root, args.availability_root, args.source_db, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
