from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


AXIS_ID = "current_buyable_forward_paper_validation_v1"
DEFAULT_PROJECTION_ROOT = Path(
    r"G:\Tradex\current_buyable_candidate_projection_v1\20260526T010333Z-current-buyable-candidate-projection-v1"
)
DEFAULT_RISK_ROOT = Path(
    r"G:\Tradex\intersection_family_current_period_risk_containment_v1\20260526T010028Z-intersection-family-current-period-risk-containment-v1"
)
DEFAULT_SOURCE_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\current_buyable_forward_paper_validation_v1")
REQUIRED_ARTIFACTS = (
    "forward_paper_validation_summary.json",
    "forward_paper_validation_rows.csv",
    "candidate_freeze_contract.json",
    "outcome_window_status.json",
    "ret5_ret20_metrics.json",
    "invalidation_tracking.json",
    "drawdown_tracking.json",
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


def load_projection(projection_root: Path) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
    rows_path = projection_root / "current_buyable_candidate_rows.csv"
    summary_path = projection_root / "current_buyable_projection_summary.json"
    decision_path = projection_root / "research_decision.json"
    if not rows_path.exists():
        raise FileNotFoundError(rows_path)
    if not summary_path.exists():
        raise FileNotFoundError(summary_path)
    if not decision_path.exists():
        raise FileNotFoundError(decision_path)
    rows = pd.read_csv(rows_path, dtype={"code": str})
    rows["as_of_date"] = pd.to_numeric(rows["as_of_date"], errors="coerce").astype("Int64")
    return rows, _load_json(summary_path), _load_json(decision_path)


def load_bars(source_db: Path, codes: list[str], min_date: int) -> pd.DataFrame:
    if not source_db.exists():
        raise FileNotFoundError(source_db)
    con = duckdb.connect(str(source_db), read_only=True)
    try:
        expr = "CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)"
        query = f"""
            SELECT CAST(code AS VARCHAR) AS code, {expr} AS bar_date, o AS open, h AS high, l AS low, c AS close
            FROM daily_bars
            WHERE CAST(code AS VARCHAR) IN (SELECT * FROM UNNEST(?))
              AND {expr} >= ?
            ORDER BY code, bar_date
        """
        return con.execute(query, [codes, int(min_date)]).fetchdf()
    finally:
        con.close()


def build_candidate_freeze_contract(
    projection_root: Path,
    risk_root: Path,
    projection_summary: dict[str, Any],
    freeze_timestamp: str,
) -> dict[str, Any]:
    return {
        "axis_id": AXIS_ID,
        "selected_as_of_date": projection_summary.get("as_of_date"),
        "selected_codes": projection_summary.get("research_buyable_candidate_codes", []),
        "selector_name": "variant_b_entry_qualified_top50",
        "risk_containment_name": "variant_a_candle_risk_clean",
        "source_artifact": str(risk_root),
        "projection_artifact": str(projection_root),
        "buyable_selection_ready_at_projection": bool(projection_summary.get("buyable_selection_ready") is True),
        "validated_buy_count_at_projection": int(projection_summary.get("validated_buy_count", 0)),
        "freeze_timestamp": freeze_timestamp,
        "no_candidate_replacement": True,
        "research_only": True,
    }


def attach_forward_outcomes(candidates: pd.DataFrame, bars: pd.DataFrame) -> pd.DataFrame:
    out = candidates.copy()
    rows: list[dict[str, Any]] = []
    by_code = {str(code): grp.sort_values("bar_date") for code, grp in bars.groupby("code")}
    for row in out.itertuples(index=False):
        payload = row._asdict()
        code = str(payload["code"])
        as_of = int(payload["as_of_date"])
        grp = by_code.get(code, pd.DataFrame(columns=["bar_date", "close", "low"]))
        grp = grp[pd.to_numeric(grp["bar_date"], errors="coerce") >= as_of].sort_values("bar_date")
        if grp.empty or int(grp.iloc[0]["bar_date"]) != as_of:
            base_close = None
            future = pd.DataFrame(columns=["bar_date", "close", "low"])
        else:
            base_close = float(grp.iloc[0]["close"])
            future = grp[pd.to_numeric(grp["bar_date"], errors="coerce") > as_of]
        future_count = int(len(future))
        payload["entry_close"] = base_close
        payload["available_future_session_count"] = future_count
        payload["latest_available_future_bar_date"] = int(future["bar_date"].max()) if not future.empty else None
        payload["ret5"] = float(future.iloc[4]["close"]) / base_close - 1.0 if base_close and future_count >= 5 else None
        payload["ret20"] = float(future.iloc[19]["close"]) / base_close - 1.0 if base_close and future_count >= 20 else None
        payload["close_vs_entry"] = float(future.iloc[-1]["close"]) / base_close - 1.0 if base_close and future_count > 0 else None
        payload["max_drawdown_5d"] = float(future.iloc[:5]["low"].min()) / base_close - 1.0 if base_close and future_count >= 1 else None
        payload["max_drawdown_20d"] = float(future.iloc[:20]["low"].min()) / base_close - 1.0 if base_close and future_count >= 1 else None
        if future_count <= 0:
            status = "blocked_no_future_confirmed_bars"
        elif future_count < 5:
            status = "pending_ret5"
        elif future_count < 20:
            status = "ret5_ready_ret20_pending"
        else:
            status = "ret20_ready"
        payload["status"] = status
        payload["buy_recommendation"] = False
        payload["validated_buy"] = False
        payload["active_gate_created"] = False
        rows.append(payload)
    return pd.DataFrame(rows)


def outcome_window_status(rows: pd.DataFrame) -> dict[str, Any]:
    return {
        "axis_id": AXIS_ID,
        "candidate_count": int(len(rows)),
        "minimum_available_future_sessions": int(rows["available_future_session_count"].min()) if not rows.empty else 0,
        "median_available_future_sessions": float(rows["available_future_session_count"].median()) if not rows.empty else 0.0,
        "maximum_available_future_sessions": int(rows["available_future_session_count"].max()) if not rows.empty else 0,
        "ret5_ready_count": int(rows["ret5"].notna().sum()) if not rows.empty else 0,
        "ret20_ready_count": int(rows["ret20"].notna().sum()) if not rows.empty else 0,
        "ret5_all_candidates_ready": bool(rows["ret5"].notna().all()) if not rows.empty else False,
        "ret20_all_candidates_ready": bool(rows["ret20"].notna().all()) if not rows.empty else False,
        "status_counts": {str(k): int(v) for k, v in rows["status"].value_counts(dropna=False).to_dict().items()} if "status" in rows else {},
    }


def ret_metrics(rows: pd.DataFrame) -> dict[str, Any]:
    ret5_ready = rows[rows["ret5"].notna()]
    ret20_ready = rows[rows["ret20"].notna()]
    return {
        "axis_id": AXIS_ID,
        "ret5": {
            "sample_count": int(len(ret5_ready)),
            "mean_ret5": float(ret5_ready["ret5"].mean()) if not ret5_ready.empty else None,
            "min_ret5": float(ret5_ready["ret5"].min()) if not ret5_ready.empty else None,
            "pass_count": int((ret5_ready["ret5"] > 0).sum()) if not ret5_ready.empty else 0,
            "fail_count": int((ret5_ready["ret5"] <= -0.03).sum()) if not ret5_ready.empty else 0,
        },
        "ret20": {
            "sample_count": int(len(ret20_ready)),
            "mean_ret20": float(ret20_ready["ret20"].mean()) if not ret20_ready.empty else None,
            "winner_rate_ret20_gt_10pct": float((ret20_ready["ret20"] > 0.10).mean()) if not ret20_ready.empty else None,
            "bad_rate_ret20_lt_minus_5pct": float((ret20_ready["ret20"] < -0.05).mean()) if not ret20_ready.empty else None,
            "severe_rate_ret20_lt_minus_10pct": float((ret20_ready["ret20"] < -0.10).mean()) if not ret20_ready.empty else None,
        },
    }


def invalidation_tracking(rows: pd.DataFrame) -> dict[str, Any]:
    return {
        "axis_id": AXIS_ID,
        "invalidation_configured": False,
        "reason": "projection_artifact_contains_no_price_level_or_invalidation_contract",
        "candidate_count": int(len(rows)),
        "invalidation_hit_count": None,
    }


def drawdown_tracking(rows: pd.DataFrame) -> dict[str, Any]:
    return {
        "axis_id": AXIS_ID,
        "candidate_count": int(len(rows)),
        "max_drawdown_5d_available_count": int(rows["max_drawdown_5d"].notna().sum()) if "max_drawdown_5d" in rows else 0,
        "max_drawdown_20d_available_count": int(rows["max_drawdown_20d"].notna().sum()) if "max_drawdown_20d" in rows else 0,
        "mean_max_drawdown_5d": float(rows["max_drawdown_5d"].dropna().mean()) if rows.get("max_drawdown_5d") is not None and rows["max_drawdown_5d"].notna().any() else None,
        "mean_max_drawdown_20d": float(rows["max_drawdown_20d"].dropna().mean()) if rows.get("max_drawdown_20d") is not None and rows["max_drawdown_20d"].notna().any() else None,
    }


def no_lookahead_audit(
    rows: pd.DataFrame,
    projection_decision: dict[str, Any],
    freeze_contract: dict[str, Any],
) -> dict[str, Any]:
    projection_ok = projection_decision.get("research_decision") == "current_research_buyable_candidates_selected"
    freeze_ok = bool(freeze_contract.get("no_candidate_replacement") is True and freeze_contract.get("validated_buy_count_at_projection") == 0)
    passed = bool(projection_ok and freeze_ok)
    return {
        "audit_result": "pass" if passed else "blocked",
        "no_lookahead_pass": passed,
        "candidates_frozen_before_outcome_join": freeze_ok,
        "projection_ready": projection_ok,
        "outcomes_used_for_selection": False,
        "outcomes_joined_after_freeze_for_evaluation_only": True,
        "candidate_replacement": False,
        "runtime_db_write": False,
        "research_fallback_used": False,
    }


def decide(status: dict[str, Any], metrics: dict[str, Any], audit: dict[str, Any]) -> tuple[str, str, list[str]]:
    if not audit["no_lookahead_pass"]:
        return "blocked_no_lookahead_violation", "BLOCKED", ["candidate_freeze_or_projection_contract_failed"]
    if status["minimum_available_future_sessions"] < 5:
        return "forward_validation_pending_more_confirmed_bars", "HOLD_UNDERPOWERED", ["future_confirmed_sessions_below_ret5_window"]
    if not status["ret20_all_candidates_ready"]:
        ret5 = metrics["ret5"]
        if ret5["fail_count"] > 0 or (ret5["mean_ret5"] is not None and ret5["mean_ret5"] <= -0.03):
            return "ret5_fail_close_candidate_projection", "DROP", ["ret5_materially_failed_before_ret20_maturity"]
        return "ret5_pass_ret20_pending", "HOLD_UNDERPOWERED", ["ret5_available_without_material_failure_ret20_pending"]
    ret20 = metrics["ret20"]
    ret20_pass = (
        ret20["mean_ret20"] is not None
        and ret20["mean_ret20"] > 0.03
        and ret20["winner_rate_ret20_gt_10pct"] is not None
        and ret20["winner_rate_ret20_gt_10pct"] >= 0.20
        and ret20["bad_rate_ret20_lt_minus_5pct"] is not None
        and ret20["bad_rate_ret20_lt_minus_5pct"] <= 0.20
        and ret20["severe_rate_ret20_lt_minus_10pct"] is not None
        and ret20["severe_rate_ret20_lt_minus_10pct"] <= 0.10
    )
    if ret20_pass:
        return "ret20_pass_ready_for_robustness_gate", "KEEP", ["ret20_forward_paper_gate_passed_next_robustness_gate"]
    return "ret20_fail_close_candidate_projection", "DROP", ["ret20_forward_paper_gate_failed"]


def run(
    projection_root: Path = DEFAULT_PROJECTION_ROOT,
    risk_root: Path = DEFAULT_RISK_ROOT,
    source_db: Path = DEFAULT_SOURCE_DB,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
) -> Path:
    freeze_timestamp = _now_tag()
    candidates, projection_summary, projection_decision = load_projection(projection_root)
    freeze_contract = build_candidate_freeze_contract(projection_root, risk_root, projection_summary, freeze_timestamp)
    selected_as_of_date = int(projection_summary["as_of_date"])
    codes = sorted(candidates["code"].astype(str).unique().tolist())
    bars = load_bars(source_db, codes, selected_as_of_date)
    evaluated = attach_forward_outcomes(candidates, bars)
    status = outcome_window_status(evaluated)
    metrics = ret_metrics(evaluated)
    audit = no_lookahead_audit(evaluated, projection_decision, freeze_contract)
    decision, decision_class, reasons = decide(status, metrics, audit)

    out = output_root / f"{freeze_timestamp}-current-buyable-forward-paper-validation-v1"
    out.mkdir(parents=True, exist_ok=True)
    evaluated.to_csv(out / "forward_paper_validation_rows.csv", index=False)
    _write_json(
        out / "forward_paper_validation_summary.json",
        {
            "axis_id": AXIS_ID,
            "decision": decision,
            "decision_class": decision_class,
            "reason_typed": reasons,
            "selected_as_of_date": selected_as_of_date,
            "selected_codes": codes,
            "candidate_count": int(len(evaluated)),
            "buyable_selection_ready": False,
            "validated_buy_count": 0,
            "buy_recommendation": False,
            "active_gate_created": False,
            **status,
        },
    )
    _write_json(out / "candidate_freeze_contract.json", freeze_contract)
    _write_json(out / "outcome_window_status.json", status)
    _write_json(out / "ret5_ret20_metrics.json", metrics)
    _write_json(out / "invalidation_tracking.json", invalidation_tracking(evaluated))
    _write_json(out / "drawdown_tracking.json", drawdown_tracking(evaluated))
    _write_json(out / "no_lookahead_audit.json", audit)
    _write_json(
        out / "source_coverage.json",
        {
            "axis_id": AXIS_ID,
            "source_db": str(source_db),
            "projection_root": str(projection_root),
            "risk_root": str(risk_root),
            "candidate_count": int(len(candidates)),
            "bar_row_count_for_candidate_codes": int(len(bars)),
            "latest_bar_date_for_candidate_codes": int(bars["bar_date"].max()) if not bars.empty else None,
            "confirmed_bars_only": True,
            "research_fallback_used": False,
        },
    )
    _write_json(
        out / "research_decision.json",
        {
            "axis_id": AXIS_ID,
            "research_decision": decision,
            "decision_class": decision_class,
            "reason_typed": reasons,
            "research_watch_only": True,
            "buyable_selection_ready": False,
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
    parser.add_argument("--projection-root", type=Path, default=DEFAULT_PROJECTION_ROOT)
    parser.add_argument("--risk-root", type=Path, default=DEFAULT_RISK_ROOT)
    parser.add_argument("--source-db", type=Path, default=DEFAULT_SOURCE_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.projection_root, args.risk_root, args.source_db, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
