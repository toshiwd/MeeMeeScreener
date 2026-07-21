from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))

from scripts.tradex_long_fresh_family_events_v1 import FAMILIES, add_scores
from scripts.tradex_long_fresh_pullback_tail_guard_v1 import DEVELOPMENT_RETENTION_QUANTILE, FEATURES, model
from scripts.tradex_long_ordinary_pit_compound_tree_v1 import load_rows
from scripts.tradex_runtime_freshness_guard_v1 import build_runtime_freshness_guard


AXIS_ID = "tradex_long_fresh_operational_board_v1"
STRATEGY_ID = "long_fresh_pullback_tail_guard_day5_exit_v1"
DAY5_EXIT_TRIGGER_PCT = -2.0


def _iso_from_epoch(value: int) -> str:
    return pd.to_datetime(int(value), unit="s").date().isoformat()


def new_entry_action(*, held: bool, chart_usable: bool, chart_status: str | None) -> str:
    if held:
        return "HOLDING_REVIEW_NO_NEW_ENTRY"
    if not chart_usable:
        return "CHART_REVIEW_REQUIRED"
    return {
        "Starter": "ENTRY_REVIEW_NEXT_OPEN",
        "Watch": "WATCH_NO_ENTRY",
        "Wait": "WAIT_NO_ENTRY",
        "Avoid": "AVOID_NO_ENTRY",
    }.get(str(chart_status), "CHART_REVIEW_REQUIRED")


def evaluate_managed_entry(entry: dict[str, Any], bars: list[dict[str, Any]], holding: dict[str, Any] | None) -> dict[str, Any]:
    required = ["code", "signal_date", "entry_date", "entry_price", "high_tail_risk"]
    missing = [name for name in required if entry.get(name) is None]
    if missing:
        return {"code": str(entry.get("code") or ""), "action": "STOP", "reason": "entry_ledger_missing_fields", "missing_fields": missing}
    code = str(entry["code"])
    if not holding or float(holding.get("long_qty") or 0) <= 0:
        return {"code": code, "action": "STOP", "reason": "registered_entry_not_found_in_positions_live"}
    if bool(holding.get("has_issue")):
        return {"code": code, "action": "STOP", "reason": "positions_live_has_issue", "issue_note": holding.get("issue_note")}
    entry_price = float(entry["entry_price"])
    ordered = sorted(bars, key=lambda row: int(row["date"]))
    if not ordered:
        return {"code": code, "action": "STOP", "reason": "confirmed_bars_missing_since_entry"}
    sessions = len(ordered)
    current = ordered[-1]
    result = {
        "code": code, "strategy_id": STRATEGY_ID, "action": "HOLD_MONITOR",
        "reason": "before_day5_or_no_exit_trigger", "holding_sessions": sessions,
        "entry_price": entry_price, "latest_confirmed_date": _iso_from_epoch(current["date"]),
        "latest_close": float(current["c"]), "latest_return_pct": 100.0 * (float(current["c"]) / entry_price - 1.0),
        "high_tail_risk": bool(entry["high_tail_risk"]),
    }
    if sessions >= 5:
        day5 = ordered[4]
        day5_return = 100.0 * (float(day5["c"]) / entry_price - 1.0) - 0.3
        result.update({"day5_date": _iso_from_epoch(day5["date"]), "day5_close": float(day5["c"]), "day5_return_after_cost_pct": day5_return})
        if bool(entry["high_tail_risk"]) and day5_return <= DAY5_EXIT_TRIGGER_PCT:
            result["action"] = "EXIT_REVIEW_CLOSE" if sessions == 5 else "STOP_MISSED_EXIT_REVIEW"
            result["reason"] = "high_tail_risk_and_day5_return_at_or_below_minus2"
            return result
    if sessions >= 20:
        result["action"] = "EXIT_REVIEW_CLOSE"
        result["reason"] = "maximum_20_sessions_reached"
    return result


def _fit_tail_model(rows: pd.DataFrame):
    family = FAMILIES[1]
    events = (rows.sort_values(["date", family, "code"], ascending=[True, False, True])
              .groupby("date", sort=False).head(3).copy())
    matured = events[events.p1_o.notna() & events.p20_c.notna()].copy()
    matured["realized_ret"] = 100.0 * (matured.p20_c / matured.p1_o - 1.0) - 0.3
    matured["bad"] = matured.realized_ret.le(-5).astype(int)
    matured["year"] = matured.signal_dt.dt.year
    development = matured[matured.year.between(2016, 2023)]
    oof = pd.Series(index=development.index, dtype=float)
    for validation_year in range(2020, 2024):
        train = development[development.year < validation_year]
        valid = development[development.year == validation_year]
        fitted = model().fit(train[FEATURES], train.bad)
        oof.loc[valid.index] = fitted.predict_proba(valid[FEATURES])[:, 1]
    threshold = float(oof.dropna().quantile(DEVELOPMENT_RETENTION_QUANTILE))
    fitted = model().fit(matured[matured.year.le(2025)][FEATURES], matured[matured.year.le(2025)].bad)
    return family, threshold, fitted


def _holdings(conn: duckdb.DuckDBPyConnection) -> dict[str, dict[str, Any]]:
    rows = conn.execute("""
      SELECT symbol, coalesce(spot_qty, 0) + coalesce(margin_long_qty, 0) AS long_qty,
             coalesce(margin_short_qty, 0) AS short_qty, opened_at, has_issue, issue_note
      FROM positions_live
    """).fetchdf()
    return {str(row.symbol): row._asdict() for row in rows.itertuples(index=False)}


def _read_ledger(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {"schema_version": "tradex_long_fresh_entry_ledger_v1", "entries": []}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != "tradex_long_fresh_entry_ledger_v1" or not isinstance(payload.get("entries"), list):
        raise ValueError("invalid entry ledger schema")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--adoption-audit", type=Path, required=True)
    parser.add_argument("--tail-exit-compare", type=Path, required=True)
    parser.add_argument("--chart-review", type=Path)
    parser.add_argument("--entry-ledger", type=Path)
    parser.add_argument("--today")
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=False)
    today = date.fromisoformat(args.today) if args.today else date.today()
    freshness = build_runtime_freshness_guard(db_path=args.db, max_stale_calendar_days=4, require_expected_latest=True, today=today)
    adoption = json.loads(args.adoption_audit.read_text(encoding="utf-8"))
    tail_exit = json.loads(args.tail_exit_compare.read_text(encoding="utf-8"))
    chart_review = json.loads(args.chart_review.read_text(encoding="utf-8")) if args.chart_review else None
    ledger = _read_ledger(args.entry_ledger)
    source_checks = {
        "runtime_freshness_pass": freshness["pass"],
        "base_adoption_practical_review_only": adoption["judgment"]["authoritative_rollup_decision"] == "practical_review_only",
        "tail_exit_keep_review_only": tail_exit["judgment"]["authoritative_rollup_decision"] == "keep_review_only",
        "tail_exit_all_checks_pass": all(tail_exit["authoritative_result"]["checks"].values()),
    }
    rows_out: list[dict[str, Any]] = []
    managed: list[dict[str, Any]] = []
    latest_iso = freshness.get("confirmed_max_date")
    if all(source_checks.values()):
        rows = load_rows(str(args.db), broad_trigger=False, min_date="2016-01-01")
        rows["signal_dt"] = pd.to_datetime(rows.date, unit="s")
        rows = add_scores(rows)
        family, risk_threshold, fitted = _fit_tail_model(rows)
        latest = int(rows.date.max())
        current = rows[rows.date.eq(latest)].copy()
        frames = []
        for family_name in FAMILIES:
            selected = current.sort_values([family_name, "code"], ascending=[False, True]).head(3).copy()
            selected["family"] = family_name
            selected["family_score"] = selected[family_name]
            selected["family_rank"] = range(1, len(selected) + 1)
            frames.append(selected)
        candidates = pd.concat(frames, ignore_index=True)
        strength = 0.5 * float(candidates.market_breadth_ma20.iloc[0]) + 0.5 * float(candidates.market_advancers_ratio.iloc[0])
        breakout = sorted(candidates.family.unique())[0]
        candidates = candidates[(candidates.family != breakout) | (candidates.family_rank <= 1) | (strength >= 0.7)].copy()
        chart_usable = bool(chart_review and chart_review.get("latest_as_of") == freshness.get("confirmed_max_date") and chart_review.get("judgment", {}).get("authoritative_rollup_decision") == "READY_REVIEW_ONLY")
        chart_by_code = {str(row["code"]): row for row in chart_review["authoritative_result"]["rows"]} if chart_usable else {}
        with duckdb.connect(str(args.db), read_only=True) as conn:
            holdings = _holdings(conn)
        for row in candidates.itertuples(index=False):
            code = str(row.code)
            tail_risk = float(fitted.predict_proba(pd.DataFrame([{name: getattr(row, name) for name in FEATURES}]))[:, 1][0]) if row.family == family else None
            high_tail = tail_risk is not None and tail_risk > risk_threshold
            chart = chart_by_code.get(code)
            held = float((holdings.get(code) or {}).get("long_qty") or 0) > 0
            action = new_entry_action(held=held, chart_usable=chart_usable, chart_status=None if not chart else chart.get("status"))
            rows_out.append({
                "code": code, "stock_name": row.stock_name, "family": row.family,
                "family_rank": int(row.family_rank), "family_score": float(row.family_score),
                "signal_date": _iso_from_epoch(latest), "close": float(row.c),
                "chart_status": None if not chart else chart.get("status"),
                "new_entry_action": action,
                "confidence": "medium",
                "proposed_weight_pct": 0.0 if held else 5.0 * min(1.0, strength / 0.7),
                "tail_risk": tail_risk, "tail_risk_threshold": risk_threshold if tail_risk is not None else None,
                "high_tail_risk": high_tail,
                "entry_registration_template": {
                    "strategy_id": STRATEGY_ID, "code": code, "signal_date": _iso_from_epoch(latest),
                    "entry_date": "REQUIRED_AFTER_EXECUTION", "entry_price": "REQUIRED_AFTER_EXECUTION",
                    "signal_tail_risk": tail_risk, "high_tail_risk": high_tail,
                } if action == "ENTRY_REVIEW_NEXT_OPEN" else None,
            })
        with duckdb.connect(str(args.db), read_only=True) as conn:
            for entry in ledger["entries"]:
                code = str(entry.get("code") or "")
                entry_date = str(entry.get("entry_date") or "")
                bars = []
                if code and entry_date:
                    bars = conn.execute("""
                      SELECT date, c FROM daily_bars
                      WHERE code = ? AND date >= epoch(strptime(?, '%Y-%m-%d'))
                        AND date <= epoch(strptime(?, '%Y-%m-%d')) AND coalesce(source, 'pan') = 'pan'
                      ORDER BY date
                    """, [code, entry_date, latest_iso]).fetchdf().to_dict("records")
                managed.append(evaluate_managed_entry(entry, bars, holdings.get(code)))
    else:
        risk_threshold = None
    stop_count = sum(row["action"].startswith("STOP") for row in managed)
    checks = {**source_checks, "no_managed_entry_stop": stop_count == 0}
    decision = "PRODUCTION_DECISION_SUPPORT_READY" if all(checks.values()) else "STOP"
    payload = {
        "schema_version": f"{AXIS_ID}.board.v1", "artifact_role": "authoritative",
        "generated_at": datetime.now(timezone.utc).isoformat(), "boundary_owner": "TRADEX",
        "latest_as_of": latest_iso, "runtime_freshness_guard": freshness,
        "fixed_contract": {
            "strategy_id": STRATEGY_ID, "entry": "next session open after explicit execution registration",
            "tail_risk_model_training_end": 2025, "high_tail_risk_threshold": risk_threshold,
            "conditional_exit": "high tail risk at signal and session-5 close return after 0.3% cost <= -2%",
            "maximum_exit": "session-20 close review", "automatic_order_execution": False,
            "chart_review_role": "date-matched advisory only; never removes a validated family candidate",
            "failure_policy": "STOP on stale data, malformed ledger, position mismatch, or missing confirmed bars",
            "runtime_db_write": False, "meemee_changed": False,
        },
        "authoritative_result": {
            "new_entry_candidates": rows_out, "managed_entries": managed,
            "entry_review_count": sum(row["new_entry_action"] == "ENTRY_REVIEW_NEXT_OPEN" for row in rows_out),
            "chart_review_advisory_used": bool(chart_review and chart_review.get("latest_as_of") == latest_iso),
            "exit_review_count": sum(row["action"] == "EXIT_REVIEW_CLOSE" for row in managed),
            "stop_count": stop_count, "checks": checks,
        },
        "judgment": {
            "candidate_local_decision": decision, "authoritative_rollup_decision": decision,
            "reason_type": "fresh_fail_closed_registered_entry_operational_board",
        },
        "remaining_risks": [
            "This produces decision support and never sends an order",
            "Executed entries must be registered with actual date and price before management begins",
            "Intraday provisional prices are excluded; actions use confirmed closes only",
        ],
    }
    (output / "operational_board.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    (output / "entry_ledger_template.json").write_text(json.dumps({"schema_version": "tradex_long_fresh_entry_ledger_v1", "entries": [row["entry_registration_template"] for row in rows_out if row["entry_registration_template"]]}, ensure_ascii=False, indent=2), encoding="utf-8")
    (output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "operational_board.json"}), encoding="utf-8")
    print(json.dumps({"decision": decision, "latest_as_of": latest_iso, "entry_reviews": payload["authoritative_result"]["entry_review_count"], "managed": managed, "checks": checks}, ensure_ascii=False))
    if decision != "PRODUCTION_DECISION_SUPPORT_READY":
        raise SystemExit(2)


if __name__ == "__main__":
    sys.path[:0] = [str(Path.cwd()), str(Path.cwd() / "scripts"), str(Path.cwd() / "app")]
    main()
