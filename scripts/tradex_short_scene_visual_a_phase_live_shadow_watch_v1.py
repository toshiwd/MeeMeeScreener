from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.tradex_market_scene_signal_probe_v1 import _load_daily
from scripts.tradex_short_scene_visual_additive_a_phase_anti_long_high_hold_oos_v1 import _anti_long_high_hold_features
from scripts.tradex_short_scene_visual_additive_a_phase_100ma_slope_tight_oos_v1 import REQUIRED_SHAPE_INTENT
from scripts.tradex_short_scene_visual_additive_a_phase_slope_floor_tight_oos_v1 import MA20_SLOPE_10_FLOOR_TIGHT
from scripts.tradex_short_scene_visual_additive_candidate_v1 import _ma20_slope_10, _select_one_per_date
from scripts.tradex_short_scene_visual_candidate_gap_v1 import _key, _to_visual_bars, _write_json
from scripts.tradex_visual_ai_entry_benchmark_v1 import _visual_features_from_ohlc, _visual_review
from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path
from tools.debug.trade_shape_classifier import classify_shape_from_bars


DEFAULT_OUTPUT_ROOT = Path("G:/Tradex/short_scene_visual_a_phase_live_shadow_watch_v1")
CONTRACT_READY_DECISION = "candidate_generation_contract_ready"
BASE_SIGNAL_KEY = "downtrend_a_phase|sell_rebound_rejection_or_lower_low|short|pullback_probe_candidate"


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _contract_checks(contract: dict[str, Any]) -> dict[str, bool]:
    candidate_contract = contract.get("candidate_contract", {})
    scope = contract.get("scope", {})
    return {
        "authoritative_contract_ready": contract.get("authoritative_rollup_decision") == CONTRACT_READY_DECISION,
        "paper_replay_ready": candidate_contract.get("paper_replay_ready") is True,
        "tradex_owner": candidate_contract.get("owner") == "TRADEX",
        "sell_side_contract": candidate_contract.get("side") == "sell",
        "meemee_reflectable_false": candidate_contract.get("meemee_reflectable") is False,
        "no_meemee_or_runtime_mutation": all(
            scope.get(field) is False
            for field in ("meemee_ranking_changed", "meemee_ui_changed", "runtime_db_written")
        ),
        "no_fallback": scope.get("silent_fallback_used") is False and scope.get("research_fallback_used") is False,
    }


def _latest_signal_dt(con: duckdb.DuckDBPyConnection) -> int:
    value = con.execute("SELECT max(dt) FROM signal_decision_daily").fetchone()[0]
    if value is None:
        raise RuntimeError("signal_decision_daily has no rows")
    return int(value)


def _load_latest_signal_context(con: duckdb.DuckDBPyConnection, *, dt: int) -> dict[str, Any]:
    grouped = con.execute(
        """
        SELECT side, entry_qualified, COUNT(*)
        FROM signal_decision_daily
        WHERE dt = ?
        GROUP BY side, entry_qualified
        ORDER BY side, entry_qualified
        """,
        [dt],
    ).fetchall()
    rows = con.execute(
        """
        SELECT code, name, side, entry_qualified, setup_type, score_snapshot_json, rank_snapshot_json
        FROM signal_decision_daily
        WHERE dt = ?
        ORDER BY code, side
        """,
        [dt],
    ).fetchall()
    names: dict[str, str] = {}
    sell_qualified: set[str] = set()
    sell_qualified_rows: list[dict[str, Any]] = []
    all_codes: set[str] = set()
    for code, name, side, entry_qualified, setup_type, score_json, rank_json in rows:
        code_str = str(code)
        all_codes.add(code_str)
        if name:
            names[code_str] = str(name)
        if side == "sell" and bool(entry_qualified):
            sell_qualified.add(code_str)
            sell_qualified_rows.append(
                {
                    "dt": dt,
                    "code": code_str,
                    "name": name,
                    "side": side,
                    "entry_qualified": True,
                    "setup_type": setup_type,
                    "score_snapshot_json": score_json,
                    "rank_snapshot_json": rank_json,
                }
            )
    return {
        "grouped_counts": [
            {"side": side, "entry_qualified": bool(entry_qualified), "count": int(count)}
            for side, entry_qualified, count in grouped
        ],
        "names": names,
        "all_codes": all_codes,
        "sell_qualified_codes": sell_qualified,
        "sell_qualified_rows": sell_qualified_rows,
    }


def _bar_index_for_dt(bars: list[dict[str, Any]], dt: int) -> int | None:
    for index in range(len(bars) - 1, -1, -1):
        ymd = int(bars[index]["ymd"])
        if ymd == dt:
            return index
        if ymd < dt:
            return None
    return None


def _build_live_candidates(
    by_code: dict[str, list[dict[str, Any]]],
    *,
    dt: int,
    names: dict[str, str],
    existing_sell_codes: set[str],
    allowed_codes: set[str],
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for code, bars in by_code.items():
        if code not in allowed_codes or code in existing_sell_codes:
            continue
        index = _bar_index_for_dt(bars, dt)
        if index is None or index < 239:
            continue
        close = float(bars[index]["c"])
        if close <= 0:
            continue
        window = bars[index - 159 : index + 1]
        display_window = bars[index - 239 : index + 1]
        slope = _ma20_slope_10(window)
        if slope is None or slope < MA20_SLOPE_10_FLOOR_TIGHT:
            continue
        visual = _visual_review(_visual_features_from_ohlc(_to_visual_bars(window[-60:])))
        if visual.get("decision") != "pullback_probe_candidate":
            continue
        shape = classify_shape_from_bars([[row["ymd"], row["o"], row["h"], row["l"], row["c"], row["v"]] for row in window])
        signal_key = _key(shape, visual)
        if signal_key != BASE_SIGNAL_KEY or shape.get("shape_intent") != REQUIRED_SHAPE_INTENT:
            continue
        anti_features = _anti_long_high_hold_features(display_window)
        event = {
            "dt": dt,
            "code": code,
            "name": names.get(code, code),
            "side": "sell",
            "entry_qualified": True,
            "setup_type": "short_scene_visual_a_phase_live_shadow_watch_v1",
            "close": close,
            "scene_visual_key": signal_key,
            "market_scene": shape.get("market_scene"),
            "trade_side": shape.get("trade_side"),
            "action_bias": shape.get("action_bias"),
            "shape_intent": shape.get("shape_intent"),
            "entry_timing": shape.get("entry_timing"),
            "visual_decision": visual.get("decision"),
            "visual_entry_method": visual.get("entry_method"),
            "visual_reasons": visual.get("reasons"),
            "ma20_slope_10": slope,
            "anti_long_high_hold_features": anti_features,
            "anti_long_high_hold_gate": "reject" if anti_features.get("anti_short_high_hold") else "pass",
            "in_existing_sell_pool": False,
            "shadow_trade_candidate": True,
            "runtime_db_written": False,
        }
        if anti_features.get("anti_short_high_hold"):
            event["shadow_trade_candidate"] = False
            event["anti_long_high_hold_reject_reason"] = "one_year_display_high_hold_is_not_short_candidate"
            rejected.append(event)
        else:
            events.append(event)
    return events, rejected


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "dt",
        "code",
        "name",
        "side",
        "setup_type",
        "close",
        "scene_visual_key",
        "market_scene",
        "action_bias",
        "shape_intent",
        "visual_decision",
        "visual_entry_method",
        "ma20_slope_10",
        "anti_long_high_hold_gate",
        "selected_shadow_candidate",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _decision(*, checks: dict[str, bool], selected_count: int) -> dict[str, Any]:
    blockers = [name for name, passed in checks.items() if not passed]
    if blockers:
        return {
            "judgment": "hold",
            "reason_type": "live_shadow_contract_blocked",
            "blockers": blockers,
            "forward_shadow_ready": False,
        }
    if selected_count <= 0:
        return {
            "judgment": "hold_no_live_candidate",
            "reason_type": "contract_ready_but_no_current_outside_gap_candidate",
            "blockers": [],
            "forward_shadow_ready": True,
        }
    return {
        "judgment": "continue_live_shadow",
        "reason_type": "contract_ready_and_current_shadow_candidate_selected",
        "blockers": [],
        "forward_shadow_ready": True,
    }


def run_watch(*, contract_path: Path, db_path: Path, output_root: Path, signal_dt: int | None = None) -> dict[str, Any]:
    output_dir = output_root / f"{_now_tag()}-short_scene_visual_a_phase_live_shadow_watch_v1"
    output_dir.mkdir(parents=True, exist_ok=True)
    before_stat = db_path.stat()
    contract = _load_json(contract_path)
    checks = _contract_checks(contract)
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        latest_dt = signal_dt if signal_dt is not None else _latest_signal_dt(con)
        signal_context = _load_latest_signal_context(con, dt=latest_dt)
        by_code = _load_daily(con, start_dt=latest_dt, end_dt=latest_dt, history=260, forward=0)
    finally:
        con.close()
    after_stat = db_path.stat()

    events, rejected_events = _build_live_candidates(
        by_code,
        dt=latest_dt,
        names=signal_context["names"],
        existing_sell_codes=signal_context["sell_qualified_codes"],
        allowed_codes=signal_context["all_codes"],
    )
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        grouped[int(event["dt"])].append(event)
    selected_by_date = _select_one_per_date(events)
    selected_codes = {(dt, row["code"]) for dt, row in selected_by_date.items()}
    for event in events:
        event["selected_shadow_candidate"] = (int(event["dt"]), event["code"]) in selected_codes

    selected = [row for row in events if row.get("selected_shadow_candidate")]
    decision = _decision(checks=checks, selected_count=len(selected))
    runtime_read_only_verified = before_stat.st_mtime_ns == after_stat.st_mtime_ns and before_stat.st_size == after_stat.st_size
    if not runtime_read_only_verified:
        decision = {
            "judgment": "hold",
            "reason_type": "runtime_db_stat_changed_during_shadow_watch",
            "blockers": [*decision.get("blockers", []), "runtime_read_only_stat_check"],
            "forward_shadow_ready": False,
        }

    candidate_csv = output_dir / "live_shadow_daily_candidates.csv"
    contract_out = output_dir / "live_shadow_watch_contract.json"
    summary_path = output_dir / "live_shadow_candidate_summary.json"
    decision_path = output_dir / "live_shadow_operability_decision.json"
    complete_path = output_dir / "_ARTIFACT_COMPLETE.json"
    _write_csv(candidate_csv, [*events, *rejected_events])

    result = {
        "schema_version": "tradex_short_scene_visual_a_phase_live_shadow_watch_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "authoritative_result": True,
        "research_phase": "infrastructure_stabilization",
        "fixed_evaluation_conditions": {
            "source_table": "signal_decision_daily + daily_bars",
            "signal_dt": latest_dt,
            "side": "sell",
            "existing_pool_definition": "signal_decision_daily side='sell' and entry_qualified=true",
            "same_contract_rule": True,
            "selection_rule": "filter A phase 100MA rejection + visual pullback probe + ma20_slope_10 >= -0.005, reject one-year display high-hold short conflicts, outside existing sell pool, at most one by lower ma20_slope_10",
            "forward_outcome_available": False,
        },
        "scope": {
            "tradex_only": True,
            "meemee_ranking_changed": False,
            "meemee_ui_changed": False,
            "runtime_db_written": False,
            "silent_fallback_used": False,
            "research_fallback_used": False,
            "runtime_read_only_stat_verified": runtime_read_only_verified,
        },
        "runtime_stock_db_status": inspect_runtime_stock_db(runtime_db_path=db_path),
        "source_contract_json": str(contract_path),
        "contract_checks": checks,
        "latest_signal_context": {
            "signal_dt": latest_dt,
            "grouped_counts": signal_context["grouped_counts"],
            "sell_qualified_count": len(signal_context["sell_qualified_codes"]),
            "sell_qualified_rows": signal_context["sell_qualified_rows"],
        },
        "coverage": {
            "scanned_code_count": len(signal_context["all_codes"]),
            "bars_code_count": len(by_code),
            "outside_candidate_count": len(events),
            "anti_long_high_hold_reject_count": len(rejected_events),
            "selected_shadow_candidate_count": len(selected),
        },
        "rejected_shadow_candidates": rejected_events,
        "selected_shadow_candidates": selected,
        "observed_branching": {
            "changed_top5_members_count": len(selected),
            "changed_top10_members_count": len(selected),
            "changed_rank_count": len(selected),
            "selection_divergence_reason": "current outside-gap A phase candidate is selected only if 100MA rejection, slope floor, and anti-long-high-hold gate pass",
        },
        "authoritative_rollup_decision": decision["judgment"],
        "reason_type": decision["reason_type"],
        "blockers": decision["blockers"],
        "paper_replay_ready": decision["forward_shadow_ready"],
        "shadow_trade_candidate": len(selected) > 0,
        "meemee_reflectable": False,
        "remaining_risks": [
            "current watch has no forward outcome yet",
            "candidate is a shadow/paper intent only, not live order submission",
            "additive candidate has no champion-native score or rank",
            "OHLC visual proxy is not pixel screenshot analysis",
        ],
        "artifacts": {
            "output_dir": str(output_dir),
            "contract_json": str(contract_out),
            "candidates_csv": str(candidate_csv),
            "summary_json": str(summary_path),
            "decision_json": str(decision_path),
            "artifact_complete": str(complete_path),
        },
    }
    _write_json(contract_out, {"source_contract": contract, "contract_checks": checks, "scope": result["scope"]})
    _write_json(summary_path, result)
    _write_json(
        decision_path,
        {
            "schema_version": result["schema_version"],
            "authoritative_rollup_decision": result["authoritative_rollup_decision"],
            "reason_type": result["reason_type"],
            "blockers": result["blockers"],
            "paper_replay_ready": result["paper_replay_ready"],
            "shadow_trade_candidate": result["shadow_trade_candidate"],
            "selected_shadow_candidates": result["selected_shadow_candidates"],
            "meemee_reflectable": result["meemee_reflectable"],
            "artifacts": result["artifacts"],
        },
    )
    _write_json(complete_path, {"complete": True, **result["artifacts"]})
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--contract-path", type=Path, required=True)
    parser.add_argument("--db-path", type=Path, default=resolve_runtime_stock_db_path())
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--signal-dt", type=int, default=None)
    args = parser.parse_args()
    result = run_watch(
        contract_path=args.contract_path,
        db_path=args.db_path,
        output_root=args.output_root,
        signal_dt=args.signal_dt,
    )
    print(
        json.dumps(
            {
                "decision": result["authoritative_rollup_decision"],
                "reason_type": result["reason_type"],
                "coverage": result["coverage"],
                "selected_shadow_candidates": result["selected_shadow_candidates"],
                "artifacts": result["artifacts"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
