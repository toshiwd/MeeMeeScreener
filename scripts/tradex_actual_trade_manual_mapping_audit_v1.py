from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any

import duckdb


CANDIDATE_NAME = "actual_trade_manual_mapping_audit_v1"
NORMALIZATION_ROOT = Path(
    r"G:\Tradex\actual_trade_ledger_normalization_v1\20260512T003853Z-actual_trade_ledger_normalization_v1"
)
SOURCE_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
SOURCE_CSV = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\csv\楽天証券取引履歴.csv")
OUT_BASE = Path(r"G:\Tradex") / CANDIDATE_NAME


AMBIGUOUS_AUDIT_FIELDS = [
    "source_event_id",
    "broker",
    "account_type",
    "symbol",
    "date",
    "raw_side",
    "raw_trade_type",
    "transaction_type",
    "quantity",
    "price",
    "amount",
    "current_ambiguity_reason",
    "nearby_events_same_symbol_account",
    "likely_represents",
    "mapping_audit_note",
]

QTY_AUDIT_FIELDS = [
    "symbol",
    "side",
    "account_bucket",
    "date_of_error",
    "quantity_before_event",
    "event_quantity",
    "quantity_after_event",
    "expected_direction",
    "offending_event_id",
    "likely_cause",
]

OPEN_AUDIT_FIELDS = [
    "open_trade_id",
    "symbol",
    "side",
    "entry_date",
    "entry_price",
    "open_quantity",
    "entry_event_count",
    "last_seen_date",
    "latest_source_date",
    "open_classification",
    "open_audit_note",
]


def now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, fields: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str), encoding="utf-8")


def load_events() -> list[dict[str, Any]]:
    con = duckdb.connect(str(SOURCE_DB), read_only=True)
    try:
        rows = con.execute(
            """
            SELECT
              COALESCE(CAST(id AS VARCHAR), source_row_hash) AS event_id,
              broker,
              exec_dt,
              symbol,
              action,
              qty,
              price,
              source_row_hash,
              transaction_type,
              side_type,
              margin_type
            FROM trade_events
            ORDER BY exec_dt, source_row_hash
            """
        ).fetchall()
    finally:
        con.close()
    return [
        {
            "event_id": str(r[0] or ""),
            "broker": str(r[1] or ""),
            "exec_dt": r[2],
            "date": r[2].date() if r[2] else None,
            "symbol": str(r[3] or ""),
            "action": str(r[4] or ""),
            "qty": float(r[5] or 0),
            "price": float(r[6]) if r[6] is not None else None,
            "source_row_hash": str(r[7] or ""),
            "transaction_type": r[8] or "",
            "side_type": r[9] or "",
            "margin_type": r[10] or "",
        }
        for r in rows
    ]


def classify_action(event: dict[str, Any]) -> tuple[str, str, str, str | None]:
    action = event["action"]
    margin = event.get("margin_type") or "unknown"
    if action == "SPOT_BUY":
        return "spot", "long", "entry", None
    if action == "SPOT_SELL":
        return "spot", "long", "exit", None
    if action == "MARGIN_OPEN_LONG":
        return f"margin_long:{margin}", "long", "entry", None
    if action == "MARGIN_CLOSE_LONG":
        return f"margin_long:{margin}", "long", "exit", None
    if action == "MARGIN_OPEN_SHORT":
        return f"margin_short:{margin}", "short", "entry", None
    if action == "MARGIN_CLOSE_SHORT":
        return f"margin_short:{margin}", "short", "exit", None
    if action in {"SPOT_IN", "SPOT_OUT", "DELIVERY_SHORT"}:
        return "transfer_delivery", "unknown", "transfer", "transfer_or_delivery_event"
    return "unknown", "unknown", "unknown", "unknown"


def event_by_id(events: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {e["event_id"]: e for e in events}


def nearby_events(event: dict[str, Any], events: list[dict[str, Any]], unique_dates: list[date]) -> str:
    if event.get("date") not in unique_dates:
        return ""
    idx = unique_dates.index(event["date"])
    lo = max(0, idx - 10)
    hi = min(len(unique_dates) - 1, idx + 10)
    account, side, _, _ = classify_action(event)
    same = []
    for e in events:
        e_account, e_side, e_flow, _ = classify_action(e)
        if e["symbol"] != event["symbol"]:
            continue
        if e.get("date") is None or not (unique_dates[lo] <= e["date"] <= unique_dates[hi]):
            continue
        if account not in ("unknown", "transfer_delivery") and e_account != account:
            continue
        same.append(f"{e['date']}:{e['action']}:{e['qty']}@{e['price']}:{e_flow}")
    return " | ".join(same[:30])


def likely_represents(event: dict[str, Any], reason: str) -> tuple[str, str]:
    action = event["action"]
    if reason == "missing_price":
        return "transfer/delivery", "price missing, not usable as normal priced execution"
    if action in {"SPOT_IN", "SPOT_OUT", "DELIVERY_SHORT"}:
        return "transfer/delivery", "source action is transfer or delivery-like"
    if "MARGIN_CLOSE" in action:
        return "margin close", "close event exceeded reconstructed position quantity"
    if action == "SPOT_SELL":
        return "cash trade", "spot sell exceeded reconstructed spot long quantity"
    return "true unresolved ambiguity", "no deterministic interpretation from DB fields"


def audit_ambiguous(ambiguous_rows: list[dict[str, str]], events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_id = event_by_id(events)
    unique_dates = sorted({e["date"] for e in events if e.get("date")})
    out = []
    for row in ambiguous_rows:
        event = by_id.get(row["event_id"], {})
        account, _, _, _ = classify_action(event) if event else ("unknown", "unknown", "unknown", None)
        represents, note = likely_represents(event or row, row["ambiguity_reason"])
        out.append(
            {
                "source_event_id": row["event_id"],
                "broker": row["broker"],
                "account_type": account,
                "symbol": row["symbol"],
                "date": row["exec_dt"],
                "raw_side": row.get("side_type", ""),
                "raw_trade_type": row.get("action", ""),
                "transaction_type": row.get("transaction_type", ""),
                "quantity": row.get("qty", ""),
                "price": row.get("price", ""),
                "amount": None,
                "current_ambiguity_reason": row["ambiguity_reason"],
                "nearby_events_same_symbol_account": nearby_events(event, events, unique_dates) if event else "",
                "likely_represents": represents,
                "mapping_audit_note": note,
            }
        )
    return out


def replay_quantity_errors(events: list[dict[str, Any]], ambiguous_ids: set[str]) -> list[dict[str, Any]]:
    qty_by_bucket: dict[tuple[str, str, str, str], float] = defaultdict(float)
    errors: list[dict[str, Any]] = []
    for event in events:
        account, side, flow, cause = classify_action(event)
        if flow not in {"entry", "exit"}:
            continue
        key = (event["broker"], account, event["symbol"], side)
        before = qty_by_bucket[key]
        qty = float(event["qty"] or 0)
        after = before + qty if flow == "entry" else before - qty
        if event["event_id"] in ambiguous_ids or after < -1e-9:
            likely = "unknown"
            if before <= 0 and flow == "exit":
                likely = "missing_prior_event"
            if event["action"] in {"MARGIN_CLOSE_LONG", "MARGIN_CLOSE_SHORT"} and before < qty:
                likely = "missing_prior_event"
            if event["action"] == "SPOT_SELL" and before < qty:
                likely = "initial_position_missing"
            errors.append(
                {
                    "symbol": event["symbol"],
                    "side": side,
                    "account_bucket": account,
                    "date_of_error": event["date"].isoformat() if event.get("date") else None,
                    "quantity_before_event": before,
                    "event_quantity": qty,
                    "quantity_after_event": after,
                    "expected_direction": flow,
                    "offending_event_id": event["event_id"],
                    "likely_cause": cause or likely,
                }
            )
            continue
        qty_by_bucket[key] = after
    return [r for r in errors if r["offending_event_id"] in ambiguous_ids]


def audit_open(open_rows: list[dict[str, str]], events: list[dict[str, Any]], ambiguous_symbols: set[str]) -> list[dict[str, Any]]:
    latest_date = max(e["date"] for e in events if e.get("date"))
    out = []
    for row in open_rows:
        symbol = row["symbol"]
        last_seen = datetime.fromisoformat(row["last_seen_date"]).date() if row.get("last_seen_date") else None
        open_qty = float(row.get("open_quantity") or 0)
        latest_same_symbol = max((e["date"] for e in events if e["symbol"] == symbol and e.get("date")), default=last_seen)
        if symbol in ambiguous_symbols:
            cls = "likely_mapping_error"
            note = "same symbol appears in ambiguous/conservation errors"
        elif last_seen and (latest_date - last_seen).days <= 45:
            cls = "true_currently_open_position"
            note = "last seen near latest source date"
        elif open_qty <= 1:
            cls = "small_residual_from_rounding_split_partial_fill"
            note = "open residual quantity is tiny"
        elif latest_same_symbol and last_seen and latest_same_symbol > last_seen:
            cls = "likely_missing_exit_event"
            note = "later events exist for the same symbol but this bucket remains open"
        else:
            cls = "ambiguous"
            note = "no later close evidence in current source"
        out.append(
            {
                "open_trade_id": row["open_trade_id"],
                "symbol": symbol,
                "side": row["side"],
                "entry_date": row["entry_date"],
                "entry_price": row["entry_price"],
                "open_quantity": row["open_quantity"],
                "entry_event_count": row["entry_event_count"],
                "last_seen_date": row["last_seen_date"],
                "latest_source_date": latest_date.isoformat(),
                "open_classification": cls,
                "open_audit_note": note,
            }
        )
    return out


def parse_float(row: dict[str, str], key: str) -> float | None:
    value = row.get(key)
    if value in (None, ""):
        return None
    return float(value)


def clean_subset(
    closed_rows: list[dict[str, str]],
    qty_errors: list[dict[str, Any]],
    open_audit: list[dict[str, Any]],
) -> dict[str, Any]:
    tainted_ids: set[str] = set()
    taint_reasons: Counter[str] = Counter()

    error_buckets: dict[tuple[str, str], date] = {}
    for err in qty_errors:
        if err["likely_cause"] in {"missing_prior_event", "initial_position_missing"}:
            d = datetime.fromisoformat(err["date_of_error"]).date()
            key = (err["symbol"], err["side"])
            error_buckets[key] = min(error_buckets.get(key, d), d)

    open_problem_symbols = {
        r["symbol"]
        for r in open_audit
        if r["open_classification"] in {"likely_mapping_error", "likely_missing_exit_event"}
    }

    for row in closed_rows:
        trade_id = row["normalized_trade_id"]
        entry_date = datetime.fromisoformat(row["entry_date"]).date()
        key = (row["symbol"], row["side"])
        if key in error_buckets and entry_date >= error_buckets[key]:
            tainted_ids.add(trade_id)
            taint_reasons["same_symbol_side_after_conservation_error"] += 1
        elif row["symbol"] in open_problem_symbols and entry_date >= min(error_buckets.values(), default=entry_date):
            tainted_ids.add(trade_id)
            taint_reasons["same_symbol_as_problem_open_residue"] += 1

    gross_total = sum(parse_float(r, "gross_pnl") or 0.0 for r in closed_rows)
    clean_rows = [r for r in closed_rows if r["normalized_trade_id"] not in tainted_ids]
    clean_gross_total = sum(parse_float(r, "gross_pnl") or 0.0 for r in clean_rows)
    gross_delta = clean_gross_total - gross_total
    material_change = abs(gross_delta) > max(100000.0, abs(gross_total) * 0.1)

    can_proceed = len(clean_rows) >= 900 and len(tainted_ids) / max(len(closed_rows), 1) < 0.1
    decision = "ready_for_context_reconstruction_subset" if can_proceed and not material_change else "needs_normalizer_mapping_fix"
    reason = (
        "ambiguous and open residues are localized enough to exclude a clean closed-trade subset"
        if decision == "ready_for_context_reconstruction_subset"
        else "taint or PnL impact is too broad for subset progression"
    )
    return {
        "candidate_name": CANDIDATE_NAME,
        "decision": decision,
        "reason": reason,
        "closed_trade_count_total": len(closed_rows),
        "closed_trade_count_clean": len(clean_rows),
        "closed_trade_count_tainted": len(tainted_ids),
        "tainted_trade_ids": sorted(tainted_ids),
        "taint_reason_counts": dict(taint_reasons),
        "can_proceed_with_clean_subset": decision == "ready_for_context_reconstruction_subset",
        "excluded_event_count": len(qty_errors),
        "excluded_trade_count": len(tainted_ids),
        "gross_pnl_total_original": gross_total,
        "gross_pnl_total_clean_subset": clean_gross_total,
        "gross_pnl_total_delta_after_excluding_tainted": gross_delta,
        "gross_pnl_summary_changes_materially": material_change,
        "context_features_computed": False,
    }


def main() -> None:
    stamp = now_stamp()
    run_root = OUT_BASE / f"{stamp}-{CANDIDATE_NAME}"
    run_root.mkdir(parents=True, exist_ok=True)

    ambiguous_rows = read_csv(NORMALIZATION_ROOT / "ambiguous_trade_events.csv")
    open_rows = read_csv(NORMALIZATION_ROOT / "open_trade_ledger.csv")
    closed_rows = read_csv(NORMALIZATION_ROOT / "normalized_trade_ledger.csv")
    summary = json.loads((NORMALIZATION_ROOT / "trade_normalization_summary.json").read_text(encoding="utf-8"))
    validation = json.loads((NORMALIZATION_ROOT / "trade_normalization_validation.json").read_text(encoding="utf-8"))
    events = load_events()

    ambiguous_audit = audit_ambiguous(ambiguous_rows, events)
    ambiguous_ids = {r["event_id"] for r in ambiguous_rows}
    qty_audit = replay_quantity_errors(events, ambiguous_ids)
    ambiguous_symbols = {r["symbol"] for r in ambiguous_rows}
    open_audit = audit_open(open_rows, events, ambiguous_symbols)
    clean_decision = clean_subset(closed_rows, qty_audit, open_audit)

    ambiguous_counts = Counter(r["likely_represents"] for r in ambiguous_audit)
    qty_cause_counts = Counter(r["likely_cause"] for r in qty_audit)
    open_counts = Counter(r["open_classification"] for r in open_audit)

    broad_mapping_error = qty_cause_counts.get("margin_close_mapping_error", 0) > 0
    recommended_action = (
        "proceed_with_clean_subset"
        if clean_decision["decision"] == "ready_for_context_reconstruction_subset"
        else "add_tradex_normalizer_mapping_review"
    )
    mapping_recommendations = {
        "candidate_name": CANDIDATE_NAME,
        "recommended_action": recommended_action,
        "affected_event_count": len(ambiguous_rows),
        "affected_trade_count": clean_decision["closed_trade_count_tainted"],
        "mapping_rules_to_add": [],
        "broker_csv_fields_needed": ["建約定日", "建単価", "信用区分", "取引区分"] if clean_decision["decision"] != "ready_for_context_reconstruction_subset" else [],
        "whether_MeeMee_import_code_should_remain_untouched": True,
        "whether_TRADEX_only_normalizer_can_handle_the_mapping_independently": True,
        "notes": [
            "Do not change MeeMee import behavior in this task.",
            "If future repair is needed, implement it as TRADEX-only normalizer mapping or broker CSV provenance join.",
        ],
    }

    manual_mapping_audit = {
        "candidate_name": CANDIDATE_NAME,
        "created_at_utc": stamp,
        "normalization_root": str(NORMALIZATION_ROOT),
        "source_db": str(SOURCE_DB),
        "source_csv": str(SOURCE_CSV),
        "normalization_summary_decision": summary.get("decision"),
        "normalization_validation_decision": validation.get("decision"),
        "ambiguous_event_count": len(ambiguous_rows),
        "quantity_conservation_error_count": len(qty_audit),
        "open_trade_count": len(open_rows),
        "ambiguous_likely_represents_counts": dict(ambiguous_counts),
        "quantity_conservation_cause_counts": dict(qty_cause_counts),
        "open_trade_classification_counts": dict(open_counts),
        "broad_mapping_error_detected": broad_mapping_error,
        "clean_subset_decision": clean_decision["decision"],
        "context_features_computed": False,
        "non_scope_confirmation": {
            "meemee_changed": False,
            "meemee_trade_import_changed": False,
            "live_ranking_changed": False,
            "champion_scoring_changed": False,
            "publish_promotion_changed": False,
            "chart_features_computed": False,
            "setup_analysis_run": False,
            "counterfactual_rules_run": False,
            "broker_csv_rewritten": False,
        },
    }

    complete = {
        "candidate_name": CANDIDATE_NAME,
        "created_at_utc": stamp,
        "run_root": str(run_root),
        "decision": clean_decision["decision"],
        "complete": True,
        "required_artifacts": [
            "manual_mapping_audit.json",
            "ambiguous_event_audit.csv",
            "quantity_conservation_error_audit.csv",
            "open_trade_audit.csv",
            "clean_subset_decision.json",
            "mapping_recommendations.json",
        ],
    }

    write_json(run_root / "manual_mapping_audit.json", manual_mapping_audit)
    write_csv(run_root / "ambiguous_event_audit.csv", AMBIGUOUS_AUDIT_FIELDS, ambiguous_audit)
    write_csv(run_root / "quantity_conservation_error_audit.csv", QTY_AUDIT_FIELDS, qty_audit)
    write_csv(run_root / "open_trade_audit.csv", OPEN_AUDIT_FIELDS, open_audit)
    write_json(run_root / "clean_subset_decision.json", clean_decision)
    write_json(run_root / "mapping_recommendations.json", mapping_recommendations)
    write_json(run_root / "_ARTIFACT_COMPLETE.json", complete)

    print(json.dumps({"run_root": str(run_root), "decision": clean_decision["decision"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
