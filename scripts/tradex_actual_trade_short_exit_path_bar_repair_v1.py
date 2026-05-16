from __future__ import annotations

import csv
import json
import re
from collections import Counter, defaultdict
from datetime import date, datetime, time, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any

import duckdb


CANDIDATE_NAME = "actual_trade_short_exit_path_bar_repair_v1"
GAP_AUDIT_ROOT = Path(r"G:\Tradex\actual_trade_short_exit_path_bar_gap_audit_v1\20260512T020028Z-actual_trade_short_exit_path_bar_gap_audit_v1")
FEASIBILITY_ROOT = Path(r"G:\Tradex\actual_trade_short_exit_feasibility_v1\20260512T015542Z-actual_trade_short_holding_duration_exit_feasibility_v1")
COUNTERFACTUAL_ROOT = Path(r"G:\Tradex\actual_trade_counterfactual_rule_audit_v1\20260512T013658Z-actual_trade_short_ma20_regime_filter_v1")
SOURCE_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
TXT_DIR = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\txt")
OUT_BASE = Path(r"G:\Tradex\actual_trade_short_exit_path_bar_repair_v1")


def now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    if fields is None:
        fields = []
        for row in rows:
            for key in row:
                if key not in fields:
                    fields.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def parse_d(value: str) -> date:
    return datetime.fromisoformat(value).date()


def date_to_epoch(d: date) -> int:
    return int(datetime.combine(d, time.min, tzinfo=timezone.utc).timestamp())


def epoch_to_date(value: int) -> date:
    return datetime.fromtimestamp(int(value), tz=timezone.utc).date()


def fnum(row: dict[str, Any], key: str) -> float | None:
    value = row.get(key)
    if value in (None, ""):
        return None
    return float(value)


def symbol_variants(symbol: str) -> list[str]:
    raw = str(symbol).strip()
    variants = [raw]
    for suffix in (".T", "-T", " JP", ".JP"):
        if raw.endswith(suffix):
            variants.append(raw[: -len(suffix)])
    digits = re.sub(r"\D", "", raw)
    if digits:
        variants.extend([digits, digits.zfill(4)])
        if len(digits) > 4:
            variants.append(digits[:4])
    out: list[str] = []
    for v in variants:
        if v and v not in out:
            out.append(v)
    return out


def load_db_index(symbols: set[str]) -> dict[str, Any]:
    candidates: set[str] = set()
    for symbol in symbols:
        candidates.update(symbol_variants(symbol))
    con = duckdb.connect(str(SOURCE_DB), read_only=True)
    try:
        tables = {r[0] for r in con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()}
        code_ranges: dict[str, dict[str, Any]] = {}
        if candidates:
            placeholders = ",".join(["?"] * len(candidates))
            rows = con.execute(
                f"""
                SELECT code, COUNT(*) AS row_count, MIN(date) AS first_date, MAX(date) AS last_date
                FROM daily_bars
                WHERE source = 'pan' AND code IN ({placeholders})
                GROUP BY code
                """,
                sorted(candidates),
            ).fetchall()
            for code, row_count, first_date, last_date in rows:
                code_ranges[str(code)] = {
                    "row_count": int(row_count),
                    "first_date": epoch_to_date(int(first_date)).isoformat() if first_date is not None else None,
                    "last_date": epoch_to_date(int(last_date)).isoformat() if last_date is not None else None,
                }
        master_hits: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
        for table, cols in {
            "tickers": ["code", "name"],
            "industry_master": ["code", "name", "market_code", "sector33_name"],
            "stock_meta": ["code"],
            "taisyaku_issue_master": ["code"],
        }.items():
            if table not in tables:
                continue
            desc = con.execute(f"DESCRIBE {table}").fetchall()
            available_cols = [r[0] for r in desc]
            selected = [c for c in cols if c in available_cols]
            if "code" not in selected:
                continue
            placeholders = ",".join(["?"] * len(candidates))
            rows = con.execute(
                f"SELECT {', '.join(selected)} FROM {table} WHERE code IN ({placeholders})",
                sorted(candidates),
            ).fetchall()
            for row in rows:
                payload = {selected[i]: row[i] for i in range(len(selected))}
                master_hits[str(payload["code"])][table].append(payload)
    finally:
        con.close()
    return {"code_ranges": code_ranges, "master_hits": master_hits}


def find_txt_for_symbol(symbol: str) -> tuple[Path | None, str | None]:
    if not TXT_DIR.exists():
        return None, "txt_dir_missing"
    for variant in symbol_variants(symbol):
        matches = sorted(TXT_DIR.glob(f"{variant}_*.txt"))
        if matches:
            return matches[0], None
    return None, "pan_txt_not_found"


def parse_txt_range(path: Path) -> dict[str, Any]:
    rows = 0
    min_d: date | None = None
    max_d: date | None = None
    loadable = True
    error = None
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) < 7:
                    continue
                try:
                    d = parse_d(row[1])
                except Exception:
                    continue
                rows += 1
                min_d = d if min_d is None else min(min_d, d)
                max_d = d if max_d is None else max(max_d, d)
    except Exception as exc:  # pragma: no cover - diagnostic artifact should carry failure.
        loadable = False
        error = str(exc)
    return {
        "row_count": rows,
        "pan_txt_date_min": min_d.isoformat() if min_d else None,
        "pan_txt_date_max": max_d.isoformat() if max_d else None,
        "pan_txt_loadable": loadable and rows > 0,
        "pan_txt_parse_error": error,
    }


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "trade_count": 0,
            "gross_pnl_total": 0.0,
            "gross_return_mean": None,
            "gross_return_median": None,
            "win_rate_gross": None,
            "avg_holding_days": None,
            "median_holding_days": None,
            "symbol_count": 0,
            "large_loss_count": 0,
            "large_win_count": 0,
        }
    pnls = [float(r.get("gross_pnl") or r.get("gross_pnl_actual") or 0.0) for r in rows]
    rets = [float(r.get("gross_return_pct") or 0.0) for r in rows if r.get("gross_return_pct") not in (None, "")]
    holding = [float(r.get("holding_days") or r.get("holding_days_actual") or 0.0) for r in rows]
    return {
        "trade_count": len(rows),
        "gross_pnl_total": sum(pnls),
        "gross_return_mean": mean(rets) if rets else None,
        "gross_return_median": median(rets) if rets else None,
        "win_rate_gross": sum(1 for p in pnls if p > 0) / len(pnls),
        "avg_holding_days": mean(holding) if holding else None,
        "median_holding_days": median(holding) if holding else None,
        "symbol_count": len({str(r.get("symbol")) for r in rows}),
        "large_loss_count": sum(1 for p in pnls if p <= -50000),
        "large_win_count": sum(1 for p in pnls if p >= 50000),
    }


def distribution(rows: list[dict[str, Any]], field: str) -> dict[str, int]:
    out: Counter[str] = Counter()
    for row in rows:
        value = row.get(field)
        if value in (None, ""):
            value = "unknown"
        out[str(value)] += 1
    return dict(sorted(out.items()))


def month_from_row(row: dict[str, Any]) -> str:
    return str(row.get("entry_date", ""))[:7]


def year_from_row(row: dict[str, Any]) -> str:
    return str(row.get("entry_date", ""))[:4]


def path_available_for_trade(trade: dict[str, Any], code_ranges: dict[str, dict[str, Any]]) -> bool:
    symbol = str(trade.get("symbol", "")).strip()
    entry_date = parse_d(str(trade["entry_date"]))
    exit_date = parse_d(str(trade.get("exit_date") or trade.get("actual_exit_date")))
    for variant in symbol_variants(symbol):
        span = code_ranges.get(variant)
        if not span:
            continue
        first = parse_d(span["first_date"])
        last = parse_d(span["last_date"])
        if first <= entry_date and last >= exit_date:
            return True
    return False


def main() -> None:
    stamp = now_stamp()
    run_root = OUT_BASE / f"{stamp}-{CANDIDATE_NAME}"
    run_root.mkdir(parents=True, exist_ok=True)

    missing_audit = read_csv(GAP_AUDIT_ROOT / "missing_path_trade_audit.csv")
    prior_missing = read_csv(FEASIBILITY_ROOT / "short_trade_path_missing.csv")
    kept = read_csv(COUNTERFACTUAL_ROOT / "kept_trades.csv")
    kept_shorts = [
        r for r in kept
        if r.get("side") == "short"
        and r.get("counterfactual_action") == "keep"
        and r.get("tainted_excluded_flag", "").lower() == "false"
    ]
    bias_check = json.loads((GAP_AUDIT_ROOT / "available_vs_missing_path_bias_check.json").read_text(encoding="utf-8"))
    prior_coverage = json.loads((FEASIBILITY_ROOT / "short_trade_path_coverage.json").read_text(encoding="utf-8"))
    prior_family = json.loads((FEASIBILITY_ROOT / "exit_rule_family_feasibility.json").read_text(encoding="utf-8"))

    missing_ids = {r["normalized_trade_id"] for r in missing_audit}
    available_rows = [r for r in kept_shorts if r["normalized_trade_id"] not in missing_ids]
    missing_rows_by_id = {r["normalized_trade_id"]: r for r in missing_audit}
    kept_by_id = {r["normalized_trade_id"]: r for r in kept_shorts}

    symbols = {r["symbol"] for r in missing_audit}
    db_index = load_db_index(symbols | {r["symbol"] for r in kept_shorts})
    code_ranges: dict[str, dict[str, Any]] = db_index["code_ranges"]
    master_hits = db_index["master_hits"]

    attempts: list[dict[str, Any]] = []
    repaired_ids: list[str] = []
    still_missing_ids: list[str] = []
    symbol_mapping: dict[str, Any] = {}
    repair_failure_counts: Counter[str] = Counter()
    repair_method_counts: Counter[str] = Counter()

    for row in missing_audit:
        trade_id = row["normalized_trade_id"]
        symbol = str(row["symbol"]).strip()
        trade = kept_by_id.get(trade_id, {})
        entry_date = parse_d(row["entry_date"])
        exit_date = parse_d(row["actual_exit_date"])
        variants = symbol_variants(symbol)
        exact_or_variant = next((v for v in variants if v in code_ranges), None)
        txt_path, txt_missing_reason = find_txt_for_symbol(symbol)
        txt_meta: dict[str, Any] = {}
        if txt_path:
            txt_meta = parse_txt_range(txt_path)
        master_payload = {v: master_hits.get(v, {}) for v in variants if master_hits.get(v)}

        repair_attempted = True
        repair_success = False
        repair_method = None
        repaired_symbol = None
        repaired_source = None
        first_repaired = None
        last_repaired = None
        path_rows_after = 0
        failure_reason = None

        if exact_or_variant:
            span = code_ranges[exact_or_variant]
            first_repaired = span["first_date"]
            last_repaired = span["last_date"]
            if parse_d(first_repaired) <= entry_date and parse_d(last_repaired) >= exit_date:
                repair_success = True
                repair_method = "symbol_normalization"
                repaired_symbol = exact_or_variant
                repaired_source = "daily_bars.pan"
                path_rows_after = int(span["row_count"])
            else:
                failure_reason = "daily_bars_span_does_not_cover_entry_exit"
                repair_method = "symbol_normalization_checked"
        elif txt_path and txt_meta.get("pan_txt_loadable"):
            first_repaired = txt_meta.get("pan_txt_date_min")
            last_repaired = txt_meta.get("pan_txt_date_max")
            if first_repaired and last_repaired and parse_d(first_repaired) <= entry_date and parse_d(last_repaired) >= exit_date:
                repair_success = True
                repair_method = "pan_txt_temp_repair_candidate"
                repaired_symbol = symbol
                repaired_source = "pan_txt"
                path_rows_after = int(txt_meta.get("row_count") or 0)
            else:
                failure_reason = "pan_txt_span_does_not_cover_entry_exit"
                repair_method = "pan_txt_checked"
        elif master_payload:
            failure_reason = "master_code_exists_but_no_pan_bars_or_txt"
            repair_method = "master_checked"
        else:
            failure_reason = txt_missing_reason or "no_deterministic_repair_source_found"
            repair_method = "pan_txt_and_db_checked"

        if repair_success:
            repaired_ids.append(trade_id)
        else:
            still_missing_ids.append(trade_id)
            repair_failure_counts[failure_reason or "unknown"] += 1
        repair_method_counts[repair_method or "none"] += 1

        symbol_mapping[trade_id] = {
            "original_symbol": symbol,
            "symbol_variants_checked": variants,
            "repaired_symbol": repaired_symbol,
            "repair_method": repair_method,
            "repair_success": repair_success,
            "mapping_source": "deterministic_symbol_variant_or_pan_txt" if repair_success else None,
            "mapping_confidence": "high" if repair_success else "none",
        }

        attempts.append(
            {
                "normalized_trade_id": trade_id,
                "symbol": symbol,
                "entry_date": row["entry_date"],
                "actual_exit_date": row["actual_exit_date"],
                "gross_pnl": row.get("gross_pnl"),
                "holding_days_actual": row.get("holding_days_actual"),
                "current_missing_reason": row.get("missing_reason_current"),
                "any_daily_bars_exist_before_repair": row.get("any_daily_bars_exist"),
                "repair_attempted": repair_attempted,
                "repair_method": repair_method,
                "repair_success": repair_success,
                "repaired_symbol": repaired_symbol,
                "repaired_bar_source": repaired_source,
                "first_repaired_bar_date": first_repaired,
                "last_repaired_bar_date": last_repaired,
                "entry_to_exit_path_available_after_repair": repair_success,
                "path_row_count_after_repair": path_rows_after if repair_success else 0,
                "repair_failure_reason": failure_reason,
                "symbol_normalization_candidate": exact_or_variant,
                "symbol_normalization_success": repair_success and repair_method == "symbol_normalization",
                "mapped_symbol_candidate": None,
                "mapping_source": None,
                "mapping_confidence": "none",
                "pan_txt_found": bool(txt_path),
                "pan_txt_path": str(txt_path) if txt_path else None,
                "pan_txt_date_min": txt_meta.get("pan_txt_date_min"),
                "pan_txt_date_max": txt_meta.get("pan_txt_date_max"),
                "pan_txt_loadable": txt_meta.get("pan_txt_loadable", False),
                "pan_txt_repair_candidate": repair_method == "pan_txt_temp_repair_candidate",
                "instrument_category": "unknown_non_repairable" if not repair_success else None,
                "master_hit_tables": ",".join(sorted({table for hit in master_payload.values() for table in hit.keys()})),
                "account_type": trade.get("account_type"),
                "broker": trade.get("broker"),
            }
        )

    original_available = int(prior_coverage.get("path_available_trade_count", 320))
    repaired_count = len(repaired_ids)
    still_missing_count = len(still_missing_ids)
    new_available = original_available + repaired_count
    kept_count = len(kept_shorts)
    new_rate = new_available / kept_count if kept_count else 0.0

    repaired_coverage = {
        "original_path_available_count": original_available,
        "original_missing_count": int(prior_coverage.get("path_missing_trade_count", 71)),
        "repaired_path_count": repaired_count,
        "still_missing_count": still_missing_count,
        "new_path_available_count": new_available,
        "new_path_coverage_rate": new_rate,
        "newly_repaired_trade_ids": repaired_ids,
        "still_missing_trade_ids": still_missing_ids,
        "still_missing_reason_counts": dict(repair_failure_counts),
        "repair_method_counts": dict(repair_method_counts),
    }

    available_after_rows = [r for r in kept_shorts if r["normalized_trade_id"] not in set(still_missing_ids)]
    still_missing_rows = [kept_by_id[i] for i in still_missing_ids if i in kept_by_id]
    bias_summary = {
        "available_after_repair_group": summarize(available_after_rows),
        "still_missing_group": summarize(still_missing_rows),
        "year_distribution_available": distribution(
            [{**r, "entry_year": year_from_row(r)} for r in available_after_rows], "entry_year"
        ),
        "year_distribution_missing": distribution(
            [{**r, "entry_year": year_from_row(r)} for r in still_missing_rows], "entry_year"
        ),
        "month_distribution_available": distribution(
            [{**r, "entry_month": month_from_row(r)} for r in available_after_rows], "entry_month"
        ),
        "month_distribution_missing": distribution(
            [{**r, "entry_month": month_from_row(r)} for r in still_missing_rows], "entry_month"
        ),
        "prior_bias_classification": bias_check.get("bias_classification"),
        "bias_classification_after_repair": bias_check.get("bias_classification"),
    }

    allowed_subset_families = [
        name for name, payload in prior_family.items()
        if payload.get("feasible_on_available_path_subset") is True
    ]
    full_set_feasible = new_rate >= 0.95 and still_missing_count <= 5
    subset_contract_approved = (not full_set_feasible) and new_available >= 300 and bias_check.get("bias_classification") in {"missing_paths_low_bias", "missing_paths_moderate_bias"}
    if full_set_feasible:
        decision = "ready_for_full_exit_rule_replay"
        reason = "path coverage after deterministic repair is high enough for full-set replay"
    elif repaired_count > 0 and still_missing_count > 0:
        decision = "ready_for_available_subset_exit_rule_replay" if subset_contract_approved else "needs_manual_symbol_mapping"
        reason = "partial repair completed; remaining paths require explicit subset contract or manual review"
    elif subset_contract_approved:
        decision = "ready_for_available_subset_exit_rule_replay"
        reason = "deterministic repair found no confirmed missing bars, but prior bias audit supports a stable available-path subset"
    elif repair_failure_counts.get("master_code_exists_but_no_pan_bars_or_txt", 0) > 0:
        decision = "needs_pan_history_load"
        reason = "master codes exist but confirmed PAN history is missing"
    else:
        decision = "needs_manual_symbol_mapping"
        reason = "no deterministic PAN/TXT or symbol-variant repair source was found for missing symbols"

    family_after = {}
    for name, payload in prior_family.items():
        family_after[name] = {
            "full_set_after_repair_feasible": bool(full_set_feasible and payload.get("feasible_on_available_path_subset")),
            "available_subset_feasible": bool(payload.get("feasible_on_available_path_subset") and subset_contract_approved),
            "required_fields_available_on_prior_available_subset": payload.get("required_fields_available"),
            "known_limitation": payload.get("known_limitation"),
        }

    subset_contract = {
        "approved_for_subset_replay": bool(subset_contract_approved),
        "included_trade_count": new_available if subset_contract_approved else 0,
        "excluded_missing_trade_count": still_missing_count if subset_contract_approved else len(still_missing_ids),
        "excluded_trade_ids": still_missing_ids,
        "excluded_reason_counts": dict(repair_failure_counts),
        "available_vs_missing_bias_summary": bias_summary,
        "gross_pnl_available_group": bias_summary["available_after_repair_group"]["gross_pnl_total"],
        "gross_pnl_excluded_group": bias_summary["still_missing_group"]["gross_pnl_total"],
        "caveats": [
            "subset result must not be called a full-ledger result",
            "missing trades remain excluded by frozen trade id list",
            "no Yahoo or provisional bars may be used as fallback",
        ],
        "allowed_future_replay_families": allowed_subset_families,
        "prohibition_against_claiming_full_ledger_result": True,
    }

    repair_payload = {
        "candidate_name": CANDIDATE_NAME,
        "decision": decision,
        "reason": reason,
        "source_gap_audit_root": str(GAP_AUDIT_ROOT),
        "source_feasibility_root": str(FEASIBILITY_ROOT),
        "source_counterfactual_root": str(COUNTERFACTUAL_ROOT),
        "repair_summary": repaired_coverage,
        "repair_failure_counts": dict(repair_failure_counts),
        "repair_method_counts": dict(repair_method_counts),
        "available_subset_contract_path": str(run_root / "available_path_subset_contract.json"),
        "boundary_no_lookahead_check": {
            "exit_rule_tested": False,
            "post_entry_rule_selected": False,
            "yahoo_or_provisional_fallback_used": False,
            "tainted_trades_included": False,
            "long_trades_included": False,
            "repair_data_confirmed_or_provenance_tracked": True,
            "production_daily_bars_mutated": False,
        },
    }

    write_json(run_root / "short_exit_path_bar_repair.json", repair_payload)
    write_csv(run_root / "missing_path_repair_attempts.csv", attempts)
    write_json(run_root / "repaired_path_coverage.json", repaired_coverage)
    write_json(run_root / "repaired_exit_rule_family_feasibility.json", family_after)
    write_json(run_root / "repaired_symbol_mapping.json", symbol_mapping)
    write_csv(run_root / "unrepaired_missing_paths.csv", [a for a in attempts if not a["repair_success"]])
    write_json(run_root / "available_path_subset_contract.json", subset_contract)
    complete = {
        "candidate_name": CANDIDATE_NAME,
        "created_at_utc": stamp,
        "run_root": str(run_root),
        "decision": decision,
        "complete": True,
        "required_artifacts": [
            "short_exit_path_bar_repair.json",
            "missing_path_repair_attempts.csv",
            "repaired_path_coverage.json",
            "repaired_exit_rule_family_feasibility.json",
            "repaired_symbol_mapping.json",
            "unrepaired_missing_paths.csv",
            "available_path_subset_contract.json",
        ],
    }
    write_json(run_root / "_ARTIFACT_COMPLETE.json", complete)
    print(json.dumps({"run_root": str(run_root), "decision": decision, "repaired_path_count": repaired_count, "still_missing_count": still_missing_count}, ensure_ascii=False))


if __name__ == "__main__":
    main()
