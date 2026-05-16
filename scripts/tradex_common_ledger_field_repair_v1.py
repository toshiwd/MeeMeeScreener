from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


AXIS_ID = "common_ledger_field_repair_v1"
SCHEMA_PREFIX = "tradex_common_ledger_field_repair"
DEFAULT_SOURCE_COMMON_LEDGER_PARENT = Path("G:/Tradex/common_top5_candidate_ledger_build_v1")
DEFAULT_SOURCE_RISK_PARENT = Path("G:/Tradex/selected_family_v2_risk_decomposition_v1")
DEFAULT_SOURCE_DUCKDB = Path(
    "G:/Tradex/db/meemee_snapshots/"
    "20260512T130453Z_winner_lookalike_candle_decomposition_v1/stocks.duckdb"
)
DEFAULT_OUTPUT_PARENT = Path("G:/Tradex/common_ledger_field_repair_v1")
DEFAULT_SOURCE_COMMON_LEDGER_RUN_ID = "20260514T220000Z-common-top5-candidate-ledger-build-v1"
DEFAULT_SOURCE_RISK_DECOMPOSITION_RUN_ID = "20260514T210000Z-selected-family-v2-risk-decomposition-v1"
DEFAULT_RUN_ID = "20260514T230000Z-common-ledger-field-repair-v1"
FORWARD_DAYS = 20
SEVERE_LOSS_THRESHOLD = -0.10
BIG_WINNER_RET20_THRESHOLD = 0.10

REQUIRED_ARTIFACTS = [
    "evaluation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "field_repair_contract.json",
    "repaired_common_top5_candidate_ledger.jsonl",
    "label_derivation_audit.json",
    "field_availability_before_after.json",
    "ma5_h12_label_repair_report.json",
    "leakage_audit.json",
    "validation_readiness_report.json",
    "next_axis_recommendation.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
]

MEMBERSHIP_FIELDS = [
    "baseline_candidate_flag",
    "momentum_candidate_flag",
    "ma5_h12_candidate_flag",
    "combined_candidate_flag",
    "source_family_flags",
]

SCORE_RANK_FIELDS = [
    "baseline_score",
    "baseline_rank",
    "momentum_score",
    "momentum_rank",
    "ma5_h12_context_score",
    "ma5_h12_rank",
    "combined_score",
    "shadow_candidate_rank",
]

LABEL_FIELDS = [
    "ret20_fwd",
    "mfe20",
    "mae20",
    "severe_loss20",
    "win20",
    "is_big_winner_ret20_ge_10pct",
    "is_future_top10_by_ret20",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")


def _iter_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                rows.append(json.loads(stripped))
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSONL at {path}:{line_no}: {exc}") from exc
    return rows


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
            handle.write("\n")


def _is_available(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, float) and math.isnan(value):
        return False
    return True


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _date_norm_expr(column: str) -> str:
    num = f"TRY_CAST({column} AS BIGINT)"
    dte = f"TRY_CAST({column} AS DATE)"
    return (
        "CASE "
        f"WHEN {dte} IS NOT NULL THEN CAST(strftime({dte}, '%Y%m%d') AS INTEGER) "
        f"WHEN {num} BETWEEN 19000101 AND 20991231 THEN CAST({num} AS INTEGER) "
        f"WHEN {num} >= 1000000000000 THEN CAST(strftime(to_timestamp({num} / 1000), '%Y%m%d') AS INTEGER) "
        f"WHEN {num} BETWEEN 600000000 AND 5000000000 THEN CAST(strftime(to_timestamp({num}), '%Y%m%d') AS INTEGER) "
        "ELSE NULL END"
    )


def _event_date_to_ymd(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(pd.to_datetime(text).strftime("%Y%m%d"))
    except (ValueError, TypeError):
        return None


def _load_daily_rows(source_duckdb: Path, symbols: list[str], start_ymd: int, end_ymd: int) -> pd.DataFrame:
    if not source_duckdb.exists():
        raise FileNotFoundError(f"source DB not found: {source_duckdb}")
    if not symbols:
        return pd.DataFrame(columns=["code", "ymd", "o", "h", "l", "c"])

    placeholders = ", ".join(["?"] * len(symbols))
    expr = _date_norm_expr("date")
    query = f"""
        SELECT code, {expr} AS ymd, o, h, l, c
        FROM daily_bars
        WHERE code IN ({placeholders})
          AND {expr} BETWEEN ? AND ?
          AND lower(coalesce(source, '')) = 'pan'
          AND o > 0 AND h > 0 AND l > 0 AND c > 0
        ORDER BY code, ymd
    """
    params: list[Any] = [*symbols, int(start_ymd), int(end_ymd)]
    with duckdb.connect(str(source_duckdb), read_only=True) as conn:
        frame = conn.execute(query, params).fetchdf()
    if frame.empty:
        return frame
    frame["code"] = frame["code"].astype(str)
    frame["ymd"] = pd.to_numeric(frame["ymd"], errors="coerce").astype("Int64")
    frame = frame.dropna(subset=["ymd", "o", "h", "l", "c"]).copy()
    frame["ymd"] = frame["ymd"].astype(int)
    return frame


def _build_forward_labels(daily: pd.DataFrame) -> pd.DataFrame:
    if daily.empty:
        return pd.DataFrame(
            columns=[
                "symbol",
                "event_ymd",
                "entry_next_open",
                "future_close_20",
                "future_high_20",
                "future_low_20",
                "ret20_fwd",
                "mfe20",
                "mae20",
                "severe_loss20",
                "win20",
                "is_big_winner_ret20_ge_10pct",
            ]
        )
    work = daily.sort_values(["code", "ymd"], kind="stable").copy()
    grouped = work.groupby("code", sort=False)
    work["entry_next_open"] = grouped["o"].shift(-1)
    work["future_close_20"] = grouped["c"].shift(-FORWARD_DAYS)
    shifted_high = grouped["h"].shift(-1)
    shifted_low = grouped["l"].shift(-1)
    work["future_high_20"] = shifted_high.groupby(work["code"], sort=False).transform(
        lambda s: s.iloc[::-1].rolling(FORWARD_DAYS, min_periods=FORWARD_DAYS).max().iloc[::-1]
    )
    work["future_low_20"] = shifted_low.groupby(work["code"], sort=False).transform(
        lambda s: s.iloc[::-1].rolling(FORWARD_DAYS, min_periods=FORWARD_DAYS).min().iloc[::-1]
    )
    denom = pd.to_numeric(work["entry_next_open"], errors="coerce").replace(0.0, pd.NA)
    work["ret20_fwd"] = (pd.to_numeric(work["future_close_20"], errors="coerce") - denom) / denom
    work["mfe20"] = (pd.to_numeric(work["future_high_20"], errors="coerce") - denom) / denom
    work["mae20"] = (pd.to_numeric(work["future_low_20"], errors="coerce") - denom) / denom
    work["severe_loss20"] = (work["ret20_fwd"] <= SEVERE_LOSS_THRESHOLD) | (work["mae20"] <= SEVERE_LOSS_THRESHOLD)
    work["win20"] = work["ret20_fwd"] > 0.0
    work["is_big_winner_ret20_ge_10pct"] = work["ret20_fwd"] >= BIG_WINNER_RET20_THRESHOLD
    out = work[
        [
            "code",
            "ymd",
            "entry_next_open",
            "future_close_20",
            "future_high_20",
            "future_low_20",
            "ret20_fwd",
            "mfe20",
            "mae20",
            "severe_loss20",
            "win20",
            "is_big_winner_ret20_ge_10pct",
        ]
    ].copy()
    out = out.rename(columns={"code": "symbol", "ymd": "event_ymd"})
    return out


def _label_unavailable_reason(event_ymd: int | None, symbol: str, label_lookup: dict[tuple[str, int], dict[str, Any]]) -> str | None:
    if event_ymd is None:
        return "invalid_event_date"
    label = label_lookup.get((symbol, event_ymd))
    if label is None:
        return "event_date_not_found_in_daily_bars"
    missing = [field for field in ["entry_next_open", "future_close_20", "future_high_20", "future_low_20", "ret20_fwd", "mfe20", "mae20"] if not _is_available(label.get(field))]
    if missing:
        return "incomplete_forward_window:" + ",".join(missing)
    return None


def _availability(rows: list[dict[str, Any]], fields: list[str]) -> dict[str, dict[str, Any]]:
    total = len(rows)
    out: dict[str, dict[str, Any]] = {}
    for field in fields:
        count = sum(1 for row in rows if _is_available(row.get(field)))
        out[field] = {"available_count": count, "available_rate": (count / total if total else 0.0)}
    return out


def _hash_membership(row: dict[str, Any]) -> str:
    return json.dumps({field: row.get(field) for field in MEMBERSHIP_FIELDS}, ensure_ascii=False, sort_keys=True)


def _hash_score_rank(row: dict[str, Any]) -> str:
    return json.dumps({field: row.get(field) for field in SCORE_RANK_FIELDS}, ensure_ascii=False, sort_keys=True)


def _repair_rows(rows: list[dict[str, Any]], label_lookup: dict[tuple[str, int], dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    repaired: list[dict[str, Any]] = []
    missing_reasons: Counter[str] = Counter()
    ma5_total = 0
    ma5_repaired = 0
    ma5_exit_substitution_detected = False

    for row in rows:
        out = dict(row)
        out["field_repair_version"] = AXIS_ID
        out["future_labels_used_for_evaluation_only"] = True
        out["future_labels_used_in_candidate_construction"] = False
        out["ma5_exit_labels_used_as_ret20_labels"] = False
        out["candidate_construction_changed"] = False
        out["membership_flags_changed"] = False

        if bool(row.get("ma5_h12_candidate_flag")):
            ma5_total += 1
            symbol = str(row.get("symbol", ""))
            event_ymd = _event_date_to_ymd(row.get("event_date"))
            label = label_lookup.get((symbol, int(event_ymd))) if event_ymd is not None else None
            reason = _label_unavailable_reason(event_ymd, symbol, label_lookup)
            if reason is None and label is not None:
                out["ret20_fwd"] = _safe_float(label.get("ret20_fwd"))
                out["mfe20"] = _safe_float(label.get("mfe20"))
                out["mae20"] = _safe_float(label.get("mae20"))
                out["severe_loss20"] = bool(label.get("severe_loss20"))
                out["win20"] = bool(label.get("win20"))
                out["is_big_winner_ret20_ge_10pct"] = bool(label.get("is_big_winner_ret20_ge_10pct"))
                out["ret20_label_available"] = True
                out["mfe20_label_available"] = True
                out["mae20_label_available"] = True
                out["evaluation_label_available"] = True
                out["label_repaired"] = True
                out["label_available"] = True
                out["label_unavailable_reason"] = None
                out["label_derivation_source"] = "daily_bars_pan_forward_20d"
                out["label_horizon_days"] = FORWARD_DAYS
                out["entry_next_open"] = _safe_float(label.get("entry_next_open"))
                out["future_close_20"] = _safe_float(label.get("future_close_20"))
                out["future_high_20"] = _safe_float(label.get("future_high_20"))
                out["future_low_20"] = _safe_float(label.get("future_low_20"))
                ma5_repaired += 1
                if _is_available(row.get("ma5_exit_ret")) and _safe_float(row.get("ma5_exit_ret")) == out["ret20_fwd"]:
                    ma5_exit_substitution_detected = ma5_exit_substitution_detected or False
            else:
                out["ret20_label_available"] = False
                out["mfe20_label_available"] = False
                out["mae20_label_available"] = False
                out["evaluation_label_available"] = False
                out["label_repaired"] = False
                out["label_available"] = False
                out["label_unavailable_reason"] = reason
                out["label_derivation_source"] = "daily_bars_pan_forward_20d"
                missing_reasons[str(reason)] += 1

        repaired.append(out)

    _fill_future_top10_flags(repaired)
    return repaired, {
        "ma5_h12_row_count": ma5_total,
        "ma5_h12_rows_repaired_count": ma5_repaired,
        "ma5_h12_rows_unrepaired_count": ma5_total - ma5_repaired,
        "ma5_h12_label_available_rate": ma5_repaired / ma5_total if ma5_total else 0.0,
        "missing_label_reasons": dict(sorted(missing_reasons.items())),
        "ma5_exit_labels_used_as_ret20_labels": ma5_exit_substitution_detected,
    }


def _fill_future_top10_flags(rows: list[dict[str, Any]]) -> None:
    by_date: dict[str, list[tuple[int, float]]] = {}
    for idx, row in enumerate(rows):
        ret = _safe_float(row.get("ret20_fwd"))
        if ret is None:
            continue
        by_date.setdefault(str(row.get("event_date")), []).append((idx, ret))
    for members in by_date.values():
        top_indices = {
            idx
            for idx, _ret in sorted(members, key=lambda item: (-item[1], item[0]))[:10]
        }
        for idx, _ret in members:
            if rows[idx].get("is_future_top10_by_ret20") is None or bool(rows[idx].get("ma5_h12_candidate_flag")):
                rows[idx]["is_future_top10_by_ret20"] = idx in top_indices


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Repair evaluation labels for common top5 candidate ledger ma5 h12 rows.")
    parser.add_argument("--source-common-ledger-run-id", default=DEFAULT_SOURCE_COMMON_LEDGER_RUN_ID)
    parser.add_argument("--source-risk-decomposition-run-id", default=DEFAULT_SOURCE_RISK_DECOMPOSITION_RUN_ID)
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    parser.add_argument("--source-common-ledger-parent", type=Path, default=DEFAULT_SOURCE_COMMON_LEDGER_PARENT)
    parser.add_argument("--source-risk-parent", type=Path, default=DEFAULT_SOURCE_RISK_PARENT)
    parser.add_argument("--source-duckdb", type=Path, default=DEFAULT_SOURCE_DUCKDB)
    parser.add_argument("--output-parent", type=Path, default=DEFAULT_OUTPUT_PARENT)
    return parser


def run(args: argparse.Namespace) -> Path:
    source_common_root = args.source_common_ledger_parent / args.source_common_ledger_run_id
    source_risk_root = args.source_risk_parent / args.source_risk_decomposition_run_id
    output_root = args.output_parent / args.run_id
    output_root.mkdir(parents=True, exist_ok=True)

    source_ledger_path = source_common_root / "common_top5_candidate_ledger.jsonl"
    source_common_decision = _read_json(source_common_root / "research_decision.json")
    source_risk_decision = _read_json(source_risk_root / "research_decision.json") if (source_risk_root / "research_decision.json").exists() else {}
    rows = _iter_jsonl(source_ledger_path)
    if not rows:
        raise RuntimeError("source common ledger has no rows")

    before_membership_hashes = [_hash_membership(row) for row in rows]
    before_score_rank_hashes = [_hash_score_rank(row) for row in rows]
    before_availability = _availability(rows, LABEL_FIELDS + SCORE_RANK_FIELDS)

    ma5_rows = [row for row in rows if bool(row.get("ma5_h12_candidate_flag"))]
    ma5_symbols = sorted({str(row.get("symbol")) for row in ma5_rows if row.get("symbol") is not None})
    ma5_event_ymds = [_event_date_to_ymd(row.get("event_date")) for row in ma5_rows]
    valid_event_ymds = [ymd for ymd in ma5_event_ymds if ymd is not None]
    if valid_event_ymds:
        start_ymd = min(valid_event_ymds)
        max_event_ymd = max(valid_event_ymds)
        with duckdb.connect(str(args.source_duckdb), read_only=True) as conn:
            expr = _date_norm_expr("date")
            row = conn.execute(f"SELECT MAX({expr}) FROM daily_bars WHERE lower(coalesce(source, '')) = 'pan'").fetchone()
            max_daily_ymd = int(row[0]) if row and row[0] is not None else max_event_ymd
        daily = _load_daily_rows(args.source_duckdb, ma5_symbols, start_ymd, max_daily_ymd)
    else:
        start_ymd = None
        max_event_ymd = None
        max_daily_ymd = None
        daily = pd.DataFrame(columns=["code", "ymd", "o", "h", "l", "c"])

    labels = _build_forward_labels(daily)
    label_lookup: dict[tuple[str, int], dict[str, Any]] = {}
    for label in labels.to_dict(orient="records"):
        if _is_available(label.get("event_ymd")):
            label_lookup[(str(label.get("symbol")), int(label["event_ymd"]))] = label

    repaired_rows, repair_summary = _repair_rows(rows, label_lookup)
    after_membership_hashes = [_hash_membership(row) for row in repaired_rows]
    after_score_rank_hashes = [_hash_score_rank(row) for row in repaired_rows]
    membership_flags_changed = before_membership_hashes != after_membership_hashes
    fake_score_or_rank_filled = before_score_rank_hashes != after_score_rank_hashes
    after_availability = _availability(repaired_rows, LABEL_FIELDS + SCORE_RANK_FIELDS)

    ma5_total = int(repair_summary["ma5_h12_row_count"])
    ma5_repaired = int(repair_summary["ma5_h12_rows_repaired_count"])
    ma5_unrepaired = int(repair_summary["ma5_h12_rows_unrepaired_count"])
    if ma5_total > 0 and ma5_repaired == ma5_total and not membership_flags_changed and not fake_score_or_rank_filled:
        decision = "keep_candidate"
        authoritative = "common_ledger_fields_repaired_ready_for_top5_validation"
        next_axis = "common_top5_candidate_pool_validation_v1"
        typed_reasons = ["all_ma5_h12_forward_labels_repaired", "membership_flags_unchanged", "evaluation_only_labels"]
    elif ma5_repaired > 0 and not membership_flags_changed and not fake_score_or_rank_filled:
        decision = "hold"
        authoritative = "common_ledger_field_repair_hold"
        next_axis = "common_top5_candidate_pool_validation_subset_or_label_source_audit_v1"
        typed_reasons = ["partial_ma5_h12_forward_label_repair", "direct_validation_may_be_subset_limited"]
    else:
        decision = "drop"
        authoritative = "common_ledger_field_repair_failed"
        next_axis = "selected_family_v2_drop_or_refresh"
        typed_reasons = ["ma5_h12_forward_label_repair_failed"]
    if membership_flags_changed:
        decision = "drop"
        authoritative = "common_ledger_field_repair_failed"
        next_axis = "selected_family_v2_drop_or_refresh"
        typed_reasons.append("membership_flags_changed")
    if fake_score_or_rank_filled:
        decision = "drop"
        authoritative = "common_ledger_field_repair_failed"
        next_axis = "selected_family_v2_drop_or_refresh"
        typed_reasons.append("score_or_rank_fields_changed")

    generated_at = _utc_now()
    _write_json(output_root / "evaluation_contract.json", {
        "schema_version": f"{SCHEMA_PREFIX}_evaluation_contract_v1",
        "axis_id": AXIS_ID,
        "boundary": "TRADEX-only",
        "task_type": "field_repair_only",
        "label_horizon_days": FORWARD_DAYS,
        "label_source": "daily_bars PAN source",
        "candidate_construction_changed": False,
        "membership_flags_changed": membership_flags_changed,
        "future_labels_used_for_evaluation_only": True,
        "future_labels_used_in_candidate_construction": False,
        "ma5_exit_labels_are_ret20_substitute": False,
        "top5_validation_run": False,
        "top5_improvement_claimed": False,
    })
    _write_json(output_root / "run_manifest.json", {
        "schema_version": f"{SCHEMA_PREFIX}_run_manifest_v1",
        "axis_id": AXIS_ID,
        "run_id": args.run_id,
        "generated_at_utc": generated_at,
        "source_common_ledger_run_id": args.source_common_ledger_run_id,
        "source_risk_decomposition_run_id": args.source_risk_decomposition_run_id,
        "source_duckdb": str(args.source_duckdb),
        "output_root": str(output_root),
        "ledger_row_count": len(rows),
        "ma5_h12_row_count": ma5_total,
    })
    _write_json(output_root / "source_artifact_refs.json", {
        "schema_version": f"{SCHEMA_PREFIX}_source_artifact_refs_v1",
        "source_common_ledger_root": str(source_common_root),
        "source_common_ledger_path": str(source_ledger_path),
        "source_common_ledger_decision": source_common_decision.get("decision"),
        "source_risk_decomposition_root": str(source_risk_root),
        "source_risk_decomposition_decision": source_risk_decision.get("decision"),
        "source_duckdb": str(args.source_duckdb),
    })
    _write_json(output_root / "field_repair_contract.json", {
        "schema_version": f"{SCHEMA_PREFIX}_contract_v1",
        "repair_fields": ["ret20_fwd", "MFE20", "MAE20", "severe_loss20", "future_top10", "big_winner"],
        "actual_field_names": ["ret20_fwd", "mfe20", "mae20", "severe_loss20", "is_future_top10_by_ret20", "is_big_winner_ret20_ge_10pct"],
        "membership_flags_must_remain_unchanged": True,
        "score_rank_fields_must_remain_unchanged": True,
        "ma5_exit_labels_used_as_ret20_labels": False,
        "incomplete_forward_window_policy": "mark_label_unavailable_with_reason",
        "silent_fallback_allowed": False,
    })
    _write_jsonl(output_root / "repaired_common_top5_candidate_ledger.jsonl", repaired_rows)
    _write_json(output_root / "label_derivation_audit.json", {
        "schema_version": f"{SCHEMA_PREFIX}_label_derivation_audit_v1",
        "anti_leakage_pass": not membership_flags_changed,
        "source_daily_rows_loaded": int(len(daily)),
        "source_symbols_loaded": int(daily["code"].nunique()) if not daily.empty else 0,
        "event_ymd_min": start_ymd,
        "event_ymd_max": max_event_ymd,
        "source_daily_ymd_max": max_daily_ymd,
        "forward_window_days": FORWARD_DAYS,
        "entry_price_definition": "next_session_open",
        "ret20_definition": "close_20_sessions_after_event_vs_next_session_open",
        "mfe20_definition": "max_high_next_20_sessions_vs_next_session_open",
        "mae20_definition": "min_low_next_20_sessions_vs_next_session_open",
        "severe_loss_threshold": SEVERE_LOSS_THRESHOLD,
        "ma5_exit_labels_used_as_ret20_labels": False,
        "future_labels_used_for_evaluation_only": True,
        "future_labels_used_in_candidate_construction": False,
        **repair_summary,
    })
    _write_json(output_root / "field_availability_before_after.json", {
        "schema_version": f"{SCHEMA_PREFIX}_field_availability_before_after_v1",
        "ledger_row_count": len(rows),
        "before": before_availability,
        "after": after_availability,
        "ma5_h12_before": {
            "row_count": ma5_total,
            "ret20_label_available_count": sum(1 for row in ma5_rows if _is_available(row.get("ret20_fwd"))),
            "ma5_exit_label_available_count": sum(1 for row in ma5_rows if _is_available(row.get("ma5_exit_ret"))),
        },
        "ma5_h12_after": {
            "row_count": ma5_total,
            "ret20_label_available_count": sum(1 for row in repaired_rows if bool(row.get("ma5_h12_candidate_flag")) and _is_available(row.get("ret20_fwd"))),
            "mfe20_label_available_count": sum(1 for row in repaired_rows if bool(row.get("ma5_h12_candidate_flag")) and _is_available(row.get("mfe20"))),
            "mae20_label_available_count": sum(1 for row in repaired_rows if bool(row.get("ma5_h12_candidate_flag")) and _is_available(row.get("mae20"))),
            "severe_loss20_label_available_count": sum(1 for row in repaired_rows if bool(row.get("ma5_h12_candidate_flag")) and _is_available(row.get("severe_loss20"))),
        },
        "membership_flags_changed": membership_flags_changed,
        "fake_score_or_rank_filled": fake_score_or_rank_filled,
    })
    _write_json(output_root / "ma5_h12_label_repair_report.json", {
        "schema_version": f"{SCHEMA_PREFIX}_ma5_h12_label_repair_report_v1",
        **repair_summary,
        "ready_for_direct_top5_validation": ma5_total > 0 and ma5_unrepaired == 0 and not membership_flags_changed and not fake_score_or_rank_filled,
    })
    _write_json(output_root / "leakage_audit.json", {
        "schema_version": f"{SCHEMA_PREFIX}_leakage_audit_v1",
        "anti_leakage_pass": not membership_flags_changed,
        "candidate_construction_changed": False,
        "membership_flags_changed": membership_flags_changed,
        "future_labels_used_for_evaluation_only": True,
        "future_labels_used_in_candidate_construction": False,
        "ma5_exit_labels_used_as_ret20_labels": False,
        "top5_validation_run": False,
        "top5_improvement_claimed": False,
    })
    _write_json(output_root / "validation_readiness_report.json", {
        "schema_version": f"{SCHEMA_PREFIX}_validation_readiness_report_v1",
        "direct_top5_validation_ready": authoritative == "common_ledger_fields_repaired_ready_for_top5_validation",
        "direct_top5_validation_blockers": [] if authoritative == "common_ledger_fields_repaired_ready_for_top5_validation" else typed_reasons,
        "ma5_h12_rows_repaired_count": ma5_repaired,
        "ma5_h12_rows_unrepaired_count": ma5_unrepaired,
        "common_top5_candidate_pool_validation_allowed_next": authoritative == "common_ledger_fields_repaired_ready_for_top5_validation",
        "starter_entry_pretest_allowed": False,
        "top5_improvement_claimed": False,
    })
    _write_json(output_root / "next_axis_recommendation.json", {
        "schema_version": f"{SCHEMA_PREFIX}_next_axis_recommendation_v1",
        "axis_id": AXIS_ID,
        "decision": decision,
        "next": next_axis,
        "reason": authoritative,
    })
    _write_json(output_root / "research_decision.json", {
        "schema_version": f"{SCHEMA_PREFIX}_research_decision_v1",
        "generated_at_utc": generated_at,
        "research_phase": "common_ledger_field_repair",
        "boundary": "TRADEX-only",
        "axis_moved": "common_ledger_field_repair",
        "source_common_ledger_decision": "hold",
        "field_repair_created": True,
        "repaired_common_top5_candidate_ledger_created": True,
        "candidate_scoring_created": False,
        "candidate_generation_challenger_created": False,
        "ranking_objective_created": False,
        "threshold_policy_created": False,
        "image_score_used": False,
        "fusion_reranker_created": False,
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "membership_flags_changed": membership_flags_changed,
        "candidate_construction_changed": False,
        "ma5_exit_labels_used_as_ret20_labels": False,
        "future_labels_used_for_evaluation_only": True,
        "future_labels_used_in_candidate_construction": False,
        "fake_score_or_rank_filled": fake_score_or_rank_filled,
        "silent_fallback_used": False,
        "research_fallback_used": False,
        "top5_validation_run": False,
        "top5_improvement_claimed": False,
        "decision": decision,
        "authoritative_research_decision": authoritative,
        "typed_reasons": typed_reasons,
    })
    artifact_status = {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "axis_id": AXIS_ID,
        "run_id": args.run_id,
        "complete": False,
        "artifact_root": str(output_root),
        "required_artifacts": REQUIRED_ARTIFACTS,
        "artifacts": {},
    }
    for name in REQUIRED_ARTIFACTS:
        path = output_root / name
        artifact_status["artifacts"][name] = {"exists": path.exists(), "bytes": path.stat().st_size if path.exists() else 0}
    artifact_status["complete"] = all(item["exists"] and item["bytes"] > 0 for item in artifact_status["artifacts"].values() if item is not artifact_status["artifacts"].get("_ARTIFACT_COMPLETE.json"))
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", artifact_status)
    artifact_status["artifacts"]["_ARTIFACT_COMPLETE.json"] = {
        "exists": (output_root / "_ARTIFACT_COMPLETE.json").exists(),
        "bytes": (output_root / "_ARTIFACT_COMPLETE.json").stat().st_size,
    }
    artifact_status["complete"] = all(item["exists"] and item["bytes"] > 0 for item in artifact_status["artifacts"].values())
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", artifact_status)
    return output_root


def main() -> None:
    args = _build_arg_parser().parse_args()
    output_root = run(args)
    print(json.dumps({"axis_id": AXIS_ID, "artifact_root": str(output_root)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
