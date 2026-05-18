from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import duckdb

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.services.codex_bridge_service import get_rankings_freshness, get_runtime_stock_db_status
from app.backend.services.data.taisyaku_import import load_taisyaku_snapshot


SCHEMA_PREFIX = "tradex_entry_precision_short_bottom_risk_borrow_decomposition_v1"
VARIANT_ID = "short_cleanup_bottom_risk_v1"
DEFAULT_SOURCE_ROOT = Path(
    r"G:\Tradex\entry_precision_short_bottom_risk_stability_replay_v1"
    r"\20260517T041737Z-entry-short-bottom-risk-stability-replay-v1"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\entry_precision_short_bottom_risk_borrow_decomposition_v1")
MAX_HARD_BORROW_GAP_SHARE = 0.10
MAX_HARD_BORROW_GAP_CODE_SHARE = 0.20
MIN_CLEAN_BORROWABLE_EVENTS = 3

REQUIRED_OUTPUTS = [
    "short_bottom_risk_borrow_decomposition_contract.json",
    "short_bottom_risk_borrow_bucket_events.csv",
    "short_bottom_risk_borrow_bucket_summary.json",
    "short_bottom_risk_soft_cost_concentration.json",
    "short_bottom_risk_borrow_adjusted_compare.json",
    "short_bottom_risk_borrow_decomposition_decision.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, set):
        return [_json_ready(item) for item in sorted(value, key=str)]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _csv_ready(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "True" if value else "False"
    if isinstance(value, (dict, list, tuple, set)):
        return json.dumps(_json_ready(value), ensure_ascii=False, sort_keys=True)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, float) and not math.isfinite(value):
        return ""
    return value


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )


def _write_csv(path: Path, rows: Iterable[Mapping[str, Any]], *, columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: _csv_ready(row.get(column)) for column in columns})


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"missing required source artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _load_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"missing required source artifact: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _safe_int(value: Any) -> int | None:
    if value in (None, "", "None"):
        return None
    try:
        return int(str(value).strip())
    except Exception:
        return None


def _safe_float(value: Any) -> float | None:
    if value in (None, "", "None"):
        return None
    try:
        parsed = float(str(value).strip())
    except Exception:
        return None
    return parsed if math.isfinite(parsed) else None


def _ymd_to_iso(value: int | None) -> str | None:
    if value is None:
        return None
    try:
        text = str(int(value))
        return datetime(int(text[:4]), int(text[4:6]), int(text[6:8])).date().isoformat()
    except Exception:
        return None


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _load_input_context(source_root: Path) -> dict[str, Any]:
    contract = _load_json(source_root / "short_bottom_risk_stability_replay_contract.json")
    stability_decision = _load_json(source_root / "short_bottom_risk_stability_replay_decision.json")
    borrow_report = _load_json(source_root / "short_bottom_risk_borrow_proxy_report.json")
    no_lookahead = _load_json(source_root / "no_lookahead_audit.json")

    compare_root = Path(str(contract["source_root"]))
    diagnostic_root = Path(str(contract["source_diagnostic_root"]))
    full_recheck_compare = _load_json(compare_root / "short_bottom_risk_full_recheck_compare.json")
    full_recheck_decision = _load_json(compare_root / "short_bottom_risk_full_recheck_decision.json")
    confusion_rows = _load_csv_rows(diagnostic_root / "short_bottom_risk_confusion_groups.csv")

    return {
        "source_root": source_root,
        "contract": contract,
        "stability_decision": stability_decision,
        "borrow_report": borrow_report,
        "no_lookahead": no_lookahead,
        "compare_root": compare_root,
        "diagnostic_root": diagnostic_root,
        "full_recheck_compare": full_recheck_compare,
        "full_recheck_decision": full_recheck_decision,
        "confusion_rows": confusion_rows,
    }


def _load_runtime_context() -> dict[str, Any]:
    runtime_status = get_runtime_stock_db_status()
    rankings_freshness_short = get_rankings_freshness(
        tf="D",
        which="latest",
        direction="short",
        mode="trade",
        risk_mode="balanced",
        limit=20,
    )
    return {
        "runtime_status": runtime_status,
        "rankings_freshness_short": rankings_freshness_short,
        "runtime_db_path": Path(str(runtime_status["selected_runtime_db_path"])),
    }


def _load_sector_lookup(runtime_db_path: Path, codes: Iterable[str]) -> dict[str, dict[str, Any]]:
    code_list = sorted({str(code).strip() for code in codes if str(code).strip()})
    if not code_list:
        return {}
    if not runtime_db_path.exists():
        raise FileNotFoundError(f"runtime stock db not found: {runtime_db_path}")
    placeholders = ",".join(["?"] * len(code_list))
    conn = duckdb.connect(str(runtime_db_path), read_only=True)
    try:
        rows = conn.execute(
            f"""
            SELECT CAST(code AS VARCHAR) AS code, name, sector33_code, sector33_name, market_code
            FROM industry_master
            WHERE CAST(code AS VARCHAR) IN ({placeholders})
            """,
            code_list,
        ).fetchall()
    finally:
        conn.close()
    lookup: dict[str, dict[str, Any]] = {}
    for code, name, sector33_code, sector33_name, market_code in rows:
        key = _normalize_text(code)
        if not key:
            continue
        lookup[key] = {
            "code": key,
            "name": _normalize_text(name) or None,
            "sector33_code": _normalize_text(sector33_code) or None,
            "sector33_name": _normalize_text(sector33_name) or None,
            "market_code": _normalize_text(market_code) or None,
        }
    return lookup


def _borrow_proxy_for_code(code: str, *, runtime_db_path: Path) -> dict[str, Any]:
    try:
        snapshot = load_taisyaku_snapshot(code, db_path=runtime_db_path, history_limit=3)
    except Exception as exc:
        return {
            "code": code,
            "available": False,
            "hard_gap_reason": f"snapshot_error:{type(exc).__name__}",
            "soft_cost_reasons": [],
            "restriction_count": None,
            "current_fee_yen": None,
            "loan_ratio": None,
            "shortable_proxy_ok": False,
        }

    latest_balance = snapshot.get("latestBalance") if isinstance(snapshot, dict) else None
    latest_fee = snapshot.get("latestFee") if isinstance(snapshot, dict) else None
    restrictions = list(snapshot.get("restrictions") or []) if isinstance(snapshot, dict) else []
    has_snapshot = snapshot is not None
    current_fee = _safe_float(latest_fee.get("currentFeeYen")) if isinstance(latest_fee, dict) else None
    loan_ratio = _safe_float(latest_balance.get("loanRatio")) if isinstance(latest_balance, dict) else None
    hard_gap_reason = None
    if not has_snapshot:
        hard_gap_reason = "missing_snapshot"
    elif restrictions:
        hard_gap_reason = "restriction_notice"

    soft_cost_reasons: list[str] = []
    if current_fee is not None and current_fee > 0:
        soft_cost_reasons.append("current_fee_positive")
    if loan_ratio is not None and loan_ratio >= 1.0:
        soft_cost_reasons.append("loan_ratio_high")

    return {
        "code": code,
        "available": has_snapshot,
        "hard_gap_reason": hard_gap_reason,
        "soft_cost_reasons": soft_cost_reasons,
        "restriction_count": len(restrictions),
        "current_fee_yen": current_fee,
        "loan_ratio": loan_ratio,
        "shortable_proxy_ok": bool(hard_gap_reason is None and not soft_cost_reasons),
    }


def _metric_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    ret20_values = [_safe_float(row.get("short_ret_20")) for row in rows]
    ret20_values = [value for value in ret20_values if value is not None]
    positive_count = sum(1 for row in rows if _truthy(row.get("outcome_positive")) or (_safe_float(row.get("short_ret_20")) or 0.0) > 0.0)
    count = len(rows)
    code_count = len({str(row.get("code") or "").strip() for row in rows if str(row.get("code") or "").strip()})
    return {
        "count": int(count),
        "code_count": int(code_count),
        "positive_count": int(positive_count),
        "nonpositive_count": int(count - positive_count),
        "hit_rate": None if count == 0 else float(positive_count / count),
        "mean_ret20": None if not ret20_values else float(statistics.fmean(ret20_values)),
        "median_ret20": None if not ret20_values else float(statistics.median(ret20_values)),
    }


def _group_balance(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(str(row.get("confusion_group") or "") for row in rows)
    return {
        "kept_good": int(counts.get("kept_good", 0)),
        "retained_bad": int(counts.get("retained_bad", 0)),
        "retained_unknown": int(counts.get("retained_unknown", 0)),
        "removed_good": int(counts.get("removed_good", 0)),
        "removed_bad": int(counts.get("removed_bad", 0)),
        "removed_unknown": int(counts.get("removed_unknown", 0)),
    }


def _concentration(rows: list[dict[str, Any]], *, sector_lookup: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    code_counts = Counter(str(row.get("code") or "").strip() for row in rows if str(row.get("code") or "").strip())
    sector_counts = Counter(
        str(
            (sector_lookup.get(str(row.get("code") or "").strip()) or {}).get("sector33_name")
            or (sector_lookup.get(str(row.get("code") or "").strip()) or {}).get("market_code")
            or "<unknown>"
        )
        for row in rows
        if str(row.get("code") or "").strip()
    )
    total = len(rows) or 1
    top1_code = code_counts.most_common(1)[0][1] if code_counts else 0
    top3_code = sum(count for _, count in code_counts.most_common(3))
    top1_sector = sector_counts.most_common(1)[0][1] if sector_counts else 0
    top3_sector = sum(count for _, count in sector_counts.most_common(3))
    return {
        "code": {
            "unique_count": len(code_counts),
            "top1_code": code_counts.most_common(1)[0][0] if code_counts else None,
            "top1_count": top1_code,
            "top1_share": float(top1_code / total),
            "top3_count": top3_code,
            "top3_share": float(top3_code / total),
        },
        "sector": {
            "unique_count": len(sector_counts),
            "top1_sector": sector_counts.most_common(1)[0][0] if sector_counts else None,
            "top1_count": top1_sector,
            "top1_share": float(top1_sector / total),
            "top3_count": top3_sector,
            "top3_share": float(top3_sector / total),
        },
    }


def _classify_borrow(borrow: Mapping[str, Any]) -> tuple[str, bool, bool]:
    hard_gap = bool(borrow.get("hard_gap_reason"))
    soft_cost = bool(list(borrow.get("soft_cost_reasons") or []))
    if hard_gap:
        return "hard_borrow_gap", True, soft_cost
    if soft_cost:
        return "soft_borrow_cost_flagged", False, True
    return "clean_borrowable", False, False


def _build_event_rows(
    confusion_rows: list[dict[str, str]],
    *,
    borrow_lookup: Mapping[str, Mapping[str, Any]],
    sector_lookup: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in sorted(confusion_rows, key=lambda item: (int(item["ymd"]), str(item["code"]))):
        code = _normalize_text(row.get("code"))
        borrow = borrow_lookup.get(code) or {}
        bucket, hard_gap, soft_cost = _classify_borrow(borrow)
        ymd = _safe_int(row.get("ymd"))
        sector = sector_lookup.get(code) or {}
        rows.append(
            {
                "event_id": f"{ymd or 'unknown'}:{code}:{row.get('confusion_group') or 'unknown'}",
                "ymd": ymd,
                "signal_date_iso": _ymd_to_iso(ymd),
                "code": code,
                "name": sector.get("name") or row.get("name"),
                "confusion_group": row.get("confusion_group"),
                "baseline_selected": _truthy(row.get("baseline_selected")),
                "challenger_selected": _truthy(row.get("challenger_selected")),
                "outcome_known": _truthy(row.get("outcome_known")),
                "outcome_positive": _truthy(row.get("outcome_positive")),
                "outcome_bucket": row.get("outcome_bucket") or None,
                "short_ret_20": _safe_float(row.get("short_ret_20")),
                "short_ret_10": _safe_float(row.get("short_ret_10")),
                "short_ret_5": _safe_float(row.get("short_ret_5")),
                "borrow_bucket": bucket,
                "borrow_bucket_reason": "hard_gap" if hard_gap else ("soft_cost" if soft_cost else "clean"),
                "hard_borrow_gap": hard_gap,
                "hard_borrow_gap_reason": borrow.get("hard_gap_reason"),
                "soft_borrow_cost_flagged": soft_cost,
                "soft_borrow_cost_reasons": list(borrow.get("soft_cost_reasons") or []),
                "borrowable_proxy_ok": bool(borrow.get("shortable_proxy_ok")),
                "current_fee_yen": _safe_float(borrow.get("current_fee_yen")),
                "loan_ratio": _safe_float(borrow.get("loan_ratio")),
                "restriction_count": _safe_int(borrow.get("restriction_count")),
                "sector33_code": sector.get("sector33_code"),
                "sector33_name": sector.get("sector33_name"),
                "market_code": sector.get("market_code"),
                "monthlyRangeProb": _safe_float(row.get("monthlyRangeProb")),
                "tradePriorityScore": _safe_float(row.get("tradePriorityScore")),
                "marketRegime": row.get("marketRegime") or None,
                "trendDownStrict": row.get("trendDownStrict") if row.get("trendDownStrict") not in (None, "") else None,
            }
        )
    return rows


def _borrow_bucket_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    selected_rows = [row for row in rows if _truthy(row.get("challenger_selected"))]
    buckets = {
        "hard_borrow_gap": [row for row in selected_rows if row["borrow_bucket"] == "hard_borrow_gap"],
        "soft_borrow_cost_flagged": [row for row in selected_rows if row["borrow_bucket"] == "soft_borrow_cost_flagged"],
        "clean_borrowable": [row for row in selected_rows if row["borrow_bucket"] == "clean_borrowable"],
    }
    return {
        bucket_name: {
            **_metric_summary(bucket_rows),
            "borrow_bucket": bucket_name,
            "confusion_balance": _group_balance(bucket_rows),
        }
        for bucket_name, bucket_rows in buckets.items()
    }


def _build_soft_cost_concentration(
    rows: list[dict[str, Any]],
    *,
    sector_lookup: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    selected_rows = [row for row in rows if _truthy(row.get("challenger_selected"))]
    selected_soft_rows = [row for row in selected_rows if row["borrow_bucket"] == "soft_borrow_cost_flagged"]
    selected_clean_rows = [row for row in selected_rows if row["borrow_bucket"] == "clean_borrowable"]
    selected_hard_rows = [row for row in selected_rows if row["borrow_bucket"] == "hard_borrow_gap"]
    all_soft_rows = [row for row in rows if row["borrow_bucket"] == "soft_borrow_cost_flagged"]
    soft_code_counts = Counter(str(row["code"]) for row in selected_soft_rows if str(row.get("code") or "").strip())
    soft_sector_counts = Counter(
        str(
            (sector_lookup.get(str(row["code"])) or {}).get("sector33_name")
            or (sector_lookup.get(str(row["code"])) or {}).get("market_code")
            or "<unknown>"
        )
        for row in selected_soft_rows
        if str(row.get("code") or "").strip()
    )
    soft_code_counts_all = Counter(str(row["code"]) for row in all_soft_rows if str(row.get("code") or "").strip())
    soft_sector_counts_all = Counter(
        str(
            (sector_lookup.get(str(row["code"])) or {}).get("sector33_name")
            or (sector_lookup.get(str(row["code"])) or {}).get("market_code")
            or "<unknown>"
        )
        for row in all_soft_rows
        if str(row.get("code") or "").strip()
    )
    repeated_soft_cost_names = [
        {"code": code, "selected_event_count": count}
        for code, count in soft_code_counts.items()
        if count > 1
    ]
    return {
        "selected": {
            "soft_cost_event_count": len(selected_soft_rows),
            "soft_cost_code_count": len(soft_code_counts),
            "soft_cost_event_share": float(len(selected_soft_rows) / max(1, len(selected_rows))),
            "soft_cost_code_share": float(len(soft_code_counts) / max(1, len({str(row.get("code") or "").strip() for row in selected_rows if str(row.get("code") or "").strip()}))),
            "code": {
                "unique_count": len(soft_code_counts),
                "top1_code": soft_code_counts.most_common(1)[0][0] if soft_code_counts else None,
                "top1_count": soft_code_counts.most_common(1)[0][1] if soft_code_counts else 0,
                "top1_share": float(soft_code_counts.most_common(1)[0][1] / max(1, len(selected_soft_rows))) if soft_code_counts else 0.0,
                "top3_count": sum(count for _, count in soft_code_counts.most_common(3)),
                "top3_share": float(sum(count for _, count in soft_code_counts.most_common(3)) / max(1, len(selected_soft_rows))) if soft_code_counts else 0.0,
            },
            "sector": {
                "unique_count": len(soft_sector_counts),
                "top1_sector": soft_sector_counts.most_common(1)[0][0] if soft_sector_counts else None,
                "top1_count": soft_sector_counts.most_common(1)[0][1] if soft_sector_counts else 0,
                "top1_share": float(soft_sector_counts.most_common(1)[0][1] / max(1, len(selected_soft_rows))) if soft_sector_counts else 0.0,
                "top3_count": sum(count for _, count in soft_sector_counts.most_common(3)),
                "top3_share": float(sum(count for _, count in soft_sector_counts.most_common(3)) / max(1, len(selected_soft_rows))) if soft_sector_counts else 0.0,
            },
            "repeated_soft_cost_names": repeated_soft_cost_names,
            "bucket_metrics": _metric_summary(selected_soft_rows),
        },
        "all_rows": {
            "selected_event_count": len(rows),
            "soft_cost_event_count": len(all_soft_rows),
            "soft_cost_code_count": len(soft_code_counts_all),
            "soft_cost_event_share": float(len(all_soft_rows) / max(1, len(rows))),
            "soft_cost_code_share": float(len(soft_code_counts_all) / max(1, len({str(row.get("code") or "").strip() for row in rows if str(row.get("code") or "").strip()}))),
            "code": {
                "unique_count": len(soft_code_counts_all),
                "top1_code": soft_code_counts_all.most_common(1)[0][0] if soft_code_counts_all else None,
                "top1_count": soft_code_counts_all.most_common(1)[0][1] if soft_code_counts_all else 0,
                "top1_share": float(soft_code_counts_all.most_common(1)[0][1] / max(1, len(all_soft_rows))) if soft_code_counts_all else 0.0,
                "top3_count": sum(count for _, count in soft_code_counts_all.most_common(3)),
                "top3_share": float(sum(count for _, count in soft_code_counts_all.most_common(3)) / max(1, len(all_soft_rows))) if soft_code_counts_all else 0.0,
            },
            "sector": {
                "unique_count": len(soft_sector_counts_all),
                "top1_sector": soft_sector_counts_all.most_common(1)[0][0] if soft_sector_counts_all else None,
                "top1_count": soft_sector_counts_all.most_common(1)[0][1] if soft_sector_counts_all else 0,
                "top1_share": float(soft_sector_counts_all.most_common(1)[0][1] / max(1, len(all_soft_rows))) if soft_sector_counts_all else 0.0,
                "top3_count": sum(count for _, count in soft_sector_counts_all.most_common(3)),
                "top3_share": float(sum(count for _, count in soft_sector_counts_all.most_common(3)) / max(1, len(all_soft_rows))) if soft_sector_counts_all else 0.0,
            },
            "repeated_soft_cost_names": [
                {"code": code, "all_row_event_count": count}
                for code, count in soft_code_counts_all.items()
                if count > 1
            ],
        },
    }


def _build_borrow_adjusted_compare(
    *,
    source_compare: Mapping[str, Any],
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    selected_rows = [row for row in rows if _truthy(row.get("challenger_selected"))]
    bucket_rows = {
        "hard_borrow_gap": [row for row in selected_rows if row["borrow_bucket"] == "hard_borrow_gap"],
        "soft_borrow_cost_flagged": [row for row in selected_rows if row["borrow_bucket"] == "soft_borrow_cost_flagged"],
        "clean_borrowable": [row for row in selected_rows if row["borrow_bucket"] == "clean_borrowable"],
    }
    selected_code_count = len({str(row["code"]) for row in selected_rows})
    hard_only_gate = [row for row in selected_rows if row["borrow_bucket"] != "hard_borrow_gap"]
    clean_only_gate = bucket_rows["clean_borrowable"]
    hard_only_summary = _metric_summary(hard_only_gate)
    clean_only_summary = _metric_summary(clean_only_gate)
    selected_summary = _metric_summary(selected_rows)
    return {
        "schema_version": f"{SCHEMA_PREFIX}_adjusted_compare_v1",
        "generated_at": _utc_now(),
        "source_compare": {
            "baseline": source_compare["full_recheck_summary"]["baseline"],
            "challenger": source_compare["full_recheck_summary"]["challenger"],
            "delta": source_compare["full_recheck_summary"]["delta"],
            "selection_branching": source_compare["selection_branching"],
        },
        "selected_borrow_gate_projection": {
            "selected_event_count": selected_summary["count"],
            "selected_code_count": selected_summary["code_count"],
            "hard_only_gate_selected_event_count": hard_only_summary["count"],
            "hard_only_gate_selected_code_count": hard_only_summary["code_count"],
            "clean_only_gate_selected_event_count": clean_only_summary["count"],
            "clean_only_gate_selected_code_count": clean_only_summary["code_count"],
            "hard_only_gate_breadth_ok": bool(hard_only_summary["count"] >= max(MIN_CLEAN_BORROWABLE_EVENTS, int(selected_summary["count"] * 0.8))),
            "clean_only_gate_breadth_ok": bool(clean_only_summary["count"] >= MIN_CLEAN_BORROWABLE_EVENTS),
            "hard_gap_event_share": float(sum(1 for row in selected_rows if row["borrow_bucket"] == "hard_borrow_gap") / max(1, len(selected_rows))),
            "soft_borrow_cost_event_share": float(sum(1 for row in selected_rows if row["borrow_bucket"] == "soft_borrow_cost_flagged") / max(1, len(selected_rows))),
            "clean_borrowable_event_share": float(sum(1 for row in selected_rows if row["borrow_bucket"] == "clean_borrowable") / max(1, len(selected_rows))),
            "soft_cost_bucket_metrics": _metric_summary(bucket_rows["soft_borrow_cost_flagged"]),
            "clean_borrowable_bucket_metrics": clean_only_summary,
            "hard_borrow_gap_bucket_metrics": _metric_summary(bucket_rows["hard_borrow_gap"]),
        },
        "dependency_readout": {
            "soft_cost_bucket_positive_count": _metric_summary(bucket_rows["soft_borrow_cost_flagged"])["positive_count"],
            "soft_cost_bucket_nonpositive_count": _metric_summary(bucket_rows["soft_borrow_cost_flagged"])["nonpositive_count"],
            "soft_cost_bucket_mean_ret20": _metric_summary(bucket_rows["soft_borrow_cost_flagged"])["mean_ret20"],
            "clean_bucket_positive_count": clean_only_summary["positive_count"],
            "clean_bucket_nonpositive_count": clean_only_summary["nonpositive_count"],
            "clean_bucket_mean_ret20": clean_only_summary["mean_ret20"],
            "edge_depends_on_soft_cost_names": bool(
                _metric_summary(bucket_rows["soft_borrow_cost_flagged"])["mean_ret20"] is not None
                and clean_only_summary["mean_ret20"] is not None
                and _metric_summary(bucket_rows["soft_borrow_cost_flagged"])["mean_ret20"] > clean_only_summary["mean_ret20"]
                and len(bucket_rows["soft_borrow_cost_flagged"]) > len(clean_only_gate)
            ),
            "clean_sample_too_small": bool(clean_only_summary["count"] < MIN_CLEAN_BORROWABLE_EVENTS),
        },
        "borrow_buckets": {
            bucket: {
                **_metric_summary(bucket_rows[bucket]),
                "confusion_balance": _group_balance(bucket_rows[bucket]),
            }
            for bucket in bucket_rows
        },
    }


def _build_decision(
    *,
    source_context: Mapping[str, Any],
    runtime_context: Mapping[str, Any],
    rows: list[dict[str, Any]],
    bucket_summary: Mapping[str, Any],
    soft_cost_concentration: Mapping[str, Any],
    borrow_adjusted_compare: Mapping[str, Any],
) -> dict[str, Any]:
    selected_summary = bucket_summary.get("selected") or bucket_summary.get("selected_summary") or bucket_summary
    clean_count = int(selected_summary["clean_borrowable_event_count"])
    soft_count = int(
        selected_summary.get(
            "soft_borrow_cost_event_count",
            round(float(selected_summary.get("soft_cost_event_share", 0.0)) * max(1, int(selected_summary.get("selected_event_count", 0)))),
        )
    )
    hard_count = int(
        selected_summary.get(
            "hard_borrow_gap_event_count",
            round(float(selected_summary.get("hard_borrow_gap_event_share", 0.0)) * max(1, int(selected_summary.get("selected_event_count", 0)))),
        )
    )
    hard_gap_event_share = float(selected_summary["hard_borrow_gap_event_share"])
    hard_gap_code_count = int(selected_summary["hard_borrow_gap_code_count"])
    selected_code_count = int(selected_summary["selected_code_count"])
    hard_gap_code_share = float(hard_gap_code_count / max(1, selected_code_count))
    soft_cost_event_share = float(
        selected_summary.get(
            "soft_borrow_cost_event_share",
            soft_count / max(1, int(selected_summary.get("selected_event_count", 0))),
        )
    )
    clean_event_share = float(
        selected_summary.get(
            "clean_borrowable_event_share",
            clean_count / max(1, int(selected_summary.get("selected_event_count", 0))),
        )
    )

    soft_dep = bool(borrow_adjusted_compare["dependency_readout"]["edge_depends_on_soft_cost_names"])
    clean_too_small = bool(borrow_adjusted_compare["dependency_readout"]["clean_sample_too_small"])
    concentration = soft_cost_concentration["selected"]
    severe_concentration = bool(concentration["code"]["top1_share"] >= 0.5 or concentration["sector"]["top1_share"] >= 0.5)
    hard_gap_broad = bool(
        hard_gap_event_share >= MAX_HARD_BORROW_GAP_SHARE
        or hard_gap_code_share >= MAX_HARD_BORROW_GAP_CODE_SHARE
    )

    if hard_gap_broad:
        decision = "drop_due_to_borrow_untradable"
        reasons = ["hard_borrow_gap_is_too_broad_for_paper_replay"]
    elif soft_dep and clean_too_small:
        decision = "hold_due_to_insufficient_clean_borrowable_sample"
        reasons = [
            "clean_borrowable_sample_is_too_small_for_paper_replay",
            "soft_cost_bucket_carries_most_of_the_edge",
        ]
    elif soft_dep and severe_concentration:
        decision = "drop_as_edge_depends_on_soft_cost_names"
        reasons = [
            "edge_is_tied_to_soft_cost_flagged_names",
            "soft_cost_concentration_is_too_high",
        ]
    elif soft_dep:
        decision = "hold_due_to_soft_cost_proxy_unclear"
        reasons = [
            "soft_cost_proxy_is_broad",
            "clean_borrowable_sample_is_not_yet_convincing",
        ]
    elif clean_too_small:
        decision = "hold_due_to_insufficient_clean_borrowable_sample"
        reasons = ["clean_borrowable_sample_is_too_small_for_paper_replay"]
    else:
        decision = "keep_for_borrow_caveated_paper_replay"
        reasons = [
            "hard_borrow_gap_remains_near_zero",
            "clean_borrowable_sample_is_sufficient",
            "edge_does_not_depend_mainly_on_soft_cost_flagged_names",
        ]

    borrow_summary = selected_summary
    production_unchanged_ok = True
    no_lookahead_ok = bool(source_context["no_lookahead"].get("no_lookahead_pass"))
    current_runtime_ready = bool(runtime_context["runtime_status"].get("validated"))
    criteria_state = {
        "hard_borrow_gap_near_zero": bool(hard_gap_event_share < MAX_HARD_BORROW_GAP_SHARE and hard_gap_code_count < max(3, int(selected_code_count * MAX_HARD_BORROW_GAP_CODE_SHARE + 0.9999))),
        "clean_or_acceptable_borrow_sample_sufficient": bool(clean_count >= MIN_CLEAN_BORROWABLE_EVENTS),
        "edge_not_mainly_soft_cost_flagged": bool(not soft_dep),
        "no_severe_concentration": bool(not severe_concentration),
        "no_lookahead_pass": no_lookahead_ok,
        "production_state_unchanged": production_unchanged_ok,
        "runtime_context_validated": current_runtime_ready,
    }

    return {
        "schema_version": f"{SCHEMA_PREFIX}_decision_v1",
        "session_id": f"{VARIANT_ID}-borrow-decomposition-{_utc_stamp()}",
        "generated_at": _utc_now(),
        "decision": decision,
        "decision_reasons": reasons,
        "criteria_state": criteria_state,
        "borrow_summary": {
            "selected_event_count": borrow_summary["selected_event_count"],
            "selected_code_count": borrow_summary["selected_code_count"],
            "hard_borrow_gap_event_count": borrow_summary.get("hard_borrow_gap_event_count", hard_count),
            "hard_borrow_gap_event_share": borrow_summary.get("hard_borrow_gap_event_share", hard_gap_event_share),
            "soft_borrow_cost_event_count": borrow_summary.get("soft_borrow_cost_event_count", soft_count),
            "soft_borrow_cost_event_share": borrow_summary.get("soft_borrow_cost_event_share", soft_cost_event_share),
            "clean_borrowable_event_count": borrow_summary.get("clean_borrowable_event_count", clean_count),
            "clean_borrowable_event_share": borrow_summary.get("clean_borrowable_event_share", clean_event_share),
            "hard_only_gate_breadth_ok": bool(borrow_adjusted_compare["selected_borrow_gate_projection"]["hard_only_gate_breadth_ok"]),
            "clean_only_gate_breadth_ok": bool(borrow_adjusted_compare["selected_borrow_gate_projection"]["clean_only_gate_breadth_ok"]),
        },
        "borrow_adjusted_compare": borrow_adjusted_compare,
        "soft_cost_concentration": soft_cost_concentration,
        "source_keep_replay_decision": source_context["stability_decision"].get("decision"),
        "source_full_recheck_decision": source_context["full_recheck_decision"].get("decision"),
        "production_blocking_reasons": [] if decision == "keep_for_borrow_caveated_paper_replay" else reasons,
        "borrow_caveated_paper_replay_candidate": decision == "keep_for_borrow_caveated_paper_replay",
        "paper_replay_ready": decision == "keep_for_borrow_caveated_paper_replay",
        "no_lookahead_pass": no_lookahead_ok,
        "production_ranking_changed": False,
        "active_champion_changed": False,
        "publish_run": False,
        "live_sell_signal_added": False,
        "silent_fallback_used": False,
        "research_fallback": False,
        "next_gate": "paper_execution_replay" if decision == "keep_for_borrow_caveated_paper_replay" else "keep_watching_current_frozen_candidate",
    }


def _build_contract(
    *,
    source_context: Mapping[str, Any],
    runtime_context: Mapping[str, Any],
    rows: list[dict[str, Any]],
    decision: Mapping[str, Any],
) -> dict[str, Any]:
    selected_rows = [row for row in rows if _truthy(row.get("challenger_selected"))]
    selected_codes = sorted({str(row.get("code") or "").strip() for row in selected_rows if str(row.get("code") or "").strip()})
    return {
        "schema_version": f"{SCHEMA_PREFIX}_contract_v1",
        "session_id": decision["session_id"],
        "generated_at": _utc_now(),
        "axis": VARIANT_ID,
        "decision_labels": [
            "keep_for_borrow_caveated_paper_replay",
            "hold_due_to_soft_cost_proxy_unclear",
            "hold_due_to_insufficient_clean_borrowable_sample",
            "drop_due_to_borrow_untradable",
            "drop_as_edge_depends_on_soft_cost_names",
        ],
        "fixed_evaluation_conditions": {
            "long_logic_frozen": True,
            "no_lookahead_contract": True,
            "no_meemee_ui_change": True,
            "no_production_state_change": True,
            "one_axis_only": True,
            "same_artifact_detail_level": True,
            "same_cost_slippage": True,
            "same_period": True,
            "same_regime": True,
            "same_top_k": True,
            "same_universe": True,
        },
        "frozen_source_state": {
            "borrow_proxy_gap_decision": source_context["stability_decision"].get("decision"),
            "full_recheck_decision": source_context["full_recheck_decision"].get("decision"),
            "hard_borrow_gap_event_share": source_context["stability_decision"].get("borrow_proxy_summary", {}).get("hard_borrow_gap_event_share"),
            "selected_code_count": len(selected_codes),
            "selected_codes": selected_codes,
            "soft_borrow_cost_event_share": source_context["stability_decision"].get("borrow_proxy_summary", {}).get("soft_borrow_cost_event_share"),
            "clean_borrowable_event_share": 1.0 - float(source_context["stability_decision"].get("borrow_proxy_summary", {}).get("soft_borrow_cost_event_share") or 0.0),
            "borrow_proxy_summary": source_context["stability_decision"].get("borrow_proxy_summary"),
            "no_lookahead_pass": bool(source_context["no_lookahead"].get("no_lookahead_pass")),
        },
        "input_artifacts": {
            "stability_replay_contract": str(Path(source_context["source_root"]) / "short_bottom_risk_stability_replay_contract.json"),
            "stability_replay_decision": str(Path(source_context["source_root"]) / "short_bottom_risk_stability_replay_decision.json"),
            "borrow_proxy_report": str(Path(source_context["source_root"]) / "short_bottom_risk_borrow_proxy_report.json"),
            "full_recheck_compare": str(Path(source_context["compare_root"]) / "short_bottom_risk_full_recheck_compare.json"),
            "confusion_groups": str(Path(source_context["diagnostic_root"]) / "short_bottom_risk_confusion_groups.csv"),
            "no_lookahead": str(Path(source_context["source_root"]) / "no_lookahead_audit.json"),
        },
        "runtime_context": runtime_context,
        "non_scope": [
            "create_new_short_rule",
            "threshold_tuning",
            "change_short_cleanup_bottom_risk_v1",
            "close_pos_tuning",
            "monthly_alignment_tuning",
            "long_logic",
            "cost_model",
            "MeeMee",
            "production ranking",
            "active champion",
            "publish",
            "live sell signal",
        ],
        "research_fallback": False,
        "borrow_proxy_rules": {
            "hard_gap": {
                "max_hard_borrow_gap_share": MAX_HARD_BORROW_GAP_SHARE,
                "max_hard_borrow_gap_code_share": MAX_HARD_BORROW_GAP_CODE_SHARE,
            },
            "clean_sample_minimum_events": MIN_CLEAN_BORROWABLE_EVENTS,
        },
        "validation_focus": [
            "hard_gap_vs_soft_cost_split",
            "bucket_outcome_quality",
            "soft_cost_concentration",
            "hard_only_breadth_projection",
            "production_blocking_reasons",
        ],
    }


def _artifact_complete(output_root: Path) -> dict[str, Any]:
    return {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "generated_at": _utc_now(),
        "complete": True,
        "artifact_refs": {
            "borrow_decomposition_contract": str(output_root / "short_bottom_risk_borrow_decomposition_contract.json"),
            "borrow_bucket_events": str(output_root / "short_bottom_risk_borrow_bucket_events.csv"),
            "borrow_bucket_summary": str(output_root / "short_bottom_risk_borrow_bucket_summary.json"),
            "soft_cost_concentration": str(output_root / "short_bottom_risk_soft_cost_concentration.json"),
            "borrow_adjusted_compare": str(output_root / "short_bottom_risk_borrow_adjusted_compare.json"),
            "borrow_decomposition_decision": str(output_root / "short_bottom_risk_borrow_decomposition_decision.json"),
            "no_lookahead_audit": str(output_root / "no_lookahead_audit.json"),
        },
        "required_outputs": REQUIRED_OUTPUTS,
    }


def run(*, source_root: Path = DEFAULT_SOURCE_ROOT, output_root: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    source_context = _load_input_context(source_root)
    runtime_context = _load_runtime_context()
    runtime_db_path = Path(runtime_context["runtime_db_path"])

    confusion_rows = list(source_context["confusion_rows"])
    selected_from_confusion = [row for row in confusion_rows if _truthy(row.get("challenger_selected"))]
    selected_codes = sorted({str(row.get("code") or "").strip() for row in selected_from_confusion if str(row.get("code") or "").strip()})
    borrow_report_codes = [str(row.get("code") or "").strip() for row in source_context["borrow_report"].get("codes", []) if str(row.get("code") or "").strip()]
    if borrow_report_codes and sorted(set(borrow_report_codes)) != selected_codes:
        raise RuntimeError("borrow proxy report codes do not match challenger-selected confusion rows")

    sector_lookup = _load_sector_lookup(runtime_db_path, [row.get("code") for row in confusion_rows])
    borrow_lookup = {code: _borrow_proxy_for_code(code, runtime_db_path=runtime_db_path) for code in sorted({str(row.get("code") or "").strip() for row in confusion_rows if str(row.get("code") or "").strip()})}

    event_rows = _build_event_rows(confusion_rows, borrow_lookup=borrow_lookup, sector_lookup=sector_lookup)
    selected_rows = [row for row in event_rows if _truthy(row.get("challenger_selected"))]
    bucket_metrics = _borrow_bucket_metrics(event_rows)
    selected_summary = {
        "selected_event_count": len(selected_rows),
        "selected_code_count": len({str(row.get("code") or "").strip() for row in selected_rows if str(row.get("code") or "").strip()}),
        "hard_borrow_gap_event_count": sum(1 for row in selected_rows if row["borrow_bucket"] == "hard_borrow_gap"),
        "soft_borrow_cost_event_count": sum(1 for row in selected_rows if row["borrow_bucket"] == "soft_borrow_cost_flagged"),
        "clean_borrowable_event_count": sum(1 for row in selected_rows if row["borrow_bucket"] == "clean_borrowable"),
    }
    selected_summary["hard_borrow_gap_event_share"] = float(selected_summary["hard_borrow_gap_event_count"] / max(1, selected_summary["selected_event_count"]))
    selected_summary["soft_borrow_cost_event_share"] = float(selected_summary["soft_borrow_cost_event_count"] / max(1, selected_summary["selected_event_count"]))
    selected_summary["clean_borrowable_event_share"] = float(selected_summary["clean_borrowable_event_count"] / max(1, selected_summary["selected_event_count"]))
    selected_summary["hard_borrow_gap_code_count"] = len({row["code"] for row in selected_rows if row["borrow_bucket"] == "hard_borrow_gap"})
    selected_summary["soft_borrow_cost_code_count"] = len({row["code"] for row in selected_rows if row["borrow_bucket"] == "soft_borrow_cost_flagged"})
    selected_summary["clean_borrowable_code_count"] = len({row["code"] for row in selected_rows if row["borrow_bucket"] == "clean_borrowable"})

    soft_cost_concentration = _build_soft_cost_concentration(event_rows, sector_lookup=sector_lookup)
    borrow_adjusted_compare = _build_borrow_adjusted_compare(
        source_compare=source_context["full_recheck_compare"],
        rows=event_rows,
    )

    selected_rows_summary = _metric_summary(selected_rows)
    bucket_summary = {
        "schema_version": f"{SCHEMA_PREFIX}_bucket_summary_v1",
        "generated_at": _utc_now(),
        "source_root": str(source_root),
        "runtime_db_path": str(runtime_db_path),
        "selected_summary": selected_summary,
        "selected_metric_summary": selected_rows_summary,
        "borrow_bucket_metrics": bucket_metrics,
        "selected_group_balance": _group_balance(selected_rows),
        "all_group_balance": _group_balance(event_rows),
        "hard_only_gate_projection": {
            "selected_event_count": selected_summary["selected_event_count"] - selected_summary["hard_borrow_gap_event_count"],
            "selected_code_count": len({row["code"] for row in selected_rows if row["borrow_bucket"] != "hard_borrow_gap"}),
            "breadth_preserved": bool(selected_summary["hard_borrow_gap_event_count"] == 0),
            "sufficient_for_paper_replay": bool(selected_summary["selected_event_count"] - selected_summary["hard_borrow_gap_event_count"] >= MIN_CLEAN_BORROWABLE_EVENTS),
        },
        "clean_only_gate_projection": {
            "selected_event_count": selected_summary["clean_borrowable_event_count"],
            "selected_code_count": selected_summary["clean_borrowable_code_count"],
            "breadth_preserved": bool(selected_summary["clean_borrowable_event_count"] >= MIN_CLEAN_BORROWABLE_EVENTS),
            "sufficient_for_paper_replay": bool(selected_summary["clean_borrowable_event_count"] >= MIN_CLEAN_BORROWABLE_EVENTS),
        },
        "borrow_dependency_readout": borrow_adjusted_compare["dependency_readout"],
        "borrow_proxy_summary": source_context["stability_decision"].get("borrow_proxy_summary"),
    }

    decision = _build_decision(
        source_context=source_context,
        runtime_context=runtime_context,
        rows=event_rows,
        bucket_summary=bucket_summary,
        soft_cost_concentration=soft_cost_concentration,
        borrow_adjusted_compare=borrow_adjusted_compare,
    )
    contract = _build_contract(source_context=source_context, runtime_context=runtime_context, rows=event_rows, decision=decision)

    session_id = str(decision["session_id"])
    output_root = Path(output_root)
    run_dir = output_root / session_id
    run_dir.mkdir(parents=True, exist_ok=True)

    _write_json(run_dir / "short_bottom_risk_borrow_decomposition_contract.json", contract)
    _write_csv(
        run_dir / "short_bottom_risk_borrow_bucket_events.csv",
        event_rows,
        columns=[
            "event_id",
            "ymd",
            "signal_date_iso",
            "code",
            "name",
            "confusion_group",
            "baseline_selected",
            "challenger_selected",
            "outcome_known",
            "outcome_positive",
            "outcome_bucket",
            "short_ret_20",
            "short_ret_10",
            "short_ret_5",
            "borrow_bucket",
            "borrow_bucket_reason",
            "hard_borrow_gap",
            "hard_borrow_gap_reason",
            "soft_borrow_cost_flagged",
            "soft_borrow_cost_reasons",
            "borrowable_proxy_ok",
            "current_fee_yen",
            "loan_ratio",
            "restriction_count",
            "sector33_code",
            "sector33_name",
            "market_code",
            "monthlyRangeProb",
            "tradePriorityScore",
            "marketRegime",
            "trendDownStrict",
        ],
    )
    _write_json(run_dir / "short_bottom_risk_borrow_bucket_summary.json", bucket_summary)
    _write_json(run_dir / "short_bottom_risk_soft_cost_concentration.json", soft_cost_concentration)
    _write_json(run_dir / "short_bottom_risk_borrow_adjusted_compare.json", borrow_adjusted_compare)
    _write_json(run_dir / "short_bottom_risk_borrow_decomposition_decision.json", decision)
    _write_json(
        run_dir / "no_lookahead_audit.json",
        {
            "schema_version": f"{SCHEMA_PREFIX}_no_lookahead_audit_v1",
            "generated_at": _utc_now(),
            "no_lookahead_pass": bool(source_context["no_lookahead"].get("no_lookahead_pass")),
            "future_outcome_fields_used_in_selection": [],
            "future_outcome_fields_used_in_bucket_classification": [],
            "future_outcome_fields_used_in_concentration": [],
            "silent_fallback_used": False,
            "research_fallback": False,
            "source_no_lookahead": str(source_root / "no_lookahead_audit.json"),
        },
    )
    _write_json(run_dir / "_ARTIFACT_COMPLETE.json", _artifact_complete(run_dir))

    return {
        "output_root": str(run_dir),
        "decision": decision["decision"],
        "session_id": session_id,
        "source_root": str(source_root),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="TRADEX borrow proxy decomposition for frozen short_cleanup_bottom_risk_v1.")
    parser.add_argument("--source-root", type=Path, default=DEFAULT_SOURCE_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    result = run(source_root=args.source_root, output_root=args.output_root)
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
