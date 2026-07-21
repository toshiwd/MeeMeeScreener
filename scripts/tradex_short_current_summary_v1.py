from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


AXIS_ID = "short_current_summary_v1"
DEFAULT_PORTFOLIO = Path(r"G:\Tradex\short_pattern_portfolio_rollup_v1\pipeline\latest_short_portfolio_pipeline.json")
DEFAULT_MULTI_COMPARE = Path(r"G:\Tradex\short_multi_pattern_compare_v1\latest_short_multi_pattern_compare.json")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_current_summary_v1")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _pattern_index(compare: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(row.get("pattern_id")): row for row in compare.get("family_leaderboard", [])}


def _shape_pattern_id(row: dict[str, Any]) -> str | None:
    shape = row.get("shape_family")
    ma = row.get("ma_shape_family")
    status = row.get("trigger_status")
    if not shape or not ma or not status:
        return None
    return f"shape_family::{shape}__{ma}::{status}"


def _candidate_row(row: dict[str, Any], patterns: dict[str, dict[str, Any]]) -> dict[str, Any]:
    pattern_id = _shape_pattern_id(row) or str(row.get("pattern_id") or "")
    pattern = patterns.get(pattern_id, {})
    historical = row.get("historical_same_status") or {}
    management = row.get("entry_management") or {}
    code = row.get("code")
    as_of = row.get("as_of")
    evaluated_ymd = (row.get("evaluated_bar") or {}).get("ymd")
    screenshot_samples = []
    if code and as_of:
        text = str(int(as_of))
        screenshot_samples.append(
            {
                "stage": "judgment",
                "sample": f"{code}:{text[:4]}-{text[4:6]}-{text[6:8]}",
                "purpose": "entry_judgment_chart_centered_on_signal",
            }
        )
    if code and evaluated_ymd:
        text = str(int(evaluated_ymd))
        screenshot_samples.append(
            {
                "stage": "result",
                "sample": f"{code}:{text[:4]}-{text[4:6]}-{text[6:8]}",
                "purpose": "post_judgment_result_chart_centered_on_followup",
            }
        )
    return {
        "code": code,
        "display_name": row.get("display_name"),
        "pattern_id": pattern_id,
        "pattern_judgment": (pattern.get("judgment") or {}).get("judgment"),
        "pattern_reason_type": (pattern.get("judgment") or {}).get("reason_type"),
        "mapping_quality": "exact_shape_ma_trigger_match" if pattern else "pattern_not_found_in_compare",
        "verdict": row.get("verdict"),
        "trigger_status": row.get("trigger_status"),
        "as_of": row.get("as_of"),
        "entry_price_reference": management.get("entry_price_reference"),
        "target_price_reference": management.get("target_price_reference_from_avg_ret5"),
        "adverse_price_reference": management.get("historical_adverse_price_reference_from_avg_mfe5"),
        "stop_price_reference": management.get("stop_price_reference"),
        "hard_stop_price_reference": management.get("hard_stop_price_reference"),
        "holding_horizon": row.get("holding_horizon"),
        "historical_n": historical.get("n"),
        "historical_wrong_rate": historical.get("wrong_rate"),
        "historical_avg_ret5": historical.get("avg_ret5"),
        "historical_avg_MAE5": historical.get("avg_MAE5"),
        "historical_avg_MFE5": historical.get("avg_MFE5"),
        "review_decision": "review_short_probe" if (pattern.get("judgment") or {}).get("judgment") == "keep_monitor" else "watch_only",
        "no_entry_if": management.get("no_entry_if", []),
        "take_profit_review_if": management.get("take_profit_review_if", []),
        "screenshot_samples": screenshot_samples,
        "visual_review_tags_schema": {
            "monthly_context": [
                "monthly_failed_new_high",
                "monthly_upper_wick",
                "monthly_mid_break",
                "monthly_prev_low_break",
                "monthly_not_broken",
            ],
            "daily_context": [
                "gap_resistance",
                "failed_rebound_after_large_red",
                "support_break_confirmed",
                "support_hold_risk",
                "too_extended_down",
            ],
            "ma_context": [
                "below_7ma",
                "7ma_resistance",
                "20ma_resistance",
                "ma_compressed",
                "ma_not_bearish_enough",
            ],
            "entry_quality": [
                "sell_now_review",
                "wait_rebound_fail",
                "avoid_after_drop",
                "avoid_support_nearby",
            ],
        },
    }


def run(*, portfolio: Path, multi_compare: Path, output_root: Path) -> Path:
    portfolio_payload = _read_json(portfolio)
    compare_payload = _read_json(multi_compare)
    patterns = _pattern_index(compare_payload)
    rows = [
        _candidate_row(row, patterns)
        for row in (portfolio_payload.get("portfolio_summary") or {}).get("rows", [])
    ]
    rows.sort(
        key=lambda row: (
            row.get("review_decision") == "review_short_probe",
            -(row.get("historical_wrong_rate") or 1),
            row.get("historical_avg_ret5") or 0,
            str(row.get("code") or ""),
        ),
        reverse=True,
    )
    report = {
        "schema_version": f"{AXIS_ID}_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "source_portfolio": str(portfolio),
        "source_multi_compare": str(multi_compare),
        "review_only": True,
        "not_trade_signal": True,
        "row_count": len(rows),
        "review_short_probe_count": sum(1 for row in rows if row.get("review_decision") == "review_short_probe"),
        "rows": rows,
        "screenshot_batch_command": _screenshot_batch_command(rows),
        "decision": {
            "candidate_local_decision": "review_short_probe_present" if any(row.get("review_decision") == "review_short_probe" for row in rows) else "no_review_short_probe",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "current short review candidates summarized from exact pattern compare and portfolio entry management",
        },
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
    }
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    _write_json(output_dir / "short_current_summary.json", report)
    _write_json(output_root / "latest_short_current_summary.json", {"run_root": str(output_dir), **report})
    print(json.dumps({
        "run_root": str(output_dir),
        "decision": report["decision"]["candidate_local_decision"],
        "row_count": report["row_count"],
        "review_short_probe_count": report["review_short_probe_count"],
        "rows": rows,
    }, ensure_ascii=False, indent=2, default=str))
    return output_dir


def _screenshot_batch_command(rows: list[dict[str, Any]]) -> str | None:
    samples: list[str] = []
    for row in rows:
        for sample in row.get("screenshot_samples", []):
            value = sample.get("sample")
            if value:
                samples.append(str(value))
    if not samples:
        return None
    unique_samples = list(dict.fromkeys(samples))
    return (
        "node scripts/meemee_detail_clean_screenshot_batch_v1.mjs "
        f"--centered --samples {','.join(unique_samples)} --viewport-fallback"
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--portfolio", type=Path, default=DEFAULT_PORTFOLIO)
    parser.add_argument("--multi-compare", type=Path, default=DEFAULT_MULTI_COMPARE)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    run(portfolio=args.portfolio, multi_compare=args.multi_compare, output_root=args.output_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
