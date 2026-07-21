from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


AXIS_ID = "short_multi_pattern_compare_v1"
DEFAULT_SHAPE_FAMILY = Path(r"G:\Tradex\short_entry_shape_family_probe_v1\latest_short_entry_shape_family_probe.json")
DEFAULT_BLOWOFF = Path(r"G:\Tradex\short_watch_to_entry_retest_probe_v1\latest_short_watch_to_entry_retest_report.json")
DEFAULT_PORTFOLIO = Path(r"G:\Tradex\short_pattern_portfolio_rollup_v1\pipeline\latest_short_portfolio_pipeline.json")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_multi_pattern_compare_v1")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _pattern_score(*, n: int, wrong_rate: float | None, avg_ret5: float | None, target_first_rate: float | None = None, stop_first_rate: float | None = None) -> float:
    score = 0.0
    score += min(n, 250) / 250.0
    if wrong_rate is not None:
        score += max(0.0, 0.20 - wrong_rate) * 5.0
    if avg_ret5 is not None:
        score += max(0.0, -avg_ret5) * 25.0
    if target_first_rate is not None:
        score += target_first_rate * 1.5
    if stop_first_rate is not None:
        score -= stop_first_rate * 2.0
    return round(score, 6)


def _shape_patterns(shape_payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for row in shape_payload.get("expectancy_leaderboard", []):
        score = row.get("score") or {}
        if score.get("decision") != "keep_expectancy_candidate":
            continue
        summary = row.get("summary") or {}
        stability = row.get("stability") or {}
        n = int(summary.get("n") or 0)
        wrong_rate = None if summary.get("wrong_rate") is None else float(summary["wrong_rate"])
        avg_ret5 = None if summary.get("avg_ret5") is None else float(summary["avg_ret5"])
        pattern_id = f"shape_family::{row.get('candidate_key')}::{row.get('trigger_status')}"
        rows.append(
            {
                "pattern_id": pattern_id,
                "pattern_group": "entry_shape_family",
                "shape_family": row.get("shape_family"),
                "ma_shape_family": row.get("ma_shape_family"),
                "trigger_status": row.get("trigger_status"),
                "decision": score.get("decision"),
                "n": n,
                "wrong_rate": wrong_rate,
                "avg_ret5": avg_ret5,
                "entry_now_rate": summary.get("entry_now_rate"),
                "watch_next_rate": summary.get("watch_next_rate"),
                "avg_MAE5": summary.get("avg_MAE5"),
                "avg_MFE5": summary.get("avg_MFE5"),
                "usable_year_count": stability.get("usable_year_count"),
                "weak_year_count": stability.get("weak_year_count"),
                "priority_score": _pattern_score(n=n, wrong_rate=wrong_rate, avg_ret5=avg_ret5),
                "holding_horizon": "5_sessions_probe",
                "actionability": "review_only",
                "source_axis_id": shape_payload.get("axis_id"),
            }
        )
    return rows


def _blowoff_patterns(blowoff_payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for key, label in [
        ("practical_blowoff_champion", "strict_or_best"),
        ("practical_blowoff_n20_champion", "n20_main"),
    ]:
        item = blowoff_payload.get(key)
        if not item:
            continue
        n = int(item.get("n") or 0)
        target_first = None if item.get("target_first_rate") is None else float(item["target_first_rate"])
        stop_first = None if item.get("stop_first_rate") is None else float(item["stop_first_rate"])
        rows.append(
            {
                "pattern_id": f"blowoff::{item.get('rule_id')}",
                "pattern_group": "high_zone_peak_failure",
                "variant": label,
                "rule_id": item.get("rule_id"),
                "decision": item.get("candidate_local_decision"),
                "n": n,
                "target_first_rate": target_first,
                "stop_first_rate": stop_first,
                "avg_model_return": item.get("avg_model_return"),
                "sized_total_return": item.get("sized_total_return"),
                "worst_sized_return": item.get("worst_sized_return"),
                "target20_down3_rate": item.get("target20_down3_rate"),
                "adverse20_up3_rate": item.get("adverse20_up3_rate"),
                "priority_score": _pattern_score(n=n, wrong_rate=None, avg_ret5=None, target_first_rate=target_first, stop_first_rate=stop_first),
                "holding_horizon": "20_sessions_target_first",
                "actionability": "review_only",
                "source_axis_id": blowoff_payload.get("axis_id"),
            }
        )
    return rows


def _current_rows_by_pattern(portfolio_payload: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in (portfolio_payload.get("portfolio_summary") or {}).get("rows", []):
        base = {
            "code": row.get("code"),
            "display_name": row.get("display_name"),
            "trigger_status": row.get("trigger_status"),
            "as_of": row.get("as_of"),
            "verdict": row.get("verdict"),
            "actionability": row.get("actionability"),
            "mapping_quality": "fallback_from_portfolio_pattern_id",
        }
        grouped.setdefault(str(row.get("pattern_id")), []).append(base)
        shape_family = row.get("shape_family")
        ma_shape_family = row.get("ma_shape_family")
        trigger_status = row.get("trigger_status")
        if shape_family and ma_shape_family and trigger_status:
            exact_key = f"shape_family::{shape_family}__{ma_shape_family}::{trigger_status}"
            grouped.setdefault(exact_key, []).append({**base, "mapping_quality": "exact_shape_ma_trigger_match"})
    return grouped


def _judge(pattern: dict[str, Any], current_count: int) -> dict[str, Any]:
    n = int(pattern.get("n") or 0)
    group = pattern.get("pattern_group")
    if group == "high_zone_peak_failure":
        if n >= 20 and float(pattern.get("target_first_rate") or 0) >= 0.90 and float(pattern.get("stop_first_rate") or 1) <= 0.05:
            return {"judgment": "keep_monitor", "reason_type": "high_target_first_low_stop_first"}
        return {"judgment": "hold_research", "reason_type": "sample_or_stop_gate_not_met"}
    wrong_rate = float(pattern.get("wrong_rate") or 1.0)
    avg_ret5 = float(pattern.get("avg_ret5") or 0.0)
    weak_year_count = int(pattern.get("weak_year_count") or 0)
    if n >= 50 and wrong_rate <= 0.13 and avg_ret5 < -0.01 and weak_year_count <= 1:
        return {"judgment": "keep_monitor", "reason_type": "low_wrong_rate_negative_ret5_stable"}
    if current_count > 0 and n >= 50 and avg_ret5 < 0:
        return {"judgment": "hold_current_review", "reason_type": "current_candidate_but_not_top_quality_gate"}
    return {"judgment": "hold_research", "reason_type": "quality_gate_not_met"}


def run(*, shape_family: Path, blowoff: Path, portfolio: Path, output_root: Path) -> Path:
    shape_payload = _read_json(shape_family)
    blowoff_payload = _read_json(blowoff)
    portfolio_payload = _read_json(portfolio)
    current_by_pattern = _current_rows_by_pattern(portfolio_payload)
    patterns = _shape_patterns(shape_payload) + _blowoff_patterns(blowoff_payload)
    for pattern in patterns:
        current_rows = current_by_pattern.get(str(pattern.get("pattern_id")), [])
        pattern["current_candidate_count"] = len(current_rows)
        pattern["current_candidates"] = current_rows
        pattern["judgment"] = _judge(pattern, len(current_rows))
    patterns.sort(key=lambda row: (row["judgment"]["judgment"] == "keep_monitor", row.get("priority_score") or 0, row.get("n") or 0), reverse=True)
    report = {
        "schema_version": f"{AXIS_ID}_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "fixed_evaluation_conditions": {
            "source_shape_family": str(shape_family),
            "source_blowoff": str(blowoff),
            "source_portfolio": str(portfolio),
            "no_retraining": True,
            "no_rule_mutation": True,
            "review_only": True,
        },
        "pattern_count": len(patterns),
        "patterns": patterns,
        "family_leaderboard": patterns,
        "decision": {
            "candidate_local_decision": "multi_pattern_compare_ready" if patterns else "no_patterns_loaded",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "multiple short pattern families ranked from existing authoritative artifacts without changing rules",
        },
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
    }
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    _write_json(output_dir / "short_multi_pattern_compare.json", report)
    _write_json(output_root / "latest_short_multi_pattern_compare.json", {"run_root": str(output_dir), **report})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shape-family", type=Path, default=DEFAULT_SHAPE_FAMILY)
    parser.add_argument("--blowoff", type=Path, default=DEFAULT_BLOWOFF)
    parser.add_argument("--portfolio", type=Path, default=DEFAULT_PORTFOLIO)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(shape_family=args.shape_family, blowoff=args.blowoff, portfolio=args.portfolio, output_root=args.output_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
