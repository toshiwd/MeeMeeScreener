from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


HYPOTHESIS_NAME = "short_visual_chase_without_short_confirmation"
SEVERE_BAD_THRESHOLD = -0.07
WATCH_CODES = {"4293", "9513"}


def _safe_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except Exception:
        return None
    return parsed if parsed == parsed else None


def _mean(values: list[float]) -> float | None:
    return float(sum(values) / len(values)) if values else None


def _distribution(rows: list[dict[str, Any]]) -> dict[str, int]:
    counter = Counter(str(row.get("prognosis") or "missing") for row in rows)
    return {
        "good": int(counter.get("good", 0)),
        "neutral": int(counter.get("neutral", 0)),
        "bad": int(counter.get("bad", 0)),
        "missing": int(counter.get("missing", 0)),
    }


def _guard_blocks(row: dict[str, Any]) -> bool:
    if row.get("side") != "short":
        return False
    visual_ai = row.get("visual_ai") if isinstance(row.get("visual_ai"), dict) else {}
    short_review = visual_ai.get("short_visual_review") if isinstance(visual_ai.get("short_visual_review"), dict) else {}
    return bool(visual_ai.get("chase_risk")) and short_review.get("decision") in {None, "watch"}


def _case(row: dict[str, Any]) -> dict[str, Any]:
    fields = row.get("source_fields") if isinstance(row.get("source_fields"), dict) else {}
    visual_ai = row.get("visual_ai") if isinstance(row.get("visual_ai"), dict) else {}
    short_review = visual_ai.get("short_visual_review") if isinstance(visual_ai.get("short_visual_review"), dict) else {}
    return {
        "eval_date": row.get("eval_date"),
        "code": row.get("code"),
        "name": row.get("name"),
        "rank": row.get("rank"),
        "outcome_10d": _safe_float(row.get("outcome_10d")),
        "prognosis": row.get("prognosis"),
        "tradePriorityScore": _safe_float(fields.get("tradePriorityScore")),
        "tradeEntryClass": fields.get("tradeEntryClass"),
        "setupType": fields.get("setupType"),
        "changePct": _safe_float(fields.get("changePct")),
        "candleUpperWickRatio": _safe_float(fields.get("candleUpperWickRatio")),
        "candleLowerWickRatio": _safe_float(fields.get("candleLowerWickRatio")),
        "monthlyBoxState": fields.get("monthlyBoxState"),
        "monthlyBoxPos": _safe_float(fields.get("monthlyBoxPos")),
        "visual_chase_risk": bool(visual_ai.get("chase_risk")),
        "visual_short_decision": short_review.get("decision"),
        "visual_short_reasons": list(short_review.get("reasons") or []),
        "visual_issue_tags": list(row.get("visual_issue_tags") or []),
        "screenshot_path": row.get("screenshot_path"),
    }


def _summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    outcomes = [_safe_float(row.get("outcome_10d")) for row in rows]
    outcomes = [value for value in outcomes if value is not None]
    severe = [row for row in rows if (_safe_float(row.get("outcome_10d")) or 0.0) <= SEVERE_BAD_THRESHOLD]
    return {
        "count": len(rows),
        "distribution": _distribution(rows),
        "mean_outcome_10d": _mean(outcomes),
        "severe_bad_count": len(severe),
        "severe_bad_threshold": SEVERE_BAD_THRESHOLD,
    }


def _per_date_simulation(short_rows: list[dict[str, Any]], kept_rows: list[dict[str, Any]]) -> dict[str, Any]:
    baseline_by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    kept_by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in short_rows:
        baseline_by_date[str(row.get("eval_date"))].append(row)
    for row in kept_rows:
        kept_by_date[str(row.get("eval_date"))].append(row)
    dates = sorted(baseline_by_date)
    affected = []
    fewer_than_5 = []
    for date in dates:
        before = sorted(baseline_by_date[date], key=lambda row: int(row.get("rank") or 999))
        after = sorted(kept_by_date.get(date, []), key=lambda row: int(row.get("rank") or 999))
        blocked = [row for row in before if _guard_blocks(row)]
        if blocked:
            affected.append(
                {
                    "eval_date": date,
                    "before_codes": [row.get("code") for row in before],
                    "after_codes": [row.get("code") for row in after],
                    "blocked_codes": [row.get("code") for row in blocked],
                    "after_count": len(after),
                }
            )
        if len(after) < 5:
            fewer_than_5.append(date)
    return {
        "reconstruction_type": "veto_only_simulation",
        "replacement_candidates_available": False,
        "reason": "source artifact contains only observed per-date top candidates, not the full ranked candidate pool after top5.",
        "eval_date_count": len(dates),
        "affected_eval_dates": affected,
        "days_with_fewer_than_5_candidates": fewer_than_5,
    }


def run(browser_path: Path, decomposition_path: Path, output_path: Path) -> dict[str, Any]:
    browser = json.loads(browser_path.read_text(encoding="utf-8"))
    decomposition = json.loads(decomposition_path.read_text(encoding="utf-8"))
    short_rows = [row for row in browser.get("records", []) if row.get("side") == "short"]
    blocked = [row for row in short_rows if _guard_blocks(row)]
    kept = [row for row in short_rows if not _guard_blocks(row)]
    baseline = _summary(short_rows)
    simulated = _summary(kept)
    baseline_mean = baseline["mean_outcome_10d"]
    simulated_mean = simulated["mean_outcome_10d"]
    severe_before = [row for row in short_rows if (_safe_float(row.get("outcome_10d")) or 0.0) <= SEVERE_BAD_THRESHOLD]
    severe_after = [row for row in kept if (_safe_float(row.get("outcome_10d")) or 0.0) <= SEVERE_BAD_THRESHOLD]
    watch_before = [row for row in short_rows if str(row.get("code")) in WATCH_CODES and row.get("prognosis") == "bad"]
    watch_after = [row for row in kept if str(row.get("code")) in WATCH_CODES and row.get("prognosis") == "bad"]

    bad_blocked = _distribution(blocked).get("bad", 0)
    good_blocked = _distribution(blocked).get("good", 0)
    final_decision = "hold_needs_more_data"
    if bad_blocked >= 5 and good_blocked == 0 and simulated_mean is not None and baseline_mean is not None and simulated_mean > baseline_mean:
        final_decision = "keep_for_runtime_design"

    per_date = _per_date_simulation(short_rows, kept)
    artifact = {
        "schema_version": "meemee_short_visual_guard_simulation_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_artifacts": {
            "short_failure_decomposition": str(decomposition_path),
            "browser_validation": str(browser_path),
        },
        "scope": {
            "debug_only": True,
            "production_logic_changed": False,
            "rankings_cache_changed": False,
            "runtime_db_mutated": False,
            "second_hypothesis_stacking": False,
        },
        "hypothesis": {
            "name": HYPOTHESIS_NAME,
            "predicate": "side == short AND visual_ai.chase_risk == true AND visual_ai.short_visual_review.decision in {null, watch}",
            "source_decomposition_top_hypothesis": next(
                (
                    item
                    for item in decomposition.get("blocker_hypotheses", [])
                    if item.get("hypothesis_name") == HYPOTHESIS_NAME
                ),
                None,
            ),
        },
        "baseline_summary": baseline,
        "simulated_summary": simulated,
        "blocked_count": len(blocked),
        "kept_count": len(kept),
        "blocked_distribution": _distribution(blocked),
        "kept_distribution": _distribution(kept),
        "outcome_delta": {
            "baseline_mean_outcome_10d": baseline_mean,
            "simulated_kept_mean_outcome_10d": simulated_mean,
            "delta_mean_outcome_10d": None if baseline_mean is None or simulated_mean is None else simulated_mean - baseline_mean,
        },
        "severe_bad_before_after": {
            "threshold": SEVERE_BAD_THRESHOLD,
            "before_count": len(severe_before),
            "after_count": len(severe_after),
            "blocked_severe_bad_count": len(severe_before) - len(severe_after),
            "before_examples": [_case(row) for row in sorted(severe_before, key=lambda row: _safe_float(row.get("outcome_10d")) or 0.0)],
            "after_examples": [_case(row) for row in sorted(severe_after, key=lambda row: _safe_float(row.get("outcome_10d")) or 0.0)],
            "watch_codes_4293_9513_bad_before": [_case(row) for row in sorted(watch_before, key=lambda row: _safe_float(row.get("outcome_10d")) or 0.0)],
            "watch_codes_4293_9513_bad_after": [_case(row) for row in sorted(watch_after, key=lambda row: _safe_float(row.get("outcome_10d")) or 0.0)],
        },
        "affected_eval_dates": per_date["affected_eval_dates"],
        "per_eval_date_top5_simulation": per_date,
        "blocked_examples": [_case(row) for row in sorted(blocked, key=lambda row: _safe_float(row.get("outcome_10d")) or 0.0)],
        "remaining_bad_examples": [_case(row) for row in sorted([row for row in kept if row.get("prognosis") == "bad"], key=lambda row: _safe_float(row.get("outcome_10d")) or 0.0)],
        "implementation_feasibility": {
            "can_be_runtime_gate_now": False,
            "required_runtime_fields": ["visual_ai.chase_risk", "visual_ai.short_visual_review.decision"],
            "dependency_on_visual_ai": "hard_dependency_on_browser_screenshot_or_equivalent_visual_classifier",
            "recommended_boundary": "MeeMee runtime design candidate only after replacing screenshot dependency with deterministic chart-shape fields or validating a lightweight visual classifier path.",
        },
        "final_decision": final_decision,
        "remaining_risks": [
            "Simulation is veto-only because replacement candidates below top5 are not present in the source artifacts.",
            "The guard depends on visual_ai screenshot-derived fields and is not production-ready.",
            "Sample size is short 60 observations across 12 eval dates; severe residuals remain.",
        ],
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(artifact, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "artifact_path": str(output_path),
        "blocked_distribution": artifact["blocked_distribution"],
        "kept_distribution": artifact["kept_distribution"],
        "outcome_delta": artifact["outcome_delta"],
        "severe_bad_before_after": {
            "before_count": artifact["severe_bad_before_after"]["before_count"],
            "after_count": artifact["severe_bad_before_after"]["after_count"],
        },
        "final_decision": final_decision,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Simulate short visual guard against browser-validated past top short candidates.")
    parser.add_argument("--browser", default="artifacts/actionability/past-trade-top-browser-validation-20260522/browser_validation_summary.json")
    parser.add_argument("--decomposition", default="artifacts/actionability/short_failure_decomposition_20260522/short_failure_decomposition.json")
    parser.add_argument("--output", default="artifacts/actionability/short_visual_guard_simulation_20260522/short_visual_guard_simulation.json")
    args = parser.parse_args()
    result = run(Path(args.browser), Path(args.decomposition), Path(args.output))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
