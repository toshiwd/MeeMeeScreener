from __future__ import annotations

import argparse
import json
import math
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any


NUMERIC_FIELDS = [
    "rank",
    "outcome_10d",
    "tradePriorityScore",
    "changePct",
    "candleUpperWickRatio",
    "candleLowerWickRatio",
    "distMa20Signed",
    "failedHighRetestRetestRatio",
    "failedHighRetestAnchorDropPct",
    "shortEntryActionabilityScore",
    "monthlyBoxPos",
    "visual_latest_price_position_pct",
    "visual_trend_slope_pct",
    "visual_recent_vertical_span_pct",
]


def _safe_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except Exception:
        return None
    return parsed if parsed == parsed and math.isfinite(parsed) else None


def _mean(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def _quantiles(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"min": None, "p25": None, "median": None, "p75": None, "max": None, "mean": None}
    ordered = sorted(values)

    def pick(q: float) -> float:
        idx = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * q)))
        return float(ordered[idx])

    return {
        "min": float(ordered[0]),
        "p25": pick(0.25),
        "median": float(median(ordered)),
        "p75": pick(0.75),
        "max": float(ordered[-1]),
        "mean": _mean(ordered),
    }


def _counter(values: list[Any]) -> dict[str, int]:
    return dict(Counter(str(value) for value in values if value is not None).most_common())


def _key(row: dict[str, Any]) -> tuple[str, str, str, int]:
    return (
        str(row.get("code") or ""),
        str(row.get("eval_date") or row.get("as_of_iso") or ""),
        str(row.get("side") or ""),
        int(row.get("rank") or 0),
    )


def _merge_rows(browser: dict[str, Any], source_by_key: dict[tuple[str, str, str, int], dict[str, Any]]) -> dict[str, Any]:
    source = source_by_key.get(_key(browser), {})
    source_fields = browser.get("source_fields") if isinstance(browser.get("source_fields"), dict) else {}
    visual_ai = browser.get("visual_ai") if isinstance(browser.get("visual_ai"), dict) else {}
    short_review = visual_ai.get("short_visual_review") if isinstance(visual_ai.get("short_visual_review"), dict) else {}
    long_review = visual_ai.get("long_visual_review") if isinstance(visual_ai.get("long_visual_review"), dict) else {}
    merged = {
        **source,
        "eval_date": browser.get("eval_date") or source.get("as_of_iso"),
        "code": browser.get("code") or source.get("code"),
        "name": browser.get("name") or source.get("name"),
        "side": browser.get("side") or source.get("side"),
        "rank": browser.get("rank") if browser.get("rank") is not None else source.get("rank"),
        "outcome_10d": browser.get("outcome_10d") if browser.get("outcome_10d") is not None else source.get("side_forward_return"),
        "prognosis": browser.get("prognosis") or source.get("prognosis"),
        "screenshot_path": browser.get("screenshot_path"),
        "chart_right_edge_date": browser.get("chart_right_edge_date"),
        "browser_asof_date_matches_eval_date": browser.get("browser_asof_date_matches_eval_date"),
        "visual_issue_tags": list(browser.get("visual_issue_tags") or []),
        "visual_short_decision": short_review.get("decision"),
        "visual_short_reasons": list(short_review.get("reasons") or []),
        "visual_long_decision": long_review.get("decision"),
        "visual_long_reasons": list(long_review.get("reasons") or []),
        "visual_latest_price_position_pct": _safe_float(visual_ai.get("latest_price_position_pct")),
        "visual_trend_slope_pct": _safe_float(visual_ai.get("trend_slope_pct")),
        "visual_recent_vertical_span_pct": _safe_float(visual_ai.get("recent_vertical_span_pct")),
        "visual_high_rejection_risk": bool(visual_ai.get("high_rejection_risk")),
        "visual_chase_risk": bool(visual_ai.get("chase_risk")),
        "visual_breakdown_risk": bool(visual_ai.get("breakdown_risk")),
    }
    for key, value in source_fields.items():
        merged.setdefault(key, value)
    for key in [
        "tradePriorityScore",
        "tradeEntryClass",
        "setupType",
        "changePct",
        "candleUpperWickRatio",
        "candleLowerWickRatio",
        "distMa20Signed",
        "monthlyBoxState",
        "monthlyBoxPos",
    ]:
        if merged.get(key) is None and source_fields.get(key) is not None:
            merged[key] = source_fields.get(key)
    return merged


def _group_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    tag_counter: Counter[str] = Counter()
    short_reason_counter: Counter[str] = Counter()
    long_reason_counter: Counter[str] = Counter()
    for row in rows:
        tag_counter.update(row.get("visual_issue_tags") or [])
        short_reason_counter.update(row.get("visual_short_reasons") or [])
        long_reason_counter.update(row.get("visual_long_reasons") or [])
    numeric = {
        field: _quantiles([value for row in rows if (value := _safe_float(row.get(field))) is not None])
        for field in NUMERIC_FIELDS
    }
    return {
        "count": len(rows),
        "tradeEntryClass": _counter([row.get("tradeEntryClass") for row in rows]),
        "setupType": _counter([row.get("setupType") for row in rows]),
        "monthlyBoxState": _counter([row.get("monthlyBoxState") for row in rows]),
        "visual_issue_tags": dict(tag_counter.most_common()),
        "visual_short_reasons": dict(short_reason_counter.most_common()),
        "visual_long_reasons": dict(long_reason_counter.most_common()),
        "numeric": numeric,
        "missing_fields": {
            field: sum(1 for row in rows if _safe_float(row.get(field)) is None)
            for field in NUMERIC_FIELDS
        },
    }


def _rate(counter: dict[str, int], total: int, key: str) -> float:
    return float(counter.get(key, 0)) / float(total or 1)


def _bad_good_deltas(bad: list[dict[str, Any]], good: list[dict[str, Any]]) -> dict[str, Any]:
    bad_summary = _group_summary(bad)
    good_summary = _group_summary(good)
    tag_keys = sorted(set(bad_summary["visual_issue_tags"]) | set(good_summary["visual_issue_tags"]))
    class_keys = sorted(set(bad_summary["tradeEntryClass"]) | set(good_summary["tradeEntryClass"]))
    numeric_deltas = {}
    for field in NUMERIC_FIELDS:
        bad_med = bad_summary["numeric"][field]["median"]
        good_med = good_summary["numeric"][field]["median"]
        numeric_deltas[field] = {
            "bad_median": bad_med,
            "good_median": good_med,
            "bad_minus_good_median": None if bad_med is None or good_med is None else bad_med - good_med,
            "bad_mean": bad_summary["numeric"][field]["mean"],
            "good_mean": good_summary["numeric"][field]["mean"],
        }
    return {
        "tag_rate_deltas": {
            key: {
                "bad_count": bad_summary["visual_issue_tags"].get(key, 0),
                "good_count": good_summary["visual_issue_tags"].get(key, 0),
                "bad_rate": _rate(bad_summary["visual_issue_tags"], len(bad), key),
                "good_rate": _rate(good_summary["visual_issue_tags"], len(good), key),
                "bad_minus_good_rate": _rate(bad_summary["visual_issue_tags"], len(bad), key) - _rate(good_summary["visual_issue_tags"], len(good), key),
            }
            for key in tag_keys
        },
        "trade_entry_class_rate_deltas": {
            key: {
                "bad_count": bad_summary["tradeEntryClass"].get(key, 0),
                "good_count": good_summary["tradeEntryClass"].get(key, 0),
                "bad_rate": _rate(bad_summary["tradeEntryClass"], len(bad), key),
                "good_rate": _rate(good_summary["tradeEntryClass"], len(good), key),
                "bad_minus_good_rate": _rate(bad_summary["tradeEntryClass"], len(bad), key) - _rate(good_summary["tradeEntryClass"], len(good), key),
            }
            for key in class_keys
        },
        "numeric_deltas": numeric_deltas,
    }


def _case(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "eval_date": row.get("eval_date"),
        "code": row.get("code"),
        "name": row.get("name"),
        "rank": row.get("rank"),
        "outcome_10d": _safe_float(row.get("outcome_10d")),
        "side_adverse_path_return": _safe_float(row.get("side_adverse_path_return")),
        "tradePriorityScore": _safe_float(row.get("tradePriorityScore")),
        "tradeEntryClass": row.get("tradeEntryClass"),
        "setupType": row.get("setupType"),
        "changePct": _safe_float(row.get("changePct")),
        "candleUpperWickRatio": _safe_float(row.get("candleUpperWickRatio")),
        "candleLowerWickRatio": _safe_float(row.get("candleLowerWickRatio")),
        "distMa20Signed": _safe_float(row.get("distMa20Signed")),
        "monthlyBoxState": row.get("monthlyBoxState"),
        "monthlyBoxPos": _safe_float(row.get("monthlyBoxPos")),
        "visual_issue_tags": list(row.get("visual_issue_tags") or []),
        "visual_short_decision": row.get("visual_short_decision"),
        "visual_short_reasons": list(row.get("visual_short_reasons") or []),
        "visual_chase_risk": bool(row.get("visual_chase_risk")),
        "screenshot_path": row.get("screenshot_path"),
    }


def _hypothesis(
    name: str,
    rows: list[dict[str, Any]],
    predicate,
    *,
    fields_required: list[str],
    owner: str,
    risk: str,
) -> dict[str, Any]:
    bad = [row for row in rows if row.get("prognosis") == "bad"]
    good = [row for row in rows if row.get("prognosis") == "good"]
    neutral = [row for row in rows if row.get("prognosis") == "neutral"]
    bad_hits = [row for row in bad if predicate(row)]
    good_hits = [row for row in good if predicate(row)]
    neutral_hits = [row for row in neutral if predicate(row)]
    total_hits = len(bad_hits) + len(good_hits) + len(neutral_hits)
    return {
        "hypothesis_name": name,
        "would_block_bad_count": len(bad_hits),
        "would_block_good_count": len(good_hits),
        "would_block_neutral_count": len(neutral_hits),
        "precision_estimate_bad_over_blocked": None if total_hits == 0 else len(bad_hits) / total_hits,
        "bad_coverage": None if not bad else len(bad_hits) / len(bad),
        "risk_of_overblocking": risk,
        "fields_required": fields_required,
        "belongs_to": owner,
        "representative_bad_hits": [_case(row) for row in sorted(bad_hits, key=lambda r: _safe_float(r.get("outcome_10d")) or 0.0)[:5]],
        "representative_good_hits": [_case(row) for row in sorted(good_hits, key=lambda r: -(_safe_float(r.get("outcome_10d")) or 0.0))[:5]],
    }


def run(browser_path: Path, source_path: Path, output_path: Path) -> dict[str, Any]:
    browser = json.loads(browser_path.read_text(encoding="utf-8"))
    source = json.loads(source_path.read_text(encoding="utf-8"))
    source_rows = [row for row in source.get("observations", []) if row.get("side") == "short"]
    source_by_key = {_key(row): row for row in source_rows}
    rows = [
        _merge_rows(row, source_by_key)
        for row in browser.get("records", [])
        if row.get("side") == "short"
    ]
    groups = {
        "short_good": [row for row in rows if row.get("prognosis") == "good"],
        "short_neutral": [row for row in rows if row.get("prognosis") == "neutral"],
        "short_bad": [row for row in rows if row.get("prognosis") == "bad"],
    }
    bad = groups["short_bad"]
    good = groups["short_good"]
    bad_untagged = [row for row in bad if not row.get("visual_issue_tags")]

    hypotheses = [
        _hypothesis(
            "short_visual_chase_without_short_confirmation",
            rows,
            lambda row: bool(row.get("visual_chase_risk")) and row.get("visual_short_decision") in {None, "watch"},
            fields_required=["visual_ai.chase_risk", "visual_ai.short_visual_review.decision"],
            owner="MeeMee display gate candidate",
            risk="medium: screenshot-derived and can catch strong trend reversals if used alone",
        ),
        _hypothesis(
            "failed_high_retest_without_upper_rejection",
            rows,
            lambda row: row.get("tradeEntryClass") == "failed_high_retest_short"
            and (_safe_float(row.get("candleUpperWickRatio")) is not None and (_safe_float(row.get("candleUpperWickRatio")) or 0.0) < 0.35)
            and not ((_safe_float(row.get("changePct")) is not None and (_safe_float(row.get("changePct")) or 0.0) <= -0.0075)),
            fields_required=["tradeEntryClass", "candleUpperWickRatio", "changePct"],
            owner="MeeMee display gate candidate",
            risk="low-medium: uses existing short rejection fields but may miss anchor-drop context absent from artifacts",
        ),
        _hypothesis(
            "short_at_visual_high_zone_chase_context",
            rows,
            lambda row: "visual_high_zone" in (row.get("visual_long_reasons") or [])
            and row.get("visual_short_decision") in {None, "watch"},
            fields_required=["visual_ai.long_visual_review.reasons", "visual_ai.short_visual_review.decision"],
            owner="TRADEX research challenger",
            risk="high: high-zone is not inherently bad for shorts; needs controlled validation",
        ),
        _hypothesis(
            "short_low_upper_wick_and_positive_box_position",
            rows,
            lambda row: (_safe_float(row.get("candleUpperWickRatio")) is not None and (_safe_float(row.get("candleUpperWickRatio")) or 0.0) < 0.25)
            and (_safe_float(row.get("monthlyBoxPos")) is not None and (_safe_float(row.get("monthlyBoxPos")) or 0.0) >= 0.65),
            fields_required=["candleUpperWickRatio", "monthlyBoxPos"],
            owner="TRADEX research challenger",
            risk="medium-high: coarse monthly position proxy, not enough as production gate",
        ),
    ]
    hypotheses.sort(
        key=lambda item: (
            -(item["precision_estimate_bad_over_blocked"] or 0.0),
            -(item["would_block_bad_count"] or 0),
            item["would_block_good_count"],
        )
    )

    artifact = {
        "schema_version": "meemee_short_failure_decomposition_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_artifacts": {
            "browser_validation": str(browser_path),
            "past_trade_top_validation": str(source_path),
        },
        "scope": {
            "diagnostic_only": True,
            "ranking_logic_changed": False,
            "actionable_gate_changed": False,
            "runtime_db_mutated": False,
        },
        "observation_count_short": len(rows),
        "short_good_count": len(groups["short_good"]),
        "short_neutral_count": len(groups["short_neutral"]),
        "short_bad_count": len(groups["short_bad"]),
        "group_distribution_summary": {
            name: _group_summary(group)
            for name, group in groups.items()
        },
        "bad_vs_good_deltas": _bad_good_deltas(bad, good),
        "worst_10_short_bad": [_case(row) for row in sorted(bad, key=lambda r: _safe_float(r.get("outcome_10d")) or 0.0)[:10]],
        "best_10_short_good": [_case(row) for row in sorted(good, key=lambda r: -(_safe_float(r.get("outcome_10d")) or 0.0))[:10]],
        "bad_untagged_cases": [_case(row) for row in sorted(bad_untagged, key=lambda r: _safe_float(r.get("outcome_10d")) or 0.0)],
        "bad_untagged_count": len(bad_untagged),
        "good_but_would_be_blocked_candidates": {
            hypothesis["hypothesis_name"]: hypothesis["representative_good_hits"]
            for hypothesis in hypotheses
            if hypothesis["would_block_good_count"] > 0
        },
        "blocker_hypotheses": hypotheses,
        "recommended_next_step": "test_one_gate_hypothesis",
        "final_decision": "diagnostic_complete_no_gate_change",
        "remaining_risks": [
            "failedHighRetestRetestRatio, failedHighRetestAnchorDropPct, and shortEntryActionabilityScore were not present in the source/browser artifacts, so they are reported as missing rather than inferred.",
            "Visual issue tags are diagnostic labels from existing screenshots and existing fields; they are not production gate evidence by themselves.",
        ],
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(artifact, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "artifact_path": str(output_path),
        "short_bad_count": artifact["short_bad_count"],
        "short_good_count": artifact["short_good_count"],
        "bad_untagged_count": artifact["bad_untagged_count"],
        "top_hypothesis": hypotheses[0],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Decompose bad short outcomes from browser validated past trade top candidates.")
    parser.add_argument("--browser", default="artifacts/actionability/past-trade-top-browser-validation-20260522/browser_validation_summary.json")
    parser.add_argument("--source", default="artifacts/actionability/past-trade-top-validation-20260522/past_trade_top_candidate_validation.json")
    parser.add_argument("--output", default="artifacts/actionability/short_failure_decomposition_20260522/short_failure_decomposition.json")
    args = parser.parse_args()
    result = run(Path(args.browser), Path(args.source), Path(args.output))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
