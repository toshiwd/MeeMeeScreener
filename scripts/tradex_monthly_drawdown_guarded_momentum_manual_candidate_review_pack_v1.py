from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import tradex_monthly_drawdown_guarded_momentum_starter_entry_pretest_v1 as pretest
from scripts import tradex_monthly_drawdown_guarded_momentum_top5_gate_v1 as top5_gate


AXIS_ID = "monthly_drawdown_guarded_momentum_manual_candidate_review_pack_v1"
SCHEMA_PREFIX = "tradex_monthly_drawdown_guarded_momentum_manual_candidate_review_pack"
DEFAULT_SOURCE_PRETEST_ROOT = Path(
    "G:/Tradex/monthly_drawdown_guarded_momentum_starter_entry_pretest_v1/"
    "20260515T003000Z-monthly-drawdown-guarded-momentum-starter-entry-pretest-v1"
)
DEFAULT_OUTPUT_PARENT = Path("G:/Tradex/monthly_drawdown_guarded_momentum_manual_candidate_review_pack_v1")
DEFAULT_RUN_ID = "20260515T010000Z-monthly-drawdown-guarded-momentum-manual-candidate-review-pack-v1"

REQUIRED_ARTIFACTS = [
    "review_pack_contract.json",
    "candidate_day_summary.json",
    "representative_top5_candidate_lists.json",
    "best_case_examples.json",
    "worst_case_examples.json",
    "weak_year_examples_2023_2025_2026.json",
    "added_vs_removed_candidate_examples.json",
    "family_mix_report.json",
    "human_selectable_examples.json",
    "risk_examples.json",
    "manual_review_questions.json",
    "next_axis_recommendation.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSONL at {path}:{line_no}: {exc}") from exc
    return rows


def _clean(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, (pd.Timestamp,)):
        return str(value.date())
    if hasattr(value, "item"):
        return value.item()
    return value


def _candidate_row(row: pd.Series, *, source: str) -> dict[str, Any]:
    return {
        "source": source,
        "event_date": str(row["event_date"]),
        "symbol": str(row["symbol"]),
        "rank": int(row["rank"]),
        "score": float(row["_candidate_score"]),
        "ret20_fwd": float(row["ret20_fwd"]),
        "win20": bool(row["win20"]),
        "severe_loss20": bool(row["severe_loss20"]),
        "is_bad_pick": bool(row["is_bad_pick"]),
        "human_selectable": bool(row["human_selectable"]),
        "is_big_winner_ret20_ge_10pct": bool(row["is_big_winner_ret20_ge_10pct"]),
        "is_future_top10_by_ret20": bool(row["is_future_top10_by_ret20"]),
        "baseline_candidate_flag": bool(row["baseline_candidate_flag"]),
        "momentum_candidate_flag": bool(row["momentum_candidate_flag"]),
        "ma5_h12_candidate_flag": bool(row["ma5_h12_candidate_flag"]),
        "monthly_prior_state": _clean(row.get("monthly_prior_state")),
    }


def _ranked(selected: pd.DataFrame) -> pd.DataFrame:
    ranked = selected.sort_values(["event_date", "_candidate_score", "symbol"], ascending=[True, False, True], kind="stable").copy()
    ranked["rank"] = ranked.groupby("event_date", sort=False).cumcount() + 1
    return ranked


def _date_summary(starter: pd.DataFrame, baseline: pd.DataFrame) -> list[dict[str, Any]]:
    starter_keys_by_date = {
        date: set(zip(group["event_date"].astype(str), group["symbol"].astype(str))) for date, group in starter.groupby("event_date")
    }
    baseline_keys_by_date = {
        date: set(zip(group["event_date"].astype(str), group["symbol"].astype(str))) for date, group in baseline.groupby("event_date")
    }
    rows: list[dict[str, Any]] = []
    for event_date, group in starter.groupby("event_date", sort=True):
        base_group = baseline[baseline["event_date"] == event_date]
        starter_keys = starter_keys_by_date.get(event_date, set())
        baseline_keys = baseline_keys_by_date.get(event_date, set())
        rows.append(
            {
                "event_date": str(event_date),
                "year": str(event_date)[:4],
                "starter_top5_avg_ret20": float(group["ret20_fwd"].mean()),
                "baseline_top5_avg_ret20": float(base_group["ret20_fwd"].mean()) if not base_group.empty else None,
                "top5_avg_ret20_delta_vs_baseline": float(group["ret20_fwd"].mean() - base_group["ret20_fwd"].mean())
                if not base_group.empty
                else None,
                "human_selectable_count": int(group["human_selectable"].sum()),
                "bad_pick_count": int(group["is_bad_pick"].sum()),
                "severe_loss_count": int(group["severe_loss20"].sum()),
                "big_winner_count": int(group["is_big_winner_ret20_ge_10pct"].sum()),
                "future_top10_count": int(group["is_future_top10_by_ret20"].sum()),
                "added_count": len(starter_keys - baseline_keys),
                "removed_count": len(baseline_keys - starter_keys),
                "candidate_count": int(len(group)),
            }
        )
    return rows


def _list_for_date(starter: pd.DataFrame, baseline: pd.DataFrame, event_date: str, reason: str) -> dict[str, Any]:
    starter_group = starter[starter["event_date"] == event_date]
    baseline_group = baseline[baseline["event_date"] == event_date]
    starter_keys = set(zip(starter_group["event_date"].astype(str), starter_group["symbol"].astype(str)))
    baseline_keys = set(zip(baseline_group["event_date"].astype(str), baseline_group["symbol"].astype(str)))
    return {
        "event_date": event_date,
        "reason": reason,
        "starter_top5": [_candidate_row(row, source="starter_entry") for _, row in starter_group.iterrows()],
        "baseline_top5": [_candidate_row(row, source="baseline") for _, row in baseline_group.iterrows()],
        "added_symbols": sorted(symbol for _, symbol in starter_keys - baseline_keys),
        "removed_symbols": sorted(symbol for _, symbol in baseline_keys - starter_keys),
    }


def _pick_representative_dates(day_rows: list[dict[str, Any]]) -> dict[str, list[str]]:
    ordered = sorted(day_rows, key=lambda row: row["event_date"])
    best = sorted(day_rows, key=lambda row: (row["top5_avg_ret20_delta_vs_baseline"] or -999, row["starter_top5_avg_ret20"]), reverse=True)[:5]
    worst = sorted(day_rows, key=lambda row: (row["top5_avg_ret20_delta_vs_baseline"] or 999, row["starter_top5_avg_ret20"]))[:5]
    high_human = sorted(day_rows, key=lambda row: (row["human_selectable_count"], row["starter_top5_avg_ret20"]), reverse=True)[:5]
    high_risk = sorted(day_rows, key=lambda row: (row["severe_loss_count"], row["bad_pick_count"], -row["starter_top5_avg_ret20"]), reverse=True)[:5]
    changed = sorted(day_rows, key=lambda row: (row["added_count"] + row["removed_count"], abs(row["top5_avg_ret20_delta_vs_baseline"] or 0)), reverse=True)[:5]
    latest = ordered[-5:]
    return {
        "best": [row["event_date"] for row in best],
        "worst": [row["event_date"] for row in worst],
        "high_human_selectable": [row["event_date"] for row in high_human],
        "high_risk": [row["event_date"] for row in high_risk],
        "changed": [row["event_date"] for row in changed],
        "latest": [row["event_date"] for row in latest],
    }


def _examples(starter: pd.DataFrame, baseline: pd.DataFrame, day_rows: list[dict[str, Any]], dates: list[str], reason: str) -> list[dict[str, Any]]:
    return [_list_for_date(starter, baseline, event_date, reason) for event_date in dates]


def _added_removed_examples(starter: pd.DataFrame, baseline: pd.DataFrame, day_rows: list[dict[str, Any]]) -> dict[str, Any]:
    changed_dates = [row["event_date"] for row in sorted(day_rows, key=lambda item: item["added_count"] + item["removed_count"], reverse=True)[:12]]
    return {
        "example_count": len(changed_dates),
        "examples": [_list_for_date(starter, baseline, event_date, "largest_added_removed_difference") for event_date in changed_dates],
    }


def _weak_year_examples(starter: pd.DataFrame, baseline: pd.DataFrame, day_rows: list[dict[str, Any]]) -> dict[str, Any]:
    years = ["2023", "2025", "2026"]
    rows: dict[str, Any] = {}
    for year in years:
        year_days = [row for row in day_rows if row["year"] == year]
        worst = sorted(year_days, key=lambda row: (row["top5_avg_ret20_delta_vs_baseline"] or 999, row["starter_top5_avg_ret20"]))[:5]
        rows[year] = {
            "day_count": len(year_days),
            "avg_delta_vs_baseline": float(pd.Series([row["top5_avg_ret20_delta_vs_baseline"] for row in year_days]).mean())
            if year_days
            else None,
            "examples": _examples(starter, baseline, day_rows, [row["event_date"] for row in worst], f"weak_year_{year}"),
        }
    return {"weak_years": rows}


def _family_mix(starter: pd.DataFrame) -> dict[str, Any]:
    family = top5_gate._family_share(starter)
    by_year = []
    for year, group in starter.assign(year=starter["event_date"].astype(str).str[:4]).groupby("year", sort=True):
        row = top5_gate._family_share(group)
        by_year.append({"year": str(year), **row})
    return {
        "overall": family,
        "by_year": by_year,
        "single_family_dominated": family["max_family_share"] > 0.90,
    }


def _human_selectable_examples(starter: pd.DataFrame, baseline: pd.DataFrame, day_rows: list[dict[str, Any]]) -> dict[str, Any]:
    dates_3plus = [row["event_date"] for row in sorted(day_rows, key=lambda row: (row["human_selectable_count"], row["starter_top5_avg_ret20"]), reverse=True)[:10]]
    dates_0 = [row["event_date"] for row in sorted([row for row in day_rows if row["human_selectable_count"] == 0], key=lambda row: row["starter_top5_avg_ret20"])[:10]]
    return {
        "three_plus_usable_examples": _examples(starter, baseline, day_rows, dates_3plus, "three_plus_human_selectable"),
        "zero_usable_examples": _examples(starter, baseline, day_rows, dates_0, "zero_human_selectable"),
    }


def _risk_examples(starter: pd.DataFrame, baseline: pd.DataFrame, day_rows: list[dict[str, Any]]) -> dict[str, Any]:
    severe_dates = [row["event_date"] for row in sorted(day_rows, key=lambda row: (row["severe_loss_count"], row["bad_pick_count"]), reverse=True)[:10]]
    bad_dates = [row["event_date"] for row in sorted(day_rows, key=lambda row: row["bad_pick_count"], reverse=True)[:10]]
    return {
        "severe_loss_examples": _examples(starter, baseline, day_rows, severe_dates, "highest_severe_loss_count"),
        "bad_pick_examples": _examples(starter, baseline, day_rows, bad_dates, "highest_bad_pick_count"),
    }


def run(args: argparse.Namespace) -> Path:
    output_root = args.output_parent / args.run_id
    output_root.mkdir(parents=True, exist_ok=True)
    source_decision = _read_json(args.source_pretest_root / "research_decision.json")
    source_manifest = _read_json(args.source_pretest_root / "run_manifest.json")
    source_pool = _read_json(args.source_pretest_root / "starter_entry_candidate_pool_report.json")
    source_complete = _read_json(args.source_pretest_root / "_ARTIFACT_COMPLETE.json")
    source_top5_gate_root = Path(source_manifest["source_top5_gate_root"])
    source_field_repair_root = Path(source_manifest["source_field_repair_root"])
    source_top5_leaderboard = _read_json(source_top5_gate_root / "strict_gate_leaderboard.json")
    source_rows = top5_gate._read_jsonl(source_field_repair_root / "repaired_common_top5_candidate_ledger.jsonl")
    frame = top5_gate._prepare_frame(source_rows)
    baseline_spec = top5_gate._variant_specs()[0]
    best_spec = source_top5_leaderboard["best_variant"]["spec"]
    baseline = pretest._select_with_score(frame, baseline_spec)
    starter = pretest._select_with_score(frame, best_spec)
    baseline = _ranked(baseline)
    starter = _ranked(starter)
    day_rows = _date_summary(starter, baseline)
    representative_dates = _pick_representative_dates(day_rows)
    generated_at = _utc_now()
    ready = bool(
        source_decision.get("authoritative_research_decision") == "starter_entry_pretest_keep"
        and source_complete.get("complete")
        and source_pool.get("pretest_gates", {}).get("top5_candidate_pool_clearly_better")
    )
    decision = "keep_candidate" if ready else "hold"
    authoritative = "manual_review_pack_ready" if ready else "manual_review_pack_hold"
    next_axis = "await_user_manual_review_decision_v1" if ready else "manual_review_pack_repair_v1"
    payloads: dict[str, dict[str, Any]] = {
        "review_pack_contract.json": {
            "schema_version": f"{SCHEMA_PREFIX}_contract_v1",
            "axis_id": AXIS_ID,
            "boundary": "TRADEX-only",
            "source_pretest_decision": source_decision.get("authoritative_research_decision"),
            "purpose": "human_reviews_top5_candidate_lists_and_selects_up_to_3",
            "auto_select_exactly_3": False,
            "review_pack_is_publish_bundle": False,
            "meemee_reflection_allowed": False,
            "future_labels_used_for_review_only": True,
            "future_labels_used_in_candidate_construction": False,
        },
        "candidate_day_summary.json": {
            "schema_version": f"{SCHEMA_PREFIX}_candidate_day_summary_v1",
            "day_count": len(day_rows),
            "rows": day_rows,
        },
        "representative_top5_candidate_lists.json": {
            "schema_version": f"{SCHEMA_PREFIX}_representative_top5_lists_v1",
            "date_groups": representative_dates,
            "examples": {
                group: _examples(starter, baseline, day_rows, dates, group) for group, dates in representative_dates.items()
            },
        },
        "best_case_examples.json": {
            "schema_version": f"{SCHEMA_PREFIX}_best_case_examples_v1",
            "examples": _examples(starter, baseline, day_rows, representative_dates["best"], "best_delta_days"),
        },
        "worst_case_examples.json": {
            "schema_version": f"{SCHEMA_PREFIX}_worst_case_examples_v1",
            "examples": _examples(starter, baseline, day_rows, representative_dates["worst"], "worst_delta_days"),
        },
        "weak_year_examples_2023_2025_2026.json": {
            "schema_version": f"{SCHEMA_PREFIX}_weak_year_examples_v1",
            **_weak_year_examples(starter, baseline, day_rows),
        },
        "added_vs_removed_candidate_examples.json": {
            "schema_version": f"{SCHEMA_PREFIX}_added_removed_examples_v1",
            **_added_removed_examples(starter, baseline, day_rows),
        },
        "family_mix_report.json": {
            "schema_version": f"{SCHEMA_PREFIX}_family_mix_report_v1",
            **_family_mix(starter),
        },
        "human_selectable_examples.json": {
            "schema_version": f"{SCHEMA_PREFIX}_human_selectable_examples_v1",
            **_human_selectable_examples(starter, baseline, day_rows),
        },
        "risk_examples.json": {
            "schema_version": f"{SCHEMA_PREFIX}_risk_examples_v1",
            **_risk_examples(starter, baseline, day_rows),
        },
        "manual_review_questions.json": {
            "schema_version": f"{SCHEMA_PREFIX}_manual_review_questions_v1",
            "questions": [
                "Do the representative top5 lists contain names you would plausibly buy?",
                "On high-human-selectable days, can you choose up to 3 without forcing a weak pick?",
                "Are added candidates understandable versus removed baseline candidates?",
                "Are 2023, 2025, and 2026 weak-period examples acceptable?",
                "Do severe-loss and zero-usable examples expose a blocker for shadow integration planning?",
            ],
            "decision_options": ["manual_review_approve", "manual_review_hold", "manual_review_reject"],
        },
        "next_axis_recommendation.json": {
            "schema_version": f"{SCHEMA_PREFIX}_next_axis_recommendation_v1",
            "axis_id": AXIS_ID,
            "decision": authoritative,
            "next": next_axis,
            "reason": "human_manual_review_required_before_shadow_integration_plan",
        },
        "research_decision.json": {
            "schema_version": f"{SCHEMA_PREFIX}_research_decision_v1",
            "generated_at_utc": generated_at,
            "research_phase": "manual_candidate_review_pack",
            "boundary": "TRADEX-only",
            "axis_moved": "monthly_drawdown_guarded_momentum_manual_candidate_review_pack",
            "source_pretest_decision": source_decision.get("authoritative_research_decision"),
            "manual_review_pack_created": True,
            "candidate_day_summary_created": True,
            "representative_top5_lists_created": True,
            "best_worst_weak_year_examples_created": True,
            "added_removed_examples_created": True,
            "manual_review_questions_created": True,
            "production_ranking_changed": False,
            "publish_bundle_created": False,
            "meemee_reflectable": False,
            "auto_select_exactly_3": False,
            "future_labels_used_for_review_only": True,
            "future_labels_used_in_candidate_construction": False,
            "silent_fallback_used": False,
            "research_fallback_used": False,
            "decision": decision,
            "authoritative_research_decision": authoritative,
            "typed_reasons": ["manual_review_pack_ready_for_user_inspection"] if ready else ["source_pretest_not_ready"],
        },
    }
    for name, payload in payloads.items():
        _write_json(output_root / name, payload)
    complete = {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "axis_id": AXIS_ID,
        "run_id": args.run_id,
        "artifact_root": str(output_root),
        "required_artifacts": REQUIRED_ARTIFACTS,
        "artifacts": {},
        "complete": False,
    }
    for name in REQUIRED_ARTIFACTS:
        path = output_root / name
        complete["artifacts"][name] = {"exists": path.exists(), "bytes": path.stat().st_size if path.exists() else 0}
    complete["complete"] = all(item["exists"] and item["bytes"] > 0 for name, item in complete["artifacts"].items() if name != "_ARTIFACT_COMPLETE.json")
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    complete["artifacts"]["_ARTIFACT_COMPLETE.json"] = {
        "exists": (output_root / "_ARTIFACT_COMPLETE.json").exists(),
        "bytes": (output_root / "_ARTIFACT_COMPLETE.json").stat().st_size,
    }
    complete["complete"] = all(item["exists"] and item["bytes"] > 0 for item in complete["artifacts"].values())
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return output_root


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-pretest-root", type=Path, default=DEFAULT_SOURCE_PRETEST_ROOT)
    parser.add_argument("--output-parent", type=Path, default=DEFAULT_OUTPUT_PARENT)
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    return parser


def main() -> None:
    output_root = run(_parser().parse_args())
    print(json.dumps({"axis_id": AXIS_ID, "artifact_root": str(output_root)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
