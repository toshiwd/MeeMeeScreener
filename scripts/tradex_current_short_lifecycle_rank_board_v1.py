from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


AXIS_ID = "current_short_lifecycle_rank_board_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\current_short_lifecycle_rank_board_v1")
DEFAULT_ANNOTATION = Path(
    r"G:\Tradex\current_short_setup_profitability_annotation_v1"
    r"\20260605T024034Z-current_short_setup_profitability_annotation_v1"
    r"\current_short_setup_profitability_annotation.json"
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _f(row: dict[str, Any], key: str, default: float = 0.0) -> float:
    try:
        value = row.get(key)
        if value is None:
            return default
        out = float(value)
    except Exception:
        return default
    return out if math.isfinite(out) else default


def _lifecycle_state(row: dict[str, Any]) -> tuple[str, list[str]]:
    setup = str(row.get("setup_state") or "")
    continuation = str(row.get("continuation_status") or "")
    visual = str(row.get("visual_micro_label") or "")
    final_status = str(row.get("final_review_status") or "")
    target = str(row.get("base_target_actionability") or "")
    expected_downside = _f(row, "expected_downside_pct")
    rr = _f(row, "risk_reward_to_sl8")
    reasons: list[str] = []

    if setup == "SetupRisk" or visual in {"BounceRiskHigh", "TooExtendedDown"}:
        reasons.append("bounce_or_extension_risk")
        return "Avoid", reasons
    if continuation == "ContinuationBlock":
        reasons.append("early_continuation_blocked")
        return "Exit", reasons
    if target in {"AvoidPoorReward", "AvoidNoTarget"} and setup != "SetupReady":
        reasons.append("poor_or_missing_downside_target")
        return "Avoid", reasons
    if setup == "SetupReady" and continuation == "ContinuationPending":
        reasons.append("setup_ready_wait_for_3_session_confirmation")
        return "Watch", reasons
    if setup == "SetupReady" and continuation == "ContinuationPermit":
        reasons.append("setup_ready_confirmed_continuation")
        if final_status == "Avoid":
            reasons.append("confirmed_but_final_status_avoid")
            return "AddWatch", reasons
        if expected_downside >= 0.08 and rr >= 0.75:
            reasons.append("sufficient_downside_and_rr_for_probe_review")
            return "Probe", reasons
        reasons.append("confirmed_but_reward_is_limited")
        return "AddWatch", reasons
    if setup == "SetupReady":
        reasons.append("setup_ready_without_continuation_permit")
        return "Watch", reasons
    if continuation == "ContinuationPermit" and final_status != "Avoid":
        reasons.append("continuation_confirmed_without_setup_ready")
        return "HoldReview", reasons
    if expected_downside > 0 and expected_downside < 0.045 and continuation in {"ContinuationPermit", "ContinuationWatch"}:
        reasons.append("near_shallow_target_take_profit_review")
        return "TakeProfit", reasons
    if setup == "SetupPressure":
        reasons.append("pressure_present_but_no_setup_ready")
        return "Watch", reasons
    reasons.append("no_clean_lifecycle_path")
    return "Avoid", reasons


def _state_priority(state: str) -> int:
    return {
        "Probe": 0,
        "Watch": 1,
        "AddWatch": 2,
        "HoldReview": 3,
        "TakeProfit": 4,
        "Exit": 5,
        "Avoid": 6,
    }.get(state, 9)


def _rank_score(row: dict[str, Any], state: str) -> float:
    original = _f(row, "original_score")
    confirmed_mean = _f(row, "setup_confirmed_mean_short_ret")
    confirmed_win = _f(row, "setup_confirmed_win_rate")
    confirmed_stop = _f(row, "setup_confirmed_stop_hit_rate", 0.5)
    immediate_mean = _f(row, "setup_immediate_mean_short_ret")
    expected_downside = _f(row, "expected_downside_pct")
    rr = _f(row, "risk_reward_to_sl8")
    permit = 1.0 if str(row.get("continuation_status")) == "ContinuationPermit" else 0.0
    pending = 1.0 if str(row.get("continuation_status")) == "ContinuationPending" else 0.0
    base = (
        original
        + confirmed_mean * 2.0
        + confirmed_win * 0.30
        - confirmed_stop * 0.35
        + immediate_mean
        + min(expected_downside, 0.15) * 0.70
        + min(max(rr, 0.0), 2.0) * 0.08
        + permit * 0.20
        + pending * 0.05
    )
    if state == "Avoid":
        base -= 1.0
    if state == "Exit":
        base -= 0.5
    return base


def _markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Current Short Lifecycle Rank Board v1",
        "",
        f"- authoritative_decision: `{payload['authoritative_decision']}`",
        f"- source_annotation_path: `{payload['source_annotation_path']}`",
        "",
        "## Counts",
        "",
    ]
    for key, value in payload["counts"].items():
        lines.append(f"- {key}: {value}")
    lines.extend(
        [
            "",
            "## Ranked Candidates",
            "",
            "| lifecycle_rank | state | original_rank | code | signal | final | setup | continuation | downside | rr | score | reasons |",
            "|---:|---|---:|---|---:|---|---|---|---:|---:|---:|---|",
        ]
    )
    for row in payload["candidates"]:
        lines.append(
            f"| {row['lifecycle_rank']} | {row['lifecycle_state']} | {row['original_rank']} | {row['code']} | "
            f"{row['signal_ymd']} | {row.get('final_review_status')} | {row.get('setup_state')} | "
            f"{row.get('continuation_status')} | {_f(row, 'expected_downside_pct'):.3f} | "
            f"{_f(row, 'risk_reward_to_sl8'):.2f} | {row['lifecycle_rank_score']:.4f} | "
            f"{', '.join(row['lifecycle_reasons'])} |"
        )
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- Review-only lifecycle classification, not a trade recommendation.",
            "- Ranking is within lifecycle intent; production ranking is unchanged.",
            "- AddWatch/HoldReview are pseudo-states because no real position ledger is attached.",
        ]
    )
    return "\n".join(lines) + "\n"


def run(annotation_path: Path, output_root: Path) -> Path:
    run_dir = output_root / _run_id()
    annotation = _read_json(annotation_path)
    candidates: list[dict[str, Any]] = []
    for row in annotation.get("candidates", []):
        state, reasons = _lifecycle_state(row)
        enriched = {
            **row,
            "lifecycle_state": state,
            "lifecycle_reasons": reasons,
            "lifecycle_state_priority": _state_priority(state),
        }
        enriched["lifecycle_rank_score"] = _rank_score(enriched, state)
        candidates.append(enriched)
    candidates.sort(
        key=lambda row: (
            int(row["lifecycle_state_priority"]),
            -float(row["lifecycle_rank_score"]),
            int(row["original_rank"]),
        )
    )
    for idx, row in enumerate(candidates, start=1):
        row["lifecycle_rank"] = idx
        row["lifecycle_rank_delta"] = int(row["original_rank"]) - idx
    counts = {
        "total_candidates": len(candidates),
        "lifecycle_state_counts": dict(Counter(str(row["lifecycle_state"]) for row in candidates)),
        "top10_lifecycle_state_counts": dict(Counter(str(row["lifecycle_state"]) for row in candidates[:10])),
        "setup_state_counts": dict(Counter(str(row.get("setup_state")) for row in candidates)),
        "continuation_status_counts": dict(Counter(str(row.get("continuation_status")) for row in candidates)),
    }
    payload = {
        "run_id": run_dir.name,
        "created_at": _utc_now(),
        "axis_id": AXIS_ID,
        "source_annotation_path": str(annotation_path),
        "classification_contract": {
            "primary_states": ["Watch", "Probe", "AddWatch", "HoldReview", "TakeProfit", "Exit", "Avoid"],
            "ranking_scope": "within lifecycle intent, review-only",
            "pseudo_states": ["AddWatch", "HoldReview"],
            "no_trade_recommendation": True,
            "production_change_allowed": False,
        },
        "counts": counts,
        "candidates": candidates,
        "authoritative_decision": (
            "current_short_lifecycle_rank_board_has_probe_or_watch_candidates"
            if counts["lifecycle_state_counts"].get("Probe", 0) or counts["lifecycle_state_counts"].get("Watch", 0)
            else "current_short_lifecycle_rank_board_no_actionable_review_states"
        ),
        "runtime_db_write": False,
        "meemee_modified": False,
        "production_ranking_modified": False,
    }
    _write_json(run_dir / "current_short_lifecycle_rank_board.json", payload)
    (run_dir / "current_short_lifecycle_rank_board_summary.md").write_text(_markdown(payload), encoding="utf-8")
    _write_json(
        run_dir / "_ARTIFACT_COMPLETE.json",
        {
            "status": "complete",
            "axis_id": AXIS_ID,
            "generated_at": _utc_now(),
            "required_files": [
                "current_short_lifecycle_rank_board.json",
                "current_short_lifecycle_rank_board_summary.md",
                "_ARTIFACT_COMPLETE.json",
            ],
        },
    )
    return run_dir


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--annotation-path", type=Path, default=DEFAULT_ANNOTATION)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(args.annotation_path, args.output_root))


if __name__ == "__main__":
    main()
