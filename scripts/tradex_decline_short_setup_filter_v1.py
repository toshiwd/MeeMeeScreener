from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any


AXIS_ID = "decline_short_setup_filter_v1"
DEFAULT_EVENT_DIR = Path(r"G:\Tradex\decline_shape_event_mining_full_count_v1\20260701T012047Z-decline_shape_event_mining_v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\decline_short_setup_filter_v1")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.write_text("".join(json.dumps(row, ensure_ascii=False, default=str) + "\n" for row in rows), encoding="utf-8")


def _round(value: float | None, digits: int = 6) -> float | None:
    return None if value is None else round(float(value), digits)


def _bool(feature: dict[str, Any] | None, key: str) -> bool:
    return bool(feature and feature.get(key) is True)


def _num(feature: dict[str, Any] | None, key: str) -> float | None:
    value = feature.get(key) if feature else None
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _setup_tags(event: dict[str, Any]) -> list[str]:
    trigger = event.get("trigger_features") or {}
    breakdown = event.get("breakdown_features") or {}
    tags: list[str] = []

    high_zone = (_num(trigger, "close_position_60") or 0.0) >= 0.75 or _bool(trigger, "near_high60")
    very_extended = (_num(trigger, "dist_ma20") or 0.0) >= 0.08 or (_num(trigger, "dist_ma60") or 0.0) >= 0.18
    upper_wick = (_num(trigger, "upper_wick_ratio") or 0.0) >= 0.40
    failed_high = _bool(trigger, "failed_high20")
    bull_stack = _bool(trigger, "ma_stack_bull")
    lost_ma7 = _bool(trigger, "cross_down_ma7") or _bool(breakdown, "below_ma7")
    lost_ma20 = _bool(trigger, "cross_down_ma20") or _bool(breakdown, "cross_down_ma20") or _bool(breakdown, "below_ma20")
    support_break = _bool(breakdown, "break_support20_close") or _bool(breakdown, "break_support20_low")
    large_bear = _bool(breakdown, "large_body") and _bool(breakdown, "bearish")
    volume_break = (_num(breakdown, "volume_ratio20") or 0.0) >= 1.5
    ma20_pressure = _bool(trigger, "below_ma20") and ((_num(trigger, "ma20_slope5") or 0.0) <= 0.0)
    below_ma60_after = _bool(breakdown, "below_ma60") or _bool(event.get("bottom_features"), "below_ma60")

    if high_zone and upper_wick and lost_ma20:
        tags.append("top_upper_wick_then_ma20_loss")
    if high_zone and failed_high and lost_ma20:
        tags.append("top_failed_high_then_ma20_loss")
    if bull_stack and high_zone and lost_ma7 and lost_ma20:
        tags.append("bull_stack_lost_ma7_to_ma20")
    if high_zone and very_extended and (upper_wick or failed_high):
        tags.append("extended_high_rejection")
    if ma20_pressure and support_break:
        tags.append("ma20_pressure_support_break")
    if lost_ma20 and large_bear and volume_break:
        tags.append("ma20_loss_large_bear_volume")
    if lost_ma20 and below_ma60_after:
        tags.append("ma20_loss_to_ma60_under")
    return tags


def _score(event: dict[str, Any], setup_tags: list[str]) -> float:
    trigger = event.get("trigger_features") or {}
    breakdown = event.get("breakdown_features") or {}
    score = 0.0
    score += min(0.25, max(0.0, (_num(trigger, "close_position_60") or 0.0) - 0.65))
    score += 0.12 if (_num(trigger, "upper_wick_ratio") or 0.0) >= 0.40 else 0.0
    score += 0.12 if _bool(trigger, "failed_high20") else 0.0
    score += 0.10 if _bool(trigger, "ma_stack_bull") else 0.0
    score += 0.15 if _bool(breakdown, "cross_down_ma20") else 0.0
    score += 0.12 if _bool(breakdown, "break_support20_close") else 0.0
    score += 0.10 if (_num(breakdown, "volume_ratio20") or 0.0) >= 1.5 else 0.0
    score += 0.08 if _bool(breakdown, "large_body") and _bool(breakdown, "bearish") else 0.0
    score += 0.04 * min(3, len(setup_tags))
    return round(score, 6)


def _stage_for_entry(event: dict[str, Any], setup_tags: list[str]) -> str:
    if any(tag in setup_tags for tag in ("top_upper_wick_then_ma20_loss", "top_failed_high_then_ma20_loss", "extended_high_rejection")):
        return "trigger"
    return "breakdown" if event.get("breakdown_as_of") else "trigger"


def _summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"count": 0}
    declines = [float(row["decline_pct"]) for row in rows]
    rebounds = [float(row["rebound_10d_from_bottom"]) for row in rows if row.get("rebound_10d_from_bottom") is not None]
    return {
        "count": len(rows),
        "unique_code_count": len({row["code"] for row in rows}),
        "decline_pct_mean": _round(mean(declines)),
        "decline_pct_median": _round(median(declines)),
        "deep_decline_20pct_rate": _round(sum(1 for value in declines if value <= -0.20) / len(declines)),
        "bottom_rebound_10d_mean": _round(mean(rebounds)) if rebounds else None,
    }


def run(*, event_dir: Path, output_root: Path, min_score: float, max_images_per_tag: int) -> Path:
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    events = _read_jsonl(event_dir / "decline_events.jsonl")
    selected: list[dict[str, Any]] = []
    all_tag_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        tags = _setup_tags(event)
        if not tags:
            continue
        score = _score(event, tags)
        entry_stage = _stage_for_entry(event, tags)
        row = {
            "event_id": event["event_id"],
            "code": event["code"],
            "entry_stage": entry_stage,
            "entry_as_of": event["trigger_as_of"] if entry_stage == "trigger" else event.get("breakdown_as_of"),
            "trigger_as_of": event["trigger_as_of"],
            "breakdown_as_of": event.get("breakdown_as_of"),
            "bottom_as_of": event["bottom_as_of"],
            "decline_pct": event["decline_pct"],
            "rebound_10d_from_bottom": event.get("rebound_10d_from_bottom"),
            "primary_decline_tag": event.get("primary_tag"),
            "short_setup_tags": tags,
            "short_setup_score": score,
            "trigger_features": event.get("trigger_features"),
            "breakdown_features": event.get("breakdown_features"),
        }
        for tag in tags:
            all_tag_rows[tag].append(row)
        if score >= min_score:
            selected.append(row)

    selected.sort(key=lambda row: (-float(row["short_setup_score"]), row["entry_as_of"], row["code"]))
    _write_jsonl(output_dir / "short_setup_candidates.jsonl", selected)
    tag_summary = {
        tag: _summarize(rows)
        for tag, rows in sorted(all_tag_rows.items(), key=lambda item: (-len(item[1]), item[0]))
    }
    selected_tag_summary = {
        tag: _summarize([row for row in selected if tag in row["short_setup_tags"]])
        for tag in tag_summary
    }
    image_plan: list[dict[str, Any]] = []
    per_tag_counts: Counter[str] = Counter()
    for row in selected:
        for tag in row["short_setup_tags"]:
            if per_tag_counts[tag] >= max_images_per_tag:
                continue
            as_of = int(row["entry_as_of"])
            text = str(as_of)
            image_plan.append({
                "event_id": row["event_id"],
                "tag": tag,
                "stage": row["entry_stage"],
                "code": row["code"],
                "as_of": as_of,
                "as_of_iso": f"{text[:4]}-{text[4:6]}-{text[6:8]}",
                "short_setup_score": row["short_setup_score"],
                "decline_pct": row["decline_pct"],
            })
            per_tag_counts[tag] += 1
    _write_jsonl(output_dir / "short_setup_image_plan.jsonl", image_plan)
    audit = {
        "schema_version": "tradex_decline_short_setup_filter_v1_audit",
        "generated_at": _utc_now(),
        "boundary_owner": "TRADEX",
        "axis_id": AXIS_ID,
        "event_dir": str(event_dir),
        "input_event_count": len(events),
        "tagged_event_count": len({row["event_id"] for rows in all_tag_rows.values() for row in rows}),
        "selected_event_count": len(selected),
        "min_score": min_score,
        "tag_summary_all_decline_events": tag_summary,
        "tag_summary_selected": selected_tag_summary,
        "image_plan_count": len(image_plan),
        "image_plan_policy": f"top scored entries, max {max_images_per_tag} per tag",
        "selection_bias_warning": "These rates are conditional on already-found decline events; run same tags over all days before claiming live short win rate.",
        "labels_used_in_image_rendering": False,
        "runtime_db_write": False,
        "production_ranking_changed": False,
        "judgment": "pass_short_setup_filter_for_image_review" if selected else "hold_no_short_setup_candidates",
        "non_scope": ["live short win-rate claim", "model training", "MeeMee reflection", "production ranking mutation"],
    }
    _write_json(output_dir / "short_setup_filter_audit.json", audit)
    _write_json(output_root / "latest_short_setup_filter_audit.json", {"run_root": str(output_dir), **audit})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-dir", type=Path, default=DEFAULT_EVENT_DIR)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--min-score", type=float, default=0.55)
    parser.add_argument("--max-images-per-tag", type=int, default=80)
    args = parser.parse_args()
    print(run(event_dir=args.event_dir, output_root=args.output_root, min_score=args.min_score, max_images_per_tag=args.max_images_per_tag))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
