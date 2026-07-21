from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_EVENTS = Path(
    r"G:\Tradex\2201_like_visual_event_dataset_v1\20260705T154230Z-tradex_2201_like_visual_event_dataset_v1\events_all.jsonl"
)
DEFAULT_OUTPUT = Path(
    r"G:\Tradex\2201_like_visual_event_dataset_v1\20260705T154230Z-tradex_2201_like_visual_event_dataset_v1\shape_proxy_eval_v1.json"
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _metric(rows: list[dict[str, Any]]) -> dict[str, Any]:
    ret20 = [float(row["forward"]["ret20"]) for row in rows if row["forward"].get("ret20") is not None]
    mae20 = [float(row["forward"]["mae20"]) for row in rows if row["forward"].get("mae20") is not None]
    adv10 = [float(row["forward"]["adverse10"]) for row in rows if row["forward"].get("adverse10") is not None]
    return {
        "n": len(rows),
        "unique_codes": len({row["code"] for row in rows}),
        "avg_ret20": sum(ret20) / len(ret20) if ret20 else None,
        "down20_rate": sum(value < 0 for value in ret20) / len(ret20) if ret20 else None,
        "close_down_10pct_20d_rate": sum(value <= -0.10 for value in ret20) / len(ret20) if ret20 else None,
        "touch_down_10pct_20d_rate": sum(value <= -0.10 for value in mae20) / len(mae20) if mae20 else None,
        "adverse_up_5pct_10d_rate": sum(value >= 0.05 for value in adv10) / len(adv10) if adv10 else None,
    }


def _features(row: dict[str, Any]) -> dict[str, Any]:
    return row.get("features") or {}


def _value(row: dict[str, Any], key: str, default: float = 0.0) -> float:
    value = _features(row).get(key)
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def long_ma_pressure(row: dict[str, Any]) -> bool:
    return (
        _value(row, "dist_ma60") <= 0.03
        and _value(row, "dist_ma100") <= 0.01
        and _value(row, "dist_ma20") <= 0.04
    )


def lower_high_or_overhead(row: dict[str, Any]) -> bool:
    return -0.18 <= _value(row, "drawdown_from_pre_high90") <= -0.04


def recovery_breakout_risk(row: dict[str, Any]) -> bool:
    return (
        _value(row, "rebound_from_pre_low45") >= 0.18
        and _value(row, "dist_ma7") >= 0.02
        and _value(row, "dist_ma20") >= 0.02
        and _value(row, "dist_ma60") >= 0.02
    )


def gap_holding_proxy(row: dict[str, Any]) -> bool:
    return (
        _value(row, "body_ratio") <= 0.08
        and _value(row, "dist_ma7") >= 0.00
        and _value(row, "dist_ma20") >= 0.04
        and _value(row, "drawdown_from_pre_high90") >= -0.04
    )


def sellable_proxy(row: dict[str, Any]) -> bool:
    return long_ma_pressure(row) and lower_high_or_overhead(row) and not recovery_breakout_risk(row)


def strict_sellable_proxy(row: dict[str, Any]) -> bool:
    return (
        _value(row, "dist_ma60") <= 0.00
        and _value(row, "dist_ma100") <= 0.00
        and _value(row, "dist_ma20") <= 0.03
        and -0.18 <= _value(row, "drawdown_from_pre_high90") <= -0.06
        and _value(row, "room_to_low60") >= 0.12
        and not gap_holding_proxy(row)
    )


def not_gap_holding(row: dict[str, Any]) -> bool:
    return not gap_holding_proxy(row)


def avoid_proxy(row: dict[str, Any]) -> bool:
    return recovery_breakout_risk(row) or gap_holding_proxy(row)


def run(events_path: Path, output_path: Path) -> dict[str, Any]:
    rows = _read_jsonl(events_path)
    groups = {
        "all": rows,
        "sellable_proxy": [row for row in rows if sellable_proxy(row)],
        "strict_sellable_proxy": [row for row in rows if strict_sellable_proxy(row)],
        "avoid_proxy": [row for row in rows if avoid_proxy(row)],
        "not_gap_holding": [row for row in rows if not_gap_holding(row)],
        "long_ma_pressure": [row for row in rows if long_ma_pressure(row)],
        "lower_high_or_overhead": [row for row in rows if lower_high_or_overhead(row)],
        "recovery_breakout_risk": [row for row in rows if recovery_breakout_risk(row)],
        "gap_holding_proxy": [row for row in rows if gap_holding_proxy(row)],
        "sellable_not_avoid": [row for row in rows if sellable_proxy(row) and not avoid_proxy(row)],
        "strict_sellable_not_avoid": [row for row in rows if strict_sellable_proxy(row) and not avoid_proxy(row)],
    }
    metrics = {name: _metric(group_rows) for name, group_rows in groups.items()}
    payload = {
        "schema_version": "tradex_2201_like_shape_proxy_eval_v1",
        "generated_at": _utc_now(),
        "boundary_owner": "TRADEX",
        "source_events": str(events_path),
        "manual_seed_basis": "manual_shape_label_seed_v1",
        "fixed_evaluation_conditions": {
            "pattern_id": "2201_like_rebound_upper_rejection",
            "universe": "events_all from fixed extraction",
            "period": "all rows with 20d forward outcome",
            "changed_axis": "shape proxy derived from manual image labels",
        },
        "proxy_definitions": {
            "sellable_proxy": "long_ma_pressure and lower_high_or_overhead and not recovery_breakout_risk",
            "strict_sellable_proxy": "stricter long MA pressure, lower high, room to low60 >= 12pct, not gap holding",
            "avoid_proxy": "recovery_breakout_risk or gap_holding_proxy",
        },
        "metrics": metrics,
        "decision": {
            "candidate_local_decision": "hold",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "proxy metrics are diagnostic only until compared against more visual labels",
        },
        "runtime_db_write": False,
        "meemee_unchanged": True,
        "production_ranking_changed": False,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=Path, default=DEFAULT_EVENTS)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    payload = run(args.events, args.output)
    print(json.dumps(payload["metrics"], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
