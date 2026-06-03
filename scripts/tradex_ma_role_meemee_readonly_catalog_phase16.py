from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


AXIS_ID = "ma_role_meemee_readonly_catalog_phase16"
DEFAULT_PHASE15_ROOT = Path(r"G:\Tradex\ma_role_transition_research_phase15")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\ma_role_meemee_readonly_catalog_phase16")
MIN_SPLIT_COUNT = 200
MIN_RET20_MEAN = 0.02
MIN_TEST_POSITIVE_RATE = 0.58
MAX_TEST_BAD_RATE = 0.20
MAX_RULES = 25


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _latest_phase15_run(root: Path) -> Path:
    latest = _read_json(root / "latest_research_decision.json")
    run_root = Path(str(latest.get("run_root") or ""))
    if not run_root.exists():
        raise RuntimeError("phase15_latest_run_root_missing")
    return run_root


def _state_parts(text: str) -> dict[str, str]:
    parts: dict[str, str] = {}
    for item in str(text or "").split("|"):
        if ":" not in item:
            continue
        key, value = item.split(":", 1)
        parts[key] = value
    return parts


def _display_label(state: dict[str, Any]) -> str:
    entry = _state_parts(state["entry_exit"])
    trend = _state_parts(state["trend"])
    env = _state_parts(state["environment"])
    return " / ".join(
        [
            f"candle:{entry.get('candle_shape')}+{entry.get('three_candle')}",
            f"MA7/20:{entry.get('close_ma7')},{entry.get('close_ma20')},{entry.get('ma7_ma20')}",
            f"MA60:{trend.get('close_ma60')},{trend.get('ma60_slope')}",
            f"environment:{env.get('alignment')},{env.get('ma100_slope')},{env.get('ma200_slope')}",
        ]
    )


def _rule_id(index: int) -> str:
    return f"ma_candle_review_{index:03d}"


def _qualifies(state: dict[str, Any]) -> bool:
    return (
        bool(state.get("stable_positive_ret20"))
        and int(state.get("minimum_split_count") or 0) >= MIN_SPLIT_COUNT
        and float(state.get("minimum_ret20_mean") or 0) >= MIN_RET20_MEAN
        and float(state.get("test_positive_ret20_rate") or 0) >= MIN_TEST_POSITIVE_RATE
        and float(state.get("test_bad_ret20_lt_minus_5pct_rate") or 1) <= MAX_TEST_BAD_RATE
    )


def _build_rules(states: list[dict[str, Any]]) -> list[dict[str, Any]]:
    qualified = [state for state in states if _qualifies(state)]
    qualified.sort(
        key=lambda state: (
            -float(state["minimum_ret20_mean"]),
            -int(state["minimum_split_count"]),
            float(state["test_bad_ret20_lt_minus_5pct_rate"]),
        )
    )
    rules = []
    for index, state in enumerate(qualified[:MAX_RULES], start=1):
        rules.append(
            {
                "rule_id": _rule_id(index),
                "display_label": _display_label(state),
                "entry_exit": state["entry_exit"],
                "trend": state["trend"],
                "environment": state["environment"],
                "evidence": {
                    "minimum_split_count": state["minimum_split_count"],
                    "minimum_ret20_mean": state["minimum_ret20_mean"],
                    "ret20_mean_by_split": state["ret20_mean_by_split"],
                    "test_positive_ret20_rate": state["test_positive_ret20_rate"],
                    "test_bad_ret20_lt_minus_5pct_rate": state["test_bad_ret20_lt_minus_5pct_rate"],
                },
                "meemee_display_mode": "review_only",
                "actionability": "watch_context_not_trade_signal",
            }
        )
    return rules


def run(*, phase15_root: Path, output_root: Path) -> Path:
    phase15_run = _latest_phase15_run(phase15_root)
    leaderboard = _read_json(phase15_run / "stable_state_leaderboard.json")
    states = leaderboard.get("states") if isinstance(leaderboard.get("states"), list) else []
    rules = _build_rules(states)
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    catalog = {
        "schema_version": "tradex_ma_role_meemee_readonly_catalog_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "source_phase15_run": str(phase15_run),
        "meemee_contract": {
            "read_only": True,
            "display_only": True,
            "ranking_effect": False,
            "runtime_db_write": False,
            "automatic_trade_action": False,
            "validated_buy_claim": False,
            "validated_sell_claim": False,
        },
        "fixed_roles": {"entry_exit": [7, 20], "trend": [60], "environment": [100, 200]},
        "gates": {
            "minimum_split_count": MIN_SPLIT_COUNT,
            "minimum_ret20_mean": MIN_RET20_MEAN,
            "minimum_test_positive_ret20_rate": MIN_TEST_POSITIVE_RATE,
            "maximum_test_bad_ret20_lt_minus_5pct_rate": MAX_TEST_BAD_RATE,
        },
        "rule_count": len(rules),
        "rules": rules,
    }
    decision = {
        "schema_version": "tradex_ma_role_meemee_readonly_catalog_decision_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "authoritative_rollup_decision": "keep_for_meemee_readonly_catalog" if rules else "drop_no_meemee_readonly_candidates",
        "reason_type": "display_only_catalog_generated" if rules else "no_candidate_passed_display_gates",
        "source_phase15_run": str(phase15_run),
        "rule_count": len(rules),
        "meemee_reflectable": bool(rules),
        "meemee_reflection_mode": "read_only_review_panel" if rules else None,
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "automatic_trade_action": False,
        "non_scope": ["production ranking", "automatic trade action", "validated buy claim", "validated sell claim"],
    }
    _write_json(output_dir / "meemee_readonly_signal_catalog.json", catalog)
    _write_json(output_dir / "research_decision.json", decision)
    _write_json(output_root / "latest_research_decision.json", {"run_root": str(output_dir), **decision})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase15-root", type=Path, default=DEFAULT_PHASE15_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(phase15_root=args.phase15_root, output_root=args.output_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
