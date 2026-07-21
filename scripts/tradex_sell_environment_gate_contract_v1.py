#!/usr/bin/env python
"""Write the review-only sell environment gate contract."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


def dump(path: Path, obj: object) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)

    contract = {
        "schema_version": "tradex_sell_environment_gate_contract_v1",
        "owner": "TRADEX",
        "review_only": True,
        "proposition": "monthly chart environment is selected before daily setup, candle, MA, gap, wick, and support-resistance scoring",
        "decision_order": [
            "monthly_environment_gate",
            "daily_allowed_setup_family",
            "daily_entry_timing",
            "rebound_and_profit_take_management",
        ],
        "environments": {
            "BOX": {
                "required_context": ["monthly bounded prior range", "monthly range slope approximately flat", "monthly close remains inside the box or has returned into it"],
                "allowed_short": ["range ceiling rejection"],
                "disallowed_short": ["bear candle in range middle", "bear candle near unbroken range floor"],
                "paired_long_context_not_signal": "range floor is a buy-side location but short avoidance is not automatically a buy",
            },
            "UPTREND": {
                "required_context": ["monthly rising structure or confirmed monthly box breakout", "higher-price zone or mature above-MA duration"],
                "allowed_short": ["top-zone failed try", "double-top or recovery shortfall with bearish confirmation"],
                "disallowed_short": ["single daily bear candle", "ordinary pullback into rising support", "box-ceiling short when price is consolidating above the old monthly box without box re-entry"],
            },
            "DOWNTREND": {
                "required_context": ["declining or broken trend structure", "price below relevant resistance structure"],
                "allowed_short": ["support or range-floor breakdown", "broken support turns resistance then rejects", "gap-down plus confirmed prior-low break for add-short"],
                "disallowed_short": ["fresh short directly into unbroken MA60/100/200 support", "overextended decline without renewed breakdown"],
            },
            "AMBIGUOUS": {
                "required_context": ["environment requirements conflict or are not instrumented"],
                "allowed_short": [],
                "disallowed_short": ["new full-size short"],
                "action": "UNJUDGEABLE_OR_REVIEW_ONLY",
            },
            "POST_BOX_BREAKOUT_CONSOLIDATION": {
                "required_context": ["monthly box breakout is confirmed", "subsequent monthly consolidation remains above the former box ceiling", "no monthly box re-entry"],
                "allowed_short": ["top-zone failed try only after a distinct top structure forms"],
                "disallowed_short": ["former box-ceiling short", "daily bear candle without top failure", "new short with insufficient room to daily MA60/100/200 support"],
            },
        },
        "candidate_pit_features": {
            "monthly_environment": ["monthly prior box upper/lower", "monthly close position", "monthly breakout age", "monthly box re-entry flag", "monthly post-breakout consolidation count", "monthly MA slopes"],
            "daily_box_context_only": ["box_length_days", "box_upper_distance_pct", "box_lower_distance_pct", "box_breakdown_flag", "range20_pct"],
            "daily_trend_and_timing": ["ma20_slope5_atr", "ma60_slope5_atr", "dist_ma20_atr", "dist_ma60_atr", "above_ma_streaks"],
            "boundaries": ["support20", "resistance20", "support_break", "support_break_depth_atr", "MA60/100/200 distances"],
            "timing_only_after_gate": ["body_ratio", "upper_wick_ratio", "lower_wick_ratio", "close_pos", "gap", "retry shortfall"],
        },
        "hard_separations": [
            "new short versus add short",
            "direction versus tradeability",
            "short avoidance versus buy entry",
            "date-t entry versus date-t-plus-1 management",
        ],
        "evaluation_plan": {
            "fixed": ["same Nikkei225 universe", "same period", "same h1/h3/h5/h10 labels", "same no-cost short research condition"],
            "compare": "environment-matched setup against same-environment non-setup controls",
            "outcomes": ["down-first rate", "rebound-first rate", "down excursion", "up excursion", "per-year breadth", "code-month clustered bootstrap"],
            "keep": "effect direction survives frozen years and setup has sufficient breadth",
            "drop": "setup reverses by year or is indistinguishable from matched controls",
            "hold": "direction is plausible but breadth or instrumentation is insufficient",
        },
        "not_changed": ["MeeMee", "production ranking", "runtime DB", "existing S-state implementation", "model weights or thresholds"],
    }
    contract_path = args.output / "contract.json"
    dump(contract_path, contract)
    audit = {
        "schema_version": "tradex_sell_environment_gate_contract_v1.audit",
        "contract": {"path": str(contract_path), "sha256": sha(contract_path)},
        "boundary": {"owner": "TRADEX", "review_only": True, "meemee_changed": False, "runtime_db_write": False},
    }
    audit_path = args.output / "audit.json"
    dump(audit_path, audit)
    dump(args.output / "complete.json", {"complete": True, "sha256": {"contract.json": sha(contract_path), "audit.json": sha(audit_path)}})


if __name__ == "__main__":
    main()
