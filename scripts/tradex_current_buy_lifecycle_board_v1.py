from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import tradex_position_lifecycle_state_machine_v1 as lifecycle


AXIS_ID = "current_buy_lifecycle_board_v1"
DEFAULT_SOURCE_ROOT = Path(r"G:\Tradex\practical_current_review_board_v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\current_buy_lifecycle_board_v1")
SOURCE_FILE = "current_review_board.csv"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _latest_source(root: Path) -> Path:
    candidates = [path / SOURCE_FILE for path in root.iterdir() if path.is_dir() and (path / SOURCE_FILE).exists()]
    if not candidates:
        raise FileNotFoundError(f"{SOURCE_FILE} not found under {root}")
    return max(candidates, key=lambda path: (path.stat().st_mtime, path.parent.name))


def _bool_columns(rows: pd.DataFrame) -> None:
    columns = [
        "weekly_supportive_flag",
        "failed_high_flag",
        "volatility_compression_breakout_preparation_candidate",
        "constructive_pullback_support_bullish_confirmation_reference_match",
        "early_trend_reclaim_controlled_extension_candidate",
        "monthly_weekly_supportive_daily_confirmation_candidate",
    ]
    for column in columns:
        if column not in rows:
            raise ValueError(f"current board missing lifecycle column: {column}")
        rows[column] = rows[column].astype(str).str.lower().eq("true")


def run(*, source_path: Path | None, source_root: Path, output_root: Path, limit: int = 100) -> Path:
    resolved_source = source_path or _latest_source(source_root)
    rows = pd.read_csv(resolved_source)
    _bool_columns(rows)
    rows["entry_state"] = rows.apply(lifecycle._entry_state, axis=1)
    rows["held_position_review_state"] = rows.apply(lifecycle._held_state, axis=1)
    rows["lifecycle_reasons"] = rows.apply(lifecycle._reasons, axis=1)
    rows["lifecycle_state_priority"] = rows["entry_state"].map({"Starter": 0, "Accumulate": 1, "Wait": 2, "Avoid": 3})
    rows = rows.sort_values(
        ["lifecycle_state_priority", "entry_actionability_score", "upside_probability_20d"],
        ascending=[True, False, False],
    ).head(max(1, int(limit)))

    candidates: list[dict[str, Any]] = []
    for rank, row in enumerate(rows.to_dict(orient="records"), start=1):
        candidates.append(
            {
                "code": str(row.get("code") or ""),
                "as_of_date": int(row["as_of_date"]) if pd.notna(row.get("as_of_date")) else None,
                "lifecycle_rank": rank,
                "entry_state": row.get("entry_state"),
                "held_position_review_state": row.get("held_position_review_state"),
                "entry_actionability_score": row.get("entry_actionability_score"),
                "upside_probability_20d": row.get("upside_probability_20d"),
                "downside_risk_probability_20d": row.get("downside_risk_probability_20d"),
                "review_bucket": row.get("review_bucket"),
                "avoid_level": row.get("avoid_level"),
                "event_risk_contract_status": row.get("event_risk_contract_status"),
                "lifecycle_reasons": row.get("lifecycle_reasons") or [],
            }
        )

    entry_counts = dict(Counter(str(row["entry_state"]) for row in candidates))
    held_counts = dict(Counter(str(row["held_position_review_state"]) for row in candidates))
    output = output_root / _run_id()
    output.mkdir(parents=True, exist_ok=False)
    payload = {
        "run_id": output.name,
        "created_at": _utc_now(),
        "axis_id": AXIS_ID,
        "source_artifact_path": str(resolved_source),
        "classification_contract": {
            "entry_states": ["Starter", "Accumulate", "Wait", "Avoid"],
            "held_position_review_states": ["Hold", "HoldCaution", "ExitReview"],
            "entry_and_held_states_are_separate": True,
            "position_ledger_used": False,
            "automatic_trade_action": False,
            "review_only": True,
        },
        "counts": {
            "total_candidates": len(candidates),
            "entry_state_counts": entry_counts,
            "held_position_review_state_counts": held_counts,
        },
        "candidates": candidates,
        "authoritative_decision": (
            "current_buy_lifecycle_has_starter_or_accumulate_candidates"
            if entry_counts.get("Starter", 0) or entry_counts.get("Accumulate", 0)
            else "current_buy_lifecycle_no_starter_or_accumulate_candidates"
        ),
        "runtime_db_write": False,
        "meemee_modified": False,
        "production_ranking_modified": False,
    }
    _write_json(output / "current_buy_lifecycle_board.json", payload)
    _write_json(
        output / "_ARTIFACT_COMPLETE.json",
        {"status": "complete", "axis_id": AXIS_ID, "generated_at": _utc_now(), "required_files": ["current_buy_lifecycle_board.json"]},
    )
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-path", type=Path)
    parser.add_argument("--source-root", type=Path, default=DEFAULT_SOURCE_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()
    print(run(source_path=args.source_path, source_root=args.source_root, output_root=args.output_root, limit=args.limit))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
