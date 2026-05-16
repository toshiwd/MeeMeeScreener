from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import tradex_ma5_reclaim_hypothesis_batch_v1 as mod


def _ledger_frame() -> pd.DataFrame:
    rows = []
    for idx in range(80):
        rows.append(
            {
                "symbol": f"70{idx % 8:02d}",
                "entry_date": f"2025-01-{idx % 20 + 1:02d}",
                "exit_date": f"2025-02-{idx % 20 + 1:02d}",
                "ret": 0.03 if idx < 40 else -0.01,
                "mfe": 0.06,
                "mae": -0.02,
                "win": idx < 40,
                "severe_loss": False,
                "exit_reason": "close_below_ma20",
                "holding_days": 8,
                "ma_stack": "bull_stack_5_20_60" if idx < 40 else "pullback_in_ma20_above_60",
                "price_vs_ma20": "price_above_ma20",
                "price_vs_ma60": "price_above_ma60" if idx < 40 else "price_below_ma60",
                "ma20_vs_ma60": "ma20_above_ma60",
                "ma20_slope_state": "ma20_rising",
                "ma60_slope_state": "ma60_rising" if idx < 40 else "ma60_falling",
            }
        )
    return pd.DataFrame(rows)


def _write_ledger(path: Path, frame: pd.DataFrame | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    (frame if frame is not None else _ledger_frame()).to_json(path, orient="records", lines=True, force_ascii=False)


def test_hypothesis_filters_do_not_use_future_labels() -> None:
    assert mod.SIGNAL_FEATURE_COLUMNS.isdisjoint(mod.LABEL_COLUMNS)
    audit = mod.build_feature_availability_audit(_ledger_frame())
    assert audit["used_future_labels_in_hypothesis_filters"] is False
    assert audit["silent_fallback_used"] is False


def test_evaluate_hypotheses_ranks_bull_stack_above_negative_control() -> None:
    rows = mod.evaluate_hypotheses(_ledger_frame())
    by_id = {row["hypothesis_id"]: row for row in rows}

    assert by_id["h10_bull_stack_ma60_rising"]["avg_ret"] > by_id["h15_pullback_price_below_ma60"]["avg_ret"]
    assert by_id["h15_pullback_price_below_ma60"]["hypothesis_decision"] in {"drop", "insufficient_sample"}


def test_run_writes_required_artifacts(tmp_path: Path) -> None:
    ledger = tmp_path / "trade_ledger.jsonl"
    frame = pd.concat([_ledger_frame()] * 20, ignore_index=True)
    _write_ledger(ledger, frame)

    result = mod.run_ma5_reclaim_hypothesis_batch_v1(
        source_trade_ledger=ledger,
        output_root=tmp_path / "out",
        run_id="smoke",
    )
    output_dir = Path(result["output_dir"])

    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (output_dir / artifact).exists(), artifact

    complete = json.loads((output_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    decision = json.loads((output_dir / "research_decision.json").read_text(encoding="utf-8"))
    leaderboard = json.loads((output_dir / "hypothesis_leaderboard.json").read_text(encoding="utf-8"))

    assert complete["complete"] is True
    assert complete["silent_fallback_used"] is False
    assert decision["candidate_scoring_created"] is False
    assert leaderboard["overview"]["hypothesis_count"] == len(mod.hypothesis_specs())
