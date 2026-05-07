from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_entry_timing_confirmed_signal_v1 import (
    LABEL_COLUMNS,
    TIMING_FEATURE_COLUMNS,
    _apply_candidate,
    _build_outputs,
)


def _frame() -> pd.DataFrame:
    rows = []
    for rank in range(1, 21):
        rows.append(
            {
                "anchor_date": "2026-01-05",
                "side": "long",
                "symbol": f"S{rank:02d}",
                "champion_rank": rank,
                "champion_score": 1.0 - (rank * 0.01),
                "champion_selected_top20": True,
                "forward_ret_20d": -0.02,
                "bottom15_label": False,
                "candle_body_ratio": 0.2,
                "candle_lower_wick_ratio": 0.1,
                "candle_upper_wick_ratio": 0.4,
                "candle_triplet_up_prob": 0.2,
                "candle_triplet_down_prob": 0.2,
                "gap_pct": 0.01,
                "vol_ratio5_20": 1.0,
                "monthly_context_no_lookahead": True,
                "weekly_context_no_lookahead": True,
            }
        )
    frame = pd.DataFrame(rows)
    frame.loc[frame["champion_rank"].eq(6), ["candle_body_ratio", "candle_lower_wick_ratio", "candle_upper_wick_ratio", "candle_triplet_up_prob", "vol_ratio5_20", "forward_ret_20d"]] = [
        0.8,
        0.7,
        0.05,
        0.95,
        2.5,
        0.12,
    ]
    frame.loc[frame["champion_rank"].eq(5), "forward_ret_20d"] = -0.10
    return frame


def test_timing_score_uses_confirmed_features_without_label_columns() -> None:
    assert not (TIMING_FEATURE_COLUMNS & LABEL_COLUMNS)
    ranked = _apply_candidate(_frame())
    promoted = ranked[ranked["symbol"].eq("S06")].iloc[0]

    assert int(promoted["candidate_rank"]) < int(promoted["champion_rank"])
    assert bool(promoted["candidate_selected_top5"])
    assert bool(promoted["changed_top5_member"])


def test_artifacts_include_decision_and_anti_leakage(tmp_path: Path) -> None:
    payload = _build_outputs(_frame(), output_root=tmp_path, source_rows_parquet=Path(r"G:\Tradex\source.parquet"))

    required = {
        "candidate_manifest.json",
        "evaluation_contract.json",
        "timing_feature_summary.json",
        "compare.json",
        "decision_summary.json",
        "anti_leakage_audit.json",
        "_ARTIFACT_COMPLETE.json",
    }
    assert required.issubset(payload["paths"])
    for path in payload["paths"].values():
        assert Path(path).exists()

    compare = json.loads(Path(payload["paths"]["compare.json"]).read_text(encoding="utf-8"))
    decision = json.loads(Path(payload["paths"]["decision_summary.json"]).read_text(encoding="utf-8"))
    anti = json.loads(Path(payload["paths"]["anti_leakage_audit.json"]).read_text(encoding="utf-8"))

    assert compare["same_condition_contract"]["same_universe"] is True
    assert compare["branching"]["changed_top5_members_count"] > 0
    assert decision["authoritative_rollup_decision"] in {"keep", "hold", "drop"}
    assert anti["pass"] is True
    assert anti["used_future_labels_in_scoring"] is False
