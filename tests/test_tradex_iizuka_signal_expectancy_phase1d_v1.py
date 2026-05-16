from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import tradex_iizuka_signal_expectancy_phase1d_v1 as phase1d


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _phase1c_root(tmp_path: Path) -> Path:
    root = tmp_path / "phase1c"
    root.mkdir(parents=True, exist_ok=True)
    rows = pd.DataFrame(
        [
            {
                "symbol": "1001",
                "decision_date": "2026-01-10",
                "execution_price": 1200.0,
                "ret5": -0.01,
                "ret20": -0.02,
                "mae20": -0.09,
                "mixed_internal_combination": "koma+horizontal",
                "sector": "tech",
                "liquidity_bucket": "liquidity_mid",
                "monthly_C_regime": True,
            },
            {
                "symbol": "1002",
                "decision_date": "2026-01-10",
                "execution_price": 2200.0,
                "ret5": 0.01,
                "ret20": 0.04,
                "mae20": -0.03,
                "mixed_internal_combination": "koma+horizontal",
                "sector": "machinery",
                "liquidity_bucket": "liquidity_high",
                "monthly_C_regime": True,
            },
        ]
    )
    rows.to_parquet(root / "phase1c_mixed_signal_rows.parquet", index=False)
    _write_json(
        root / "phase1c_signal_decision.json",
        {"authoritative_rollup_decision": "keep", "candidate_id": phase1d.CANDIDATE_ID},
    )
    source = tmp_path / "source.parquet"
    source_rows = []
    dates = pd.bdate_range("2026-01-01", periods=28).strftime("%Y-%m-%d")
    for symbol in ["1001", "1002", "1003"]:
        for i, date in enumerate(dates):
            close = 100 + i
            source_rows.append(
                {
                    "symbol": symbol,
                    "date": date,
                    "open": close,
                    "high": close + 1,
                    "low": close - 1,
                    "close": close,
                    "volume": 1000,
                    "ma7": close - 1,
                    "ma20": close - 2,
                    "atr14": 1,
                    "monthly_C_regime": True,
                    "liquidity_bucket": "liquidity_mid",
                    "sector": "tech",
                }
            )
    pd.DataFrame(source_rows).to_parquet(source, index=False)
    _write_json(root / "input_resolution.json", {"source_rows_parquet": str(source)})
    _write_json(
        root / "phase1c_internal_combination_compare.json",
        {
            "combinations": {
                "koma+horizontal": {
                    "decision": {"decision": "keep", "decision_reason": "test"},
                    "signal": {"count": 165, "ret20_median": 0.01, "win_rate_20": 0.55},
                }
            }
        },
    )
    _write_json(
        root / "phase1c_reference_koma_comparison.json",
        {
            "decision": {"decision": "hold"},
            "signal": {"count": 64},
            "promotion_allowed": False,
            "promotion_block_reason": "count below gate",
        },
    )
    return root


def test_run_phase1d_writes_required_artifacts(tmp_path: Path) -> None:
    phase1c_root = _phase1c_root(tmp_path)
    ranking = tmp_path / "ranking.parquet"
    pd.DataFrame(
        [
            {
                "symbol": "1001",
                "anchor_date": "2026-01-10",
                "side": "long",
                "champion_selected_top5": True,
                "champion_selected_top10": True,
                "champion_selected_top20": True,
                "champion_rank": 1,
            }
        ]
    ).to_parquet(ranking, index=False)

    result = phase1d.run_phase1d(
        phase1c_root=phase1c_root,
        output_root=tmp_path / "out",
        ranking_surface_path=ranking,
    )

    root = Path(result["session_root"])
    for artifact in phase1d.REQUIRED_ARTIFACTS:
        assert (root / artifact).exists(), artifact
    complete = json.loads((root / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    assert complete["all_present"] is True
    decision = json.loads((root / "phase1d_decision.json").read_text(encoding="utf-8"))
    assert decision["authoritative_decision"] in {
        "proceed_to_ranking_challenger_pretest",
        "hold_for_risk_filter_design",
        "analysis_only",
        "blocked",
        "drop",
    }
    ranking_preflight = json.loads((root / "phase1d_ranking_exposure_preflight.json").read_text(encoding="utf-8"))
    assert ranking_preflight["ranking_challenger_created"] is False
