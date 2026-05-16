from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from app.backend.services import tradex_research_contracts as contracts
from scripts import tradex_swing_quality_selection_v1 as mod


def _source_frame() -> pd.DataFrame:
    rows = []
    for idx in range(1, 21):
        rows.append(
            {
                "symbol": f"80{idx:02d}",
                "side": "long",
                "trade_date": 20250131,
                "anchor_date": "2025-01-31",
                "month_bucket": "2025-01",
                "market_regime_bucket": "test_regime",
                "regime_label": "test_regime",
                "champion_rank": idx,
                "champion_score": 1.0 - idx * 0.01,
                "champion_selected_top5": idx <= 5,
                "champion_selected_top10": idx <= 10,
                "champion_selected_top20": True,
                "forward_ret_20d": -0.03,
                "mfe_20d": 0.02,
                "mae_20d": -0.05,
                "path_value_score_v1": 0.0,
                "top15_label": False,
                "bottom15_label": False,
                "monthly_context": "trend_up",
                "weekly_context": "range_buy",
                "monthly_context_no_lookahead": True,
                "weekly_context_no_lookahead": True,
                "candle_body_ratio": 0.30,
                "candle_upper_wick_ratio": 0.25,
                "candle_lower_wick_ratio": 0.10,
                "candle_triplet_up_prob": 0.50,
                "candle_triplet_down_prob": 0.40,
                "gap_pct": 0.01,
                "vol_ratio5_20": 1.0,
                "v": 1000 + idx,
                "prefilter_bucket": "KEEP_WATCH",
                "prefilter_reason": "[]",
                "shape_classification": "neutral",
            }
        )
    frame = pd.DataFrame(rows)
    frame.loc[frame["champion_rank"].eq(6), ["champion_score", "forward_ret_20d", "mfe_20d", "mae_20d"]] = [0.9505, 0.18, 0.25, -0.01]
    frame.loc[frame["champion_rank"].eq(6), ["candle_body_ratio", "candle_lower_wick_ratio", "candle_upper_wick_ratio"]] = [0.90, 0.70, 0.02]
    frame.loc[frame["champion_rank"].eq(6), ["candle_triplet_up_prob", "candle_triplet_down_prob", "vol_ratio5_20"]] = [0.98, 0.05, 2.8]
    frame.loc[frame["champion_rank"].eq(5), ["forward_ret_20d", "mfe_20d", "mae_20d", "bottom15_label"]] = [-0.18, 0.01, -0.22, True]
    frame.loc[frame["champion_rank"].eq(9), ["prefilter_bucket", "prefilter_reason", "candle_upper_wick_ratio"]] = [
        "EXCLUDE_ONLY",
        "['bad_pick_diagnostic']",
        0.8,
    ]
    return frame


def _write_supporting_json(tmp_path: Path, *, fixed_topk_valid: bool = True) -> tuple[Path, Path]:
    champion = tmp_path / "final_freeze_decision.json"
    champion.write_text(json.dumps({"decision": "freeze_as_current_publish_candidate"}), encoding="utf-8")
    topk = tmp_path / "topk_operational_fit.json"
    topk.write_text(
        json.dumps(
            {
                "schema_version": "test_topk_fit",
                "fixed_topK_valid": fixed_topk_valid,
                "interpretation": "test",
            }
        ),
        encoding="utf-8",
    )
    return champion, topk


def test_scoring_does_not_use_future_label_columns() -> None:
    source = mod.load_source_rows_from_frame(_source_frame())
    ranked_a = mod.apply_candidate_logic(source)
    source_b = source.copy()
    for column in mod.LABEL_COLUMNS:
        if column in source_b.columns:
            source_b[column] = list(reversed(source_b[column].tolist()))
    ranked_b = mod.apply_candidate_logic(source_b)

    assert not (mod.SCORING_FEATURE_COLUMNS & mod.LABEL_COLUMNS)
    assert ranked_a[["symbol", "candidate_rank", "swing_quality_score"]].sort_values("symbol").reset_index(drop=True).equals(
        ranked_b[["symbol", "candidate_rank", "swing_quality_score"]].sort_values("symbol").reset_index(drop=True)
    )


def test_run_writes_required_artifacts_and_valid_contracts(tmp_path: Path) -> None:
    source_path = tmp_path / "source.parquet"
    _source_frame().to_parquet(source_path, index=False)
    champion, topk = _write_supporting_json(tmp_path, fixed_topk_valid=False)

    result = mod.run_swing_quality_selection(
        source_rows_parquet=source_path,
        output_root=tmp_path / "out",
        run_id="swingtest",
        champion_freeze_json=champion,
        topk_operational_fit_json=topk,
        publish_root=tmp_path / "publish",
    )

    output_dir = Path(result["output_dir"])
    for name in mod.REQUIRED_ARTIFACTS:
        assert (output_dir / name).exists(), name

    compare = json.loads((output_dir / "compare.json").read_text(encoding="utf-8"))
    family = json.loads((output_dir / "family_leaderboard.json").read_text(encoding="utf-8"))
    session = json.loads((output_dir / "session_leaderboard_rollup.json").read_text(encoding="utf-8"))
    audit = json.loads((output_dir / "feature_availability_audit.json").read_text(encoding="utf-8"))
    gate = json.loads((output_dir / "meemee_reflection_gate.json").read_text(encoding="utf-8"))
    complete = json.loads((output_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))

    contracts.validate_compare_artifact(compare)
    contracts.validate_family_leaderboard_artifact(family)
    contracts.validate_session_rollup_artifact(session)
    assert compare["same_condition_checks"]["silent_fallback_used"] is False
    assert audit["used_future_labels_in_scoring"] is False
    assert gate["reflectable_to_meemee"] is False
    assert "fixed_topk_production_valid_failed" in gate["blockers"]
    assert complete["complete"] is True


def test_publish_bundle_only_after_keep_and_reflection_gate(tmp_path: Path) -> None:
    source_path = tmp_path / "source.parquet"
    _source_frame().to_parquet(source_path, index=False)
    champion, topk = _write_supporting_json(tmp_path, fixed_topk_valid=True)

    result = mod.run_swing_quality_selection(
        source_rows_parquet=source_path,
        output_root=tmp_path / "out",
        run_id="swingkeep",
        champion_freeze_json=champion,
        topk_operational_fit_json=topk,
        publish_root=tmp_path / "publish",
    )

    output_dir = Path(result["output_dir"])
    gate = json.loads((output_dir / "meemee_reflection_gate.json").read_text(encoding="utf-8"))
    if gate["reflectable_to_meemee"]:
        bundle_dir = Path(result["publish_bundle_dir"])
        assert (bundle_dir / "published_logic_artifact.json").exists()
        assert (bundle_dir / "ranking_adjustment_contract.json").exists()
        assert not (bundle_dir / "candidate_ledger.jsonl").exists()
    else:
        assert result["publish_bundle_dir"] is None
