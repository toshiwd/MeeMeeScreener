from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from app.backend.services import tradex_research_contracts as contracts
from scripts import tradex_bad_pick_removal_v1 as mod


def _source_frame() -> pd.DataFrame:
    rows = []
    for idx in range(1, 21):
        rows.append(
            {
                "symbol": f"70{idx:02d}",
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
                "forward_ret_20d": 0.02,
                "mfe_20d": 0.08,
                "mae_20d": -0.03,
                "path_value_score_v1": 0.04,
                "top15_label": False,
                "bottom15_label": False,
                "monthly_context": "range_buy",
                "weekly_context": "range_buy",
                "monthly_context_no_lookahead": True,
                "weekly_context_no_lookahead": True,
                "candle_upper_wick_ratio": 0.12,
                "candle_lower_wick_ratio": 0.25,
                "candle_triplet_up_prob": 0.55,
                "candle_triplet_down_prob": 0.25,
                "gap_pct": 0.00,
                "vol_ratio5_20": 1.0,
                "prefilter_bucket": "KEEP_WATCH",
                "prefilter_reason": "[]",
                "shape_classification": "neutral",
            }
        )
    frame = pd.DataFrame(rows)
    frame.loc[frame["champion_rank"].eq(5), ["forward_ret_20d", "mfe_20d", "mae_20d", "bottom15_label"]] = [-0.16, 0.01, -0.18, True]
    frame.loc[frame["champion_rank"].eq(5), ["candle_upper_wick_ratio", "candle_lower_wick_ratio", "candle_triplet_up_prob", "candle_triplet_down_prob"]] = [0.85, 0.02, 0.20, 0.74]
    frame.loc[frame["champion_rank"].eq(5), ["gap_pct", "vol_ratio5_20", "prefilter_bucket", "prefilter_reason", "shape_classification"]] = [
        0.08,
        2.8,
        "EXCLUDE_ONLY",
        "['bad_pick_diagnostic']",
        "negative_reversal",
    ]
    frame.loc[frame["champion_rank"].eq(6), ["forward_ret_20d", "mfe_20d", "mae_20d"]] = [0.09, 0.16, -0.01]
    return frame


def _write_supporting_json(tmp_path: Path, *, fixed_topk_valid: bool) -> tuple[Path, Path]:
    champion = tmp_path / "final_freeze_decision.json"
    champion.write_text(json.dumps({"decision": "freeze_as_current_publish_candidate"}), encoding="utf-8")
    topk = tmp_path / "topk_operational_fit.json"
    topk.write_text(json.dumps({"schema_version": "test_topk_fit", "fixed_topK_valid": fixed_topk_valid}), encoding="utf-8")
    return champion, topk


def test_guard_scoring_does_not_use_future_labels() -> None:
    source = mod.load_source_rows_from_frame(_source_frame())
    ranked_a = mod.apply_candidate_logic(source)
    source_b = source.copy()
    for column in mod.LABEL_COLUMNS:
        if column in source_b.columns:
            source_b[column] = list(reversed(source_b[column].tolist()))
    ranked_b = mod.apply_candidate_logic(source_b)

    assert not (mod.SCORING_FEATURE_COLUMNS & mod.LABEL_COLUMNS)
    assert ranked_a[["symbol", "challenger_rank", "challenger_score"]].sort_values("symbol").reset_index(drop=True).equals(
        ranked_b[["symbol", "challenger_rank", "challenger_score"]].sort_values("symbol").reset_index(drop=True)
    )


def test_guard_records_typed_reason_and_penalizes_only_risk() -> None:
    source = mod.load_source_rows_from_frame(_source_frame())
    ranked = mod.apply_candidate_logic(source)
    risk = ranked[ranked["symbol"].eq("7005")].iloc[0]
    clean = ranked[ranked["symbol"].eq("7001")].iloc[0]

    assert bool(risk["bad_pick_guard_active"]) is True
    assert "upper_wick_exhaustion" in risk["bad_pick_guard_reasons"]
    assert "prefilter_or_shape_risk" in risk["bad_pick_guard_reasons"]
    assert risk["challenger_score"] < risk["champion_score"]
    assert clean["bad_pick_guard_reasons"] == "no_guard"
    assert clean["challenger_score"] == clean["champion_score"]


def test_run_writes_required_artifacts_and_valid_contracts(tmp_path: Path) -> None:
    source_path = tmp_path / "source.parquet"
    _source_frame().to_parquet(source_path, index=False)
    champion, topk = _write_supporting_json(tmp_path, fixed_topk_valid=False)

    result = mod.run_bad_pick_removal_v1(
        source_rows_parquet=source_path,
        output_root=tmp_path / "out",
        run_id="badpick",
        champion_freeze_json=champion,
        topk_operational_fit_json=topk,
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
    ledger_text = (output_dir / "candidate_ledger.jsonl").read_text(encoding="utf-8")

    contracts.validate_compare_artifact(compare)
    contracts.validate_family_leaderboard_artifact(family)
    contracts.validate_session_rollup_artifact(session)
    assert audit["used_future_labels_in_scoring"] is False
    assert compare["same_condition_checks"]["silent_fallback_used"] is False
    assert compare["topk"]["top5"]["deltas"]["bad_pick_removal_count_delta"] >= 1
    assert gate["reflectable_to_meemee"] is False
    assert gate["publish_bundle_allowed"] is False
    assert complete["complete"] is True
    assert complete["silent_fallback_used"] is False
    assert complete["publish_bundle_created"] is False
    assert "upper_wick_exhaustion" in ledger_text


def test_top10_only_small_improvement_cannot_keep(tmp_path: Path) -> None:
    source_path = tmp_path / "source.parquet"
    frame = _source_frame()
    frame.loc[frame["champion_rank"].eq(5), ["forward_ret_20d", "mfe_20d", "mae_20d", "bottom15_label"]] = [0.03, 0.08, -0.03, False]
    frame.loc[frame["champion_rank"].eq(11), ["forward_ret_20d", "mfe_20d", "mae_20d"]] = [0.031, 0.08, -0.03]
    frame.to_parquet(source_path, index=False)
    champion, topk = _write_supporting_json(tmp_path, fixed_topk_valid=True)

    result = mod.run_bad_pick_removal_v1(
        source_rows_parquet=source_path,
        output_root=tmp_path / "out",
        run_id="top10only",
        champion_freeze_json=champion,
        topk_operational_fit_json=topk,
    )
    output_dir = Path(result["output_dir"])
    family = json.loads((output_dir / "family_leaderboard.json").read_text(encoding="utf-8"))
    assert family["authoritative_rollup_decision"] != "keep"
