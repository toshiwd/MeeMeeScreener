from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import chart_context_candidate_family_map_v1 as mod


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def _candidate_row(year: int, ymd: int, code: str, rank: int, score: float, **flags: object) -> dict[str, object]:
    row: dict[str, object] = {
        "code": code,
        "decision_ymd": ymd,
        "year": year,
        "candidate_rank": rank,
        "selection_score": score,
        "gap_up_flag": False,
        "gap_down_flag": False,
        "gap_fail_same_day_flag": False,
        "failed_breakout_flag": False,
        "breakout_above_resistance_flag": False,
        "box_breakout_flag": False,
        "box_breakdown_flag": False,
        "bearish_full_retrace_flag": False,
        "bullish_full_retrace_flag": False,
        "engulfing_bearish_flag": False,
        "engulfing_bullish_flag": False,
        "denial_of_prior_bull_flag": False,
        "denial_of_prior_bear_flag": False,
        "true_breakdown_candidate_flag": False,
        "shakeout_recovery_candidate_flag": False,
        "regained_ma7_after_invalidation_flag": False,
        "regained_ma20_after_invalidation_flag": False,
        "prior_swing_failure_flag": False,
        "prior_swing_reclaim_flag": False,
        "n_wave_candidate_flag": False,
        "reverse_n_candidate_flag": False,
        "higher_low_confirmed_flag": False,
        "lower_high_confirmed_flag": False,
        "close_below_ma20_count": 0,
        "close_above_ma20_count": 1,
        "close_above_ma7_count": 1,
        "days_since_ma20_break": pd.NA,
        "days_since_ma20_reclaim": pd.NA,
        "ma7_ma20_distance_pct": 0.0,
        "sideways_length_days": 0,
        "ma_compression_flag": False,
        "volume_compression_ratio": 1.0,
        "month": ymd // 100,
    }
    row.update(flags)
    return row


def _make_root(tmp_path: Path) -> tuple[Path, Path]:
    robustness = tmp_path / "gate"
    chart = robustness / "chart_context_feature_contract_v1"
    subrun = robustness / "subruns" / "2024-baseline"
    chart.mkdir(parents=True)
    subrun.mkdir(parents=True)
    rows = [
        _candidate_row(2024, 20240110, "7001", 1, 15, breakout_above_resistance_flag=True),
        _candidate_row(2024, 20240110, "7002", 2, 14, failed_breakout_flag=True),
        _candidate_row(2024, 20240111, "7003", 1, 16, shakeout_recovery_candidate_flag=True),
        _candidate_row(2024, 20240112, "7004", 3, 16, true_breakdown_candidate_flag=True),
    ]
    pd.DataFrame(rows).to_parquet(chart / "chart_context_features_daily.parquet", index=False)
    pd.DataFrame(rows).head(3).to_parquet(chart / "chart_context_features_weekly.parquet", index=False)
    pd.DataFrame(rows).head(2).to_parquet(chart / "chart_context_features_monthly.parquet", index=False)
    _write_json(chart / "no_lookahead_audit.json", {"audit_result": "pass"})
    _write_json(chart / "chart_context_feature_manifest.json", {"audit_result": "pass"})
    candidates = pd.DataFrame(
        [
            {"decision_ymd": row["decision_ymd"], "code": row["code"], "candidate_rank": row["candidate_rank"], "selection_score": row["selection_score"], "selected_for_buy": idx % 2 == 0}
            for idx, row in enumerate(rows)
        ]
    )
    outcomes = pd.DataFrame(
        [
            {"decision_ymd": row["decision_ymd"], "code": row["code"], "post_ret_20": 0.08 if idx % 2 == 0 else -0.06, "mae_20": -0.03 if idx % 2 == 0 else -0.12, "mfe_20": 0.12}
            for idx, row in enumerate(rows)
        ]
    )
    candidates.to_csv(subrun / "daily_candidate_snapshot.csv", index=False)
    outcomes.to_csv(subrun / "post_run_outcome_labels.csv", index=False)
    pd.DataFrame([{"year": 2024, "run_dir": str(subrun)}]).to_csv(robustness / "yearly_results.csv", index=False)
    (robustness / "baseline_regime_failure_decomposition_v1").mkdir(parents=True)
    pd.DataFrame([{"year": 2024, "month": 202401, "regime_bucket": "benchmark_up", "benchmark_return": 0.05, "portfolio_return": 0.02, "excess_return": -0.03}]).to_csv(
        robustness / "baseline_regime_failure_decomposition_v1" / "monthly_failure_decomposition.csv",
        index=False,
    )
    return robustness, chart


def test_chart_context_candidate_family_map_outputs_required_artifacts(tmp_path: Path) -> None:
    robustness, chart = _make_root(tmp_path)
    result = mod.run_chart_context_candidate_family_map(robustness, chart)
    out = Path(result["output_root"])

    assert result["complete"] is True
    assert result["no_lookahead_audit"] == "pass"
    assert result["decision"] in mod.DECISIONS
    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (out / artifact).exists(), artifact
    complete = json.loads((out / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    assert complete["required_artifacts_all_present"] is True
    assert complete["silent_fallback_used"] is False
    mapped = pd.read_csv(out / "chart_context_candidate_family_map.csv")
    assert set(mapped["chart_context_family"]) >= {"resistance_breakout", "failed_breakout", "shakeout_recovery_candidate", "true_breakdown_candidate"}


def test_chart_context_family_classification_ignores_outcomes() -> None:
    row = pd.Series(_candidate_row(2024, 20240110, "7001", 1, 15, breakout_above_resistance_flag=True))
    with_outcome = row.copy()
    with_outcome["post_ret_20"] = -0.99
    with_outcome["mae_20"] = -0.99
    with_outcome["mfe_20"] = 0.99
    assert mod.classify_chart_context_family(row) == mod.classify_chart_context_family(with_outcome)
