from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import shape_vs_non_shape_driver_decomposition_v1 as mod


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def test_choose_next_axis_prefers_largest_sample_ok_driver() -> None:
    candidates = pd.DataFrame(
        [
            {"driver_name": "sector_flow_driver", "driver_spread_bought_post_ret20": 0.02, "best_bucket_bought_count": 40, "worst_bucket_bought_count": 40, "sample_ok": True},
            {"driver_name": "relative_strength_driver", "driver_spread_bought_post_ret20": 0.08, "best_bucket_bought_count": 50, "worst_bucket_bought_count": 50, "sample_ok": True},
        ]
    ).sort_values(["sample_ok", "driver_spread_bought_post_ret20"], ascending=[False, False])
    decision, reason, evidence = mod._choose_next_axis(candidates)
    assert decision == "relative_strength_filter_pretest"
    assert reason == "relative_strength_driver_has_largest_observed_signal_date_driver_separation"
    assert evidence["selected_driver"]["driver_name"] == "relative_strength_driver"


def test_driver_candidates_require_signal_date_buckets_only() -> None:
    yearly = pd.DataFrame(
        [
            {"year": 2024, "driver_name": "relative_strength_driver", "relative_strength_driver": "rs20_strong", "bought_count": 35, "bought_post_ret20_mean": 0.05, "severe_loser_rate": 0.1},
            {"year": 2024, "driver_name": "relative_strength_driver", "relative_strength_driver": "rs20_weak", "bought_count": 35, "bought_post_ret20_mean": -0.04, "severe_loser_rate": 0.2},
            {"year": 2024, "driver_name": "sector_flow_driver", "sector_flow_driver": "sector20_strong", "bought_count": 20, "bought_post_ret20_mean": 0.02, "severe_loser_rate": 0.1},
            {"year": 2024, "driver_name": "sector_flow_driver", "sector_flow_driver": "sector20_weak", "bought_count": 20, "bought_post_ret20_mean": -0.01, "severe_loser_rate": 0.2},
        ]
    )
    candidates = mod._driver_candidates(yearly, pd.DataFrame())
    rel = candidates[candidates["driver_name"] == "relative_strength_driver"].iloc[0]
    assert rel["sample_ok"] is True or bool(rel["sample_ok"]) is True
    assert rel["driver_spread_bought_post_ret20"] > 0.08


def test_run_decomposition_writes_required_artifacts_with_stubbed_drivers(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "gate"
    chart_family = root / "chart_context_candidate_family_map_v1"
    chart_feature = root / "chart_context_feature_contract_v1"
    regime = root / "regime_aware_family_filter_pretest_v1"
    subrun = root / "subrun"
    chart_family.mkdir(parents=True)
    chart_feature.mkdir(parents=True)
    regime.mkdir(parents=True)
    subrun.mkdir(parents=True)
    _write_json(subrun / "run_config.json", {"source_db": str(tmp_path / "dummy.duckdb")})
    (tmp_path / "dummy.duckdb").write_bytes(b"not-used")
    pd.DataFrame([{"year": 2024, "run_dir": str(subrun), "total_return": 0.1, "benchmark_return": 0.05}]).to_csv(root / "yearly_results.csv", index=False)
    family = pd.DataFrame(
        [
            {"year": 2024, "decision_ymd": 20240110, "code": "7001", "chart_context_family": "resistance_breakout", "selected_for_buy_bool": True, "post_ret_20": 0.1, "mae_20": -0.02, "mfe_20": 0.2, "candidate_rank": 1, "selection_score": 15, "month": 202401},
            {"year": 2024, "decision_ymd": 20240110, "code": "7002", "chart_context_family": "failed_breakout", "selected_for_buy_bool": True, "post_ret_20": -0.1, "mae_20": -0.15, "mfe_20": 0.02, "candidate_rank": 2, "selection_score": 12, "month": 202401},
        ]
    )
    family.to_csv(chart_family / "chart_context_candidate_family_map.csv", index=False)

    monkeypatch.setattr(mod, "_load_source_db", lambda robustness_root: tmp_path / "dummy.duckdb")
    driver_frame = family.copy()
    driver_frame["relative_strength_driver"] = ["rs20_strong", "rs20_weak"]
    driver_frame["sector_flow_driver"] = ["sector20_strong", "sector20_weak"]
    driver_frame["breadth_driver"] = ["breadth_broad_up", "breadth_narrow_or_down"]
    driver_frame["volatility_whipsaw_driver"] = ["whipsaw_normal", "whipsaw_high"]
    driver_frame["volume_quality_driver"] = ["volume_expansion", "volume_dry"]
    driver_frame["chart_shape_driver"] = driver_frame["chart_context_family"]
    driver_frame["stock_20d_return"] = [0.12, -0.05]
    driver_frame["benchmark_20d_return"] = [0.03, 0.03]
    driver_frame["relative_strength_20d"] = [0.09, -0.08]
    driver_frame["sector33_name"] = ["A", "B"]
    driver_frame["sector_20d_return_candidate_universe"] = [0.04, -0.03]
    driver_frame["breadth_up_ratio"] = [0.6, 0.4]
    monkeypatch.setattr(mod, "_driver_rows", lambda mapped, source_db: driver_frame)

    result = mod.run_decomposition(root, chart_feature, chart_family, regime)
    out = Path(result["output_root"])
    assert result["complete"] is True
    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (out / artifact).exists(), artifact
    complete = json.loads((out / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    assert complete["required_artifacts_all_present"] is True
    assert complete["silent_fallback_used"] is False
