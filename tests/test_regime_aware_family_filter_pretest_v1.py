from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import regime_aware_family_filter_pretest_v1 as mod


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def test_benchmark_regime_uses_past_equity_only() -> None:
    rows = []
    equity = 10_000_000.0
    for idx in range(70):
        ymd = 20240101 + idx
        equity *= 1.001
        rows.append({"ymd": ymd, "market_benchmark_equity": equity})
    frame = pd.DataFrame(rows)
    signal_ymd = int(frame.iloc[60]["ymd"])
    baseline = mod._benchmark_regime_from_equity(frame, signal_ymd)
    changed_future = frame.copy()
    changed_future.loc[changed_future["ymd"] > signal_ymd, "market_benchmark_equity"] = 1.0
    changed = mod._benchmark_regime_from_equity(changed_future, signal_ymd)
    assert baseline == changed
    assert baseline["market_regime"] == "market_risk_on"


def test_filtered_outcome_amount_is_diagnostic_not_filter_condition() -> None:
    bad = pd.Series({"post_ret_20": -0.10, "mae_20": -0.15, "mfe_20": 0.02})
    good = pd.Series({"post_ret_20": 0.15, "mae_20": -0.03, "mfe_20": 0.20})
    saved, missed, klass = mod._filtered_outcome_amount(bad, 1_000_000.0)
    assert saved > 0
    assert missed == 0
    assert klass == "saved_loss"
    saved, missed, klass = mod._filtered_outcome_amount(good, 1_000_000.0)
    assert saved == 0
    assert missed > 0
    assert klass == "missed_profit"


def test_run_pretest_writes_required_artifacts_with_stubbed_year(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "gate"
    chart = root / "chart_context_candidate_family_map_v1"
    subrun = root / "subrun"
    chart.mkdir(parents=True)
    subrun.mkdir(parents=True)
    pd.DataFrame([{"year": 2024, "run_dir": str(root / "subrun"), "total_return": 0.10, "benchmark_return": 0.05, "max_drawdown": -0.12}]).to_csv(root / "yearly_results.csv", index=False)
    pd.DataFrame([{"ymd": 20240110, "equity": 10_000_000.0, "cash": 10_000_000.0, "positions_market_value": 0.0, "market_benchmark_equity": 10_000_000.0}]).to_csv(subrun / "equity_curve.csv", index=False)
    pd.DataFrame(
        [
            {"year": 2024, "decision_ymd": 20240110, "code": "7001", "chart_context_family": "failed_breakout", "chart_context_family_reason": "x", "post_ret_20": -0.1, "mae_20": -0.12, "mfe_20": 0.02}
        ]
    ).to_csv(chart / "chart_context_candidate_family_map.csv", index=False)

    def fake_simulate(run_dir: Path, baseline_row: dict, family_map: pd.DataFrame) -> dict:
        equity = pd.DataFrame([{"year": 2024, "ymd": 20240110, "equity": 10_000_000.0, "cash": 10_000_000.0, "positions_market_value": 0.0, "market_benchmark_equity": 10_000_000.0}])
        filtered = pd.DataFrame(
            [
                {
                    "year": 2024,
                    "decision_ymd": 20240110,
                    "code": "7001",
                    "chart_context_family": "failed_breakout",
                    "market_regime": "market_risk_off",
                    "saved_loss_estimate": 100_000.0,
                    "missed_profit_estimate": 0.0,
                }
            ]
        )
        return {
            "yearly": {
                "year": 2024,
                "baseline_total_return": 0.10,
                "regime_family_filter_total_return": 0.12,
                "delta_total_return": 0.02,
                "benchmark_return": 0.05,
                "benchmark_excess_baseline": 0.05,
                "benchmark_excess_regime_filter": 0.07,
                "baseline_max_drawdown": -0.12,
                "regime_filter_max_drawdown": -0.10,
                "delta_max_drawdown": 0.02,
                "filtered_buy_count": 1,
                "cash_hold_count": 1,
                "saved_loss_total": 100_000.0,
                "missed_profit_total": 0.0,
                "cost_delta": -1000.0,
                "exact_next_open_replay": True,
            },
            "orders": pd.DataFrame(),
            "positions": pd.DataFrame(),
            "equity": equity,
            "filtered": filtered,
            "exact_missing": [],
        }

    monkeypatch.setattr(mod, "_simulate_year", fake_simulate)
    result = mod.run_pretest(root, chart)
    out = Path(result["output_root"])
    assert result["complete"] is True
    assert result["no_lookahead_audit"] == "pass"
    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (out / artifact).exists(), artifact
    complete = json.loads((out / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    assert complete["required_artifacts_all_present"] is True
    assert complete["silent_fallback_used"] is False
    assert complete["threshold_sweep"] is False
