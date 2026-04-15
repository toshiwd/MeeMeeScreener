from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from app.backend.services.tradex_portfolio_replay_service import run_policy_family_cohort_replay, run_policy_family_replay
from external_analysis.policy_replay.policy_family import _decision_from_metrics


def _business_days(start: date, count: int) -> list[date]:
    days: list[date] = []
    current = start
    while len(days) < count:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


def _rows_from_prices(prices: list[float], start: date) -> list[tuple[int, float, float, float, float, int]]:
    rows = []
    for current, price in zip(_business_days(start, len(prices)), prices, strict=True):
        ymd = int(current.strftime("%Y%m%d"))
        rows.append((ymd, price, price * 1.01, price * 0.99, price, 1000))
    return rows


class CountingRepo:
    def __init__(self, data: dict[str, list[tuple[int, float, float, float, float, int]]]):
        self.data = data
        self.batch_calls = 0

    def get_daily_bars_batch(self, codes, limit=420, asof_dt=None):  # noqa: ANN001
        self.batch_calls += 1
        return {code: [row for row in self.data.get(code, []) if asof_dt is None or row[0] <= asof_dt] for code in codes}


def test_policy_family_runner_reuses_context_and_emits_decisions():
    repo = CountingRepo(
        {
            "1111": _rows_from_prices([100, 102, 104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124, 126, 128, 130, 132, 134, 136, 138, 140, 142, 144, 146, 148], date(2026, 1, 5)),
            "2222": _rows_from_prices([140, 138, 136, 134, 132, 130, 128, 126, 124, 122, 120, 118, 116, 114, 112, 110, 108, 106, 104, 102, 100, 98, 96, 94, 92], date(2026, 1, 5)),
            "1306": _rows_from_prices([100 for _ in range(25)], date(2026, 1, 5)),
        }
    )
    payload = {
        "family_id": "family-throughput-smoke",
        "family_name": "Throughput smoke",
        "window_start_dates": ["2026-01-05", "2026-01-05", "2026-01-05"],
        "universe": ["1111", "2222"],
        "market_benchmark_symbol": "1306",
        "capital": {"initial_capital_jpy": 10_000_000, "gross_exposure_cap_jpy": 10_000_000},
        "scoring": {
            "weights": {
                "total_return": 0.08,
                "excess_vs_universe": 0.22,
                "exposure_adjusted_excess": 0.16,
                "median_window_excess": 0.16,
                "worst_window_excess": 0.10,
                "max_drawdown": -0.10,
                "turnover": -0.08,
                "concentration": -0.05,
                "weekly_activity": -0.08,
                "long_hold": 0.10,
                "premature_exit": -0.07,
            }
        },
        "weekly_activity_required": True,
        "short_cash_reusable": False,
        "execution_convention": "close_close_research_convention",
        "policy_variants": [
            {
                "policy_variant_id": "aggressive",
                "policy_id": "family_smoke",
                "policy_version": "v1_aggressive",
                "entry_rule": {"entry_threshold": 0.02},
                "add_rule": {"add_threshold": 0.04, "addon_units": [2, 3, 5]},
                "partial_take_rule": {"partial_take_threshold": 0.10},
                "full_exit_rule": {"exit_threshold": -0.02, "stop_loss_threshold": -0.50},
                "sizing_rule": {"unit_scale": 10, "gross_exposure_cap_jpy": 10_000_000, "short_cash_reusable": False},
                "selection_rule": {"policy_id": "family_smoke", "policy_version": "v1_aggressive", "weekly_activity_required": True, "execution_convention": "close_close_research_convention", "weights": {}},
                "rationale": {
                    "what_changed": "lower entry threshold and faster add-on path",
                    "why_it_changed": "capture stronger trend legs earlier",
                    "expected_effect": "higher excess vs universe with lower hold churn",
                    "reason_code": "trend_capture",
                    "author_or_source": "test",
                    "timestamp_or_run_id": "smoke",
                },
            },
            {
                "policy_variant_id": "conservative",
                "policy_id": "family_smoke",
                "policy_version": "v1_conservative",
                "entry_rule": {"entry_threshold": 0.65},
                "add_rule": {"add_threshold": 0.90, "addon_units": [2, 3, 5]},
                "partial_take_rule": {"partial_take_threshold": 0.02},
                "full_exit_rule": {"exit_threshold": -0.50, "stop_loss_threshold": -0.80},
                "sizing_rule": {"unit_scale": 10, "gross_exposure_cap_jpy": 10_000_000, "short_cash_reusable": False},
                "selection_rule": {"policy_id": "family_smoke", "policy_version": "v1_conservative", "weekly_activity_required": True, "execution_convention": "close_close_research_convention", "weights": {}},
                "rationale": {
                    "what_changed": "much higher entry threshold and no meaningful add-on path",
                    "why_it_changed": "reduce churn and test under-trading",
                    "expected_effect": "lower turnover but weaker relative performance",
                    "reason_code": "churn_reduction",
                    "author_or_source": "test",
                    "timestamp_or_run_id": "smoke",
                },
            },
        ],
    }
    result = run_policy_family_replay(repo, payload)
    family_result = result["result"]

    assert repo.batch_calls == 1
    assert family_result["schema_version"] == "tradex_policy_family_replay_v1"
    assert family_result["policy_variant_manifest"]["family_signature"]
    assert len(family_result["policy_variant_manifest"]["policy_variants"]) == 2
    assert family_result["policy_variant_manifest"]["policy_variants"][0]["selection_rule_signatures"]["entry_rule_signature"] != family_result["policy_variant_manifest"]["policy_variants"][1]["selection_rule_signatures"]["entry_rule_signature"]

    rows = family_result["policy_comparison_matrix"]["rows"]
    assert len(rows) == 2
    assert rows[0]["median_window_excess"] >= rows[1]["median_window_excess"]
    assert rows[0]["exposure_adjusted_excess_mean"] >= rows[1]["exposure_adjusted_excess_mean"]

    decisions = {row["policy_variant_id"]: row for row in family_result["policy_decision_log"]["rows"]}
    assert decisions["aggressive"]["decision"] == "drop"
    assert "weekly_activity_failures" in decisions["aggressive"]["reason_codes"]
    assert decisions["conservative"]["decision"] == "drop"

    keep_drop = family_result["policy_keep_drop_hold"]
    assert keep_drop["overview"]["candidate_count"] == 2
    assert keep_drop["overview"]["keep_count"] + keep_drop["overview"]["hold_count"] + keep_drop["overview"]["drop_count"] == 2

    family_dir = result["family_dir"]
    assert family_dir
    assert Path(family_dir).exists()
    assert (Path(family_dir) / "policy_family_result.json").exists()
    assert (Path(family_dir) / "policy_variant_manifest.json").exists()
    assert (Path(family_dir) / "policy_comparison_matrix.json").exists()
    assert (Path(family_dir) / "policy_decision_log.json").exists()
    assert (Path(family_dir) / "policy_keep_drop_hold.json").exists()


def test_policy_family_runner_can_hold_promising_small_sample():
    repo = CountingRepo(
        {
            "1111": _rows_from_prices([100, 102, 104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124, 126, 128, 130, 132, 134, 136, 138, 140, 142, 144, 146, 148], date(2026, 1, 5)),
            "2222": _rows_from_prices([140, 138, 136, 134, 132, 130, 128, 126, 124, 122, 120, 118, 116, 114, 112, 110, 108, 106, 104, 102, 100, 98, 96, 94, 92], date(2026, 1, 5)),
            "1306": _rows_from_prices([100 for _ in range(25)], date(2026, 1, 5)),
        }
    )
    payload = {
        "family_id": "family-hold-smoke",
        "family_name": "Hold smoke",
        "window_start_dates": ["2026-01-05", "2026-01-05"],
        "universe": ["1111", "2222"],
        "market_benchmark_symbol": "1306",
        "capital": {"initial_capital_jpy": 10_000_000, "gross_exposure_cap_jpy": 10_000_000},
        "weekly_activity_required": False,
        "short_cash_reusable": False,
        "execution_convention": "close_close_research_convention",
        "policy_variants": [
            {
                "policy_variant_id": "patient",
                "policy_id": "family_smoke",
                "policy_version": "v1_patient",
                "entry_rule": {"entry_threshold": -1.0},
                "add_rule": {"add_threshold": 999.0, "addon_units": [2, 3, 5]},
                "partial_take_rule": {"partial_take_threshold": 999.0},
                "full_exit_rule": {"exit_threshold": -999.0, "stop_loss_threshold": -999.0},
                "sizing_rule": {"unit_scale": 10, "gross_exposure_cap_jpy": 10_000_000, "short_cash_reusable": False},
                "selection_rule": {"policy_id": "family_smoke", "policy_version": "v1_patient", "weekly_activity_required": False, "execution_convention": "close_close_research_convention", "weights": {}},
                "rationale": {
                    "what_changed": "hold the initial position through the full window",
                    "why_it_changed": "test the long-hold preference and avoid churn",
                    "expected_effect": "better hold behavior with small sample",
                    "reason_code": "sample_hold",
                    "author_or_source": "test",
                    "timestamp_or_run_id": "smoke",
                },
            }
        ],
    }
    result = run_policy_family_replay(repo, payload)["result"]
    row = result["policy_decision_log"]["rows"][0]
    assert row["decision"] == "drop"
    assert "promising_but_sample_small" in row["reason_codes"]
    assert "weekly_activity_failures" in row["reason_codes"]


def test_policy_family_decision_helper_can_hold_promising_small_sample():
    decision, reason_codes, reason_text = _decision_from_metrics(
        {
            "window_count": 2,
            "median_window_excess": 0.05,
            "excess_vs_universe_mean": 0.04,
            "worst_window_excess": 0.02,
            "exposure_adjusted_excess_mean": 0.03,
            "avg_holding_days_mean": 30.0,
            "pct_trades_over_20d_mean": 0.6,
            "turnover_mean": 0.2,
            "weekly_activity_pass_rate_mean": 1.0,
        }
    )
    assert decision == "hold"
    assert reason_codes == ["promising_but_sample_small"]
    assert "sample is too small" in reason_text


def test_policy_family_first_cohort_calibrates_thresholds_and_persists_artifacts():
    repo = CountingRepo(
        {
            "1111": _rows_from_prices([100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129], date(2026, 1, 5)),
            "2222": _rows_from_prices([130, 129, 128, 127, 126, 125, 124, 123, 122, 121, 120, 119, 118, 117, 116, 115, 114, 113, 112, 111, 110, 109, 108, 107, 106, 105, 104, 103, 102, 101], date(2026, 1, 5)),
            "3333": _rows_from_prices([90, 91, 89, 92, 88, 93, 87, 94, 86, 95, 85, 96, 84, 97, 83, 98, 82, 99, 81, 100, 80, 101, 79, 102, 78, 103, 77, 104, 76, 105], date(2026, 1, 5)),
            "1306": _rows_from_prices([100 for _ in range(30)], date(2026, 1, 5)),
        }
    )
    payload = {
        "cohort_id": "first-cohort-smoke",
        "window_start_dates": ["2026-01-05", "2026-01-12", "2026-01-19"],
        "universe": ["1111", "2222", "3333"],
        "market_benchmark_symbol": "1306",
        "capital": {"initial_capital_jpy": 10_000_000, "gross_exposure_cap_jpy": 10_000_000},
        "weekly_activity_required": True,
        "short_cash_reusable": False,
        "execution_convention": "close_close_research_convention",
    }

    result = run_policy_family_cohort_replay(repo, payload)
    cohort = result["result"]

    assert repo.batch_calls == 3
    assert cohort["schema_version"] == "tradex_policy_family_cohort_v1"
    assert cohort["cohort_manifest"]["cohort_id"] == "first-cohort-smoke"
    assert len(cohort["cohort_manifest"]["family_manifest"]) == 4
    assert len(cohort["family_results"]) == 4
    assert cohort["threshold_calibration"]["schema_version"] == "tradex_policy_family_cohort_v1"
    assert set(cohort["threshold_calibration"]["cohort_thresholds"]["thresholds"]) == {"keep", "drop"}
    assert result["cohort_dir"]
    assert Path(result["cohort_dir"]).exists()
    assert (Path(result["cohort_dir"]) / "policy_family_cohort_manifest.json").exists()
    assert (Path(result["cohort_dir"]) / "policy_threshold_calibration.json").exists()
    assert (Path(result["cohort_dir"]) / "policy_family_cohort_result.json").exists()

    assert "cohort_thresholds" in cohort["threshold_calibration"]
    assert "family_thresholds" in cohort["threshold_calibration"]
    for family_result in cohort["family_results"]:
        family_id = family_result["family_id"]
        assert family_result["decision_thresholds"] == cohort["threshold_calibration"]["family_thresholds"][family_id]["thresholds"]
        assert family_result["policy_keep_drop_hold"]["overview"]["candidate_count"] == 3

    assert cohort["keep_drop_hold_summary"]["keep_count"] + cohort["keep_drop_hold_summary"]["hold_count"] + cohort["keep_drop_hold_summary"]["drop_count"] == 12
