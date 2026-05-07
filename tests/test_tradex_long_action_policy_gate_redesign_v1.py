from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from scripts.tradex_long_action_policy_gate_redesign_v1 import build_gate_redesign_review


def _business_days(start: date, count: int) -> list[date]:
    days: list[date] = []
    current = start
    while len(days) < count:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


def _make_rows(start: date, count: int, *, base: float, step: float, high_wick_every: int = 0, volume: int = 10_000) -> list[tuple[int, float, float, float, float, int]]:
    rows: list[tuple[int, float, float, float, float, int]] = []
    for index, current in enumerate(_business_days(start, count)):
        open_price = base + index * step
        close_price = open_price + step * 0.8
        high_price = close_price * (1.06 if high_wick_every and index % high_wick_every == 0 else 1.01)
        low_price = min(open_price, close_price) * 0.99
        rows.append((int(current.strftime("%Y%m%d")), open_price, high_price, low_price, close_price, volume))
    return rows


class MiniRepo:
    def __init__(self) -> None:
        start = date(2025, 3, 1)
        count = 120
        self.data = {
            "1001": _make_rows(start, count, base=100.0, step=1.4, high_wick_every=4, volume=60_000),
            "1002": _make_rows(start, count, base=160.0, step=-1.2, high_wick_every=0, volume=600),
            "1306": self._benchmark_rows(start, count),
        }

    def _benchmark_rows(self, start: date, count: int) -> list[tuple[int, float, float, float, float, int]]:
        rows: list[tuple[int, float, float, float, float, int]] = []
        for index, current in enumerate(_business_days(start, count)):
            if current < date(2025, 4, 10):
                close_price = 100.0 - index * 0.8
            else:
                close_price = 92.0 + (index - 25) * 1.1
            open_price = close_price - 0.4
            high_price = close_price * 1.01
            low_price = close_price * 0.99
            rows.append((int(current.strftime("%Y%m%d")), open_price, high_price, low_price, close_price, 1_000_000))
        return rows

    def get_all_codes(self):
        return list(self.data.keys())

    def get_latest_params_for_screening(self, codes=None):  # noqa: ANN001
        selected = list(codes or self.data.keys())
        return [(code, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0) for code in selected if code in self.data]

    def get_daily_bars_batch(self, codes, limit=420, asof_dt=None):  # noqa: ANN001
        result = {}
        asof_ymd = None
        if asof_dt is not None:
            asof_ymd = int(datetime.fromtimestamp(int(asof_dt), tz=timezone.utc).strftime("%Y%m%d"))
        for code in codes:
            rows = list(self.data.get(str(code), []))
            if asof_ymd is not None:
                rows = [row for row in rows if row[0] <= asof_ymd]
            result[str(code)] = rows[-limit:]
        return result


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _make_skipped_buy_cases(path: Path) -> Path:
    frame = pd.DataFrame(
        [
            {
                "window_id": "w1",
                "window_label": "flat",
                "window_start_date": "2025-03-31",
                "window_end_date": "2025-04-30",
                "date": "2025-03-31",
                "decision_date": "2025-03-31",
                "symbol": "1001",
                "baseline_action": "buy",
                "variant_action": "stay_cash",
                "baseline_order_status": "filled",
                "variant_order_status": "not_applicable",
                "baseline_reason_codes": ["entry_signal"],
                "variant_reason_codes": ["timing_block"],
                "baseline_execution_price": 101.0,
                "variant_execution_price": None,
                "baseline_cash": 9_899_000.0,
                "variant_cash": 10_000_000.0,
                "baseline_position_value": 101_000.0,
                "variant_position_value": 0.0,
                "baseline_position_qty": 100,
                "variant_position_qty": 0,
                "baseline_score": 0.064,
                "baseline_rank": 8,
                "top_candidate_score": 0.072,
                "market_regime": "uptrend",
                "month_key": "2025-03",
                "week_key": "2025-W14",
                "ret_5": 0.03,
                "ret_10": 0.04,
                "ret_20": 0.05,
                "forward_ret_20d": 0.05,
                "path_value_score_v1": 1,
                "skip_class": "skipped_good_buy",
                "reason_codes_key": "entry_signal",
                "later_buy_date": None,
                "later_buy_delay_days": None,
                "later_buy_forward_ret_20d": None,
                "later_buy_delay_cost_20d": None,
                "later_buy_action": None,
                "later_buy_within_window": False,
                "baseline_filled": True,
                "variant_filled": False,
            },
            {
                "window_id": "w1",
                "window_label": "flat",
                "window_start_date": "2025-03-31",
                "window_end_date": "2025-04-30",
                "date": "2025-03-31",
                "decision_date": "2025-03-31",
                "symbol": "1002",
                "baseline_action": "buy",
                "variant_action": "stay_cash",
                "baseline_order_status": "filled",
                "variant_order_status": "not_applicable",
                "baseline_reason_codes": ["entry_signal"],
                "variant_reason_codes": ["cost_turnover_block"],
                "baseline_execution_price": 151.0,
                "variant_execution_price": None,
                "baseline_cash": 9_849_000.0,
                "variant_cash": 10_000_000.0,
                "baseline_position_value": 151_000.0,
                "variant_position_value": 0.0,
                "baseline_position_qty": 100,
                "variant_position_qty": 0,
                "baseline_score": 0.042,
                "baseline_rank": 13,
                "top_candidate_score": 0.072,
                "market_regime": "downtrend",
                "month_key": "2025-03",
                "week_key": "2025-W14",
                "ret_5": -0.04,
                "ret_10": -0.05,
                "ret_20": -0.08,
                "forward_ret_20d": -0.08,
                "path_value_score_v1": 0,
                "skip_class": "skipped_bad_buy",
                "reason_codes_key": "entry_signal",
                "later_buy_date": None,
                "later_buy_delay_days": None,
                "later_buy_forward_ret_20d": None,
                "later_buy_delay_cost_20d": None,
                "later_buy_action": None,
                "later_buy_within_window": False,
                "baseline_filled": True,
                "variant_filled": False,
            },
        ]
    )
    frame.to_parquet(path, index=False)
    return path


def test_gate_redesign_runner_generates_required_artifacts_and_restores_timing_blocks(tmp_path: Path):
    repo = MiniRepo()
    output_root = tmp_path / "gate_redesign"
    expanded_dir = tmp_path / "expanded_review"
    foundation_dir = tmp_path / "foundation_review"
    expanded_dir.mkdir()
    foundation_dir.mkdir()
    skipped_cases_path = _make_skipped_buy_cases(tmp_path / "skipped_buy_cases.parquet")

    result = build_gate_redesign_review(
        repo,
        output_root,
        expanded_review_dir=expanded_dir,
        foundation_review_dir=foundation_dir,
        skipped_cases_path=skipped_cases_path,
        window_specs=[
            {
                "window_id": "smoke_flat",
                "label": "flat",
                "window_start_date": "2025-03-31",
                "window_end_date": "2025-04-18",
                "window_months": 1,
            },
            {
                "window_id": "smoke_up",
                "label": "up",
                "window_start_date": "2025-04-21",
                "window_end_date": "2025-05-16",
                "window_months": 1,
            },
        ],
        jobs=2,
    )

    session_dir = Path(result["output_dir"])
    expected = {
        "run_manifest.json",
        "input_resolution.json",
        "gate_redesign_feature_availability.json",
        "gate_redesign_policy_spec.json",
        "branch_effect_audit.json",
        "portfolio_economic_comparison.json",
        "skipped_buy_restoration_summary.json",
        "restored_buy_cases.parquet",
        "remaining_skipped_buy_cases.parquet",
        "entry_delay_cost_summary.json",
        "monthly_effectiveness_summary.json",
        "regime_effectiveness_summary.json",
        "drawdown_attribution_summary.json",
        "gate_redesign_decision.json",
        "_ARTIFACT_COMPLETE.json",
        "cost_slippage_summary.json",
        "opportunity_cost_summary.json",
        "hedge_pressure_summary.json",
    }
    assert expected <= {path.name for path in session_dir.iterdir()}

    feature_availability = _load_json(session_dir / "gate_redesign_feature_availability.json")
    assert "baseline_action" in feature_availability["baseline_action_fields"]
    assert "ret_20" in feature_availability["forbidden_outcome_fields"]

    policy_spec = _load_json(session_dir / "gate_redesign_policy_spec.json")
    assert policy_spec["variant_name"] == "long_entry_cash_gate_entry_signal_relax_v1"
    assert policy_spec["timing_override"]["rank_max"] == 11
    assert policy_spec["timing_override"]["score_min"] == 0.05
    assert "ret_20" in policy_spec["rule"]["excluded_inputs"]
    assert policy_spec["rule"]["overfit_guardrail"]

    decision = _load_json(session_dir / "gate_redesign_decision.json")
    assert decision["final_status"] in {
        "keep_for_larger_window_validation",
        "hold_needs_larger_window_validation",
        "drop",
        "needs_second_redesign",
    }

    restoration = _load_json(session_dir / "skipped_buy_restoration_summary.json")
    assert restoration["restored_good_buy"] >= 1
    assert restoration["remaining_skipped_bad_buy"] >= 0

    restored = pd.read_parquet(session_dir / "restored_buy_cases.parquet")
    remaining = pd.read_parquet(session_dir / "remaining_skipped_buy_cases.parquet")
    assert len(restored) >= 1
    assert len(remaining) >= 0
    assert set(restored["variant_reason_codes"].explode().dropna().astype(str).unique()) <= {"timing_block"}

    comparison = _load_json(session_dir / "portfolio_economic_comparison.json")
    assert "current_vs_redesign" in comparison["pairwise_delta"]
    assert "aggregates" in comparison

