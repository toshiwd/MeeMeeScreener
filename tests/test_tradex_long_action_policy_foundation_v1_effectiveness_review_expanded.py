from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb
import scripts.tradex_long_action_policy_foundation_v1_effectiveness_review_expanded as expanded_review

from scripts.tradex_long_action_policy_foundation_v1_effectiveness_review_expanded import (
    _make_window_specs,
    build_expanded_effectiveness_review,
)


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
        low_price = open_price * 0.99
        rows.append((int(current.strftime("%Y%m%d")), open_price, high_price, low_price, close_price, volume))
    return rows


class MiniRepo:
    def __init__(self) -> None:
        start = date(2024, 1, 2)
        count = 240
        self.data = {
            "1001": _make_rows(start, count, base=100.0, step=1.1, high_wick_every=5, volume=40_000),
            "1002": _make_rows(start, count, base=180.0, step=-0.7, high_wick_every=0, volume=700),
            "1306": self._benchmark_rows(start, count),
        }

    def _benchmark_rows(self, start: date, count: int) -> list[tuple[int, float, float, float, float, int]]:
        rows: list[tuple[int, float, float, float, float, int]] = []
        for index, current in enumerate(_business_days(start, count)):
            if current < date(2024, 4, 15):
                close_price = 100.0 - index * 0.6
            elif current < date(2024, 8, 1):
                close_price = 90.0 + (index - 70) * 0.8
            else:
                close_price = 120.0 + (index - 160) * 0.3
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

    def get_daily_bars(self, code: str, limit: int = 400, asof_dt=None):  # noqa: ANN001
        rows = list(self.data.get(str(code), []))
        if asof_dt is not None:
            if isinstance(asof_dt, datetime):
                asof_ymd = int(asof_dt.strftime("%Y%m%d"))
            else:
                asof_ymd = int(datetime.fromtimestamp(int(asof_dt), tz=timezone.utc).strftime("%Y%m%d"))
            rows = [row for row in rows if row[0] <= asof_ymd]
        return rows[-limit:]

    def get_daily_bars_batch(self, codes, limit=420, asof_dt=None):  # noqa: ANN001
        result = {}
        for code in codes:
            result[str(code)] = self.get_daily_bars(str(code), limit=limit, asof_dt=asof_dt)
        return result


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_expanded_review_generates_required_artifacts(tmp_path: Path):
    repo = MiniRepo()
    output_root = tmp_path / "expanded_review"
    session = build_expanded_effectiveness_review(
        repo,
        output_root,
        window_specs=[
            {"window_id": "flat_20240102", "label": "flat", "regime": "sideways", "window_start_date": "2024-01-02", "window_end_date": "2024-01-31", "window_months": 1, "decision_window_end_date": "2024-01-02", "execution_buffer_days": 1, "outcome_buffer_days": 21},
            {"window_id": "down_20240301", "label": "down", "regime": "downtrend", "window_start_date": "2024-03-01", "window_end_date": "2024-03-29", "window_months": 1, "decision_window_end_date": "2024-03-01", "execution_buffer_days": 1, "outcome_buffer_days": 21},
            {"window_id": "up_20240415", "label": "up", "regime": "uptrend", "window_start_date": "2024-04-15", "window_end_date": "2024-07-12", "window_months": 3, "decision_window_end_date": "2024-04-15", "execution_buffer_days": 1, "outcome_buffer_days": 63},
        ],
        jobs=1,
    )

    session_dir = Path(session["output_dir"])
    expected = {
        "run_manifest.json",
        "input_resolution.json",
        "window_coverage_summary.json",
        "branch_effect_audit.json",
        "portfolio_economic_comparison.json",
        "skipped_buy_bucket_decomposition.json",
        "skipped_buy_cases.parquet",
        "false_negative_skip_summary.json",
        "true_positive_skip_summary.json",
        "drawdown_attribution_summary.json",
        "regime_effectiveness_summary.json",
        "expanded_effectiveness_decision.json",
        "_ARTIFACT_COMPLETE.json",
        "effectiveness_review_summary.md",
        "symbol_level_effectiveness_summary.json",
        "monthly_effectiveness_summary.json",
        "entry_delay_cost_summary.json",
    }
    assert expected == {path.name for path in session_dir.iterdir()}

    run_manifest = _load_json(session_dir / "run_manifest.json")
    assert run_manifest["research_fallback"] is True
    assert run_manifest["window_count"] == 3

    coverage = _load_json(session_dir / "window_coverage_summary.json")
    assert coverage["window_count"] == 3
    assert coverage["baseline_row_count"] > 0
    assert coverage["variant_row_count"] > 0
    assert coverage["branch_transition_counts"]

    decision = _load_json(session_dir / "expanded_effectiveness_decision.json")
    assert decision["decision"] in {
        "keep_for_refinement",
        "hold_needs_more_windows",
        "drop",
        "needs_gate_redesign_before_more_replay",
    }

    parquet_count = duckdb.connect(":memory:").execute(
        "SELECT COUNT(*) FROM read_parquet(?)",
        [str(session_dir / "skipped_buy_cases.parquet")],
    ).fetchone()[0]
    assert parquet_count >= 0


def test_window_generation_expands_beyond_three_windows():
    repo = MiniRepo()
    original_prepare = expanded_review.prepare_replay_window_context
    original_classify = expanded_review._classify_regime
    try:
        def _fake_prepare_replay_window_context(repo, run_config, current):  # noqa: ANN001
            if current.month <= 3:
                market_context = {"market_ret20": -0.05, "breadth_above_ma20": 0.30}
            elif current.month <= 6:
                market_context = {"market_ret20": 0.00, "breadth_above_ma20": 0.45}
            else:
                market_context = {"market_ret20": 0.08, "breadth_above_ma20": 0.70}
            return {"market_context_by_date": {current.isoformat(): market_context}}

        expanded_review.prepare_replay_window_context = _fake_prepare_replay_window_context
        expanded_review._classify_regime = original_classify
        specs, resolution = _make_window_specs(repo, ["1306", "1001", "1002"])
    finally:
        expanded_review.prepare_replay_window_context = original_prepare
        expanded_review._classify_regime = original_classify
    assert len(specs) > 3
    assert resolution["candidate_month_count"] >= len(specs)
    assert all(spec["label"] in {"down", "flat", "up"} for spec in specs)
    assert all(spec["window_months"] in {1, 3} for spec in specs)
