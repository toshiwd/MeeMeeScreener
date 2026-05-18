from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import tradex_downside_prebreak_narrow_universe_selection_v4 as mod


class _DummyConn:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def _row(ymd: int, code: str, baseline_rank: int, ret20: float, *, strong: bool) -> dict[str, object]:
    if strong:
        close_pos = 0.82
        dist_ma20 = 0.02
        dist_low20 = 0.05
        day_change = -0.03
        trend_down = True
        ma20_slope = -0.02
        ma60_slope = -0.02
        weekly = 0.80
        monthly = 0.78
        range_prob = 0.08
        range_pos = 0.90
        event_risk = False
        borrow = False
    else:
        close_pos = 0.40
        dist_ma20 = -0.04
        dist_low20 = 0.01
        day_change = 0.01
        trend_down = False
        ma20_slope = 0.01
        ma60_slope = 0.01
        weekly = 0.32
        monthly = 0.34
        range_prob = 0.40
        range_pos = 0.50
        event_risk = True
        borrow = True
    return {
        "ymd": ymd,
        "code": code,
        "selected_by_baseline": True,
        "baseline_rank": baseline_rank,
        "tradePriorityScore": float(100 - baseline_rank * 5),
        "entryScore": float(20 - baseline_rank),
        "short_ret_5": ret20,
        "short_ret_10": ret20,
        "short_ret_20": ret20,
        "close_pos": close_pos,
        "dist_ma20_signed": dist_ma20,
        "dist_low20": dist_low20,
        "day_change_pct": day_change,
        "trendDownStrict": trend_down,
        "ma20_slope": ma20_slope,
        "ma60_slope": ma60_slope,
        "weeklyBreakoutDownProb": weekly,
        "monthlyBreakoutDownProb": monthly,
        "monthlyRangeProb": range_prob,
        "monthlyRangePos": range_pos,
        "event_risk_short": event_risk,
        "borrow_proxy_unfavorable": borrow,
    }


def _patch_frozen_inputs(monkeypatch: pytest.MonkeyPatch, rows: list[dict[str, object]], months: list[int], tmp_path: Path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    db_path.touch()
    monkeypatch.setattr(mod.base, "_resolve_db_path", lambda _cli: db_path)
    monkeypatch.setattr(mod.base, "_month_end_dates", lambda _conn, *, start_ymd, end_ymd: months)
    monkeypatch.setattr(mod.base, "_load_price_store", lambda _conn: {})
    monkeypatch.setattr(mod.base, "_load_frame_map", lambda _conn, table, ymd_col="dt": {})
    monkeypatch.setattr(mod.base, "_load_event_map", lambda _conn: {})
    monkeypatch.setattr(mod.base, "_build_rows", lambda **_kwargs: {"rows": rows})
    monkeypatch.setattr(mod.base.duckdb, "connect", lambda *args, **kwargs: _DummyConn())


def test_downside_acceleration_selector_keep(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    months = [20250131, 20250228, 20250331, 20250430, 20250530, 20250630, 20250731]
    rows: list[dict[str, object]] = []
    for month in months:
        rows.extend(
            [
                _row(month, "A", 3, 0.12, strong=True),
                _row(month, "B", 1, -0.08, strong=False),
                _row(month, "C", 2, 0.04, strong=True),
            ]
        )
    _patch_frozen_inputs(monkeypatch, rows, months, tmp_path)

    out = tmp_path / "out"
    result = mod.run_pipeline(db_path=tmp_path / "stocks.duckdb", output_dir=out, start_ymd=20240101, end_ymd=20260515, top_k=2, min_train_months=1, lookback_months=12)

    decision = json.loads((out / "downside_prebreak_narrow_universe_decision.json").read_text(encoding="utf-8"))
    compare = json.loads((out / "downside_prebreak_narrow_universe_compare.json").read_text(encoding="utf-8"))
    no_lookahead = json.loads((out / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    contract = json.loads((out / "downside_prebreak_narrow_universe_contract.json").read_text(encoding="utf-8"))

    assert result["decision"] == "keep_for_shadow_paper_replay"
    assert decision["decision"] == "keep_for_shadow_paper_replay"
    assert compare["baseline"]["selected_count"] == 12
    assert compare["challenger"]["selected_count"] == 12
    assert compare["baseline"]["hit_rate"] == pytest.approx(0.5)
    assert compare["challenger"]["hit_rate"] == pytest.approx(1.0)
    assert compare["delta"]["changed_top5_members_count"] > 0
    assert no_lookahead["pass"] is True
    assert no_lookahead["future_bars_used_for_selection"] == []
    assert no_lookahead["unknown_horizon_rows_excluded_from_training"] is True
    assert contract["boundary"] == "TRADEX-only"
    assert contract["same_condition_controls"]["same_universe"] is True
    assert (out / "_ARTIFACT_COMPLETE.json").exists()


def test_downside_acceleration_selector_hold_on_flat_signal(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    months = [20250131, 20250228]
    rows: list[dict[str, object]] = []
    for month in months:
        rows.extend(
            [
                _row(month, "A", 1, -0.02, strong=False),
                _row(month, "B", 2, -0.01, strong=False),
                _row(month, "C", 3, -0.03, strong=False),
            ]
        )
    _patch_frozen_inputs(monkeypatch, rows, months, tmp_path)

    out = tmp_path / "out_flat"
    result = mod.run_pipeline(db_path=tmp_path / "stocks.duckdb", output_dir=out, start_ymd=20240101, end_ymd=20260515, top_k=2, min_train_months=1, lookback_months=12)

    decision = json.loads((out / "downside_prebreak_narrow_universe_decision.json").read_text(encoding="utf-8"))
    compare = json.loads((out / "downside_prebreak_narrow_universe_compare.json").read_text(encoding="utf-8"))

    assert result["decision"] in {"hold_due_to_breadth_or_stability", "drop_as_statistical_edge_insufficient", "hold_due_to_small_sample"}
    assert decision["decision"] == result["decision"]
    assert compare["challenger"]["selected_count"] == 2
    assert compare["challenger"]["hit_rate"] <= compare["baseline"]["hit_rate"]


def test_downside_acceleration_selector_excludes_unknown_horizons(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    months = [20250131, 20250228, 20250331]
    rows: list[dict[str, object]] = []
    for month in months:
        rows.extend(
            [
                _row(month, "A", 1, 0.10, strong=True),
                _row(month, "B", 2, -0.05, strong=False),
                {**_row(month, "C", 3, 0.30, strong=True), "short_ret_5": None, "short_ret_10": None, "short_ret_20": None},
            ]
        )
    _patch_frozen_inputs(monkeypatch, rows, months, tmp_path)

    out = tmp_path / "out_unknown"
    result = mod.run_pipeline(db_path=tmp_path / "stocks.duckdb", output_dir=out, start_ymd=20240101, end_ymd=20260515, top_k=1, min_train_months=1, lookback_months=12)

    no_lookahead = json.loads((out / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    rankings = (out / "downside_prebreak_narrow_universe_monthly_rankings.csv").read_text(encoding="utf-8")

    assert result["compare"]["baseline"]["selected_count"] == 2
    assert no_lookahead["unknown_horizon_row_count"] == 3
    assert no_lookahead["closed_horizon_row_count"] == 6
    assert "unknown_candidate_count" in rankings


def test_narrow_universe_excludes_low_ranked_candidates(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    months = [20250131, 20250228, 20250331]
    rows: list[dict[str, object]] = []
    for month in months:
        rows.extend(
            [
                _row(month, "A", 1, 0.10, strong=True),
                _row(month, "B", 2, -0.05, strong=False),
                _row(month, "C", 30, 0.30, strong=True),
            ]
        )
    _patch_frozen_inputs(monkeypatch, rows, months, tmp_path)

    out = tmp_path / "out_narrow"
    result = mod.run_pipeline(db_path=tmp_path / "stocks.duckdb", output_dir=out, start_ymd=20240101, end_ymd=20260515, top_k=1, min_train_months=1, lookback_months=12, baseline_rank_limit=2)
    no_lookahead = json.loads((out / "no_lookahead_audit.json").read_text(encoding="utf-8"))

    assert result["compare"]["baseline"]["selected_count"] == 2
    assert no_lookahead["closed_horizon_row_count"] == 6
    assert no_lookahead["out_of_narrow_universe_row_count"] == 3
