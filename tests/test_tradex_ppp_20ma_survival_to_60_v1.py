from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_ppp_20ma_survival_to_60_v1 import (
    build_streak_events,
    classify_decision,
    no_lookahead_audit,
    run,
    summarize_survival,
)


def _rows_for_streak(code: str, start: str, length: int, *, ppp_from_count: int | None = None) -> list[dict[str, object]]:
    dates = pd.bdate_range(start, periods=length)
    rows: list[dict[str, object]] = []
    for idx, dt in enumerate(dates, start=1):
        rows.append(
            {
                "code": code,
                "date": dt,
                "c": 100.0 + idx,
                "h": 101.0 + idx,
                "l": 99.0 + idx,
                "ma20": 90.0,
                "close_above_ma20": True,
                "ppp_proxy": False if ppp_from_count is None else idx >= ppp_from_count,
                "future_c_20": 110.0 + idx,
                "future_c_40": 120.0 + idx,
                "future_h_20": 115.0 + idx,
                "future_l_20": 95.0 + idx,
            }
        )
    return rows


def test_streak_count_resets_and_one_streak_one_event() -> None:
    rows = []
    rows.extend(_rows_for_streak("1001", "2026-01-01", 25, ppp_from_count=20))
    reset_day = pd.Timestamp("2026-02-05")
    rows.append(
        {
            "code": "1001",
            "date": reset_day,
            "c": 80.0,
            "h": 81.0,
            "l": 79.0,
            "ma20": 90.0,
            "close_above_ma20": False,
            "ppp_proxy": False,
        }
    )
    rows.extend(_rows_for_streak("1001", "2026-02-06", 19, ppp_from_count=1))
    events = build_streak_events(pd.DataFrame(rows), ppp_source="proxy")

    assert len(events) == 1
    assert int(events.loc[0, "streak_length"]) == 25
    assert bool(events.loc[0, "reached_23"]) is True
    assert bool(events.loc[0, "reached_60"]) is False


def test_reached_23_and_reached_60_labels() -> None:
    rows = _rows_for_streak("1001", "2026-01-01", 60, ppp_from_count=1)
    events = build_streak_events(pd.DataFrame(rows), ppp_source="proxy")

    assert len(events) == 1
    assert bool(events.loc[0, "reached_23"]) is True
    assert bool(events.loc[0, "reached_60"]) is True
    assert pd.notna(events.loc[0, "anchor_20_date"])
    assert pd.notna(events.loc[0, "anchor_23_date"])
    assert pd.notna(events.loc[0, "anchor_60_date"])


def test_no_lookahead_column_classification() -> None:
    audit = no_lookahead_audit()

    assert audit["columns"]["ppp_at_23"] == "feature"
    assert audit["columns"]["reached_60"] == "label"
    assert audit["columns"]["ret20_from_23"] == "diagnostic"
    assert audit["lookahead_blockers"] == []


def test_ppp_missing_research_fallback_separated(tmp_path: Path) -> None:
    csv_path = tmp_path / "daily.csv"
    rows = []
    dates = pd.bdate_range("2026-01-01", periods=90)
    for idx, dt in enumerate(dates, start=1):
        rows.append(
            {
                "code": "1001",
                "date": dt.strftime("%Y-%m-%d"),
                "open": 100 + idx,
                "high": 101 + idx,
                "low": 99 + idx,
                "close": 100 + idx,
                "volume": 1000,
            }
        )
    pd.DataFrame(rows).to_csv(csv_path, index=False)

    result = run(output_root=tmp_path / "out", production_csv=csv_path)
    out = Path(result["output_dir"])
    source = json.loads((out / "ppp_source_report.json").read_text(encoding="utf-8"))
    decision = json.loads((out / "research_decision.json").read_text(encoding="utf-8"))

    assert source["ppp_source_kind"] == "research-fallback"
    assert source["proxy_definition"]["enabled"] is True
    assert decision["research_decision"] == "inconclusive"


def test_summary_decision_not_supported_when_no_stepup() -> None:
    events = pd.DataFrame(
        [
            {"streak_length": 60, "reached_23": True, "reached_60": True, "ppp_at_20": True, "ppp_at_23": True, "ret20_from_23": 0.01},
            {"streak_length": 30, "reached_23": True, "reached_60": False, "ppp_at_20": True, "ppp_at_23": True, "ret20_from_23": 0.01},
            {"streak_length": 60, "reached_23": True, "reached_60": True, "ppp_at_20": False, "ppp_at_23": False, "ret20_from_23": 0.02},
            {"streak_length": 30, "reached_23": True, "reached_60": False, "ppp_at_20": False, "ppp_at_23": False, "ret20_from_23": 0.01},
        ]
    )
    summary = summarize_survival(events, ppp_source_kind="confirmed")
    decision = classify_decision(summary)

    assert decision["research_decision"] in {"not_supported", "inconclusive"}
    assert any("not at least" in reason or "below 100" in reason for reason in decision["reason_typed"])
