from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_ma60_above_60plus_pattern_audit_v1 import (
    add_features,
    build_streaks,
    classify_break_reason,
    no_lookahead_audit,
    run,
)


def _daily_rows(code: str, lengths: list[int]) -> pd.DataFrame:
    rows = []
    date = pd.Timestamp("2026-01-01")
    price = 100.0
    for length in lengths:
        for _ in range(length):
            rows.append({"code": code, "date": date, "ymd": int(date.strftime("%Y%m%d")), "o": price, "h": price + 1, "l": price - 1, "c": price, "v": 1000, "ma7": price - 2, "ma20": price - 4, "ma60": price - 8})
            date += pd.offsets.BDay(1)
            price += 1
        rows.append({"code": code, "date": date, "ymd": int(date.strftime("%Y%m%d")), "o": price, "h": price + 1, "l": price - 10, "c": price - 20, "v": 2000, "ma7": price - 2, "ma20": price - 4, "ma60": price - 8})
        date += pd.offsets.BDay(1)
    return pd.DataFrame(rows)


def test_ma60_streak_labels_and_anchor_rows() -> None:
    featured = add_features(_daily_rows("1001", [60, 25]))
    streaks, anchors = build_streaks(featured)

    assert len(streaks) == 2
    assert streaks["reached_60"].tolist() == [True, False]
    assert set(anchors["anchor_type"]) == {"anchor_1", "anchor_10", "anchor_20", "anchor_30"}
    positive_anchors = anchors[anchors["future_reached_60"].astype(bool)]
    assert len(positive_anchors) == 4
    control_anchors = anchors[anchors["label_cohort"].eq("control_main")]
    assert set(control_anchors["anchor_type"]) == {"anchor_1", "anchor_10", "anchor_20"}


def test_break_reason_detects_ma20_and_ma60_break() -> None:
    active = [{"c": 100.0}]
    break_row = {"c": 80.0, "ma20": 90.0, "ma60": 95.0, "volume_ratio_ma20": 1.0}

    assert classify_break_reason(active, break_row) == "ma20_and_ma60_break"


def test_no_lookahead_audit_separates_features_and_labels() -> None:
    audit = no_lookahead_audit()

    assert audit["audit_result"] == "pass"
    assert "future_reached_60" in audit["label_columns"]
    assert "ma20_slope" in audit["feature_columns"]
    assert audit["threshold_sweep"] is False
    assert audit["model_training"] is False


def test_run_writes_required_artifacts_with_csv_input(tmp_path: Path) -> None:
    rows = []
    dates = pd.bdate_range("2025-01-01", periods=180)
    for idx, date in enumerate(dates):
        close = 100.0 + idx
        rows.append({"code": "1001", "date": date.strftime("%Y-%m-%d"), "open": close, "high": close + 1, "low": close - 1, "close": close, "volume": 1000 + idx})
    pd.DataFrame(rows).to_csv(tmp_path / "daily.csv", index=False)

    result = run(output_root=tmp_path / "out", production_csv=tmp_path / "daily.csv")
    out = Path(result["output_dir"])

    required = set(result["required_artifacts"])
    assert required.issubset({path.name for path in out.iterdir()})
    complete = json.loads((out / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    audit = json.loads((out / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    anchors = pd.read_csv(out / "anchor_feature_rows.csv")
    assert complete["artifact_complete"] is True
    assert audit["audit_result"] == "pass"
    assert {"code", "anchor_type", "future_reached_60", "final_streak_length", "label_columns"}.issubset(anchors.columns)
