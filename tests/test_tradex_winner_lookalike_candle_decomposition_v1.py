from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd

from scripts import tradex_winner_lookalike_candle_decomposition_v1 as mod


def _make_daily_rows(symbol: str, *, start: str, periods: int, winner: bool) -> list[dict[str, object]]:
    dates = pd.bdate_range(start=start, periods=periods)
    rows: list[dict[str, object]] = []
    price = 100.0
    for idx, day in enumerate(dates):
        if idx < periods - 25:
            drift = 0.001 if winner else 0.0008
        elif idx < periods - 20:
            drift = 0.002 if winner else 0.001
        else:
            drift = 0.018 if winner else -0.006
        open_price = price
        close = price * (1.0 + drift)
        if winner:
            high = max(open_price, close) * 1.012
            low = min(open_price, close) * 0.996
        else:
            high = max(open_price, close) * 1.045
            low = min(open_price, close) * 0.998
        volume = 1000 + idx * 10 + (300 if winner and idx % 7 == 0 else 0)
        rows.append(
            {
                "code": symbol,
                "date": int(day.strftime("%Y%m%d")),
                "o": open_price,
                "h": high,
                "l": low,
                "c": close,
                "v": volume,
                "source": "pan",
            }
        )
        price = close
    return rows


def _make_monthly_rows(symbol: str, daily_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    frame = pd.DataFrame(daily_rows)
    frame["month"] = pd.to_datetime(frame["date"].astype(str), format="%Y%m%d").dt.to_period("M")
    rows: list[dict[str, object]] = []
    for month, group in frame.groupby("month", sort=True):
        rows.append(
            {
                "code": symbol,
                "month": int(month.to_timestamp().strftime("%Y%m%d")),
                "o": float(group.iloc[0]["o"]),
                "h": float(group["h"].max()),
                "l": float(group["l"].min()),
                "c": float(group.iloc[-1]["c"]),
                "v": int(group["v"].sum()),
            }
        )
    return rows


def _write_synthetic_db(path: Path) -> None:
    symbols = [("7001", True), ("7002", True), ("7003", False), ("7004", False)]
    daily_rows: list[dict[str, object]] = []
    monthly_rows: list[dict[str, object]] = []
    for symbol, winner in symbols:
        rows = _make_daily_rows(symbol, start="2025-01-02", periods=120, winner=winner)
        daily_rows.extend(rows)
        monthly_rows.extend(_make_monthly_rows(symbol, rows))

    daily = pd.DataFrame(daily_rows)
    daily = daily.sort_values(["code", "date"]).copy()
    for column, window in (("ma7", 7), ("ma20", 20), ("ma60", 60)):
        daily[column] = daily.groupby("code")["c"].transform(lambda s: s.rolling(window, min_periods=1).mean())

    monthly = pd.DataFrame(monthly_rows).sort_values(["code", "month"]).copy()
    for column, window in (("ma7", 2), ("ma20", 3), ("ma60", 4)):
        monthly[column] = monthly.groupby("code")["c"].transform(lambda s: s.rolling(window, min_periods=1).mean())

    conn = duckdb.connect(str(path))
    try:
        conn.execute("CREATE TABLE daily_bars AS SELECT code, date, o, h, l, c, v, source FROM daily")
        conn.execute("CREATE TABLE daily_ma AS SELECT code, date, ma7, ma20, ma60 FROM daily")
        conn.execute("CREATE TABLE monthly_bars AS SELECT code, month, o, h, l, c, v FROM monthly")
        conn.execute("CREATE TABLE monthly_ma AS SELECT code, month, ma7, ma20, ma60 FROM monthly")
        conn.execute("CHECKPOINT")
    finally:
        conn.close()


def test_feature_contrast_detects_false_lookalike_wick_difference() -> None:
    rows = []
    for idx in range(40):
        rows.append(
            {
                "anchor_month": f"2025-{idx % 12 + 1:02d}",
                "code": f"W{idx}",
                "is_winner": True,
                "is_false_lookalike": False,
                "latest_upper_wick_ratio": 0.10,
                "weak_negated_by_strong_count_20": 4.0,
                **{feature: 1.0 for feature in mod.DISCOVERY_FEATURES if feature not in {"latest_upper_wick_ratio", "weak_negated_by_strong_count_20"}},
            }
        )
        rows.append(
            {
                "anchor_month": f"2025-{idx % 12 + 1:02d}",
                "code": f"F{idx}",
                "is_winner": False,
                "is_false_lookalike": True,
                "latest_upper_wick_ratio": 0.60,
                "weak_negated_by_strong_count_20": 1.0,
                **{feature: 1.0 for feature in mod.DISCOVERY_FEATURES if feature not in {"latest_upper_wick_ratio", "weak_negated_by_strong_count_20"}},
            }
        )
    contrast = mod.build_feature_contrast(pd.DataFrame(rows))
    by_feature = {row["feature"]: row for row in contrast}

    assert by_feature["latest_upper_wick_ratio"]["direction"] == "false_lookalike_higher"
    assert by_feature["latest_upper_wick_ratio"]["research_usefulness"] in {"high", "medium"}
    assert by_feature["weak_negated_by_strong_count_20"]["direction"] == "winner_higher"


def test_pattern_grouping_does_not_use_future_labels(tmp_path: Path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    _write_synthetic_db(db_path)
    result = mod.run_winner_lookalike_candle_decomposition_v1(
        source_db=db_path,
        output_root=tmp_path / "out",
        run_id="smoke",
        years=1,
        forward_days=20,
        min_history_days=20,
        max_pair_rows=20,
    )
    output_dir = Path(result["output_dir"])
    audit = json.loads((output_dir / "feature_availability_audit.json").read_text(encoding="utf-8"))

    assert audit["used_future_labels_in_pattern_grouping"] is False
    assert set(audit["coarse_lookalike_features"]).isdisjoint(mod.LABEL_COLUMNS)
    assert audit["candidate_scoring_created"] is False
    assert audit["silent_fallback_used"] is False


def test_run_writes_required_discovery_artifacts(tmp_path: Path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    _write_synthetic_db(db_path)
    result = mod.run_winner_lookalike_candle_decomposition_v1(
        source_db=db_path,
        output_root=tmp_path / "out",
        run_id="artifacts",
        years=1,
        forward_days=20,
        min_history_days=20,
        max_pair_rows=20,
    )
    output_dir = Path(result["output_dir"])

    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (output_dir / artifact).exists(), artifact

    complete = json.loads((output_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    decision = json.loads((output_dir / "research_decision.json").read_text(encoding="utf-8"))
    actionable = json.loads((output_dir / "actionable_pattern_candidates.json").read_text(encoding="utf-8"))

    assert complete["complete"] is True
    assert complete["silent_fallback_used"] is False
    assert complete["publish_bundle_created"] is False
    assert decision["candidate_scoring_created"] is False
    assert decision["meemee_reflectable"] is False
    assert actionable["schema_version"].endswith("_actionable_pattern_candidates_v1")
