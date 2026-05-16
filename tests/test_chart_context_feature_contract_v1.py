from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import chart_context_feature_contract_v1 as mod


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def _daily_rows(code: str = "7001", periods: int = 180) -> pd.DataFrame:
    dates = pd.bdate_range("2020-01-01", periods=periods)
    rows = []
    price = 100.0
    for idx, date in enumerate(dates):
        price += 0.2
        if idx == 90:
            open_ = price + 5.0
            close = price + 6.0
        elif idx == 91:
            open_ = price + 6.5
            close = price + 1.0
        else:
            open_ = price
            close = price + (0.5 if idx % 3 else -0.2)
        high = max(open_, close) + 1.0
        low = min(open_, close) - 1.0
        rows.append({"code": code, "ymd": int(date.strftime("%Y%m%d")), "date": date, "o": open_, "h": high, "l": low, "c": close, "v": 1000 + idx * 10})
    frame = pd.DataFrame(rows)
    grouped = frame.groupby("code", sort=False)
    frame["ma20"] = grouped["c"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    frame["ma60"] = grouped["c"].transform(lambda s: s.rolling(60, min_periods=60).mean())
    return frame


def _candidate_keys(daily: pd.DataFrame) -> pd.DataFrame:
    picks = daily.iloc[[95, 120]].copy()
    return pd.DataFrame(
        {
            "code": picks["code"].astype(str),
            "decision_ymd": picks["ymd"].astype(int),
            "year": picks["date"].dt.year.astype(int),
            "candidate_rank": [1, 2],
            "selection_score": [15.0, 12.0],
        }
    )


def _robustness_root(tmp_path: Path, daily: pd.DataFrame) -> Path:
    root = tmp_path / "gate"
    run_dir = root / "subruns" / "2020-baseline-portfolio_agent_replay_v1"
    run_dir.mkdir(parents=True)
    _write_json(run_dir / "run_config.json", {"source_db": str(tmp_path / "dummy.duckdb")})
    (tmp_path / "dummy.duckdb").write_bytes(b"not-used")
    keys = _candidate_keys(daily)
    keys.rename(columns={"decision_ymd": "decision_ymd"}).to_csv(run_dir / "daily_candidate_snapshot.csv", index=False)
    pd.DataFrame([{"year": 2020, "run_dir": str(run_dir)}]).to_csv(root / "yearly_results.csv", index=False)
    return root


def test_chart_context_contract_writes_artifacts(tmp_path: Path) -> None:
    daily = _daily_rows()
    root = _robustness_root(tmp_path, daily)

    result = mod.run_chart_context_feature_contract(root, daily_source_frame=daily)
    out = Path(result["output_root"])

    assert result["complete"] is True
    assert result["no_lookahead_audit"] == "pass"
    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (out / artifact).exists(), artifact
    complete = json.loads((out / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    assert complete["required_artifacts_all_present"] is True
    assert complete["silent_fallback_used"] is False
    assert complete["policy_change"] is False
    audit = json.loads((out / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    assert audit["forbidden_columns_present"] == []


def test_chart_context_features_are_point_in_time_for_signal_date() -> None:
    daily = _daily_rows()
    keys = _candidate_keys(daily)
    baseline = mod.build_chart_context_features(daily, keys).sort_values("decision_ymd").reset_index(drop=True)
    changed_future = daily.copy()
    future_mask = changed_future["date"] > pd.to_datetime(str(int(keys.iloc[0]["decision_ymd"])), format="%Y%m%d")
    changed_future.loc[future_mask, ["o", "h", "l", "c", "v"]] = [500.0, 520.0, 480.0, 510.0, 999999]
    changed = mod.build_chart_context_features(changed_future, keys).sort_values("decision_ymd").reset_index(drop=True)

    compare_columns = [
        "recent_swing_high",
        "gap_up_flag",
        "bearish_full_retrace_flag",
        "close_above_ma20_count",
        "sideways_length_days",
        "n_wave_candidate_flag",
        "invalidation_flag",
    ]
    pd.testing.assert_series_equal(baseline.loc[[0], compare_columns].iloc[0], changed.loc[[0], compare_columns].iloc[0], check_names=False)
