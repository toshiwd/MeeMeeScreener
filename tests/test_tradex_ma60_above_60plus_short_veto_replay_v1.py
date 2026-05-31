from __future__ import annotations

import pandas as pd

from scripts import tradex_ma60_above_60plus_short_veto_replay_v1 as mod


def test_normalize_short_surface_accepts_explicit_short() -> None:
    df = pd.DataFrame({"code": ["1001", "1002"], "decision_ymd": [20250101, 20250102], "side": ["short", "long"]})
    out, report = mod.normalize_short_surface(mod.Path("x_short.csv"), df)
    assert report["eligible_rows"] == 1
    assert out.iloc[0]["code"] == "1001"


def test_guard_window_stops_on_ma_break() -> None:
    guard = pd.DataFrame([{"code": "1001", "anchor_type": "anchor_10", "anchor_date": "2025-01-01"}])
    daily = pd.DataFrame(
        [
            {"code": "1001", "ymd": 20250101, "c": 100, "ma20": 90, "ma60": 80},
            {"code": "1001", "ymd": 20250102, "c": 89, "ma20": 90, "ma60": 80},
            {"code": "1001", "ymd": 20250103, "c": 120, "ma20": 90, "ma60": 80},
        ]
    )
    windows = mod.prepare_guard_windows(guard, daily)
    assert int(windows.iloc[0]["guard_active_until_ymd"]) == 20250102


def test_classify_supported() -> None:
    summary = {
        "short_guard_hit": {"n_short_rows": 100, "ret20_long_mean": 0.02, "ret20_long_median": 0.0, "ret40_long_mean": 0.03, "helped_veto_rate": 0.7, "harmed_veto_rate": 0.2},
        "short_guard_miss": {},
        "all_short_rows": {"n_short_rows": 200},
        "hit_minus_miss_spread": {"short_return20": -0.03},
    }
    decision = mod.classify(summary, pd.DataFrame(), pd.DataFrame())
    assert decision["research_decision"] == "short_veto_supported"
