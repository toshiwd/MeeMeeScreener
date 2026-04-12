from __future__ import annotations

from datetime import datetime

import pandas as pd

from app.backend.tools.weekly_top_gainers_study import build_weekly_top_gainers_study_frame


def _daily_rows_for_weekly_study() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    base = datetime(2025, 1, 6)
    for code, start_price, drift in (("A", 100.0, 0.5), ("B", 100.0, 0.0), ("C", 100.0, 2.0)):
        price = float(start_price)
        for week in range(7):
            for day in range(5):
                dt = base + pd.Timedelta(days=week * 7 + day)
                if code == "C" and week >= 4:
                    step = 4.0
                elif code == "A":
                    step = drift
                else:
                    step = drift
                open_ = price
                high = price + max(1.0, step) + 1.0
                low = price - 1.0
                close = price + step
                volume = 1000 + week * 50 + day * 10 + (200 if code == "C" and week >= 4 else 0)
                rows.append(
                    {
                        "code": code,
                        "ymd": int(dt.strftime("%Y%m%d")),
                        "date_dt": dt,
                        "o": open_,
                        "h": high,
                        "l": low,
                        "c": close,
                        "v": volume,
                    }
                )
                price = close
    return pd.DataFrame(rows)


def test_weekly_top_gainers_study_labels_following_week_top_rank() -> None:
    daily = _daily_rows_for_weekly_study()
    study = build_weekly_top_gainers_study_frame(daily, top_n=1)
    assert not study.empty
    top_rows = study[study["is_top_n"]]
    assert not top_rows.empty
    assert set(top_rows["code"].astype(str)) == {"C"}
    assert top_rows["candidate_score"].max() >= top_rows["candidate_score"].min()


def test_weekly_top_gainers_study_candidate_score_and_threshold_summary() -> None:
    daily = _daily_rows_for_weekly_study()
    study = build_weekly_top_gainers_study_frame(daily, top_n=1)
    threshold_rows = []
    total = int(len(study))
    positive = int(study["is_top_n"].sum())
    for threshold in range(0, int(study["candidate_score"].max()) + 1):
        selected = study["candidate_score"] >= threshold
        hit_count = int((selected & study["is_top_n"]).sum())
        selected_count = int(selected.sum())
        precision = float(hit_count / selected_count) if selected_count > 0 else 0.0
        threshold_rows.append((threshold, selected_count, precision))
    assert total > 0
    assert positive > 0
    assert any(selected_count > 0 and precision >= 0.1 for _, selected_count, precision in threshold_rows)
