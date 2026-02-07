from __future__ import annotations

from datetime import datetime, timezone
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.backend.services.ml_config import MLConfig
from app.backend.services.ml_service import (
    build_walk_forward_windows,
    compute_ev20_net,
    compute_label_fields,
    select_top_n_ml,
)
from app.backend.services import rankings_cache


def test_compute_label_fields_boundaries() -> None:
    up0, mask0 = compute_label_fields(0.0, 0.005)
    assert up0 == 0
    assert mask0 == 0

    up_pos, mask_pos = compute_label_fields(0.005, 0.005)
    assert up_pos == 1
    assert mask_pos == 1

    up_neg, mask_neg = compute_label_fields(-0.005, 0.005)
    assert up_neg == 0
    assert mask_neg == 1


def test_select_top_n_ml_fills_shortage() -> None:
    items = []
    for i in range(40):
        items.append(
            {
                "code": f"{1000 + i}",
                "p_up": 0.8 if i < 10 else 0.3,
                "ev20_net": float(100 - i) / 1000.0,
            }
        )
    selected = select_top_n_ml(items, top_n=30, p_up_threshold=0.55, direction="up")
    assert len(selected) == 30
    # First pass contributes only 10, so fill must include some low p_up rows.
    assert any(float(item["p_up"]) < 0.55 for item in selected)


def test_compute_ev20_net_cost_alignment() -> None:
    assert compute_ev20_net(0.015, 0.002) == 0.013
    assert compute_ev20_net(-0.010, 0.002) == -0.012


def test_build_walk_forward_windows_has_embargo_gap() -> None:
    cfg = MLConfig(
        train_days=10,
        test_days=5,
        step_days=5,
        embargo_days=20,
    )
    all_dates = list(range(1, 80))
    windows = build_walk_forward_windows(all_dates, cfg)
    assert windows
    index_map = {dt: idx for idx, dt in enumerate(all_dates)}
    for window in windows:
        gap = index_map[window["test_start_dt"]] - index_map[window["train_end_dt"]]
        assert gap == cfg.embargo_days + 1


def test_percent_rank_desc_tie_and_missing() -> None:
    values = {"A": 1.0, "B": 1.0, "C": 0.5, "D": None}
    ranks = rankings_cache._percent_rank_desc(values)  # type: ignore[attr-defined]
    assert "D" not in ranks
    assert ranks["A"] == ranks["B"]
    assert ranks["A"] > ranks["C"]


def test_get_rankings_rule_mode_backward_compatible() -> None:
    now = datetime.now(timezone.utc)
    rankings_cache._CACHE = {  # type: ignore[attr-defined]
        ("D", "latest", "up"): [{"code": "1111", "changePct": 0.1}],
    }
    rankings_cache._LAST_UPDATED = now  # type: ignore[attr-defined]
    result = rankings_cache.get_rankings("D", "latest", "up", 50, mode="rule")
    assert result["mode"] == "rule"
    assert result["items"][0]["code"] == "1111"
