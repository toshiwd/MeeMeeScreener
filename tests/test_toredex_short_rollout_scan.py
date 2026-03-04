from __future__ import annotations

import os
import sys
from datetime import date
from types import SimpleNamespace

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts import toredex_short_rollout_scan as scan


def test_run_scan_is_deterministic_and_variant_diff(monkeypatch) -> None:
    def fake_load_toredex_config(*, override):
        return SimpleNamespace(config_hash=f"hash:{sorted((override or {}).keys())}")

    def fake_build_snapshot(*, season_id, as_of, config, positions):
        return {
            "seasonId": season_id,
            "asOf": as_of.isoformat(),
        }

    def fake_build_decision(*, snapshot, config, prev_metrics, mode):
        season_id = str(snapshot.get("seasonId") or "")
        as_of = str(snapshot.get("asOf") or "")
        variant = season_id.replace("scan_", "")
        day = int(as_of[-2:])

        if variant == "baseline":
            actions = []
        elif variant == "balanced":
            actions = [
                {
                    "ticker": "1111",
                    "side": "SHORT",
                    "deltaUnits": 2,
                    "reasonId": "E_NEW_TOP1_GATE_OK",
                }
            ]
        else:
            ticker = "3333" if day % 2 == 0 else "2222"
            actions = [
                {
                    "ticker": ticker,
                    "side": "SHORT",
                    "deltaUnits": 2,
                    "reasonId": "E_NEW_TOPK_GATE_OK",
                }
            ]
        return {"actions": actions}

    monkeypatch.setattr(scan, "load_toredex_config", fake_load_toredex_config)
    monkeypatch.setattr(scan, "build_snapshot", fake_build_snapshot)
    monkeypatch.setattr(scan, "build_decision", fake_build_decision)

    variants = [
        scan.Variant(name="baseline", override={}),
        scan.Variant(name="balanced", override={"rankingMode": "hybrid"}),
        scan.Variant(name="aggressive", override={"rankingMode": "hybrid", "x": 1}),
    ]

    left = scan.run_scan(
        start_date=date(2026, 3, 1),
        end_date=date(2026, 3, 4),
        variants=variants,
        include_daily=False,
    )
    right = scan.run_scan(
        start_date=date(2026, 3, 1),
        end_date=date(2026, 3, 4),
        variants=variants,
        include_daily=False,
    )

    left.pop("generated_at_utc", None)
    right.pop("generated_at_utc", None)
    assert left == right

    by_name = {row["name"]: row for row in left["variants"]}
    assert by_name["baseline"]["short_entries"] == 0
    assert by_name["balanced"]["short_entries"] == 4
    assert by_name["aggressive"]["short_entries"] == 4
    assert by_name["balanced"]["top_short_tickers"] == [{"ticker": "1111", "count": 4}]
    assert by_name["aggressive"]["top_short_tickers"] == [
        {"ticker": "2222", "count": 2},
        {"ticker": "3333", "count": 2},
    ]
