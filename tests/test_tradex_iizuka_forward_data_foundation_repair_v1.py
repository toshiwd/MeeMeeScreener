from __future__ import annotations

import pandas as pd

import scripts.tradex_iizuka_forward_data_foundation_repair_v1 as module


def test_research_fallback_sources_exist() -> None:
    assert module.STABLE_BAD_PICK_CLASSIFICATION.exists()
    assert module.STABLE_BAD_PICK_ROWS.exists()
    assert module.STABLE_BAD_PICK_DECISION.exists()


def test_blockers_treat_stable_bad_pick_as_research_fallback_when_available() -> None:
    inventory = module._build_inventory(
        runtime_dates={
            "daily_bars_max_date": "2026-04-30",
            "feature_snapshot_daily_max_date": "2026-04-30",
            "ml_feature_daily_max_date": "2026-04-30",
            "feature_frame_daily_max_date": "2026-04-30",
            "label_20d_max_date": "2026-04-01",
            "ml_label_20d_max_date": "2026-04-01",
            "ml_pred_20d_max_date": "2026-04-01",
        },
        current_overlap={
            "rows": 200,
            "groups": 120,
            "unique_symbols": 18,
            "latest_mature_date": "2026-04-01",
        },
        source_rows=pd.DataFrame(
            {
                "anchor_date": ["2026-01-19", "2026-01-19"],
                "symbol": ["1001", "1002"],
                "side": ["long", "long"],
            }
        ),
        live_family_present=False,
        family_source={
            "available": True,
            "source": str(module.STABLE_BAD_PICK_CLASSIFICATION),
            "stable_bad_pick_family_count": 4,
        },
    )

    blockers = module._build_blockers(inventory)
    stable_blockers = [item for item in blockers if item["blocker"] == "stable_bad_pick_missing"]
    assert stable_blockers, "expected stable_bad_pick_missing blocker"
    assert stable_blockers[0]["status"] == "research-fallback"
    assert stable_blockers[0]["research_fallback_available"] is True

    decision = module._build_decision(
        blockers,
        {"current_overlap": {"rows": 74, "groups": 37, "unique_symbols": 2}},
        {
            "current_overlap": {
                "rows": 200,
                "groups": 120,
                "unique_symbols": 18,
                "latest_mature_date": "2026-04-01",
            },
            "runtime_table_dates": inventory["runtime_table_dates"],
        },
    )
    assert decision["stable_bad_pick_family_mode"] == "research_fallback"
    assert decision["decision"] == "ready_to_rerun_iizuka_forward_accumulation"
