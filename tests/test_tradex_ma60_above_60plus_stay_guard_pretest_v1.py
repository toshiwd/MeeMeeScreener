from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_ma60_above_60plus_stay_guard_pretest_v1 import (
    apply_rule,
    no_lookahead_audit,
    run,
    select_guard_rules,
)


def test_select_guard_rules_prefers_fixed_patterns() -> None:
    rules = pd.DataFrame(
        [
            {
                "anchor_type": "anchor_20",
                "condition": "monthly_box_breakout observed_high AND post_start_held_ma20 observed_high",
                "condition_count": 2,
                "n_selected": 500,
                "positive_rate": 0.55,
                "anchor_base_positive_rate": 0.32,
                "lift_vs_anchor_base": 0.23,
            },
            {
                "anchor_type": "anchor_30",
                "condition": "monthly_box_breakout == 1",
                "condition_count": 1,
                "n_selected": 1000,
                "positive_rate": 0.70,
                "anchor_base_positive_rate": 0.40,
                "lift_vs_anchor_base": 0.30,
            },
        ]
    )
    lift = pd.DataFrame(
        [
            {"anchor_type": "anchor_20", "feature": "monthly_box_breakout", "feature_group": "higher_timeframe_proxy"},
            {"anchor_type": "anchor_20", "feature": "post_start_held_ma20", "feature_group": "pullback_quality"},
        ]
    )

    selected = select_guard_rules(rules, lift)

    assert len(selected) == 1
    assert selected[0].anchor_type == "anchor_20"
    assert selected[0].features == ("monthly_box_breakout", "post_start_held_ma20")


def test_apply_rule_uses_binary_and_q75_without_sweep() -> None:
    anchors = pd.DataFrame(
        {
            "anchor_type": ["anchor_20"] * 4,
            "monthly_box_breakout": [1, 1, 0, 1],
            "dist_ma20_pct": [0.01, 0.02, 0.03, 0.40],
        }
    )
    rules = pd.DataFrame(
        [
            {
                "anchor_type": "anchor_20",
                "condition": "monthly_box_breakout observed_high AND dist_ma20_pct observed_high",
                "condition_count": 2,
                "n_selected": 400,
                "positive_rate": 0.5,
                "lift_vs_anchor_base": 0.2,
            }
        ]
    )
    lift = pd.DataFrame(
        [
            {"anchor_type": "anchor_20", "feature": "monthly_box_breakout", "feature_group": "higher_timeframe_proxy"},
            {"anchor_type": "anchor_20", "feature": "dist_ma20_pct", "feature_group": "ma_structure"},
        ]
    )
    selected = select_guard_rules(rules, lift)

    mask = apply_rule(anchors, selected[0])

    assert mask.tolist() == [False, False, False, True]


def test_no_lookahead_audit_marks_labels_and_no_sweep() -> None:
    audit = no_lookahead_audit({"audit_result": "pass"}, [])

    assert audit["audit_result"] == "pass"
    assert audit["threshold_sweep"] is False
    assert audit["anchor_30_used_for_primary_decision"] is False
    assert "future_reached_60" in audit["label_columns"]


def test_run_writes_required_artifacts(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    anchors = pd.DataFrame(
        [
            {"code": "1001", "anchor_type": "anchor_20", "anchor_date": "2025-03-26", "future_reached_60": True, "final_streak_length": 80, "monthly_box_breakout": 1, "post_start_held_ma20": 1},
            {"code": "1001", "anchor_type": "anchor_20", "anchor_date": "2025-04-23", "future_reached_60": False, "final_streak_length": 30, "monthly_box_breakout": 0, "post_start_held_ma20": 0},
            {"code": "1001", "anchor_type": "anchor_10", "anchor_date": "2025-03-12", "future_reached_60": True, "final_streak_length": 80, "monthly_box_breakout": 1, "post_start_held_ma20": 1},
        ]
    )
    anchors.to_csv(source / "anchor_feature_rows.csv", index=False)
    pd.DataFrame([{"anchor_type": "anchor_20", "feature": "monthly_box_breakout", "feature_group": "higher_timeframe_proxy"}, {"anchor_type": "anchor_20", "feature": "post_start_held_ma20", "feature_group": "pullback_quality"}]).to_csv(source / "feature_lift_by_anchor.csv", index=False)
    pd.DataFrame([{"anchor_type": "anchor_20", "condition": "monthly_box_breakout observed_high AND post_start_held_ma20 observed_high", "condition_count": 2, "n_selected": 300, "positive_rate": 0.5, "anchor_base_positive_rate": 0.2, "lift_vs_anchor_base": 0.3}]).to_csv(source / "simple_rule_candidates.csv", index=False)
    pd.DataFrame([{"code": "1001", "streak_length": 80, "reached_60": True}]).to_csv(source / "streak_events.csv", index=False)
    (source / "no_lookahead_audit.json").write_text(json.dumps({"audit_result": "pass"}), encoding="utf-8")
    dates = pd.bdate_range("2025-01-01", periods=140)
    rows = []
    for idx, date in enumerate(dates):
        close = 100 + idx * 0.5
        rows.append({"code": "1001", "date": date.strftime("%Y-%m-%d"), "open": close, "high": close + 1, "low": close - 1, "close": close, "volume": 1000})
    daily = tmp_path / "daily.csv"
    pd.DataFrame(rows).to_csv(daily, index=False)

    result = run(source_root=source, output_root=tmp_path / "out", production_csv=daily)
    out = Path(result["output_dir"])

    assert set(result["required_artifacts"]).issubset({p.name for p in out.iterdir()}) if "required_artifacts" in result else (out / "_ARTIFACT_COMPLETE.json").exists()
    complete = json.loads((out / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    assert complete["artifact_complete"] is True
    assert (out / "guard_hit_rows.csv").exists()
