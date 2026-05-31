from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import tradex_ma60_above_60plus_guard_overlay_replay_v1 as mod


def test_attach_guard_matches_active_window() -> None:
    rows = pd.DataFrame([{"code": "1001", "decision_ymd": 20250110}, {"code": "1002", "decision_ymd": 20250110}])
    windows = pd.DataFrame([{"code": "1001", "anchor_type": "anchor_10", "anchor_ymd": 20250101, "guard_active_until_ymd": 20250131}])
    hit = mod.attach_guard(rows, windows, date_col="decision_ymd")
    assert len(hit) == 1
    assert hit.iloc[0]["guard_anchor_type"] == "anchor_10"


def test_classify_supported_policy_a() -> None:
    a = {"n_overlay_hits": 100, "delta_return_mean": 0.01, "delta_return_median": 0.0, "harmful_delay_rate": 0.2, "helped_delay_rate": 0.3}
    b = {"n_overlay_hits": 0}
    c = {"n_short_veto_hits": 0}
    d = mod.classify(a, b, c)
    assert d["research_decision"] == "overlay_supported"
    assert "stay_guard" in d["best_supported_use"]


def test_run_writes_artifacts_with_monkeypatched_inputs(tmp_path: Path, monkeypatch) -> None:
    guard_root = tmp_path / "guard"
    guard_root.mkdir()
    pd.DataFrame([{"code": "1001", "anchor_type": "anchor_10", "anchor_date": "2025-01-10", "guard_rule_ids": "r1"}]).to_csv(guard_root / "guard_hit_rows.csv", index=False)
    for name in ["selected_guard_rules.json", "guard_vs_baseline_summary.json", "stay_simulation_summary.json"]:
        (guard_root / name).write_text(json.dumps({}), encoding="utf-8")
    (guard_root / "no_lookahead_audit.json").write_text(json.dumps({"audit_result": "pass"}), encoding="utf-8")
    monkeypatch.setattr(mod, "discover_inputs", lambda: {"portfolio_runs": [tmp_path / "p"], "actual_context_runs": []})
    p = tmp_path / "p"
    p.mkdir()
    pd.DataFrame([{"decision_ymd": 20250115, "code": "1001", "selected_for_buy": True}]).to_csv(p / "daily_candidate_snapshot.csv", index=False)
    pd.DataFrame([{"decision_ymd": 20250115, "code": "1001", "action": "sell", "realized_return": 0.0}]).to_csv(p / "orders_ledger.csv", index=False)
    dates = pd.bdate_range("2024-10-01", periods=140)
    daily = tmp_path / "daily.csv"
    pd.DataFrame([{"code": "1001", "date": d.strftime("%Y-%m-%d"), "open": 100+i, "high": 101+i, "low": 99+i, "close": 100+i, "volume": 1000} for i, d in enumerate(dates)]).to_csv(daily, index=False)
    result = mod.run(guard_root=guard_root, output_root=tmp_path / "out", production_csv=daily)
    out = Path(result["output_dir"])
    assert (out / "_ARTIFACT_COMPLETE.json").exists()
    assert json.loads((out / "no_lookahead_audit.json").read_text(encoding="utf-8"))["threshold_sweep"] is False
