from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest


def _load_module():
    import scripts.tradex_actual_trade_short_exit_paper_execution_replay_v1 as mod

    return mod


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _minimal_trade_rows() -> list[dict[str, object]]:
    return [
        {
            "policy_id": "takeprofit_0p0375_close",
            "execution_convention": "next_session_open",
            "normalized_trade_id": "T1",
            "symbol": "1111",
            "entry_date": "2026-01-05",
            "actual_exit_date": "2026-01-07",
            "holding_days_actual": 2,
            "entry_price": 100.0,
            "actual_exit_price": 90.0,
            "quantity": 100.0,
            "actual_gross_pnl": 1000.0,
            "actual_return_pct": 0.1,
            "sim_gross_pnl": 1100.0,
            "sim_return_pct": 0.11,
            "sim_minus_actual_pnl": 100.0,
            "sim_minus_actual_return_pct": 0.01,
            "final_exit_day": 2,
            "decision_day_index": 1,
            "fill_day_index": 2,
            "fallback_count": 0,
            "fallback_used": False,
            "actions_json": "[]",
            "entry_month": "2026-01",
            "entry_year": "2026",
        }
    ]


def test_real_run_keeps_paper_execution_replay_candidate(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    mod = _load_module()
    monkeypatch.setattr(mod, "OUT_BASE", tmp_path)
    result = mod.run(output_base=tmp_path)
    decision = result["decision"]
    assert decision["decision"] == "keep_for_paper_execution_replay"
    assert decision["paper_execution_replay_ready"] is True
    assert decision["paper_replay_ready"] is True
    assert decision["production_candidate"] is False
    assert decision["meemee_reflectable"] is False
    assert decision["no_lookahead_pass"] is True
    assert decision["best_execution_convention"] == "next_session_open"
    assert decision["best_policy_label"] == "takeprofit_0p0375_close@next_session_open"
    out_root = Path(result["run_root"])
    for name in mod.REQUIRED_OUTPUTS:
        assert (out_root / name).exists()
    payload = json.loads((out_root / "paper_execution_replay_decision.json").read_text(encoding="utf-8"))
    assert payload["decision"] == "keep_for_paper_execution_replay"
    assert payload["paper_execution_replay_ready"] is True
    assert payload["next_gate"] == "ops_review_after_paper_execution"


def test_source_not_ready_holds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    mod = _load_module()
    source_root = tmp_path / "source"
    source_root.mkdir()
    _write_json(
        source_root / "short_exit_execution_convention_decision.json",
        {
            "decision": "hold_or_drop_execution_convention",
            "best_policy_id": "takeprofit_0p0375_close",
            "best_policy_label": "takeprofit_0p0375_close@next_session_open",
            "best_execution_convention": "next_session_open",
            "paper_replay_ready": False,
            "shadow_paper_replay_candidate": False,
            "production_candidate": False,
            "meemee_reflectable": False,
            "publish_allowed": False,
            "live_sell_signal_allowed": False,
            "no_lookahead_pass": True,
            "fallback_summary": {"fallback_count": 0, "fallback_share": 0.0, "trade_count": 1},
            "best_policy_metrics": {"sim_profit_factor": 1.6, "sim_return_mean": 0.1, "sim_return_median": 0.1},
            "best_policy_concentration": {"robustness_classification": "moderately_concentrated_effect"},
        },
    )
    _write_json(
        source_root / "short_exit_execution_convention_compare.json",
        {
            "source_same_day_best_policy_id": "takeprofit_0p0375_close",
            "best_next_open_policy_id": "takeprofit_0p0375_close",
            "best_next_open_policy_label": "takeprofit_0p0375_close@next_session_open",
            "best_next_open_metrics": {"sim_profit_factor": 1.6, "sim_return_mean": 0.1, "sim_return_median": 0.1},
            "best_next_open_concentration": {"robustness_classification": "moderately_concentrated_effect"},
        },
    )
    _write_json(source_root / "short_exit_execution_convention_monthly_stability.json", {"2026-01": {"trade_count": 1}})
    _write_json(source_root / "short_exit_execution_convention_concentration_summary.json", {"trade_count": 1})
    _write_json(source_root / "no_lookahead_audit.json", {"pass": True})
    _write_csv(source_root / "short_exit_execution_convention_trade_rows.csv", _minimal_trade_rows())

    monkeypatch.setattr(mod, "SOURCE_ROOT", source_root)
    monkeypatch.setattr(mod, "OUT_BASE", tmp_path / "out")
    result = mod.run(source_root=source_root, output_base=tmp_path / "out")
    decision = result["decision"]
    assert decision["decision"] == "hold_due_to_source_not_ready"
    assert decision["paper_execution_replay_ready"] is False
    assert decision["production_candidate"] is False
    assert decision["no_lookahead_pass"] is True


def test_required_artifact_names_are_stable():
    mod = _load_module()
    assert "paper_execution_replay_contract.json" in mod.REQUIRED_OUTPUTS
    assert "paper_execution_replay_decision.json" in mod.REQUIRED_OUTPUTS
    assert mod.AXIS_ID == "tradex_actual_trade_short_exit_paper_execution_replay_v1"
