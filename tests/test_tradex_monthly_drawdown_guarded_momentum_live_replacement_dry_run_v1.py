from __future__ import annotations

import argparse
import json
from pathlib import Path

from scripts import tradex_monthly_drawdown_guarded_momentum_live_replacement_dry_run_v1 as mod


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _source_plan(tmp_path: Path) -> Path:
    root = tmp_path / "plan"
    _write_json(
        root / "research_decision.json",
        {
            "authoritative_research_decision": "active_replacement_plan_ready_for_live_dry_run",
            "replacement_direction_approved": True,
            "immediate_active_replacement_allowed": False,
        },
    )
    _write_json(
        root / "active_replacement_plan.json",
        {
            "best_variant": {
                "spec": {
                    "variant_id": "monthly_drawdown_guarded_momentum_m+0.02_l-0.02_h-0.02_md-0.005",
                    "momentum_weight": 0.02,
                    "momentum_low_risk_weight": -0.02,
                    "momentum_high_risk_penalty": -0.02,
                    "monthly_down_or_drawdown_penalty": -0.005,
                }
            }
        },
    )
    _write_json(root / "live_dry_run_contract.json", {"required_before_active_replacement": True})
    return root


def _item(
    code: str,
    score: float,
    *,
    momentum: bool = False,
    risk_off: bool = False,
    monthly: str = "box_upper",
    entry_qualified: bool = True,
) -> dict:
    return {
        "code": code,
        "name": f"name-{code}",
        "asOf": "2026-05-14",
        "tradePriorityScore": score,
        "entryScore": score - 0.1,
        "probSide": score,
        "setupType": "breakout" if entry_qualified else "reject",
        "tradeEntryClass": "box_upper_breakout" if entry_qualified else None,
        "entryQualified": entry_qualified,
        "momentumFollowThroughV1": momentum,
        "momentumFollowThroughScore": 1.0 if momentum else 0.1,
        "marketRiskOff": risk_off,
        "marketRegime": "risk_off" if risk_off else "risk_on",
        "monthlyBoxState": monthly,
        "tradeDecisionReasons": ["test"],
        "tradeRiskWatch": ["risk"] if risk_off else [],
        "qualityFlags": [] if entry_qualified else ["entry_not_qualified"],
    }


def _patch_runtime(monkeypatch, tmp_path: Path, items: list[dict]) -> None:
    db = tmp_path / "stocks.duckdb"
    db.write_bytes(b"db")
    runtime = {
        "selected_runtime_db_path": str(db),
        "stale": False,
        "freshness_state": "fresh",
        "freshness_days": 0,
        "latest_available_global_date_iso": "2026-05-14",
        "latest_confirmed_daily_bars_date_iso": "2026-05-14",
    }
    freshness = {
        "stale": False,
        "freshness_state": "fresh",
        "freshness_days": 0,
        "snapshot_as_of": "2026-05-14",
        "current_candidate_available": True,
        "runtime_db_path": str(db),
    }
    payload = {
        "snapshot_as_of": "2026-05-14",
        "freshness_state": "fresh",
        "stale": False,
        "items": items,
    }
    monkeypatch.setattr(mod, "_get_runtime_stock_db_status", lambda: dict(runtime))
    monkeypatch.setattr(mod, "_get_rankings_freshness", lambda limit: dict(freshness))
    monkeypatch.setattr(mod, "_get_active_rankings", lambda limit: dict(payload))


def _run(tmp_path: Path, monkeypatch, items: list[dict]) -> Path:
    _patch_runtime(monkeypatch, tmp_path, items)
    args = argparse.Namespace(
        source_plan_root=_source_plan(tmp_path),
        output_parent=tmp_path / "out",
        run_id="dry-run",
        limit=100,
    )
    return mod.run(args)


def test_live_replacement_dry_run_holds_when_runtime_has_less_than_top5_candidates(tmp_path: Path, monkeypatch) -> None:
    output = _run(
        tmp_path,
        monkeypatch,
        [
            _item("1001", 0.90, momentum=True, risk_off=True),
            _item("1002", 0.80, momentum=True, risk_off=True),
            _item("1003", 0.70, momentum=True, risk_off=True),
        ],
    )

    decision = _read_json(output / "research_decision.json")
    active = _read_json(output / "active_topk_snapshot.json")
    complete = _read_json(output / "_ARTIFACT_COMPLETE.json")

    assert decision["authoritative_research_decision"] == "live_replacement_dry_run_hold"
    assert decision["active_candidate_count"] == 3
    assert decision["typed_reasons"] == ["runtime_active_candidate_count_below_top5"]
    assert active["active_candidate_count"] == 3
    assert complete["complete"] is True


def test_live_replacement_dry_run_passes_when_replacement_changes_top5_without_mutation(tmp_path: Path, monkeypatch) -> None:
    output = _run(
        tmp_path,
        monkeypatch,
        [
            _item("1001", 0.90),
            _item("1002", 0.89),
            _item("1003", 0.88),
            _item("1004", 0.87),
            _item("1005", 0.86),
            _item("1006", 0.855, momentum=True),
        ],
    )

    decision = _read_json(output / "research_decision.json")
    diff = _read_json(output / "active_vs_replacement_diff.json")
    mutation = _read_json(output / "no_mutation_audit.json")

    assert decision["authoritative_research_decision"] == "live_replacement_dry_run_pass"
    assert "1006" in diff["top5"]["added_by_replacement"]
    assert mutation["no_mutation_pass"] is True
    assert mutation["runtime_duckdb_written"] is False


def test_live_replacement_dry_run_records_score_mapping_rows(tmp_path: Path, monkeypatch) -> None:
    output = _run(
        tmp_path,
        monkeypatch,
        [
            _item("1001", 0.90),
            _item("1002", 0.89, momentum=True),
            _item("1003", 0.88, risk_off=True),
            _item("1004", 0.87, monthly="box_mid"),
            _item("1005", 0.86),
        ],
    )

    report = _read_json(output / "replacement_scoring_report.json")
    rollback = _read_json(output / "rollback_verification_report.json")

    assert report["runtime_candidate_count"] == 5
    assert report["momentum_candidate_count"] >= 1
    assert report["high_risk_context_count"] >= 1
    assert report["monthly_down_or_drawdown_count"] >= 1
    assert rollback["rollback_verified_for_this_dry_run"] is True
