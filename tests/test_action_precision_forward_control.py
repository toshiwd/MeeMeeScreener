from __future__ import annotations

import json

from scripts import action_precision_forward_control as control


def test_list_unseen_complete_months_uses_validation_boundary() -> None:
    assert control._list_unseen_complete_months(202602, 202602) == []
    assert control._list_unseen_complete_months(202602, 202604) == [202603, 202604]


def test_build_readiness_holds_when_no_complete_month_exists(monkeypatch, tmp_path) -> None:
    threshold_path = tmp_path / "action_precision_thresholds.json"
    threshold_path.write_text(
        json.dumps(
            {
                "split_contract": {
                    "validation_months": [202601, 202602],
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(control, "CURRENT_THRESHOLDS_ARTIFACT", threshold_path)
    monkeypatch.setattr(
        control,
        "_snapshot_with_horizon",
        lambda *, db_path: {
            "source_db_path": "source.duckdb",
            "snapshot_db_path": "snapshot.duckdb",
            "snapshot_payload": {"snapshot_db_path": "snapshot.duckdb"},
            "snapshot_max_trade_date": 20260417,
            "replay_lookback_start_date": 20160417,
            "last_fully_confirmable_month": 202602,
            "analysis_cutoff_ymd": 20260227,
        },
    )

    readiness = control.build_readiness(db_path="source.duckdb")
    assert readiness["new_complete_unseen_month_exists"] is False
    assert readiness["decision"] == "hold_needs_more_time"
    assert readiness["current_validation_month_end"] == 202602


def test_build_frozen_manifest_uses_authoritative_cluster_lists(monkeypatch, tmp_path) -> None:
    threshold_path = tmp_path / "action_precision_thresholds.json"
    threshold_path.write_text(
        json.dumps(
            {
                "signal_replay_contract": {"entry_convention": "next_session_open_after_signal_date"},
                "thresholds": {"baseline": {"max_days_to_favorable_move": 20}},
                "split_contract": {"validation_months": [202601, 202602]},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    too_late_rules = tmp_path / "action_precision_long_too_late_rules.json"
    too_late_rules.write_text(json.dumps({"selected_block_clusters": ["cluster_a", "cluster_b"]}, ensure_ascii=False), encoding="utf-8")
    weak_rules = tmp_path / "action_precision_long_weak_direction_rules.json"
    weak_rules.write_text(json.dumps({"selected_block_clusters": ["cluster_c"]}, ensure_ascii=False), encoding="utf-8")

    monkeypatch.setattr(control, "CURRENT_THRESHOLDS_ARTIFACT", threshold_path)
    monkeypatch.setattr(control, "CURRENT_TOO_LATE_RULES_ARTIFACT", too_late_rules)
    monkeypatch.setattr(control, "CURRENT_WEAK_RULES_ARTIFACT", weak_rules)
    monkeypatch.setattr(control, "CURRENT_WEAK_CANDIDATES_ARTIFACT", tmp_path / "weak_candidates.json")
    monkeypatch.setattr(control, "CURRENT_WEAK_COMPARE_ARTIFACT", tmp_path / "weak_compare.json")
    monkeypatch.setattr(control, "CURRENT_WEAK_FORWARD_CONFIRM_ARTIFACT", tmp_path / "weak_forward.json")
    monkeypatch.setattr(control, "CURRENT_WEAK_AUTHORITATIVE_ARTIFACT", tmp_path / "weak_authoritative.json")
    monkeypatch.setattr(control, "CURRENT_TOO_LATE_CANDIDATES_ARTIFACT", tmp_path / "too_late_candidates.json")
    monkeypatch.setattr(control, "CURRENT_TOO_LATE_COMPARE_ARTIFACT", tmp_path / "too_late_compare.json")
    monkeypatch.setattr(control, "CURRENT_TOO_LATE_FORWARD_CONFIRM_ARTIFACT", tmp_path / "too_late_forward.json")
    monkeypatch.setattr(control, "CURRENT_TOO_LATE_AUTHORITATIVE_ARTIFACT", tmp_path / "too_late_authoritative.json")

    readiness = {
        "source_db_path": "source.duckdb",
        "snapshot_db_path": "snapshot.duckdb",
        "snapshot_max_trade_date": 20260417,
        "replay_lookback_start_date": 20160417,
        "last_fully_confirmable_month": 202602,
        "current_validation_month_end": 202602,
        "new_complete_unseen_month_exists": False,
    }
    manifest = control.build_frozen_manifest(readiness=readiness)

    assert manifest["threshold_contract_hash"]
    assert manifest["allowed_next_action"] == "defer_and_wait_for_complete_forward_block"
    assert manifest["frozen_revisions"][0]["status"] == "accepted_frozen"
    assert manifest["frozen_revisions"][1]["status"] == "pending_frozen"
    assert manifest["frozen_revisions"][1]["exact_cluster_list"] == ["cluster_a", "cluster_b"]


def test_orchestration_executes_stages_in_order_when_ready(monkeypatch, tmp_path) -> None:
    run_root = tmp_path / "runs"
    monkeypatch.setattr(control, "FORWARD_RUNS_ROOT", run_root)
    monkeypatch.setattr(
        control,
        "build_readiness",
        lambda *, db_path=None: {
            "source_db_path": "source.duckdb",
            "snapshot_db_path": "snapshot.duckdb",
            "snapshot_max_trade_date": 20260417,
            "replay_lookback_start_date": 20160417,
            "last_fully_confirmable_month": 202604,
            "current_validation_month_end": 202602,
            "new_complete_unseen_month_exists": True,
            "new_complete_unseen_months": [202603, 202604],
        },
    )
    monkeypatch.setattr(control, "build_frozen_manifest", lambda *, readiness: {"manifest": True})
    stage_calls: list[str] = []

    def fake_run_replay_stage(*, db_path, output_dir):
        stage_calls.append(output_dir.name)
        output_dir.mkdir(parents=True, exist_ok=True)
        if output_dir.name.endswith(control.TOO_LATE_STAGE):
            (output_dir / "action_precision_long_too_late_forward_confirm.json").write_text(
                json.dumps({"decision": "keep_long_too_late_revision_confirmed", "decision_reason": "ok"}),
                encoding="utf-8",
            )
        elif output_dir.name.endswith(control.WEAK_STAGE):
            (output_dir / "action_precision_long_weak_direction_forward_confirm.json").write_text(
                json.dumps({"decision": "keep_long_weak_direction_revision", "decision_reason": "ok"}),
                encoding="utf-8",
            )
        return {"command": ["python"], "stdout": "", "stderr": "", "returncode": 0}

    monkeypatch.setattr(control, "_run_replay_stage", fake_run_replay_stage)

    orchestration = control.build_orchestration(db_path="source.duckdb")

    assert len(stage_calls) == 2
    assert control.TOO_LATE_STAGE in stage_calls[0]
    assert control.WEAK_STAGE in stage_calls[1]
    assert orchestration["combined_long_unlocked"] is True
    assert orchestration["decision"] == "keep"
    assert orchestration["stages"][0]["stage"] == control.TOO_LATE_STAGE
    assert orchestration["stages"][1]["stage"] == control.WEAK_STAGE


def test_orchestration_holds_without_running_stages(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(control, "build_readiness", lambda *, db_path=None: {
        "source_db_path": "source.duckdb",
        "snapshot_db_path": "snapshot.duckdb",
        "snapshot_max_trade_date": 20260417,
        "replay_lookback_start_date": 20160417,
        "last_fully_confirmable_month": 202602,
        "current_validation_month_end": 202602,
        "new_complete_unseen_month_exists": False,
        "new_complete_unseen_months": [],
    })
    monkeypatch.setattr(control, "build_frozen_manifest", lambda *, readiness: {"manifest": True})

    def fail_run_stage(**_kwargs):  # pragma: no cover - should not be called
        raise AssertionError("run stage should not execute when no complete month exists")

    monkeypatch.setattr(control, "_run_replay_stage", fail_run_stage)

    orchestration = control.build_orchestration(db_path="source.duckdb")

    assert orchestration["decision"] == "hold_needs_more_time"
    assert orchestration["combined_long_unlocked"] is False
    assert orchestration["stages"] == []
