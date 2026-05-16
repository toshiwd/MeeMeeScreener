from __future__ import annotations

import json
from pathlib import Path

from scripts import tradex_selected_pattern_family_validation_v1 as mod


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def _build_root(tmp_path: Path, *, weak: bool = False, empty_selected: bool = False) -> Path:
    pre_root = tmp_path / "pre_strength"
    portfolio = tmp_path / "portfolio"
    source_artifact = pre_root / "pattern_leaderboard.json"
    selected = [] if empty_selected else [_selected_row(source_artifact, "pre_reclaim_accumulation", weak=weak)]
    _write_json(
        portfolio / "selected_pattern_families_for_validation.json",
        {
            "selection_count": len(selected),
            "teppan_policy": "keep_watch_only_not_selected_for_frequency_portfolio",
            "selected_pattern_families": selected,
        },
    )
    _write_json(portfolio / "research_decision.json", {"decision": "pattern_family_portfolio_ready"})
    _write_json(source_artifact, {"rows": []})
    _write_json(
        pre_root / "evaluation_contract.json",
        {
            "same_condition_controls": {
                "same_universe_source": "fixture",
                "same_period": True,
                "same_cost_slippage": {"mode": "flat_zero_cost"},
                "artifact_detail_level": "authoritative_full",
            },
            "entry_convention_for_evaluation": "buy next session open",
        },
    )
    _write_json(
        pre_root / "feature_availability_audit.json",
        {
            "used_future_labels_in_pattern_keys": False,
            "pattern_key_columns": sorted(mod.SIGNAL_COLUMNS),
            "label_columns": sorted(mod.LABEL_COLUMNS),
        },
    )
    _write_jsonl(pre_root / "pre_strength_event_ledger.jsonl", _event_rows(weak=weak))
    return portfolio


def _selected_row(source_artifact: Path, family_group: str, *, weak: bool) -> dict[str, object]:
    key = (
        "pre_ma20_path_state=pre_ma20_reclaim_base|"
        "pre_ma60_context_state=pre_ma60_near_or_above|"
        "pre_volume_state=pre_volume_expansion|"
        "pre_compression_state=pre_range_normal|"
        "weekly_prior_state=weekly_prior_strong_up"
    )
    return {
        "family_id": f"pre_strength::{family_group}::{key}",
        "display_name": "fixture selected family",
        "mechanism": "fixture mechanism",
        "source_artifact": str(source_artifact),
        "portfolio_priority_score": 0.1 if weak else 1.0,
    }


def _event_rows(*, weak: bool) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for idx in range(180):
        matched = idx < 140
        win = matched and (idx % 10 != 0)
        ret = -0.02 if weak else 0.025
        if matched and not win:
            ret = -0.04 if not weak else -0.12
        if not matched:
            ret = -0.01 if idx % 3 == 0 else 0.004
        rows.append(
            {
                "code": f"{7000 + idx % 60}",
                "event_date": f"{2023 + (idx % 24) // 12}-{idx % 12 + 1:02d}-{idx % 20 + 1:02d}",
                "event_month": f"{2023 + (idx % 24) // 12}-{idx % 12 + 1:02d}",
                "pre_ma20_path_state": "pre_ma20_reclaim_base" if matched else "other",
                "pre_ma60_context_state": "pre_ma60_near_or_above" if matched else "other",
                "pre_volume_state": "pre_volume_expansion" if matched else "other",
                "pre_compression_state": "pre_range_normal" if matched else "other",
                "weekly_prior_state": "weekly_prior_strong_up" if matched else "other",
                "pre_ret20_state": "pre20_flat",
                "pre_ret5_state": "pre5_flat",
                "pre_candle_energy_state": "pre_candle_energy_warning",
                "pre_wick_warning_state": "pre_upper_wick_or_failed_push",
                "monthly_prior_state": "monthly_prior_uptrend",
                "event_daily_ret20_state": "daily20_down",
                "event_daily_candle_state": "daily_strong_bull",
                "ret20_fwd": ret,
                "mfe20": 0.06,
                "mae20": -0.03,
                "win20": win,
                "severe_loss20": weak and matched and idx % 3 == 0,
            }
        )
    return rows


def test_selected_family_validation_keeps_probe_ready_family(tmp_path: Path) -> None:
    payload = mod.run_selected_pattern_family_validation_v1(
        portfolio_root=_build_root(tmp_path),
        output_parent=tmp_path / "out",
        run_id="validation",
    )

    decision = payload["research_decision"]
    contract = payload["validation_contract"]
    assert decision["decision"] == "keep_for_candidate_generation_probe"
    assert decision["keep_family_count"] == 1
    assert decision["activation_allowed"] is False
    assert decision["meemee_reflectable"] is False
    assert contract["future_label_policy"]["pattern_key_label_overlap"] == []
    assert payload["top5_candidate_pool_readiness_report"]["readiness_decision"] == "candidate_generation_probe_ready"
    assert payload["artifact_complete"]["complete"] is True
    for name in mod.REQUIRED_OUTPUTS:
        assert (Path(payload["output_root"]) / name).exists(), name


def test_selected_family_validation_drops_weak_family(tmp_path: Path) -> None:
    payload = mod.run_selected_pattern_family_validation_v1(
        portfolio_root=_build_root(tmp_path, weak=True),
        output_parent=tmp_path / "out",
        run_id="validation",
    )

    assert payload["research_decision"]["decision"] == "drop_selected_pattern_families"
    assert payload["selected_family_validation_decision"]["drop_family_count"] == 1
    assert payload["next_axis_recommendation"]["next"] == "multi_pattern_candidate_generation_portfolio_redesign_v1"


def test_selected_family_validation_rejects_future_label_in_pattern_key(tmp_path: Path) -> None:
    root = _build_root(tmp_path)
    selected_path = root / "selected_pattern_families_for_validation.json"
    payload = json.loads(selected_path.read_text(encoding="utf-8"))
    payload["selected_pattern_families"][0]["family_id"] = "pre_strength::bad::ret20_fwd=0.1"
    _write_json(selected_path, payload)

    try:
        mod.run_selected_pattern_family_validation_v1(
            portfolio_root=root,
            output_parent=tmp_path / "out",
            run_id="validation",
        )
    except ValueError as exc:
        assert "future label condition" in str(exc)
    else:
        raise AssertionError("expected future label key to be rejected")
