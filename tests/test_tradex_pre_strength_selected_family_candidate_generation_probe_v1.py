from __future__ import annotations

import json
from pathlib import Path

from scripts import tradex_pre_strength_selected_family_candidate_generation_probe_v1 as mod


FAMILY_A = (
    "pre_strength::pre_base_to_strength::"
    "pre_ret20_state=pre20_flat|pre_ret5_state=pre5_flat|"
    "pre_ma20_path_state=pre_ma20_reclaim_base|weekly_prior_state=weekly_prior_mixed|monthly_prior_state=monthly_prior_uptrend"
)
FAMILY_B = (
    "pre_strength::pre_to_event_confirmation::"
    "pre_ma20_path_state=pre_ma20_reclaim_base|pre_candle_energy_state=pre_candle_energy_warning|"
    "pre_wick_warning_state=pre_upper_wick_or_failed_push|event_daily_ret20_state=daily20_down|event_daily_candle_state=daily_strong_bull"
)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def _validation_root(tmp_path: Path, *, weak_family: bool = False) -> Path:
    root = tmp_path / "validation"
    source = tmp_path / "source"
    keep_families = [
        {"family_id": FAMILY_A, "family_group": "pre_base_to_strength"},
        {"family_id": FAMILY_B, "family_group": "pre_to_event_confirmation"},
    ]
    _write_json(
        root / "selected_family_validation_decision.json",
        {"decision": "keep_for_candidate_generation_probe", "selected_next_validation_families": keep_families},
    )
    _write_json(
        root / "validation_contract.json",
        {
            "source_pre_strength_root": str(source),
            "future_label_policy": {"future_labels_used_for_pattern_keys": False},
        },
    )
    _write_json(
        root / "family_validation_report.json",
        {
            "rows": [
                {"family_id": FAMILY_A, "conditions": _conditions(FAMILY_A)},
                {"family_id": FAMILY_B, "conditions": _conditions(FAMILY_B)},
            ]
        },
    )
    _write_jsonl(source / "pre_strength_event_ledger.jsonl", _event_rows(weak_family=weak_family))
    return root


def _conditions(family_id: str) -> dict[str, str]:
    return dict(item.split("=", 1) for item in family_id.split("::", 2)[2].split("|"))


def _event_rows(*, weak_family: bool) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for day_idx in range(12):
        date = f"2024-01-{day_idx + 1:02d}"
        for rank in range(1, 9):
            family_a = rank in {4, 5, 6}
            family_b = rank in {7, 8}
            ret = -0.02
            if rank <= 5:
                ret = 0.004 if rank % 2 == 0 else -0.015
            if family_a or family_b:
                ret = -0.03 if weak_family else 0.12 - rank * 0.002
            row = {
                "code": f"{7000 + day_idx * 10 + rank}",
                "event_date": date,
                "event_month": "2024-01",
                "event_strength_score": 100 - rank,
                "ret20_fwd": ret,
                "mfe20": max(ret, 0) + 0.03,
                "mae20": -0.03,
                "win20": ret > 0,
                "severe_loss20": weak_family and (family_a or family_b),
                "pre_ret20_state": "pre20_flat" if family_a else "other",
                "pre_ret5_state": "pre5_flat" if family_a else "other",
                "pre_ma20_path_state": "pre_ma20_reclaim_base" if (family_a or family_b) else "other",
                "weekly_prior_state": "weekly_prior_mixed" if family_a else "other",
                "monthly_prior_state": "monthly_prior_uptrend" if family_a else "other",
                "pre_candle_energy_state": "pre_candle_energy_warning" if family_b else "other",
                "pre_wick_warning_state": "pre_upper_wick_or_failed_push" if family_b else "other",
                "event_daily_ret20_state": "daily20_down" if family_b else "other",
                "event_daily_candle_state": "daily_strong_bull" if family_b else "other",
                "pre_ma60_context_state": "other",
                "pre_volume_state": "other",
                "pre_compression_state": "other",
            }
            rows.append(row)
    return rows


def test_pre_strength_probe_keeps_improving_variant(tmp_path: Path) -> None:
    payload = mod.run_pre_strength_selected_family_candidate_generation_probe_v1(
        validation_root=_validation_root(tmp_path),
        output_parent=tmp_path / "out",
        run_id="probe",
    )

    decision = payload["research_decision"]
    comparison_rows = payload["baseline_comparison_report"]["rows"]
    assert decision["decision"] == "keep_candidate"
    assert decision["activation_allowed"] is False
    assert decision["meemee_reflectable"] is False
    assert any(row["top5_avg_ret20_delta_vs_baseline"] > 0 for row in comparison_rows)
    assert any(row["candidate_added_count"] > 0 for row in comparison_rows)
    ledger_rows = (Path(payload["output_root"]) / "candidate_generation_ledger.jsonl").read_text(encoding="utf-8").splitlines()
    assert ledger_rows
    assert all(json.loads(line)["matched_family_ids"] for line in ledger_rows[:10])
    assert payload["artifact_complete"]["complete"] is True
    for name in mod.REQUIRED_OUTPUTS:
        assert (Path(payload["output_root"]) / name).exists(), name


def test_pre_strength_probe_drops_weak_variant(tmp_path: Path) -> None:
    payload = mod.run_pre_strength_selected_family_candidate_generation_probe_v1(
        validation_root=_validation_root(tmp_path, weak_family=True),
        output_parent=tmp_path / "out",
        run_id="probe",
    )

    assert payload["research_decision"]["decision"] == "drop"
    assert payload["next_axis_recommendation"]["next"] == "pattern_family_portfolio_refresh_v1"


def test_pre_strength_probe_rejects_future_label_condition(tmp_path: Path) -> None:
    root = _validation_root(tmp_path)
    report_path = root / "family_validation_report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["rows"][0]["conditions"] = {"ret20_fwd": "0.1"}
    _write_json(report_path, report)

    try:
        mod.run_pre_strength_selected_family_candidate_generation_probe_v1(
            validation_root=root,
            output_parent=tmp_path / "out",
            run_id="probe",
        )
    except ValueError as exc:
        assert "future label condition" in str(exc)
    else:
        raise AssertionError("expected future label condition rejection")
