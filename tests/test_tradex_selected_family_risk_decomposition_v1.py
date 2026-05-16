from __future__ import annotations

import json
from pathlib import Path

from scripts import tradex_selected_family_risk_decomposition_v1 as mod


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
DROP_FAMILY = (
    "pre_strength::pre_reclaim_accumulation::"
    "pre_ma20_path_state=pre_ma20_already_extended|pre_ma60_context_state=pre_ma60_extended_above|"
    "pre_volume_state=pre_volume_expansion|pre_compression_state=pre_range_normal|weekly_prior_state=weekly_prior_strong_up"
)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def _root(tmp_path: Path, *, future_label_condition: bool = False) -> Path:
    probe = tmp_path / "probe"
    validation = tmp_path / "validation"
    source = tmp_path / "source"
    variants = _variants()
    if future_label_condition:
        variants["variants"][0]["conditions_by_family"][FAMILY_A] = {"ret20_fwd": "0.1"}
    _write_json(probe / "probe_contract.json", {"source_validation_root": str(validation), "source_pre_strength_root": str(source), "evaluation_date_policy": "same_dates_where_variant_has_candidates", "baseline_policy": "baseline", "variant_policy": "variant", "future_label_policy": {"future_labels_used_for_candidate_generation": False}})
    _write_json(probe / "candidate_generation_probe_decision.json", {"decision": "hold"})
    _write_json(probe / "candidate_generation_variants.json", variants)
    _write_json(
        probe / "baseline_comparison_report.json",
        {
            "rows": [
                _comparison("pre_base_to_strength_only", safe=True, upside=False),
                _comparison("pre_to_event_confirmation_only", safe=False, upside=True),
                _comparison("combined_keep_families", safe=True, upside=True),
            ]
        },
    )
    _write_json(
        validation / "family_validation_report.json",
        {
            "rows": [
                {"family_id": FAMILY_A, "validation_decision": "keep_for_candidate_generation_probe", "conditions": _conditions(FAMILY_A)},
                {"family_id": FAMILY_B, "validation_decision": "keep_for_candidate_generation_probe", "conditions": _conditions(FAMILY_B)},
                {"family_id": DROP_FAMILY, "validation_decision": "drop_from_selected_validation", "conditions": _conditions(DROP_FAMILY)},
            ]
        },
    )
    _write_jsonl(source / "pre_strength_event_ledger.jsonl", _events())
    return probe


def _variants() -> dict[str, object]:
    return {
        "variants": [
            {"variant_id": "pre_base_to_strength_only", "family_ids": [FAMILY_A], "conditions_by_family": {FAMILY_A: _conditions(FAMILY_A)}},
            {"variant_id": "pre_to_event_confirmation_only", "family_ids": [FAMILY_B], "conditions_by_family": {FAMILY_B: _conditions(FAMILY_B)}},
            {"variant_id": "combined_keep_families", "family_ids": [FAMILY_A, FAMILY_B], "conditions_by_family": {FAMILY_A: _conditions(FAMILY_A), FAMILY_B: _conditions(FAMILY_B)}},
        ]
    }


def _conditions(family_id: str) -> dict[str, str]:
    return dict(item.split("=", 1) for item in family_id.split("::", 2)[2].split("|"))


def _comparison(variant_id: str, *, safe: bool, upside: bool) -> dict[str, object]:
    return {
        "variant_id": variant_id,
        "top5_avg_ret20_delta_vs_baseline": 0.02 if upside else 0.004,
        "top5_severe_loss_rate_delta_vs_baseline": -0.04 if safe else -0.01,
        "top5_bad_pick_count_delta_vs_baseline": -8 if safe else -2,
        "top5_big_winner_capture_rate_delta_vs_baseline": -0.1,
        "top5_future_top10_capture_rate_delta_vs_baseline": -0.1,
        "human_selectable_day_rate_delta_vs_baseline": 0.0,
    }


def _events() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for day in range(1, 8):
        date = f"2024-02-{day:02d}"
        for rank in range(1, 9):
            fam_a = rank in {5, 6}
            fam_b = rank == 7
            dropped = rank == 8
            ret = 0.15 if dropped else 0.06 if fam_b else 0.02 if fam_a else -0.01
            rows.append(
                {
                    "code": f"{7000 + day * 10 + rank}",
                    "event_date": date,
                    "event_month": "2024-02",
                    "event_strength_score": 100 - rank,
                    "ret20_fwd": ret,
                    "mfe20": ret + 0.02,
                    "mae20": -0.03,
                    "win20": ret > 0,
                    "severe_loss20": False,
                    "pre_ret20_state": "pre20_flat" if fam_a else "other",
                    "pre_ret5_state": "pre5_flat" if fam_a else "other",
                    "pre_ma20_path_state": "pre_ma20_reclaim_base" if (fam_a or fam_b) else "pre_ma20_already_extended" if dropped else "other",
                    "weekly_prior_state": "weekly_prior_mixed" if fam_a else "weekly_prior_strong_up" if dropped else "other",
                    "monthly_prior_state": "monthly_prior_uptrend" if fam_a else "other",
                    "pre_candle_energy_state": "pre_candle_energy_warning" if fam_b else "other",
                    "pre_wick_warning_state": "pre_upper_wick_or_failed_push" if fam_b else "other",
                    "event_daily_ret20_state": "daily20_down" if fam_b else "other",
                    "event_daily_candle_state": "daily_strong_bull" if fam_b else "other",
                    "pre_ma60_context_state": "pre_ma60_extended_above" if dropped else "other",
                    "pre_volume_state": "pre_volume_expansion" if dropped else "other",
                    "pre_compression_state": "pre_range_normal" if dropped else "other",
                }
            )
    return rows


def test_risk_decomposition_writes_complete_family_blend_recommendation(tmp_path: Path) -> None:
    payload = mod.run_selected_family_risk_decomposition_v1(
        probe_root=_root(tmp_path),
        output_parent=tmp_path / "out",
        run_id="risk",
    )

    assert payload["research_decision"]["decision"] == "family_blend_probe_ready"
    assert payload["next_design_recommendation"]["next"] == "safe_plus_upside_family_blend_probe_v1"
    assert payload["artifact_complete"]["complete"] is True
    assert payload["artifact_complete"]["silent_fallback_used"] is False
    assert payload["big_winner_capture_loss_report"]["rows"]
    for name in mod.REQUIRED_OUTPUTS:
        assert (Path(payload["output_root"]) / name).exists(), name


def test_risk_decomposition_rejects_future_label_condition(tmp_path: Path) -> None:
    try:
        mod.run_selected_family_risk_decomposition_v1(
            probe_root=_root(tmp_path, future_label_condition=True),
            output_parent=tmp_path / "out",
            run_id="risk",
        )
    except ValueError as exc:
        assert "future label condition" in str(exc)
    else:
        raise AssertionError("expected future label condition rejection")
