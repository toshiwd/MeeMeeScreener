from __future__ import annotations

import json
from pathlib import Path

from scripts import tradex_selected_family_v2_risk_decomposition_v1 as mod


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def _roots(tmp_path: Path) -> dict[str, Path]:
    roots = {
        "validation_root": tmp_path / "validation",
        "wide_ledger": tmp_path / "wide" / "date_level_selection_ledger.jsonl",
        "ma5_trade_ledger": tmp_path / "ma5" / "trade_ledger.jsonl",
    }
    _write_validation(roots["validation_root"])
    _write_wide(roots["wide_ledger"])
    _write_ma5(roots["ma5_trade_ledger"])
    return roots


def _write_validation(root: Path) -> None:
    _write_json(root / "research_decision.json", {"decision": "hold"})
    _write_json(root / "momentum_risk_profile_report.json", {"family_verdict": "hold_risk_decomposition_required"})
    _write_json(root / "ma5_reclaim_context_report.json", {"family_verdict": "hold_additive_candidate_generation_gap"})
    _write_json(root / "combined_family_report.json", {"family_verdict": "hold_combined_requires_common_candidate_ledger"})
    _write_json(root / "source_artifact_refs.json", {"refs": {}})
    _write_json(root / "_ARTIFACT_COMPLETE.json", {"complete": True})


def _momentum_row(i: int, *, family: str = mod.MOMENTUM_FAMILY_ID, big: bool = False, severe: bool = False, ctx: str = "good") -> dict[str, object]:
    return {
        "research_family_id": family,
        "event_date": f"2020-01-{(i % 28) + 1:02d}",
        "code": f"{1000 + (i % 40)}",
        "ret20_fwd": 0.12 if big else -0.08 if severe else 0.01,
        "mfe20": 0.15 if big else 0.03,
        "mae20": -0.02 if not severe else -0.12,
        "win20": big or not severe,
        "severe_loss20": severe,
        "is_big_winner_ret20_ge_10pct": big,
        "is_future_top10_by_ret20": big,
        "pre_ma20_path_state": "pre_ma20_reclaim_base" if ctx == "good" else "pre_ma20_near",
        "weekly_prior_state": "weekly_prior_uptrend" if ctx == "good" else "weekly_prior_strong_up",
        "monthly_prior_state": "monthly_prior_uptrend",
        "negative_guard_match": False if ctx == "good" else True,
        "guard_safe_full": False,
    }


def _write_wide(path: Path, *, no_context_split: bool = False) -> None:
    rows: list[dict[str, object]] = []
    for i in range(160):
        rows.append(_momentum_row(i, big=i < 45, severe=45 <= i < 65, ctx="good" if not no_context_split else "flat"))
    for i in range(160, 360):
        rows.append(_momentum_row(i, big=i < 185, severe=185 <= i < 270, ctx="bad" if not no_context_split else "flat"))
    for i in range(360):
        rows.append(_momentum_row(i, family="all_strength_scoreless_random_top3", big=i < 55, severe=55 <= i < 130, ctx="random"))
    _write_jsonl(path, rows)


def _write_ma5(path: Path, *, missing_fields: bool = False) -> None:
    rows = []
    for i in range(130):
        row = {
            "signal_date": f"2020-01-{(i % 28) + 1:02d}",
            "symbol": f"{1000 + (i % 50)}",
            "ma_stack": "ma5_above_20_below_60",
            "ma60_slope_state": "ma60_rising",
            "ret": 0.02 if i % 3 else -0.01,
            "mfe": 0.06,
            "mae": -0.02,
            "win": i % 3 != 0,
            "severe_loss": False,
        }
        if missing_fields:
            row.pop("signal_date")
        rows.append(row)
    _write_jsonl(path, rows)


def test_risk_decomposition_ready_for_common_ledger_build(tmp_path: Path) -> None:
    payload = mod.run_selected_family_v2_risk_decomposition_v1(output_parent=tmp_path / "out", run_id="risk", **_roots(tmp_path))

    decision = payload["research_decision"]

    assert decision["decision"] == "ready_for_common_top5_candidate_ledger_build"
    assert decision["recommended_next_axis"] == "common_top5_candidate_ledger_build_v1"
    assert decision["top5_direct_improvement_claimed"] is False
    assert payload["ma5_h12_additive_feasibility_report"]["can_map_to_event_date_symbol_source_family"] is True
    assert payload["common_top5_candidate_ledger_design"]["ledger_build_ready"] is True
    assert payload["artifact_complete"]["complete"] is True
    for name in mod.REQUIRED_OUTPUTS:
        assert (Path(payload["output_root"]) / name).exists(), name


def test_risk_decomposition_routes_to_momentum_only_when_ma5_join_fields_missing(tmp_path: Path) -> None:
    roots = _roots(tmp_path)
    _write_ma5(roots["ma5_trade_ledger"], missing_fields=True)

    payload = mod.run_selected_family_v2_risk_decomposition_v1(output_parent=tmp_path / "out", run_id="risk", **roots)

    assert payload["research_decision"]["decision"] == "momentum_only_risk_limited_probe_ready"
    assert "signal_date" in payload["ma5_h12_additive_feasibility_report"]["missing_fields"]
    assert payload["common_top5_candidate_ledger_design"]["ledger_build_ready"] is False


def test_risk_decomposition_drops_when_momentum_and_ma5_are_unusable(tmp_path: Path) -> None:
    roots = _roots(tmp_path)
    _write_wide(roots["wide_ledger"], no_context_split=True)
    _write_ma5(roots["ma5_trade_ledger"], missing_fields=True)

    payload = mod.run_selected_family_v2_risk_decomposition_v1(output_parent=tmp_path / "out", run_id="risk", **roots)

    assert payload["research_decision"]["decision"] == "selected_family_v2_drop"
    assert payload["point_in_time_context_separation_report"]["point_in_time_context_separation_available"] is False
    assert payload["artifact_complete"]["present_outputs"]["_ARTIFACT_COMPLETE.json"] is True
