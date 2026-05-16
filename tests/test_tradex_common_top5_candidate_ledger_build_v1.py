from __future__ import annotations

import json
from pathlib import Path

from scripts import tradex_common_top5_candidate_ledger_build_v1 as mod


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def _roots(tmp_path: Path, *, ma5_overlaps_baseline: bool = False) -> dict[str, Path]:
    roots = {
        "risk_root": tmp_path / "risk",
        "wide_source_refs": tmp_path / "wide" / "source_artifact_refs.json",
        "wide_selection_ledger": tmp_path / "wide" / "date_level_selection_ledger.jsonl",
        "ma5_trade_ledger": tmp_path / "ma5" / "trade_ledger.jsonl",
    }
    pre_strength = tmp_path / "pre_strength" / "pre_strength_event_ledger.jsonl"
    _write_risk(roots["risk_root"])
    _write_source_refs(roots["wide_source_refs"], pre_strength)
    _write_pre_strength(pre_strength)
    _write_selection(roots["wide_selection_ledger"])
    _write_ma5(roots["ma5_trade_ledger"], overlaps_baseline=ma5_overlaps_baseline)
    return roots


def _write_risk(root: Path) -> None:
    _write_json(root / "research_decision.json", {"decision": "ready_for_common_top5_candidate_ledger_build"})
    _write_json(root / "common_top5_candidate_ledger_design.json", {"ledger_build_ready": True})
    _write_json(root / "_ARTIFACT_COMPLETE.json", {"complete": True})


def _write_source_refs(path: Path, pre_strength: Path) -> None:
    _write_json(
        path,
        {
            "refs": [
                {
                    "name": "pre_strength_event_ledger.jsonl",
                    "exists": True,
                    "path": str(pre_strength),
                }
            ]
        },
    )


def _baseline_row(i: int) -> dict[str, object]:
    return {
        "event_date": f"2020-01-{(i % 5) + 1:02d}",
        "code": f"{1000 + i}",
        "event_strength_score": float(10 - i),
        "pre_ma20_path_state": "pre_ma20_reclaim_base",
        "pre_ret20_state": "pre20_up",
        "pre_ret5_state": "pre5_up",
        "weekly_prior_state": "weekly_prior_uptrend",
        "monthly_prior_state": "monthly_prior_uptrend",
        "ret20_fwd": 0.12 if i == 0 else 0.01,
        "mfe20": 0.15,
        "mae20": -0.02,
        "severe_loss20": False,
        "win20": True,
    }


def _write_pre_strength(path: Path) -> None:
    _write_jsonl(path, [_baseline_row(i) for i in range(6)])


def _write_selection(path: Path) -> None:
    rows = []
    for i in range(2):
        base = _baseline_row(i)
        rows.append(
            {
                "research_family_id": mod.MOMENTUM_FAMILY_ID,
                "event_date": base["event_date"],
                "code": base["code"],
                "research_score": 1.0 - i / 10,
                "selection_rank": i + 1,
                "negative_guard_match": False,
                "guard_safe_full": False,
                "pre_ma20_path_state": "pre_ma20_reclaim_base",
                "pre_ret20_state": "pre20_up",
                "pre_ret5_state": "pre5_up",
                "weekly_prior_state": "weekly_prior_uptrend",
                "monthly_prior_state": "monthly_prior_uptrend",
                "is_future_top10_by_ret20": i == 0,
                "is_big_winner_ret20_ge_10pct": i == 0,
            }
        )
    rows.append(
        {
            "research_family_id": mod.BASELINE_FAMILY_ID,
            "event_date": "2020-01-01",
            "code": "1000",
            "research_score": 0.5,
            "selection_rank": 1,
        }
    )
    _write_jsonl(path, rows)


def _write_ma5(path: Path, *, overlaps_baseline: bool) -> None:
    if overlaps_baseline:
        date, symbol = "2020-01-01", "1000"
    else:
        date, symbol = "2020-01-10", "2000"
    _write_jsonl(
        path,
        [
            {
                "signal_date": date,
                "symbol": symbol,
                "ma_stack": "ma5_above_20_below_60",
                "ma60_slope_state": "ma60_rising",
                "ret": 0.02,
                "mfe": 0.08,
                "mae": -0.01,
                "severe_loss": False,
            }
        ],
    )


def test_common_ledger_holds_when_ma5_additive_ret20_labels_are_missing(tmp_path: Path) -> None:
    payload = mod.run_common_top5_candidate_ledger_build_v1(output_parent=tmp_path / "out", run_id="ledger", **_roots(tmp_path))

    research = payload["research_decision"]
    ma5 = payload["ma5_h12_membership_report"]

    assert research["decision"] == "hold"
    assert research["recommended_next_axis"] == "common_ledger_field_repair_v1"
    assert research["top5_improvement_claimed"] is False
    assert ma5["event_date_symbol_reconstructable"] is True
    assert ma5["requires_label_repair_before_direct_top5_validation"] is True
    assert payload["ledger_field_availability_audit"]["fake_score_or_rank_filled"] is False
    assert payload["artifact_complete"]["complete"] is True
    for name in mod.REQUIRED_OUTPUTS:
        assert (Path(payload["output_root"]) / name).exists(), name


def test_common_ledger_ready_when_ma5_rows_overlap_baseline_labels(tmp_path: Path) -> None:
    payload = mod.run_common_top5_candidate_ledger_build_v1(
        output_parent=tmp_path / "out",
        run_id="ledger",
        **_roots(tmp_path, ma5_overlaps_baseline=True),
    )

    assert payload["research_decision"]["decision"] == "common_ledger_ready"
    assert payload["next_axis_recommendation"]["next"] == "common_top5_candidate_pool_validation_v1"
    assert payload["ma5_h12_membership_report"]["requires_label_repair_before_direct_top5_validation"] is False


def test_common_ledger_jsonl_contains_required_flags(tmp_path: Path) -> None:
    payload = mod.run_common_top5_candidate_ledger_build_v1(output_parent=tmp_path / "out", run_id="ledger", **_roots(tmp_path))
    ledger = Path(payload["common_top5_candidate_ledger_path"])
    rows = [json.loads(line) for line in ledger.read_text(encoding="utf-8").splitlines()]

    assert any(row["momentum_candidate_flag"] for row in rows)
    assert any(row["ma5_h12_candidate_flag"] for row in rows)
    assert any(row["combined_candidate_flag"] for row in rows)
    assert all("source_family_flags" in row for row in rows)
    assert all(row["candidate_construction_uses_future_labels"] is False for row in rows)
