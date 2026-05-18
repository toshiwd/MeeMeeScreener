from __future__ import annotations

import csv
import json
from pathlib import Path

import duckdb
import pytest

from scripts import entry_precision_short_bottom_risk_borrow_decomposition_v1 as mod


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _row(
    ymd: int,
    code: str,
    group: str,
    *,
    baseline_selected: bool,
    challenger_selected: bool,
    short_ret20: float,
    outcome_positive: bool,
    monthly_range_prob: float,
    trade_priority_score: float,
    market_regime: str,
    trend_down_strict: bool | None,
) -> dict:
    return {
        "ymd": ymd,
        "code": code,
        "confusion_group": group,
        "baseline_selected": baseline_selected,
        "challenger_selected": challenger_selected,
        "outcome_known": True,
        "outcome_positive": outcome_positive,
        "outcome_bucket": "positive" if outcome_positive else "nonpositive",
        "short_ret_20": short_ret20,
        "short_ret_10": short_ret20 / 2.0,
        "short_ret_5": short_ret20 / 4.0,
        "close_pos": 0.0,
        "dist_low20": 0.0,
        "dist_ma20_signed": 0.0,
        "day_change_pct": 0.0,
        "monthlyRangeProb": monthly_range_prob,
        "monthlyRangePos": 0.0,
        "weeklyBreakoutDownProb": 0.0,
        "monthlyBreakoutDownProb": 0.0,
        "marketRiskOff": True,
        "marketRegime": market_regime,
        "trendDownStrict": trend_down_strict,
        "entryScore": trade_priority_score,
        "tradePriorityScore": trade_priority_score,
        "liquidity20d": 100000.0,
        "mae20": abs(short_ret20) / 2.0,
        "mfe20": abs(short_ret20),
        "baseline_rank": 1,
        "tradeDecisionReasons": "[]",
        "tradeRiskWatch": "[]",
    }


def _write_runtime_db(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE industry_master (
                code TEXT,
                name TEXT,
                sector33_code TEXT,
                sector33_name TEXT,
                market_code TEXT
            )
            """
        )
        rows = [
            ("A001", "Name A001", "SEC01", "化学", "TSE"),
            ("A002", "Name A002", "SEC02", "食料品", "TSE"),
            ("A003", "Name A003", "SEC01", "化学", "TSE"),
            ("A004", "Name A004", "SEC03", "水産・農林業", "TSE"),
            ("A005", "Name A005", "SEC04", "情報・通信業", "TSE"),
        ]
        conn.executemany("INSERT INTO industry_master VALUES (?, ?, ?, ?, ?)", rows)
    finally:
        conn.close()


def _borrow_profile(code: str) -> dict:
    if code == "A005":
        return {
            "latestBalance": {"loanRatio": 0.9, "issueName": "Name A005", "marketName": "TSE"},
            "latestFee": {"currentFeeYen": 0.0, "issueName": "Name A005", "marketName": "TSE"},
            "restrictions": ["halt"],
        }
    if code == "A004":
        return {
            "latestBalance": {"loanRatio": 0.3, "issueName": "Name A004", "marketName": "TSE"},
            "latestFee": {"currentFeeYen": 0.0, "issueName": "Name A004", "marketName": "TSE"},
            "restrictions": [],
        }
    if code == "A001":
        return {
            "latestBalance": {"loanRatio": 1.20, "issueName": "Name A001", "marketName": "TSE"},
            "latestFee": {"currentFeeYen": 0.0, "issueName": "Name A001", "marketName": "TSE"},
            "restrictions": [],
        }
    if code == "A002":
        return {
            "latestBalance": {"loanRatio": 0.2, "issueName": "Name A002", "marketName": "TSE"},
            "latestFee": {"currentFeeYen": 0.05, "issueName": "Name A002", "marketName": "TSE"},
            "restrictions": [],
        }
    if code == "A003":
        return {
            "latestBalance": {"loanRatio": 1.05, "issueName": "Name A003", "marketName": "TSE"},
            "latestFee": {"currentFeeYen": 0.0, "issueName": "Name A003", "marketName": "TSE"},
            "restrictions": [],
        }
    raise KeyError(code)


def _build_fixture_root(tmp_path: Path) -> Path:
    source_root = tmp_path / "stability_root"
    compare_root = tmp_path / "full_recheck_root"
    diagnostic_root = tmp_path / "diagnostic_root"
    runtime_db_path = tmp_path / "runtime.duckdb"

    _write_runtime_db(runtime_db_path)

    rows = [
        _row(20250331, "A001", "kept_good", baseline_selected=True, challenger_selected=True, short_ret20=0.05, outcome_positive=True, monthly_range_prob=0.10, trade_priority_score=0.91, market_regime="risk_off", trend_down_strict=True),
        _row(20250331, "A002", "retained_bad", baseline_selected=True, challenger_selected=True, short_ret20=-0.02, outcome_positive=False, monthly_range_prob=0.60, trade_priority_score=0.89, market_regime="risk_off", trend_down_strict=True),
        _row(20250331, "A003", "retained_unknown", baseline_selected=True, challenger_selected=True, short_ret20=-0.01, outcome_positive=False, monthly_range_prob=0.70, trade_priority_score=0.88, market_regime="risk_off", trend_down_strict=False),
        _row(20250430, "A004", "retained_unknown", baseline_selected=True, challenger_selected=True, short_ret20=-0.03, outcome_positive=False, monthly_range_prob=0.20, trade_priority_score=0.87, market_regime="risk_on", trend_down_strict=False),
        _row(20250430, "A005", "removed_bad", baseline_selected=True, challenger_selected=False, short_ret20=-0.06, outcome_positive=False, monthly_range_prob=0.80, trade_priority_score=0.55, market_regime="risk_on", trend_down_strict=True),
    ]
    _write_csv(diagnostic_root / "short_bottom_risk_confusion_groups.csv", rows)

    compare = {
        "baseline_id": "current_rule_trade_gate_baseline",
        "challenger_id": "short_cleanup_bottom_risk_v1",
        "full_recheck_summary": {
            "baseline": {"count": 5, "hit_rate": 0.40, "mean_ret20": -0.008, "median_ret20": -0.01},
            "challenger": {"count": 4, "hit_rate": 0.50, "mean_ret20": 0.002, "median_ret20": 0.005},
            "delta": {
                "hit_rate_delta": 0.10,
                "known_selected_count_delta": -1,
                "mean_ret20_delta": 0.010,
                "median_ret20_delta": 0.015,
                "removed_bad_known": 1,
                "removed_good_known": 0,
                "retained_bad_known": 1,
                "kept_good_known": 3,
            },
        },
        "selection_branching": {
            "changed_top5_members_count": 1,
            "changed_top10_members_count": 1,
            "changed_rank_count": 1,
            "selection_divergence_reason": "borrow_proxy_fixture",
        },
    }
    _write_json(compare_root / "short_bottom_risk_full_recheck_compare.json", compare)
    _write_json(compare_root / "short_bottom_risk_full_recheck_decision.json", {"decision": "keep_for_stability_replay"})

    borrow_proxy_report = {
        "summary": {
            "candidate_count": 4,
            "code_count": 4,
            "codes": ["A001", "A002", "A003", "A004"],
            "hard_borrow_gap_blocked": False,
            "hard_borrow_gap_code_count": 0,
            "hard_borrow_gap_event_count": 0,
            "hard_borrow_gap_event_share": 0.0,
            "row_count": 4,
            "shortable_proxy_ok_code_count": 1,
            "shortable_proxy_ok_event_count": 1,
            "shortable_proxy_ok_event_share": 0.25,
            "soft_borrow_cost_blocked": True,
            "soft_borrow_cost_code_count": 3,
            "soft_borrow_cost_event_count": 3,
            "soft_borrow_cost_event_share": 0.75,
        },
        "codes": [
            {"code": "A001", "available": True, "hard_gap_reason": None, "soft_cost_reasons": ["loan_ratio_high"], "restriction_count": 0, "current_fee_yen": 0.0, "loan_ratio": 1.2, "shortable_proxy_ok": False},
            {"code": "A002", "available": True, "hard_gap_reason": None, "soft_cost_reasons": ["current_fee_positive"], "restriction_count": 0, "current_fee_yen": 0.05, "loan_ratio": 0.2, "shortable_proxy_ok": False},
            {"code": "A003", "available": True, "hard_gap_reason": None, "soft_cost_reasons": ["loan_ratio_high"], "restriction_count": 0, "current_fee_yen": 0.0, "loan_ratio": 1.05, "shortable_proxy_ok": False},
            {"code": "A004", "available": True, "hard_gap_reason": None, "soft_cost_reasons": [], "restriction_count": 0, "current_fee_yen": 0.0, "loan_ratio": 0.3, "shortable_proxy_ok": True},
        ],
    }
    _write_json(source_root / "short_bottom_risk_borrow_proxy_report.json", borrow_proxy_report)
    _write_json(
        source_root / "short_bottom_risk_stability_replay_decision.json",
        {
            "decision": "hold_due_to_borrow_proxy_gap",
            "borrow_proxy_summary": {
                "candidate_count": 4,
                "code_count": 4,
                "codes": ["A001", "A002", "A003", "A004"],
                "hard_borrow_gap_blocked": False,
                "hard_borrow_gap_code_count": 0,
                "hard_borrow_gap_event_count": 0,
                "hard_borrow_gap_event_share": 0.0,
                "row_count": 4,
                "shortable_proxy_ok_code_count": 1,
                "shortable_proxy_ok_event_count": 1,
                "shortable_proxy_ok_event_share": 0.25,
                "soft_borrow_cost_blocked": True,
                "soft_borrow_cost_code_count": 3,
                "soft_borrow_cost_event_count": 3,
                "soft_borrow_cost_event_share": 0.75,
            },
            "monthly_stability": {"months_with_challenger_absent": 1, "mixed_stability": True},
            "regime_support_summary": {"broad_down_edge_positive": False, "flat_or_mixed_edge_positive": False, "upward_or_non_short_favorable_edge_positive": True},
            "no_lookahead_pass": True,
        },
    )
    _write_json(source_root / "short_bottom_risk_stability_replay_contract.json", {"source_root": str(compare_root), "source_diagnostic_root": str(diagnostic_root)})
    _write_json(source_root / "no_lookahead_audit.json", {"no_lookahead_pass": True})

    return source_root


def test_real_root_writes_required_artifacts_and_holds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source_root = _build_fixture_root(tmp_path)
    runtime_db_path = tmp_path / "runtime.duckdb"

    runtime_status = {
        "validated": True,
        "selected_runtime_db_path": str(runtime_db_path),
        "freshness_state": "fresh",
        "freshness_days": 2,
        "runtime_db_freshness_state": "fresh",
        "runtime_db_freshness_days": 2,
        "runtime_latest_available_global_date": 20260515,
        "runtime_latest_confirmed_daily_bars_date": 20260514,
    }
    freshness = {
        "confirmed": True,
        "current_candidate_available": True,
        "direction": "short",
        "freshness_days": 3,
        "freshness_state": "fresh",
        "snapshot_as_of": "2026-05-14",
    }
    monkeypatch.setattr(mod, "get_runtime_stock_db_status", lambda: dict(runtime_status))
    monkeypatch.setattr(mod, "get_rankings_freshness", lambda **_kwargs: dict(freshness))
    monkeypatch.setattr(mod, "load_taisyaku_snapshot", lambda code, **_kwargs: _borrow_profile(str(code)))

    result = mod.run(source_root=source_root, output_root=tmp_path / "out")
    out_dir = Path(result["output_root"])

    expected = {
        "short_bottom_risk_borrow_decomposition_contract.json",
        "short_bottom_risk_borrow_bucket_events.csv",
        "short_bottom_risk_borrow_bucket_summary.json",
        "short_bottom_risk_soft_cost_concentration.json",
        "short_bottom_risk_borrow_adjusted_compare.json",
        "short_bottom_risk_borrow_decomposition_decision.json",
        "no_lookahead_audit.json",
        "_ARTIFACT_COMPLETE.json",
    }
    assert expected == {path.name for path in out_dir.iterdir()}
    assert result["decision"] == "hold_due_to_insufficient_clean_borrowable_sample"

    contract = json.loads((out_dir / "short_bottom_risk_borrow_decomposition_contract.json").read_text(encoding="utf-8"))
    summary = json.loads((out_dir / "short_bottom_risk_borrow_bucket_summary.json").read_text(encoding="utf-8"))
    conc = json.loads((out_dir / "short_bottom_risk_soft_cost_concentration.json").read_text(encoding="utf-8"))
    decision = json.loads((out_dir / "short_bottom_risk_borrow_decomposition_decision.json").read_text(encoding="utf-8"))
    compare = json.loads((out_dir / "short_bottom_risk_borrow_adjusted_compare.json").read_text(encoding="utf-8"))
    audit = json.loads((out_dir / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    complete = json.loads((out_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))

    with (out_dir / "short_bottom_risk_borrow_bucket_events.csv").open("r", encoding="utf-8", newline="") as handle:
        event_rows = list(csv.DictReader(handle))

    assert complete["complete"] is True
    assert contract["axis"] == "short_cleanup_bottom_risk_v1"
    assert contract["frozen_source_state"]["borrow_proxy_gap_decision"] == "hold_due_to_borrow_proxy_gap"
    assert summary["selected_summary"]["selected_event_count"] == 4
    assert summary["selected_summary"]["soft_borrow_cost_event_count"] == 3
    assert summary["selected_summary"]["clean_borrowable_event_count"] == 1
    assert summary["hard_only_gate_projection"]["breadth_preserved"] is True
    assert summary["clean_only_gate_projection"]["breadth_preserved"] is False
    assert conc["selected"]["soft_cost_event_count"] == 3
    assert conc["selected"]["repeated_soft_cost_names"] == []
    assert decision["decision"] == "hold_due_to_insufficient_clean_borrowable_sample"
    assert decision["criteria_state"]["no_lookahead_pass"] is True
    assert compare["selected_borrow_gate_projection"]["hard_only_gate_breadth_ok"] is True
    assert compare["dependency_readout"]["edge_depends_on_soft_cost_names"] is True
    assert audit["no_lookahead_pass"] is True
    assert len(event_rows) == 5
    assert any(row["borrow_bucket"] == "clean_borrowable" for row in event_rows)
    assert any(row["borrow_bucket"] == "soft_borrow_cost_flagged" for row in event_rows)


@pytest.mark.parametrize(
    "summary, soft_conc, compare, expected",
    [
        (
            {
                "selected": {
                    "selected_event_count": 10,
                    "selected_code_count": 10,
                    "hard_borrow_gap_event_count": 2,
                    "hard_borrow_gap_event_share": 0.2,
                    "hard_borrow_gap_code_count": 3,
                    "soft_cost_event_share": 0.4,
                    "clean_borrowable_event_share": 0.4,
                    "clean_borrowable_event_count": 4,
                    "clean_borrowable_code_count": 4,
                }
            },
            {"selected": {"code": {"top1_share": 0.1}, "sector": {"top1_share": 0.1}}},
            {"dependency_readout": {"edge_depends_on_soft_cost_names": False, "clean_sample_too_small": False}, "selected_borrow_gate_projection": {"hard_only_gate_breadth_ok": True, "clean_only_gate_breadth_ok": True}},
            "drop_due_to_borrow_untradable",
        ),
        (
            {
                "selected": {
                    "selected_event_count": 4,
                    "selected_code_count": 4,
                    "hard_borrow_gap_event_count": 0,
                    "hard_borrow_gap_event_share": 0.0,
                    "hard_borrow_gap_code_count": 0,
                    "soft_cost_event_share": 0.75,
                    "clean_borrowable_event_share": 0.25,
                    "clean_borrowable_event_count": 1,
                    "clean_borrowable_code_count": 1,
                }
            },
            {"selected": {"code": {"top1_share": 0.34}, "sector": {"top1_share": 0.34}}},
            {"dependency_readout": {"edge_depends_on_soft_cost_names": True, "clean_sample_too_small": True}, "selected_borrow_gate_projection": {"hard_only_gate_breadth_ok": True, "clean_only_gate_breadth_ok": False}},
            "hold_due_to_insufficient_clean_borrowable_sample",
        ),
        (
            {
                "selected": {
                    "selected_event_count": 6,
                    "selected_code_count": 6,
                    "hard_borrow_gap_event_count": 0,
                    "hard_borrow_gap_event_share": 0.0,
                    "hard_borrow_gap_code_count": 0,
                    "soft_cost_event_share": 0.5,
                    "clean_borrowable_event_share": 0.5,
                    "clean_borrowable_event_count": 3,
                    "clean_borrowable_code_count": 3,
                }
            },
            {"selected": {"code": {"top1_share": 0.6}, "sector": {"top1_share": 0.6}}},
            {"dependency_readout": {"edge_depends_on_soft_cost_names": True, "clean_sample_too_small": False}, "selected_borrow_gate_projection": {"hard_only_gate_breadth_ok": True, "clean_only_gate_breadth_ok": True}},
            "drop_as_edge_depends_on_soft_cost_names",
        ),
        (
            {
                "selected": {
                    "selected_event_count": 6,
                    "selected_code_count": 6,
                    "hard_borrow_gap_event_count": 0,
                    "hard_borrow_gap_event_share": 0.0,
                    "hard_borrow_gap_code_count": 0,
                    "soft_cost_event_share": 0.2,
                    "clean_borrowable_event_share": 0.8,
                    "clean_borrowable_event_count": 5,
                    "clean_borrowable_code_count": 5,
                }
            },
            {"selected": {"code": {"top1_share": 0.1}, "sector": {"top1_share": 0.1}}},
            {"dependency_readout": {"edge_depends_on_soft_cost_names": False, "clean_sample_too_small": False}, "selected_borrow_gate_projection": {"hard_only_gate_breadth_ok": True, "clean_only_gate_breadth_ok": True}},
            "keep_for_borrow_caveated_paper_replay",
        ),
    ],
)
def test_decision_builder_label_branches(summary: dict, soft_conc: dict, compare: dict, expected: str) -> None:
    source_context = {
        "stability_decision": {"decision": "hold_due_to_borrow_proxy_gap"},
        "full_recheck_decision": {"decision": "keep_for_stability_replay"},
        "no_lookahead": {"no_lookahead_pass": True},
    }
    runtime_context = {"runtime_status": {"validated": True}}
    decision = mod._build_decision(
        source_context=source_context,
        runtime_context=runtime_context,
        rows=[],
        bucket_summary=summary,
        soft_cost_concentration=soft_conc,
        borrow_adjusted_compare=compare,
    )
    assert decision["decision"] == expected
    assert decision["criteria_state"]["no_lookahead_pass"] is True
