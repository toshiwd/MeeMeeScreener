from __future__ import annotations

import json
from pathlib import Path

import duckdb

from scripts.tradex_chart_shape_label_shadow_rerank_v1 import run_shadow_rerank


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def test_chart_shape_label_shadow_rerank_improves_top5(tmp_path: Path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    with duckdb.connect(str(db_path)) as con:
        con.execute(
            """
            CREATE TABLE signal_decision_daily (
                dt INTEGER, code VARCHAR, side VARCHAR, entry_qualified BOOLEAN,
                score_snapshot_json VARCHAR, rank_snapshot_json VARCHAR, forward_return_20 DOUBLE
            )
            """
        )
        con.execute(
            """
            CREATE TABLE daily_bars (
                code VARCHAR, date INTEGER, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v BIGINT, source VARCHAR
            )
            """
        )
        rows = []
        ledger_rows = []
        for idx in range(1, 7):
            code = f"10{idx:02d}"
            label = "good_shape" if idx == 6 else "bad_shape"
            score = 0.90 - idx * 0.03
            forward = 0.10 if label == "good_shape" else -0.02
            rows.append(
                (
                    20260501,
                    code,
                    "buy",
                    True,
                    json.dumps({"tradePriorityScore": score}),
                    json.dumps({"finalRank": idx, "tradePriorityScore": score}),
                    forward,
                )
            )
            ledger_rows.append(
                {
                    "dt": 20260501,
                    "code": code,
                    "side": "buy",
                    "entry_qualified": True,
                    "shape_label": label,
                    "forward_return_20": forward,
                }
            )
        con.executemany("INSERT INTO signal_decision_daily VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
        con.executemany(
            "INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [(row[1], 20260501, 100, 101, 99, 100, 1000, "test") for row in rows],
        )

    all_summary = {
        "shape_summary": {
            "good_shape": {"sample_count": 500},
            "bad_shape": {"sample_count": 500},
        },
        "shape_tendency": {
            "good_shape": {"forward_return_20_mean_delta_vs_baseline": 0.08, "tendency": "up_tendency"},
            "bad_shape": {"forward_return_20_mean_delta_vs_baseline": -0.03, "tendency": "down_tendency"},
        },
    }
    entryq_summary = {
        "shape_summary": {
            "good_shape": {"sample_count": 40},
            "bad_shape": {"sample_count": 40},
        },
        "shape_tendency": {
            "good_shape": {"forward_return_20_mean_delta_vs_baseline": 0.08, "tendency": "up_tendency"},
            "bad_shape": {"forward_return_20_mean_delta_vs_baseline": -0.03, "tendency": "down_tendency"},
        },
    }
    all_path = tmp_path / "all.json"
    entryq_path = tmp_path / "entryq.json"
    ledger_path = tmp_path / "ledger.jsonl"
    _write_json(all_path, all_summary)
    _write_json(entryq_path, entryq_summary)
    ledger_path.write_text("\n".join(json.dumps(row) for row in ledger_rows) + "\n", encoding="utf-8")

    result = run_shadow_rerank(
        db_path=db_path,
        buy_all_summary_path=all_path,
        buy_entryq_summary_path=entryq_path,
        buy_entryq_ledger_path=ledger_path,
        output_root=tmp_path / "out",
        start_dt=20260501,
        end_dt=20260501,
        side="buy",
        min_entryq_count=30,
        min_all_count=300,
        boost_scale=1.0,
        max_abs_boost=0.10,
    )

    assert result["silent_fallback_used"] is False
    assert result["scope"]["meemee_ranking_changed"] is False
    assert result["coverage"]["usable_label_count"] == 2
    assert result["topk_compare"]["top5"]["delta"]["mean_forward_return_20"] > 0
    assert result["authoritative_rollup_decision"] in {"partial_improvement_hold", "ranking_improved_hold_for_ma_context"}
