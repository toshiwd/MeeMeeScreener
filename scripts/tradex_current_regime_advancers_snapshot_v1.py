"""Materialize review-only current advancers-ratio rows for short-board signal dates."""
import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", type=Path, required=True)
    ap.add_argument("--source-board", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    a = ap.parse_args()
    a.output.mkdir(parents=True, exist_ok=False)

    board = json.loads(a.source_board.read_text(encoding="utf-8"))
    dates = sorted({int(row["signal_ymd"]) for row in board.get("candidates", [])})
    if not dates:
        raise ValueError("source board has no signal dates")

    source = duckdb.connect(str(a.db), read_only=True)
    rows = source.execute(
        """
        WITH normalized AS (
            SELECT
                code,
                CASE
                    WHEN date >= 1000000000000 THEN CAST(strftime(to_timestamp(date / 1000), '%Y%m%d') AS INTEGER)
                    WHEN date >= 1000000000 THEN CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)
                    ELSE CAST(date AS INTEGER)
                END AS dt,
                c,
                LAG(c) OVER (PARTITION BY code ORDER BY date) AS prev_close
            FROM daily_bars
            WHERE c IS NOT NULL
        )
        SELECT
            dt,
            AVG(CASE WHEN prev_close IS NOT NULL AND c > prev_close THEN 1.0 ELSE 0.0 END) AS advancers_ratio,
            COUNT(*) AS symbol_count
        FROM normalized
        WHERE dt IN (SELECT UNNEST(?))
        GROUP BY dt
        ORDER BY dt
        """,
        [dates],
    ).fetchall()
    source.close()
    found = {int(row[0]) for row in rows}
    missing = sorted(set(dates) - found)

    regime_db = a.output / "market_regime_snapshot.duckdb"
    target = duckdb.connect(str(regime_db))
    target.execute(
        """
        CREATE TABLE market_regime_daily (
            dt INTEGER,
            regime_id VARCHAR,
            breadth_above_ma20 DOUBLE,
            breadth_above_ma60 DOUBLE,
            advancers_ratio DOUBLE,
            index_close_vs_ma20 DOUBLE,
            index_close_vs_ma60 DOUBLE,
            market_atr_pct DOUBLE,
            sector_dispersion DOUBLE,
            regime_score DOUBLE,
            label_version VARCHAR,
            created_at TIMESTAMP
        )
        """
    )
    created = datetime.now(timezone.utc).replace(tzinfo=None)
    target.executemany(
        """
        INSERT INTO market_regime_daily
        VALUES (?, 'ADVANCERS_ONLY_REVIEW', NULL, NULL, ?, NULL, NULL, NULL, NULL, NULL, 'review-current-v1', ?)
        """,
        [[int(dt), float(ratio), created] for dt, ratio, _count in rows],
    )
    target.close()

    result = {
        "schema_version": "tradex_current_regime_advancers_snapshot_v1.compare.v1",
        "artifact_role": "authoritative_review_only_current_advancers_snapshot",
        "review_only": True,
        "research_fallback": True,
        "fixed_conditions": {
            "metric": "AVG(close > previous close) across runtime daily_bars symbols",
            "signal_dates": dates,
            "permission_threshold_unchanged": 0.650360,
            "runtime_db_write": False,
        },
        "authoritative_result": {
            "rows": [
                {"dt": int(dt), "advancers_ratio": float(ratio), "symbol_count": int(count)}
                for dt, ratio, count in rows
            ],
            "missing_dates": missing,
        },
        "judgment": {
            "candidate_local_decision": "keep" if not missing else "hold",
            "authoritative_rollup_decision": (
                "ready_review_only_current_advancers_snapshot" if not missing else "hold_missing_signal_dates"
            ),
            "reason_type": "exact_current_advancers_metric_without_runtime_db_mutation",
        },
        "not_changed": ["runtime DB", "MeeMee", "ranking", "short selector thresholds"],
    }
    cp = a.output / "compare.json"
    cp.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "sources": {
            "runtime_db": {"path": str(a.db.resolve()), "read_only": True},
            "source_board": {"path": str(a.source_board.resolve()), "sha256": sha(a.source_board)},
        },
        "regime_db_sha256": sha(regime_db),
        "compare_sha256": sha(cp),
    }
    (a.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (a.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(cp)}, indent=2)
        + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(a.output), "rows": result["authoritative_result"]["rows"], "missing": missing}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
