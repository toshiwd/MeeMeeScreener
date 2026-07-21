from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from tradex_short_shape_bad_avoidance_probe_v1 import _daily_rows, _numeric_features_for
from tradex_short_shape_numeric_rule_probe_v1 import FEATURE_NAMES, _apply_rule


AXIS_ID = "short_entry_timing_current_scan_v1"
DEFAULT_DB_PATH = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_entry_timing_rule_probe_v1\current_candidates")


RULES = [
    {
        "rule_id": "stable_2018_2020_high_vs_ma60_flat_ma20",
        "source_splits": ["2018", "2020"],
        "review_strength": "primary",
        "oos_reference": {"n": 73, "entry_now_rate": 0.2602739726027397, "wrong_rate": 0.1917808219178082},
        "clauses": [
            {"feature": "close_vs_ma60", "op": ">=", "threshold": 0.06694160840660072},
            {"feature": "ma20_vs_ma60", "op": "<=", "threshold": 0.022206304917858837},
        ],
    },
    {
        "rule_id": "split_2021_pullback_above_ma60",
        "source_splits": ["2021"],
        "review_strength": "secondary",
        "oos_reference": {"n": 30, "entry_now_rate": 0.3, "wrong_rate": 0.23333333333333334},
        "clauses": [
            {"feature": "close_vs_ma20", "op": "<=", "threshold": -0.005265272066906113},
            {"feature": "ma20_vs_ma60", "op": ">=", "threshold": -0.002249986976753441},
        ],
    },
    {
        "rule_id": "split_2022_ma20_reaccelerating_below_ma60",
        "source_splits": ["2022"],
        "review_strength": "secondary",
        "oos_reference": {"n": 29, "entry_now_rate": 0.3448275862068966, "wrong_rate": 0.20689655172413793},
        "clauses": [
            {"feature": "ma20_vs_ma60", "op": "<=", "threshold": -0.002619846889180249},
            {"feature": "ma20_slope5", "op": ">=", "threshold": 0.013657267925215825},
        ],
    },
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _ma(values: list[float], end_index: int, period: int) -> float | None:
    start = end_index - period + 1
    if start < 0:
        return None
    return sum(values[start : end_index + 1]) / period


def _freshness(conn: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            COALESCE(source, 'pan') AS source,
            max(
                CASE
                    WHEN length(CAST(abs(date) AS VARCHAR)) = 8 THEN CAST(date AS INTEGER)
                    ELSE CAST(strftime(to_timestamp(CAST(date AS BIGINT)), '%Y%m%d') AS INTEGER)
                END
            ) AS max_ymd,
            count(*) AS row_count
        FROM daily_bars
        GROUP BY COALESCE(source, 'pan')
        ORDER BY source
        """
    ).fetchall()
    return [{"source": source, "max_ymd": int(max_ymd), "row_count": int(row_count)} for source, max_ymd, row_count in rows]


def _candidate_setup_rows(db_path: Path, confirmed_as_of: int) -> list[dict[str, Any]]:
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        codes = [str(row[0]) for row in conn.execute("SELECT DISTINCT code FROM daily_bars WHERE COALESCE(source, 'pan') = 'pan' ORDER BY code").fetchall()]
        names = {str(code): str(name or "") for code, name in conn.execute("SELECT code, name FROM tickers").fetchall()}
    finally:
        conn.close()
    rows: list[dict[str, Any]] = []
    for code in codes:
        conn = duckdb.connect(str(db_path), read_only=True)
        try:
            bars = _daily_rows(conn, code)
        finally:
            conn.close()
        if len(bars) < 280:
            continue
        ymds = [int(row[0]) for row in bars]
        if confirmed_as_of not in ymds:
            continue
        index = ymds.index(confirmed_as_of)
        if index < 80:
            continue
        closes = [float(row[4]) for row in bars]
        highs = [float(row[2]) for row in bars]
        volumes = [float(row[5] or 0) for row in bars]
        ma20 = _ma(closes, index, 20)
        ma60 = _ma(closes, index, 60)
        vol20_prev = _ma(volumes, index - 1, 20)
        if ma20 is None or ma60 is None or not vol20_prev:
            continue
        open_, high, low, close = map(float, bars[index][1:5])
        candle_range = high - low
        if candle_range <= 0:
            continue
        upper_wick_ratio = (high - max(open_, close)) / candle_range
        previous_high20 = max(highs[index - 20 : index])
        high_zone_wick = close > previous_high20 and upper_wick_ratio >= 0.25 and volumes[index] / vol20_prev < 1.8
        ma_bear_pullback20 = close < ma20 and high >= ma20 and ma20 < ma60 and close < open_
        if not high_zone_wick and not ma_bear_pullback20:
            continue
        rows.append(
            {
                "sample_key": f"{code}:{confirmed_as_of}",
                "code": code,
                "name": names.get(code, ""),
                "as_of": confirmed_as_of,
                "purpose_outcome_class": "unlabeled_current",
                "ret20": 0,
                "MAE20": 0,
                "MFE20": 0,
                "setup_family": "high_zone_wick" if high_zone_wick else "ma_bear_pullback20",
            }
        )
    return rows


def run(*, db_path: Path, output_root: Path) -> Path:
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        freshness = _freshness(conn)
        confirmed_as_of = max(row["max_ymd"] for row in freshness if row["source"] == "pan")
    finally:
        conn.close()
    setup_rows = _candidate_setup_rows(db_path, confirmed_as_of)
    x, kept = _numeric_features_for(setup_rows, db_path)
    feature_by_key = {
        str(row["sample_key"]): {name: float(value) for name, value in zip(FEATURE_NAMES, vector)}
        for row, vector in zip(kept, x)
    }
    rule_outputs: list[dict[str, Any]] = []
    merged: dict[str, dict[str, Any]] = {}
    for rule in RULES:
        mask = _apply_rule(x, rule["clauses"]) if len(kept) else []
        selected = [row for row, keep in zip(kept, mask) if bool(keep)]
        enriched = []
        for row in selected:
            item = {
                **row,
                "rule_id": rule["rule_id"],
                "review_strength": rule["review_strength"],
                "oos_reference": rule["oos_reference"],
                "numeric_features": feature_by_key.get(str(row["sample_key"]), {}),
            }
            enriched.append(item)
            merged.setdefault(
                str(row["sample_key"]),
                {
                    **row,
                    "matched_rules": [],
                    "numeric_features": feature_by_key.get(str(row["sample_key"]), {}),
                },
            )
            merged[str(row["sample_key"])]["matched_rules"].append(
                {
                    "rule_id": rule["rule_id"],
                    "review_strength": rule["review_strength"],
                    "oos_reference": rule["oos_reference"],
                }
            )
        rule_outputs.append(
            {
                "rule_id": rule["rule_id"],
                "review_strength": rule["review_strength"],
                "clauses": rule["clauses"],
                "oos_reference": rule["oos_reference"],
                "selected_count": len(enriched),
                "rows": enriched,
            }
        )
    current_rows = sorted(
        merged.values(),
        key=lambda row: (max(item["oos_reference"]["entry_now_rate"] for item in row["matched_rules"]), len(row["matched_rules"])),
        reverse=True,
    )
    report = {
        "schema_version": AXIS_ID,
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "db_path": str(db_path),
        "runtime_freshness_by_source": freshness,
        "confirmed_as_of": confirmed_as_of,
        "confirmed_source_policy": "pan only; yahoo provisional excluded",
        "setup_event_count": len(kept),
        "current_candidate_count": len(current_rows),
        "rule_outputs": rule_outputs,
        "current_candidates": current_rows,
        "decision": {
            "candidate_local_decision": "review_only_current_candidates_present" if current_rows else "no_current_candidate",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "current confirmed bar matches timing-rule research axis" if current_rows else "no current confirmed bar matched timing-rule research axis",
        },
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
    }
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    _write_json(output_dir / "current_entry_timing_candidates.json", report)
    _write_json(output_root / "latest_confirmed_current_entry_timing_candidates.json", {"run_root": str(output_dir), **report})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(db_path=args.db_path, output_root=args.output_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
