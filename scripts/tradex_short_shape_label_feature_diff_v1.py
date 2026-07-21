from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


AXIS_ID = "tradex_short_shape_label_feature_diff_v1"
DEFAULT_LEDGER = Path(
    r"G:\Tradex\visual_corpus_index_v1\20260705T161110Z-tradex_labeled_visual_ledger_index_v1\labeled_visual_ledger_index.jsonl"
)
DEFAULT_DB = Path("stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_shape_classification_v1")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, ensure_ascii=False, default=str) + "\n" for row in rows), encoding="utf-8")


def _as_db_date(value: Any) -> int:
    text = str(value)
    if "-" in text:
        dt = datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    if len(text) == 8 and text.isdigit():
        dt = datetime.strptime(text, "%Y%m%d").replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    return int(float(text))


def _date_text(value: Any) -> str:
    numeric = int(float(value))
    if numeric > 1_000_000_000:
        return datetime.fromtimestamp(numeric, tz=timezone.utc).strftime("%Y-%m-%d")
    text = str(numeric)
    if len(text) == 8:
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return text


def _safe_float(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(out) or math.isinf(out):
        return None
    return out


FEATURE_SQL = r"""
WITH samples(code, date) AS (
  SELECT * FROM sample_rows
),
base AS (
  SELECT
    b.code,
    b.date,
    b.o, b.h, b.l, b.c, b.v,
    avg(b.c) OVER w7 AS ma7,
    avg(b.c) OVER w20 AS ma20,
    avg(b.c) OVER w60 AS ma60,
    avg(b.c) OVER w100 AS ma100,
    avg(b.c) OVER w200 AS ma200,
    min(b.l) OVER w20 AS low20,
    min(b.l) OVER w60 AS low60,
    max(b.h) OVER w20 AS high20,
    max(b.h) OVER w60 AS high60,
    max(b.h) OVER w120 AS high120,
    min(b.l) OVER w120 AS low120,
    avg(b.v) OVER w20 AS vol20,
    lead(b.c, 20) OVER wc AS c20,
    min(b.l) OVER (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 1 FOLLOWING AND 20 FOLLOWING) AS min_l20,
    max(b.h) OVER (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 1 FOLLOWING AND 10 FOLLOWING) AS max_h10
  FROM daily_bars b
  WINDOW
    wc AS (PARTITION BY b.code ORDER BY b.date),
    w7 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
    w20 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
    w60 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
    w100 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW),
    w120 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW),
    w200 AS (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW)
),
base_lag AS (
  SELECT
    *,
    lag(c, 5) OVER wc AS c_lag5,
    lag(c, 20) OVER wc AS c_lag20,
    lag(c, 60) OVER wc AS c_lag60,
    lag(ma20, 5) OVER wc AS ma20_lag5,
    lag(ma60, 10) OVER wc AS ma60_lag10
  FROM base
  WINDOW wc AS (PARTITION BY code ORDER BY date)
),
feat AS (
  SELECT
    *,
    CASE WHEN h > l THEN (h - greatest(o, c)) / (h - l) END AS upper_wick_ratio,
    CASE WHEN h > l THEN (c - l) / (h - l) END AS close_pos,
    CASE WHEN h > l THEN abs(c - o) / (h - l) END AS body_ratio,
    CASE WHEN c > 0 THEN (c - ma7) / c END AS dist_ma7,
    CASE WHEN c > 0 THEN (c - ma20) / c END AS dist_ma20,
    CASE WHEN c > 0 THEN (c - ma60) / c END AS dist_ma60,
    CASE WHEN c > 0 THEN (c - ma100) / c END AS dist_ma100,
    CASE WHEN c > 0 THEN (c - ma200) / c END AS dist_ma200,
    CASE WHEN c > 0 THEN (c - low20) / c END AS room_to_low20,
    CASE WHEN c > 0 THEN (c - low60) / c END AS room_to_low60,
    CASE WHEN c > 0 THEN (high20 / c) - 1 END AS overhead_to_high20,
    CASE WHEN c > 0 THEN (high60 / c) - 1 END AS overhead_to_high60,
    CASE WHEN c > 0 THEN (high120 / c) - 1 END AS overhead_to_high120,
    CASE WHEN low120 > 0 THEN (c / low120) - 1 END AS pos_from_low120,
    CASE WHEN high120 > 0 THEN (c / high120) - 1 END AS drawdown_from_high120,
    CASE WHEN c_lag5 > 0 THEN (c / c_lag5) - 1 END AS ret_5_back,
    CASE WHEN c_lag20 > 0 THEN (c / c_lag20) - 1 END AS ret_20_back,
    CASE WHEN c_lag60 > 0 THEN (c / c_lag60) - 1 END AS ret_60_back,
    CASE WHEN ma20_lag5 > 0 THEN (ma20 / ma20_lag5) - 1 END AS ma20_slope5,
    CASE WHEN ma60_lag10 > 0 THEN (ma60 / ma60_lag10) - 1 END AS ma60_slope10,
    CASE WHEN vol20 > 0 THEN v / vol20 END AS volume_ratio20,
    CASE WHEN c > 0 THEN (c20 / c) - 1 END AS ret20_forward,
    CASE WHEN c > 0 THEN (min_l20 / c) - 1 END AS mae20_forward,
    CASE WHEN c > 0 THEN (max_h10 / c) - 1 END AS adverse10_forward
  FROM base_lag
)
SELECT feat.*
FROM feat
JOIN samples s ON s.code = feat.code AND s.date = feat.date
"""


NUMERIC_FEATURES = [
    "upper_wick_ratio",
    "close_pos",
    "body_ratio",
    "dist_ma7",
    "dist_ma20",
    "dist_ma60",
    "dist_ma100",
    "dist_ma200",
    "room_to_low20",
    "room_to_low60",
    "overhead_to_high20",
    "overhead_to_high60",
    "overhead_to_high120",
    "pos_from_low120",
    "drawdown_from_high120",
    "ret_5_back",
    "ret_20_back",
    "ret_60_back",
    "ma20_slope5",
    "ma60_slope10",
    "volume_ratio20",
]


def _mean(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    values = sorted(values)
    mid = len(values) // 2
    if len(values) % 2:
        return values[mid]
    return (values[mid - 1] + values[mid]) / 2


def _summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {"n": len(rows)}
    for key in NUMERIC_FEATURES:
        values = [_safe_float(row.get(key)) for row in rows]
        clean = [value for value in values if value is not None]
        out[key] = {"mean": _mean(clean), "median": _median(clean)}
    return out


def _enrich_with_clusters(row: dict[str, Any]) -> dict[str, Any]:
    def v(key: str, default: float = 0.0) -> float:
        value = _safe_float(row.get(key))
        return default if value is None else value

    tags = []
    if v("dist_ma60") < 0 and v("dist_ma100") < 0 and v("ma60_slope10") < 0:
        tags.append("falling_long_ma_pressure")
    if v("ret_60_back") > 0.25 and v("upper_wick_ratio") > 0.45:
        tags.append("climax_after_fast_rise")
    if v("ret_20_back") < -0.08 and v("dist_ma20") > 0:
        tags.append("panic_rebound_to_ma")
    if v("room_to_low60") > 0.12 and v("drawdown_from_high120") < -0.08:
        tags.append("downside_room_after_lower_high")
    if v("dist_ma7") > 0 and v("dist_ma20") > 0 and v("ma20_slope5") > 0:
        tags.append("rising_short_ma_support")
    if v("volume_ratio20") > 2.0:
        tags.append("volume_event")
    return {**row, "shape_cluster_tags": tags}


def run(*, ledger_path: Path, db_path: Path, output_root: Path) -> Path:
    run_dir = output_root / f"{_tag()}-{AXIS_ID}"
    run_dir.mkdir(parents=True, exist_ok=False)
    ledger_rows = _read_jsonl(ledger_path)
    samples = [(str(row["code"]), _as_db_date(row["as_of"])) for row in ledger_rows]
    with duckdb.connect(str(db_path), read_only=True) as conn:
        conn.execute("CREATE TEMP TABLE sample_rows(code VARCHAR, date INTEGER)")
        conn.executemany("INSERT INTO sample_rows VALUES (?, ?)", samples)
        feature_rows = conn.execute(FEATURE_SQL).fetchdf().to_dict("records")
    ledger_by_key = {f"{row['code']}:{row['as_of']}": row for row in ledger_rows}
    joined = []
    for row in feature_rows:
        key = f"{row['code']}:{_date_text(row['date'])}"
        ledger = ledger_by_key.get(key)
        payload = {**row, "key": key}
        if ledger:
            payload.update(
                {
                    "purpose_outcome_class": ledger.get("purpose_outcome_class"),
                    "label_ret20": ledger.get("ret20"),
                    "label_MAE20": ledger.get("MAE20"),
                    "label_MFE20": ledger.get("MFE20"),
                    "saved_path": ledger.get("saved_path"),
                }
            )
        joined.append(_enrich_with_clusters(payload))
    by_class: dict[str, list[dict[str, Any]]] = {}
    by_tag: dict[str, list[dict[str, Any]]] = {}
    for row in joined:
        by_class.setdefault(str(row.get("purpose_outcome_class")), []).append(row)
        for tag in row.get("shape_cluster_tags") or []:
            by_tag.setdefault(tag, []).append(row)
    class_summary = {key: _summarize(value) for key, value in sorted(by_class.items())}
    tag_summary = {key: {"n": len(value), "class_counts": _class_counts(value)} for key, value in sorted(by_tag.items())}
    diffs = []
    good = by_class.get("good_short_shape", [])
    bad = by_class.get("bad_short_shape", [])
    for feature in NUMERIC_FEATURES:
        good_values = [_safe_float(row.get(feature)) for row in good]
        bad_values = [_safe_float(row.get(feature)) for row in bad]
        gm = _mean([v for v in good_values if v is not None])
        bm = _mean([v for v in bad_values if v is not None])
        if gm is not None and bm is not None:
            diffs.append({"feature": feature, "good_mean": gm, "bad_mean": bm, "diff_good_minus_bad": gm - bm})
    diffs.sort(key=lambda row: abs(row["diff_good_minus_bad"]), reverse=True)
    _write_jsonl(run_dir / "label_feature_rows.jsonl", joined)
    audit = {
        "schema_version": f"{AXIS_ID}_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "research_phase": "branching_generation",
        "source_ledger": str(ledger_path),
        "db_path": str(db_path),
        "summary": {
            "ledger_rows": len(ledger_rows),
            "feature_rows_joined": len(joined),
            "class_counts": _class_counts(joined),
            "class_summary": class_summary,
            "tag_summary": tag_summary,
            "top_good_bad_feature_diffs": diffs[:20],
        },
        "artifacts": {
            "label_feature_rows_jsonl": str(run_dir / "label_feature_rows.jsonl"),
        },
        "decision": {
            "candidate_local_decision": "feature_diff_ready_for_cluster_rule_generation",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "labeled image outcomes joined to numeric chart features to discover broad short-shape classes",
        },
        "runtime_db_write": False,
        "meemee_unchanged": True,
        "production_ranking_changed": False,
    }
    _write_json(run_dir / "label_feature_diff_audit.json", audit)
    _write_json(output_root / "latest_label_feature_diff_audit.json", {"run_root": str(run_dir), **audit})
    return run_dir


def _class_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        key = str(row.get("purpose_outcome_class"))
        counts[key] = counts.get(key, 0) + 1
    return counts


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(ledger_path=args.ledger, db_path=args.db, output_root=args.output_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
