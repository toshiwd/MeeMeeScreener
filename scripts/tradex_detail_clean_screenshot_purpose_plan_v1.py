from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path


AXIS_ID = "detail_clean_screenshot_purpose_plan_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\meemee_detail_clean_screenshot_purpose_plan_v1")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def _ymd_to_iso(ymd: int) -> str:
    text = str(int(ymd))
    return f"{text[:4]}-{text[4:6]}-{text[6:8]}"


def _load_candidates(
    conn: duckdb.DuckDBPyConnection,
    *,
    ranking_logic_version: str,
    direction: str,
    top_k: int,
    start_ymd: int,
    end_ymd: int,
    require_entry_qualified: bool,
) -> list[dict[str, Any]]:
    return conn.execute(
        """
        WITH ranked AS (
            SELECT
                r.code,
                r.dt AS as_of,
                r.rank,
                r.name,
                r.display_score,
                r.signal_state_at_appearance,
                r.entry_qualified_at_appearance,
                r.setup_type_at_appearance,
                r.return_30d,
                r.max_favorable_30,
                r.max_adverse_30,
                r.current_directional_return,
                r.payload_json
            FROM ranking_appearance_daily r
            WHERE r.ranking_logic_version = ?
              AND r.dir = ?
              AND r.rank <= ?
              AND r.dt BETWEEN ? AND ?
              AND (? = FALSE OR r.entry_qualified_at_appearance = TRUE)
        ),
        bars AS (
            SELECT
                code,
                CASE
                    WHEN length(CAST(abs(date) AS VARCHAR)) = 8 THEN CAST(date AS INTEGER)
                    ELSE CAST(strftime(to_timestamp(CAST(date AS BIGINT)), '%Y%m%d') AS INTEGER)
                END AS ymd,
                c
            FROM daily_bars
            WHERE COALESCE(source, 'pan') <> 'yahoo'
              AND c IS NOT NULL
        ),
        sequenced AS (
            SELECT code, ymd, c, row_number() OVER (PARTITION BY code ORDER BY ymd) AS seq
            FROM bars
        )
        SELECT
            ranked.*,
            anchor.c AS anchor_close,
            future.ymd AS label_end_as_of,
            future.c / anchor.c - 1.0 AS ret20
        FROM ranked
        JOIN sequenced anchor
          ON anchor.code = ranked.code AND anchor.ymd = ranked.as_of
        JOIN sequenced future
          ON future.code = ranked.code AND future.seq = anchor.seq + 20
        ORDER BY ranked.as_of, ranked.rank, ranked.code
        """,
        [ranking_logic_version, direction, top_k, start_ymd, end_ymd, require_entry_qualified],
    ).fetchdf().to_dict("records")


def _split_for(as_of: int) -> str:
    if as_of <= 20241230:
        return "train"
    if as_of <= 20251230:
        return "validation"
    if as_of <= 20260529:
        return "test"
    return "embargo"


def _class_for(ret20: float) -> str:
    if ret20 >= 0.08:
        return "winner_ret20_ge_8pct"
    if ret20 <= -0.05:
        return "loser_ret20_le_minus_5pct"
    return "middle"


def build_plan(
    *,
    db_path: Path,
    output_root: Path,
    ranking_logic_version: str,
    direction: str,
    top_k: int,
    start_ymd: int,
    end_ymd: int,
    max_per_class_split: int,
    require_entry_qualified: bool,
) -> Path:
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        candidates = _load_candidates(
            conn,
            ranking_logic_version=ranking_logic_version,
            direction=direction,
            top_k=top_k,
            start_ymd=start_ymd,
            end_ymd=end_ymd,
            require_entry_qualified=require_entry_qualified,
        )
    finally:
        conn.close()

    enriched: list[dict[str, Any]] = []
    for row in candidates:
        as_of = int(row["as_of"])
        ret20 = float(row["ret20"])
        split = _split_for(as_of)
        outcome_class = _class_for(ret20)
        enriched.append({
            "sample_key": f"{row['code']}:{as_of}",
            "code": str(row["code"]),
            "as_of": as_of,
            "as_of_iso": _ymd_to_iso(as_of),
            "split": split,
            "outcome_class": outcome_class,
            "ret20": round(ret20, 8),
            "label_end_as_of": int(row["label_end_as_of"]),
            "rank": int(row["rank"]),
            "name": str(row.get("name") or ""),
            "display_score": None if row.get("display_score") is None else float(row["display_score"]),
            "signal_state_at_appearance": row.get("signal_state_at_appearance"),
            "entry_qualified_at_appearance": bool(row.get("entry_qualified_at_appearance")),
            "setup_type_at_appearance": row.get("setup_type_at_appearance"),
            "source_surface": {
                "table": "ranking_appearance_daily",
                "ranking_logic_version": ranking_logic_version,
                "direction": direction,
                "top_k": top_k,
                "require_entry_qualified": require_entry_qualified,
            },
        })

    selected: list[dict[str, Any]] = []
    bucket_counts: Counter[tuple[str, str]] = Counter()
    # Recent-first within each split/class because the user cares about current regime first.
    for row in sorted(enriched, key=lambda item: (item["split"], item["outcome_class"], -item["as_of"], item["rank"])):
        bucket = (row["split"], row["outcome_class"])
        if bucket_counts[bucket] >= max_per_class_split:
            continue
        selected.append(row)
        bucket_counts[bucket] += 1

    selected = sorted(selected, key=lambda item: (item["split"], item["outcome_class"], item["as_of"], item["rank"], item["code"]))
    samples_arg = ",".join(f"{row['code']}:{row['as_of_iso']}" for row in selected)
    _write_jsonl(output_dir / "purpose_sample_plan.jsonl", selected)
    _write_json(output_dir / "batch_command.json", {
        "script": "scripts/meemee_detail_clean_screenshot_batch_v1.mjs",
        "samples_arg": samples_arg,
        "example_command": f"node scripts/meemee_detail_clean_screenshot_batch_v1.mjs --samples {samples_arg}",
    })
    audit = {
        "schema_version": "tradex_detail_clean_screenshot_purpose_plan_v1_audit",
        "generated_at": _utc_now(),
        "boundary_owner": "TRADEX",
        "axis_id": AXIS_ID,
        "purpose": "Train only on scenes MeeMee actually surfaced as buy candidates, then test whether clean detail images improve profit/risk gating.",
        "db_path": str(db_path),
        "runtime_stock_db_status": inspect_runtime_stock_db(runtime_db_path=db_path),
        "candidate_surface": {
            "table": "ranking_appearance_daily",
            "ranking_logic_version": ranking_logic_version,
            "direction": direction,
            "top_k": top_k,
            "require_entry_qualified": require_entry_qualified,
            "start_ymd": start_ymd,
            "end_ymd": end_ymd,
        },
        "candidate_count": len(enriched),
        "selected_sample_count": len(selected),
        "selected_counts_by_split_class": {f"{split}:{klass}": count for (split, klass), count in sorted(bucket_counts.items())},
        "selection_policy": "recent_first_balanced_by_split_and_ret20_class",
        "label_policy": {
            "primary_target": "ret20",
            "winner_class": "ret20 >= +8%",
            "loser_class": "ret20 <= -5%",
            "middle_class": "otherwise",
            "labels_used_in_image_rendering": False,
        },
        "fixed_keep_gate_for_next_step": {
            "must_compare_against_current_surface": True,
            "validation_and_test_ret20_mean_lift_min": 0.005,
            "validation_and_test_positive_rate_lift_min": 0.02,
            "bad_ret20_le_minus_5pct_rate_must_not_worsen": True,
        },
        "non_scope": ["model training", "production ranking mutation", "runtime DB write", "MeeMee reflection"],
        "judgment": "pass_purpose_aligned_sample_plan" if selected else "hold_no_samples_selected",
    }
    _write_json(output_dir / "purpose_plan_audit.json", audit)
    _write_json(output_root / "latest_purpose_plan_audit.json", {"run_root": str(output_dir), **audit})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=None)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--ranking-logic-version", default="ranking:trade:top50:v1")
    parser.add_argument("--direction", default="up")
    parser.add_argument("--top-k", type=int, default=20)
    parser.add_argument("--start-ymd", type=int, default=20240301)
    parser.add_argument("--end-ymd", type=int, default=20260529)
    parser.add_argument("--max-per-class-split", type=int, default=4)
    parser.add_argument("--include-unqualified", action="store_true")
    args = parser.parse_args()
    path = build_plan(
        db_path=args.db_path or resolve_runtime_stock_db_path(),
        output_root=args.output_root,
        ranking_logic_version=args.ranking_logic_version,
        direction=args.direction,
        top_k=args.top_k,
        start_ymd=args.start_ymd,
        end_ymd=args.end_ymd,
        max_per_class_split=args.max_per_class_split,
        require_entry_qualified=not args.include_unqualified,
    )
    print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
