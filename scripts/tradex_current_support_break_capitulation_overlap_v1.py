from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

AXIS_ID = "tradex_current_support_break_capitulation_overlap_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\current_support_break_capitulation_overlap_v1")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _clean(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if hasattr(value, "item"):
        return _clean(value.item())
    return value


def _ymd_iso(value: Any) -> str | None:
    if value is None:
        return None
    text = str(int(value))
    if len(text) != 8:
        return text
    return f"{text[:4]}-{text[4:6]}-{text[6:8]}"


def _ranking_codes(direction: str, limit: int) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    from app.backend.services.codex_bridge_service import get_rankings_freshness
    from app.backend.services.ml import rankings_cache

    freshness = get_rankings_freshness(
        tf="D",
        which="latest",
        direction=direction,
        mode="trade",
        risk_mode="balanced",
        limit=limit,
    )
    payload = rankings_cache.get_rankings("D", "latest", direction, limit, mode="trade", risk_mode="balanced")
    items = []
    for rank, item in enumerate(payload.get("items") or [], start=1):
        code = str(item.get("code") or item.get("symbol") or "").strip()
        if not code:
            continue
        items.append(
            {
                "rank": rank,
                "code": code,
                "name": item.get("name"),
                "entryQualified": item.get("entryQualified"),
                "setupType": item.get("setupType"),
                "tradeEntryClass": item.get("tradeEntryClass"),
                "tradeDecisionReasons": item.get("tradeDecisionReasons"),
                "tradeRiskWatch": item.get("tradeRiskWatch"),
            }
        )
    return freshness, items


CURRENT_SIGNAL_SQL = r"""
WITH normalized AS (
  SELECT
    code,
    date,
    CASE
      WHEN date > 30000000 THEN CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)
      ELSE CAST(date AS INTEGER)
    END AS ymd,
    o, h, l, c, v, source
  FROM daily_bars
  WHERE o > 0 AND h > 0 AND l > 0 AND c > 0
),
latest AS (
  SELECT max(ymd) AS latest_ymd FROM normalized
),
base AS (
  SELECT
    *,
    avg(v) OVER w20 AS vol20,
    avg(c) OVER w20 AS ma20,
    avg(c) OVER w60 AS ma60,
    min(l) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS prior_low20,
    c / lag(c, 20) OVER w - 1 AS ret20,
    c / lag(c, 60) OVER w - 1 AS ret60,
    lead(ymd, 1) OVER w AS entry_ymd,
    lead(l, 1) OVER w AS entry_day_low,
    lead(c, 1) OVER w AS entry_day_close,
    lead(h, 1) OVER w AS entry_day_high
  FROM normalized
  WINDOW
    w AS (PARTITION BY code ORDER BY ymd),
    w20 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
    w60 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
),
features AS (
  SELECT
    *,
    CASE WHEN vol20 > 0 THEN v / vol20 ELSE NULL END AS volume_vs20,
    CASE WHEN h > l THEN (c - l) / (h - l) ELSE NULL END AS close_pos,
    c / ma20 - 1 AS dist_ma20,
    c / ma60 - 1 AS dist_ma60
  FROM base
  WHERE ma20 IS NOT NULL AND ma60 IS NOT NULL
),
entry_triggered AS (
  SELECT
    'entry_triggered_latest' AS candidate_state,
    code,
    ymd AS signal_ymd,
    entry_ymd,
    c AS signal_close,
    l AS signal_low,
    entry_day_low,
    entry_day_close,
    volume_vs20,
    close_pos,
    dist_ma20,
    dist_ma60,
    ret20,
    ret60,
    source
  FROM features
  WHERE entry_ymd = (SELECT latest_ymd FROM latest)
    AND c < prior_low20
    AND entry_day_low <= l
    AND volume_vs20 >= 3.0
    AND close_pos <= 0.10
    AND dist_ma20 <= -0.10
),
pending_next_entry AS (
  SELECT
    'pending_next_entry' AS candidate_state,
    code,
    ymd AS signal_ymd,
    NULL::INTEGER AS entry_ymd,
    c AS signal_close,
    l AS signal_low,
    NULL::DOUBLE AS entry_day_low,
    NULL::DOUBLE AS entry_day_close,
    volume_vs20,
    close_pos,
    dist_ma20,
    dist_ma60,
    ret20,
    ret60,
    source
  FROM features
  WHERE ymd = (SELECT latest_ymd FROM latest)
    AND c < prior_low20
    AND volume_vs20 >= 3.0
    AND close_pos <= 0.10
    AND dist_ma20 <= -0.10
)
SELECT * FROM entry_triggered
UNION ALL
SELECT * FROM pending_next_entry
ORDER BY candidate_state, code
"""


RECENT_SIGNAL_SQL = r"""
WITH normalized AS (
  SELECT
    code,
    date,
    CASE
      WHEN date > 30000000 THEN CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)
      ELSE CAST(date AS INTEGER)
    END AS ymd,
    o, h, l, c, v, source
  FROM daily_bars
  WHERE o > 0 AND h > 0 AND l > 0 AND c > 0
),
calendar AS (
  SELECT DISTINCT ymd FROM normalized ORDER BY ymd DESC LIMIT 80
),
cutoff AS (
  SELECT min(ymd) AS min_ymd FROM calendar
),
base AS (
  SELECT
    *,
    avg(v) OVER w20 AS vol20,
    avg(c) OVER w20 AS ma20,
    avg(c) OVER w60 AS ma60,
    min(l) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS prior_low20,
    c / lag(c, 20) OVER w - 1 AS ret20,
    c / lag(c, 60) OVER w - 1 AS ret60,
    lead(ymd, 1) OVER w AS entry_ymd,
    lead(l, 1) OVER w AS entry_day_low,
    lead(c, 1) OVER w AS entry_day_close
  FROM normalized
  WINDOW
    w AS (PARTITION BY code ORDER BY ymd),
    w20 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
    w60 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
),
features AS (
  SELECT
    *,
    CASE WHEN vol20 > 0 THEN v / vol20 ELSE NULL END AS volume_vs20,
    CASE WHEN h > l THEN (c - l) / (h - l) ELSE NULL END AS close_pos,
    c / ma20 - 1 AS dist_ma20,
    c / ma60 - 1 AS dist_ma60
  FROM base
  WHERE ma20 IS NOT NULL AND ma60 IS NOT NULL
)
SELECT
  code,
  ymd AS signal_ymd,
  entry_ymd,
  c AS signal_close,
  l AS signal_low,
  entry_day_low,
  entry_day_close,
  volume_vs20,
  close_pos,
  dist_ma20,
  dist_ma60,
  ret20,
  ret60,
  CASE WHEN entry_day_low <= l THEN true ELSE false END AS entry_triggered
FROM features, cutoff
WHERE ymd >= min_ymd
  AND c < prior_low20
  AND volume_vs20 >= 3.0
  AND close_pos <= 0.10
  AND dist_ma20 <= -0.10
ORDER BY signal_ymd DESC, code
"""


def run(*, output_root: Path, limit: int) -> Path:
    from app.backend.services.codex_bridge_service import get_runtime_stock_db_status

    runtime_status = get_runtime_stock_db_status()
    db_path = Path(str(runtime_status.get("selected_runtime_db_path") or runtime_status.get("runtime_db_path") or ""))
    if not db_path.exists():
        raise FileNotFoundError(f"runtime stock db not found: {db_path}")

    try:
        down_freshness, down_items = _ranking_codes("down", limit)
    except Exception as exc:
        down_freshness, down_items = {"error": str(exc)}, []
    try:
        short_freshness, short_items = _ranking_codes("short", limit)
    except Exception as exc:
        short_freshness, short_items = {"error": str(exc)}, []

    with duckdb.connect(str(db_path), read_only=True) as conn:
        rows = conn.execute(CURRENT_SIGNAL_SQL).fetchdf().to_dict("records")
        recent_rows = conn.execute(RECENT_SIGNAL_SQL).fetchdf().to_dict("records")

    candidates = [{key: _clean(value) for key, value in row.items()} for row in rows]
    recent_signals = [{key: _clean(value) for key, value in row.items()} for row in recent_rows]
    rank_by_code: dict[str, list[dict[str, Any]]] = {}
    for surface, items in [("down", down_items), ("short", short_items)]:
        for item in items:
            rank_by_code.setdefault(item["code"], []).append({"surface": surface, **item})
    for row in candidates:
        row["signal_date"] = _ymd_iso(row.get("signal_ymd"))
        row["entry_date"] = _ymd_iso(row.get("entry_ymd"))
        row["ranking_overlap"] = rank_by_code.get(str(row.get("code")), [])
    for row in recent_signals:
        row["signal_date"] = _ymd_iso(row.get("signal_ymd"))
        row["entry_date"] = _ymd_iso(row.get("entry_ymd"))
        row["ranking_overlap"] = rank_by_code.get(str(row.get("code")), [])

    triggered = [row for row in candidates if row["candidate_state"] == "entry_triggered_latest"]
    pending = [row for row in candidates if row["candidate_state"] == "pending_next_entry"]
    overlap_codes = sorted({str(row["code"]) for row in candidates if row["ranking_overlap"]})
    recent_triggered = [row for row in recent_signals if row.get("entry_triggered")]

    run_dir = output_root / f"{_tag()}-{AXIS_ID}"
    run_dir.mkdir(parents=True, exist_ok=False)
    summary = {
        "schema_version": f"{AXIS_ID}_summary_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "research_phase": "comparison_stabilization",
        "fixed_evaluation_conditions": {
            "candidate": "support_break_capitulation",
            "current_entry_trigger": "prior signal matched; latest confirmed day low <= signal low",
            "pending_next_entry": "latest confirmed day signal matched; next day low break still required",
            "ranking_surfaces_checked": ["D/latest/down/trade/balanced", "D/latest/short/trade/balanced"],
            "limit": limit,
        },
        "runtime_stock_db_status": runtime_status,
        "rankings_freshness": {"down": down_freshness, "short": short_freshness},
        "ranking_items": {"down": down_items, "short": short_items},
        "metrics": {
            "candidate_count": len(candidates),
            "entry_triggered_latest_count": len(triggered),
            "pending_next_entry_count": len(pending),
            "ranking_overlap_count": len(overlap_codes),
            "ranking_overlap_codes": overlap_codes,
            "recent_80_session_signal_count": len(recent_signals),
            "recent_80_session_entry_triggered_count": len(recent_triggered),
            "recent_80_session_signal_dates": sorted({row["signal_date"] for row in recent_signals if row.get("signal_date")}, reverse=True),
        },
        "candidates": candidates,
        "recent_80_session_signals": recent_signals,
        "decision": {
            "candidate_local_decision": "keep_for_selection_candidate_review" if candidates else "hold_no_current_appearance",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": (
                "current candidates exist; overlap shows whether this is additive to current ranking"
                if candidates
                else "no latest confirmed appearance; keep historical candidate but no current selection action"
            ),
        },
        "artifacts": {"summary_json": str(run_dir / "current_support_break_capitulation_overlap_summary.json")},
        "runtime_db_write": False,
        "meemee_unchanged": True,
        "production_ranking_changed": False,
        "silent_fallback_used": False,
        "remaining_risks": [
            "current board uses confirmed runtime DB only",
            "pending_next_entry requires next-session low break confirmation",
            "ranking overlap is diagnostic; no production ranking reflection is performed",
        ],
    }
    _write_json(run_dir / "current_support_break_capitulation_overlap_summary.json", summary)
    _write_json(output_root / "latest_current_support_break_capitulation_overlap_summary.json", {"run_root": str(run_dir), **summary})
    return run_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()
    print(run(output_root=args.output_root, limit=args.limit))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
