from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from external_analysis.exporter.export_schema import connect_export_db
from external_analysis.labels.store import connect_label_db, ensure_label_schema
from external_analysis.runtime.incremental_cache import ANCHOR_RELEVANT_EXPORT_TABLES, probe_label_cache, upsert_manifest

ANCHOR_POLICY_VERSION = "anchor-window-v1"
ANCHOR_EMBARGO_DAYS = 5


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _run_id(kind: str) -> str:
    return _utcnow().strftime(f"{kind}_%Y%m%dT%H%M%S%fZ")


def _load_bars(export_db_path: str | None = None) -> tuple[list[int], dict[str, list[dict[str, Any]]]]:
    export_conn = connect_export_db(export_db_path)
    try:
        rows = export_conn.execute(
            """
            SELECT b.code, b.trade_date, b.o, b.h, b.l, b.c, b.v, i.ma20
            FROM bars_daily_export b
            LEFT JOIN indicator_daily_export i
              ON i.code = b.code AND i.trade_date = b.trade_date
            ORDER BY b.code, b.trade_date
            """
        ).fetchall()
    finally:
        export_conn.close()
    trading_dates = sorted({int(row[1]) for row in rows})
    by_code: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_code.setdefault(str(row[0]), []).append(
            {
                "code": str(row[0]),
                "trade_date": int(row[1]),
                "o": row[2],
                "h": row[3],
                "l": row[4],
                "c": row[5],
                "v": row[6],
                "ma20": row[7],
            }
        )
    return trading_dates, by_code


def _is_big_bear(bar: dict[str, Any]) -> bool:
    if None in (bar["o"], bar["h"], bar["l"], bar["c"]):
        return False
    high_low = float(bar["h"]) - float(bar["l"])
    if high_low <= 0:
        return False
    body = abs(float(bar["c"]) - float(bar["o"]))
    ret = (float(bar["c"]) / float(bar["o"])) - 1.0 if float(bar["o"]) else 0.0
    return float(bar["c"]) < float(bar["o"]) and body / high_low >= 0.6 and ret <= -0.05


def _detect_anchor_types(history: list[dict[str, Any]], idx: int) -> list[str]:
    bar = history[idx]
    prev_bar = history[idx - 1] if idx > 0 else None
    anchor_types: list[str] = []
    if bar["ma20"] is not None and prev_bar and prev_bar["ma20"] is not None:
        if float(prev_bar["c"]) <= float(prev_bar["ma20"]) and float(bar["c"]) > float(bar["ma20"]):
            anchor_types.append("20MA_cross_up")
        if idx >= 5:
            prior_slice = history[idx - 5 : idx]
            if any(item["ma20"] is not None and float(item["c"]) < float(item["ma20"]) for item in prior_slice):
                if float(bar["c"]) > float(bar["ma20"]):
                    anchor_types.append("20MA_reclaim")
    if idx >= 20:
        prev_20 = history[idx - 20 : idx]
        prev_high = max(float(item["h"]) for item in prev_20 if item["h"] is not None)
        prev_low = min(float(item["l"]) for item in prev_20 if item["l"] is not None)
        volume_rows = [float(item["v"]) for item in prev_20 if item["v"] is not None]
        avg_vol = sum(volume_rows) / len(volume_rows) if volume_rows else 0.0
        range_pct = (prev_high - prev_low) / float(prev_20[-1]["c"]) if prev_20[-1]["c"] else 0.0
        if float(bar["c"]) > prev_high:
            anchor_types.append("prev_high_break")
            if range_pct <= 0.12:
                anchor_types.append("box_breakout")
        if float(bar["c"]) < prev_low:
            anchor_types.append("prev_low_break")
        if bar["v"] is not None and avg_vol > 0 and float(bar["v"]) >= avg_vol * 2.0:
            anchor_types.append("volume_spike")
    if prev_bar and _is_big_bear(prev_bar) and prev_bar["o"] is not None and bar["c"] is not None:
        if float(bar["c"]) >= float(prev_bar["o"]):
            anchor_types.append("big_bear_full_reclaim")
    seen: set[str] = set()
    ordered: list[str] = []
    for anchor_type in anchor_types:
        if anchor_type not in seen:
            ordered.append(anchor_type)
            seen.add(anchor_type)
    return ordered


def build_anchor_windows(export_db_path: str | None = None, label_db_path: str | None = None) -> dict[str, Any]:
    started_at = _utcnow()
    run_id = _run_id("anchor")
    probe = probe_label_cache(
        export_db_path=export_db_path,
        label_db_path=label_db_path,
        generation_key="anchor_windows",
        dependency_version=ANCHOR_POLICY_VERSION,
        relevant_tables=ANCHOR_RELEVANT_EXPORT_TABLES,
    )
    if probe["action"] == "skip":
        return {
            "ok": True,
            "run_id": run_id,
            "summary": {},
            "policy_version": ANCHOR_POLICY_VERSION,
            "skipped": True,
            "cache_state": probe["cache_state"],
            "reason": probe["reason"],
            "dirty_ranges": [],
            "source_signature": probe.get("source_signature"),
        }
    trading_dates, by_code = _load_bars(export_db_path)
    index_by_date = {value: idx for idx, value in enumerate(trading_dates)}
    dirty_codes = {str(item["code"]) for item in probe["dirty_ranges"]}
    anchor_rows: list[dict[str, Any]] = []
    anchor_bar_rows: list[dict[str, Any]] = []
    for code, history in by_code.items():
        if dirty_codes and probe["action"] == "partial" and code not in dirty_codes:
            continue
        history_by_date = {int(item["trade_date"]): item for item in history}
        anchors_for_code: list[dict[str, Any]] = []
        for idx, bar in enumerate(history):
            anchor_types = _detect_anchor_types(history, idx)
            if not anchor_types:
                continue
            anchor_date = int(bar["trade_date"])
            date_index = index_by_date.get(anchor_date)
            if date_index is None:
                continue
            start_index = date_index - 20
            end_index = date_index + 20
            embargo_index = end_index + ANCHOR_EMBARGO_DAYS
            if start_index < 0 or end_index >= len(trading_dates) or embargo_index >= len(trading_dates):
                continue
            window_dates = trading_dates[start_index : end_index + 1]
            if any(int(trade_date) not in history_by_date for trade_date in window_dates):
                continue
            future_slice = [history_by_date[int(value)] for value in trading_dates[date_index + 1 : end_index + 1]]
            if not future_slice:
                continue
            current_close = float(bar["c"]) if bar["c"] is not None else None
            if current_close in (None, 0.0):
                continue
            outcome_ret_20 = (float(future_slice[-1]["c"]) / current_close) - 1.0
            outcome_mfe_20 = (max(float(item["h"]) for item in future_slice if item["h"] is not None) / current_close) - 1.0
            outcome_mae_20 = (min(float(item["l"]) for item in future_slice if item["l"] is not None) / current_close) - 1.0
            prior_trade_dates = trading_dates[max(start_index, date_index - 20) : date_index]
            valid_volumes = [
                float(history_by_date[int(value)]["v"])
                for value in prior_trade_dates
                if int(value) in history_by_date and history_by_date[int(value)]["v"] is not None
            ]
            avg_volume = sum(valid_volumes) / len(valid_volumes) if valid_volumes else 0.0
            for anchor_type in anchor_types:
                anchor_id = f"{anchor_type}:{code}:{anchor_date}"
                anchors_for_code.append(
                    {
                        "anchor_id": anchor_id,
                        "code": code,
                        "anchor_type": anchor_type,
                        "anchor_date": anchor_date,
                        "window_start_date": int(trading_dates[start_index]),
                        "window_end_date": int(trading_dates[end_index]),
                        "future_window_end_date": int(trading_dates[end_index]),
                        "collision_group_id": f"{code}:{anchor_date}",
                        "overlap_group_id": "",
                        "purge_end_date": int(trading_dates[end_index]),
                        "embargo_until_date": int(trading_dates[embargo_index]),
                        "outcome_ret_20": outcome_ret_20,
                        "outcome_mfe_20": outcome_mfe_20,
                        "outcome_mae_20": outcome_mae_20,
                        "generation_run_id": run_id,
                        "policy_version": ANCHOR_POLICY_VERSION,
                    }
                )
                for rel_day, trade_date in enumerate(window_dates, start=-20):
                    current_bar = history_by_date[int(trade_date)]
                    ma20 = current_bar["ma20"]
                    volume_ratio = None
                    if avg_volume > 0 and current_bar["v"] is not None:
                        volume_ratio = float(current_bar["v"]) / avg_volume
                    close_to_ma20 = None
                    if ma20 not in (None, 0) and current_bar["c"] is not None:
                        close_to_ma20 = (float(current_bar["c"]) / float(ma20)) - 1.0
                    anchor_bar_rows.append(
                        {
                            "anchor_id": anchor_id,
                            "code": code,
                            "anchor_type": anchor_type,
                            "anchor_date": anchor_date,
                            "rel_day": rel_day,
                            "trade_date": int(trade_date),
                            "o": current_bar["o"],
                            "h": current_bar["h"],
                            "l": current_bar["l"],
                            "c": current_bar["c"],
                            "v": current_bar["v"],
                            "ma20": ma20,
                            "volume_ratio_20": volume_ratio,
                            "close_to_ma20_pct": close_to_ma20,
                            "generation_run_id": run_id,
                        }
                    )
        anchors_for_code.sort(key=lambda item: (item["anchor_date"], item["anchor_type"]))
        overlap_group = 0
        active_end = None
        for anchor in anchors_for_code:
            if active_end is None or int(anchor["window_start_date"]) > int(active_end):
                overlap_group += 1
                active_end = int(anchor["window_end_date"])
            else:
                active_end = max(int(active_end), int(anchor["window_end_date"]))
            anchor["overlap_group_id"] = f"{code}:overlap:{overlap_group}"
        anchor_rows.extend(anchors_for_code)
    label_conn = connect_label_db(label_db_path)
    try:
        ensure_label_schema(label_conn)
        if probe["action"] == "partial" and dirty_codes:
            label_conn.execute(
                f"DELETE FROM anchor_window_bars WHERE code IN ({', '.join(['?'] * len(dirty_codes))})",
                sorted(dirty_codes),
            )
            label_conn.execute(
                f"DELETE FROM anchor_window_master WHERE code IN ({', '.join(['?'] * len(dirty_codes))})",
                sorted(dirty_codes),
            )
        else:
            label_conn.execute("DELETE FROM anchor_window_master")
            label_conn.execute("DELETE FROM anchor_window_bars")
        if anchor_rows:
            master_columns = list(anchor_rows[0].keys())
            label_conn.executemany(
                f"INSERT INTO anchor_window_master ({', '.join(master_columns)}) VALUES ({', '.join(['?'] * len(master_columns))})",
                [[row[column] for column in master_columns] for row in anchor_rows],
            )
        if anchor_bar_rows:
            bar_columns = list(anchor_bar_rows[0].keys())
            label_conn.executemany(
                f"INSERT INTO anchor_window_bars ({', '.join(bar_columns)}) VALUES ({', '.join(['?'] * len(bar_columns))})",
                [[row[column] for column in bar_columns] for row in anchor_bar_rows],
            )
        summary = {"anchor_window_master": len(anchor_rows), "anchor_window_bars": len(anchor_bar_rows)}
        label_conn.execute(
            """
            INSERT OR REPLACE INTO label_generation_runs (
                run_id, started_at, finished_at, status, kind, export_db_path, policy_version,
                horizon_set, collision_guard_enabled, overlap_guard_enabled, embargo_days, summary_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run_id,
                started_at,
                _utcnow(),
                "success",
                "anchor_windows",
                str(export_db_path or ""),
                ANCHOR_POLICY_VERSION,
                None,
                True,
                True,
                ANCHOR_EMBARGO_DAYS,
                json.dumps(summary, ensure_ascii=False, sort_keys=True),
            ],
        )
        upsert_manifest(
            conn=label_conn,
            table_name="label_generation_manifest",
            generation_key="anchor_windows",
            source_signature=str(probe.get("source_signature") or ""),
            dependency_version=ANCHOR_POLICY_VERSION,
            cache_state="partial_stale" if probe["action"] == "partial" else "fresh",
            row_count=len(anchor_rows) + len(anchor_bar_rows),
            dirty_ranges=probe["dirty_ranges"],
            run_id=run_id,
        )
        label_conn.execute("CHECKPOINT")
        return {
            "ok": True,
            "run_id": run_id,
            "summary": summary,
            "policy_version": ANCHOR_POLICY_VERSION,
            "skipped": False,
            "cache_state": "partial_stale" if probe["action"] == "partial" else "fresh",
            "reason": probe["reason"],
            "dirty_ranges": probe["dirty_ranges"],
            "source_signature": probe.get("source_signature"),
        }
    finally:
        label_conn.close()
