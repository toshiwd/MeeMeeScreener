from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import duckdb

from app.services import position_calc, trade_events
from app.backend.services.data.yahoo_provisional import get_provisional_daily_row_from_chart, normalize_date_key
from app.backend.services.ma_role_readonly_review import build_ma_role_review_payload
from app.utils.text_utils import _normalize_code

_MA_WINDOWS = (7, 20, 60, 100, 200)


def _normalize_as_of_date(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError("as_of_date_required")
    if text.isdigit() and len(text) == 8:
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date().isoformat()
    except ValueError as exc:
        raise ValueError("invalid_as_of_date") from exc


def _as_of_compare_values(as_of_date: str) -> tuple[int, int]:
    day = datetime.strptime(as_of_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(day.timestamp()), int(as_of_date.replace("-", ""))


def _json_list(value: Any) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    try:
        parsed = json.loads(str(value))
    except Exception:
        return []
    return parsed if isinstance(parsed, list) else []


def _json_object(value: Any) -> dict:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(str(value))
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _date_to_iso(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat") and not isinstance(value, (int, float)):
        try:
            return value.isoformat()
        except Exception:
            return None
    raw = str(value).strip()
    if not raw:
        return None
    if raw.isdigit():
        if len(raw) >= 10:
            try:
                return datetime.fromtimestamp(int(raw[:10]), tz=timezone.utc).date().isoformat()
            except Exception:
                return raw
        if len(raw) == 8:
            return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
    return raw


def _bar_payload(row: tuple, *, include_ma: bool = True) -> dict[str, Any]:
    payload = {
        "date": _date_to_iso(row[0]),
        "open": row[1],
        "high": row[2],
        "low": row[3],
        "close": row[4],
        "volume": row[5],
        "source": row[11] if len(row) > 11 else None,
    }
    if include_ma:
        for index, window in enumerate(_MA_WINDOWS, start=6):
            payload[f"ma{window}"] = row[index] if len(row) > index else None
    return payload


def _build_ma_counts(rows: list[tuple]) -> dict[str, int]:
    selected = rows[-1] if rows else None
    counts: dict[str, int] = {}
    if selected:
        close = selected[4]
        for index, window in enumerate(_MA_WINDOWS, start=6):
            ma_value = selected[index]
            if close is None or ma_value is None:
                counts[f"above_ma{window}"] = 0
                counts[f"below_ma{window}"] = 0
                continue
            direction = "above" if close >= ma_value else "below"
            streak = 0
            for row in reversed(rows):
                row_close = row[4]
                row_ma = row[index]
                if row_close is None or row_ma is None:
                    break
                if direction == "above" and row_close >= row_ma:
                    streak += 1
                elif direction == "below" and row_close < row_ma:
                    streak += 1
                else:
                    break
            counts[f"{direction}_ma{window}"] = streak
    return counts


def _fetch_daily_context(conn: duckdb.DuckDBPyConnection, code: str, as_of_date: str) -> dict[str, Any]:
    asof_epoch, asof_ymd = _as_of_compare_values(as_of_date)
    rows = conn.execute(
        """
        WITH base AS (
            SELECT
                date,
                o,
                h,
                l,
                c,
                v,
                AVG(c) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma7,
                AVG(c) OVER (ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20,
                AVG(c) OVER (ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS ma60,
                AVG(c) OVER (ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) AS ma100,
                AVG(c) OVER (ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS ma200,
                COALESCE(NULLIF(TRIM(source), ''), 'pan') AS source
            FROM daily_bars
            WHERE code = ?
              AND date <= CASE WHEN date >= 1000000000 THEN ? ELSE ? END
            ORDER BY date
        )
        SELECT *
        FROM base
        ORDER BY date DESC
        LIMIT 260
        """,
        [code, asof_epoch, asof_ymd],
    ).fetchall()
    rows = list(reversed(rows))
    selected = rows[-1] if rows else None
    return {
        "timeframe": "D",
        "bar_count": len(rows),
        "selected_bar": _bar_payload(selected) if selected else None,
        "recent_bars": [_bar_payload(row, include_ma=False) for row in rows[-60:]],
        "counts": _build_ma_counts(rows),
        "_raw_recent_rows": rows,
    }


def _build_visual_evaluation(daily_rows: list[tuple], *, requested_date: str) -> dict[str, Any]:
    confirmed_rows = [row for row in daily_rows if str(row[11] or "").strip().lower() != "yahoo"]
    provisional_rows = [row for row in daily_rows if str(row[11] or "").strip().lower() == "yahoo"]
    display_row = daily_rows[-1] if daily_rows else None
    confirmed_row = confirmed_rows[-1] if confirmed_rows else None
    provisional_row = provisional_rows[-1] if provisional_rows else None
    has_provisional_overlay = bool(provisional_rows)
    return {
        "classification": "mixed" if confirmed_rows and provisional_rows else ("provisional" if provisional_rows else "confirmed"),
        "display_basis": "confirmed_plus_yahoo_provisional" if has_provisional_overlay else "confirmed_only",
        "display_evaluation_available": display_row is not None,
        "confirmed_judgment_available": confirmed_row is not None,
        "provisional_visual_evaluation_available": has_provisional_overlay,
        "requested_date": requested_date,
        "display_last_date": _date_to_iso(display_row[0]) if display_row else None,
        "confirmed_last_date": _date_to_iso(confirmed_row[0]) if confirmed_row else None,
        "yahoo_provisional_last_date": _date_to_iso(provisional_row[0]) if provisional_row else None,
        "confirmed_judgment_basis": "non_yahoo_daily_bars_only",
        "provisional_visual_evaluation_basis": "daily_bars_including_yahoo_overlay" if has_provisional_overlay else None,
        "warnings": (
            ["Yahoo overlay is provisional display data and must not be presented as confirmed judgment."]
            if has_provisional_overlay
            else []
        ),
    }


def _append_yahoo_visual_overlay(daily: dict[str, Any], raw_rows: list[tuple], code: str) -> list[tuple]:
    provisional = get_provisional_daily_row_from_chart(code)
    if provisional is None:
        return raw_rows
    provisional_date = normalize_date_key(provisional[0])
    last_date = normalize_date_key(raw_rows[-1][0]) if raw_rows else None
    if provisional_date is None or (last_date is not None and provisional_date <= last_date):
        return raw_rows
    closes = [row[4] for row in raw_rows]
    overlay_close = provisional[4]
    ma_values = []
    for window in _MA_WINDOWS:
        window_closes = [*closes, overlay_close][-window:]
        ma_values.append(sum(window_closes) / len(window_closes) if window_closes else None)
    overlay = (*provisional, *ma_values, "yahoo")
    rows = [*raw_rows, overlay]
    daily["bar_count"] = len(rows)
    daily["selected_bar"] = _bar_payload(overlay)
    daily["recent_bars"] = [_bar_payload(row, include_ma=False) for row in rows[-60:]]
    daily["counts"] = _build_ma_counts(rows)
    daily["_raw_recent_rows"] = rows
    return rows


def _build_weekly_context(daily_rows: list[tuple]) -> dict[str, Any]:
    grouped: dict[tuple[int, int], dict[str, Any]] = {}
    for row in daily_rows:
        date_text = _date_to_iso(row[0])
        if not date_text:
            continue
        try:
            parsed = datetime.strptime(date_text, "%Y-%m-%d").date()
        except ValueError:
            continue
        year, week, _ = parsed.isocalendar()
        key = (year, week)
        bucket = grouped.get(key)
        if bucket is None:
            grouped[key] = {
                "date": date_text,
                "open": row[1],
                "high": row[2],
                "low": row[3],
                "close": row[4],
                "volume": row[5] or 0,
            }
            continue
        bucket["date"] = date_text
        bucket["high"] = max(bucket["high"], row[2]) if bucket["high"] is not None and row[2] is not None else bucket["high"]
        bucket["low"] = min(bucket["low"], row[3]) if bucket["low"] is not None and row[3] is not None else bucket["low"]
        bucket["close"] = row[4]
        bucket["volume"] = (bucket["volume"] or 0) + (row[5] or 0)
    bars = [grouped[key] for key in sorted(grouped)]
    return {
        "timeframe": "W",
        "bar_count": len(bars),
        "selected_bar": bars[-1] if bars else None,
        "recent_bars": bars[-60:],
        "source": "aggregated_from_daily_bars",
    }


def _fetch_monthly_context(conn: duckdb.DuckDBPyConnection, code: str, as_of_date: str) -> dict[str, Any]:
    _, asof_ymd = _as_of_compare_values(as_of_date)
    month_limit = int(str(asof_ymd)[:6])
    rows = conn.execute(
        """
        SELECT month, o, h, l, c, v
        FROM monthly_bars
        WHERE code = ? AND month <= ?
        ORDER BY month DESC
        LIMIT 120
        """,
        [code, month_limit],
    ).fetchall()
    rows = list(reversed(rows))
    return {
        "timeframe": "M",
        "bar_count": len(rows),
        "selected_bar": _bar_payload(rows[-1], include_ma=False) if rows else None,
        "recent_bars": [_bar_payload(row, include_ma=False) for row in rows[-36:]],
    }


def _normalize_timeframe(value: Any) -> str:
    text = str(value or "").strip().lower()
    aliases = {
        "d": "daily",
        "day": "daily",
        "daily": "daily",
        "w": "weekly",
        "week": "weekly",
        "weekly": "weekly",
        "m": "monthly",
        "month": "monthly",
        "monthly": "monthly",
        "env": "environment",
        "environment": "environment",
        "mixed": "mixed",
    }
    return aliases.get(text, text or "daily")


def _fetch_notes(conn: duckdb.DuckDBPyConnection, code: str, as_of_date: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT code, as_of_date, timeframe, title, note_text, paragraphs_json, tags_json, linked_objects_json, no_lookahead, created_at, updated_at
        FROM chart_notes
        WHERE code = ? AND as_of_date <= ?
        ORDER BY as_of_date DESC, timeframe
        LIMIT 50
        """,
        [code, as_of_date],
    ).fetchall()
    return [
        {
            "schema_version": "chart_note_v1",
            "code": row[0],
            "as_of_date": row[1],
            "timeframe": _normalize_timeframe(row[2]),
            "note_id": f"{row[0]}:{row[1]}:{_normalize_timeframe(row[2])}",
            "title": row[3] or "",
            "note_text": row[4] or "",
            "paragraphs": _json_list(row[5]),
            "tags": _json_list(row[6]),
            "linked_objects": _json_list(row[7]),
            "no_lookahead": bool(row[8]),
            "created_at": row[9].isoformat() if row[9] else None,
            "updated_at": row[10].isoformat() if row[10] else None,
        }
        for row in rows
    ]


def _fetch_annotations(conn: duckdb.DuckDBPyConnection, code: str, as_of_date: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, code, as_of_date, timeframe, object_type, payload_json, tags_json, no_lookahead, created_at, updated_at
        FROM chart_annotations
        WHERE code = ? AND as_of_date <= ?
        ORDER BY as_of_date DESC, updated_at DESC
        LIMIT 100
        """,
        [code, as_of_date],
    ).fetchall()
    return [
        {
            "id": row[0],
            "code": row[1],
            "as_of_date": row[2],
            "timeframe": _normalize_timeframe(row[3]),
            "object_type": row[4],
            "payload": _json_object(row[5]),
            "tags": _json_list(row[6]),
            "no_lookahead": bool(row[7]),
            "created_at": row[8].isoformat() if row[8] else None,
            "updated_at": row[9].isoformat() if row[9] else None,
        }
        for row in rows
    ]


def _fetch_position_state(conn: duckdb.DuckDBPyConnection, code: str) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT spot_qty, margin_long_qty, margin_short_qty, buy_qty, sell_qty, has_issue, issue_note, updated_at
        FROM positions_live
        WHERE symbol = ?
        """,
        [code],
    ).fetchone()
    if not row:
        return None
    events = trade_events.get_events(conn, [code])
    daily_positions = position_calc._build_daily_positions_from_db_events(events).get(code, [])
    return {
        "spot_lots": row[0],
        "margin_long_lots": row[1],
        "margin_short_lots": row[2],
        "long_lots": row[3],
        "short_lots": row[4],
        "has_issue": bool(row[5]),
        "issue_note": row[6],
        "updated_at": row[7].isoformat() if row[7] else None,
        "daily_positions": daily_positions[-60:],
    }


def _collect_tags(notes: list[dict[str, Any]], annotations: list[dict[str, Any]]) -> list[str]:
    tags: set[str] = set()
    for item in [*notes, *annotations]:
        for tag in item.get("tags") or []:
            text = str(tag or "").strip()
            if text:
                tags.add(text)
    return sorted(tags)


def _build_timeframe_notes(notes: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped = {"daily": [], "weekly": [], "monthly": [], "environment": []}
    for note in notes:
        timeframe = _normalize_timeframe(note.get("timeframe"))
        if timeframe not in grouped:
            continue
        grouped[timeframe].append(note)
    return grouped


def _build_environment_notes(notes: list[dict[str, Any]], annotations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for note in notes:
        if _normalize_timeframe(note.get("timeframe")) == "environment":
            items.append({"source": "note", **note})
    for annotation in annotations:
        object_type = str(annotation.get("object_type") or "").strip().lower()
        payload = annotation.get("payload") if isinstance(annotation.get("payload"), dict) else {}
        if object_type in {"chart_context", "scenario"} or payload.get("context_type") == "environment":
            items.append({"source": "annotation", **annotation})
    return items


def _annotation_linked_objects(annotation: dict[str, Any]) -> list[dict[str, Any]]:
    payload = annotation.get("payload") if isinstance(annotation.get("payload"), dict) else {}
    linked = payload.get("linked_objects")
    if isinstance(linked, list):
        return [item for item in linked if isinstance(item, dict)]
    linked_object = payload.get("linked_object")
    return [linked_object] if isinstance(linked_object, dict) else []


def _build_drawing_objects(annotations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    objects: dict[str, dict[str, Any]] = {}
    for annotation in annotations:
        for linked in _annotation_linked_objects(annotation):
            object_type = str(linked.get("object_type") or annotation.get("object_type") or "").strip()
            drawing_id = str(linked.get("drawing_id") or linked.get("object_id") or "").strip()
            payload = linked.get("payload") if isinstance(linked.get("payload"), dict) else {}
            if not drawing_id:
                resolution = str(linked.get("resolution") or "payload_fallback")
                drawing_id = f"payload:{object_type}:{json.dumps(payload, sort_keys=True, default=str)}"
            existing = objects.get(drawing_id)
            if existing is None:
                objects[drawing_id] = {
                    "drawing_id": drawing_id,
                    "object_type": object_type,
                    "timeframe": _normalize_timeframe(linked.get("timeframe") or annotation.get("timeframe")),
                    "payload": payload,
                    "resolution": linked.get("resolution") or ("payload_fallback" if drawing_id.startswith("payload:") else "drawing_id"),
                    "linked_annotation_ids": [],
                    "linked_note_ids": [],
                    "created_at": annotation.get("created_at"),
                    "updated_at": annotation.get("updated_at"),
                }
                existing = objects[drawing_id]
            existing["linked_annotation_ids"].append(annotation.get("id"))
            existing["updated_at"] = annotation.get("updated_at") or existing.get("updated_at")
    return list(objects.values())


def _build_linked_notes(notes: list[dict[str, Any]], annotations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    links: list[dict[str, Any]] = []
    for note in notes:
        note_id = str(note.get("note_id") or f"{note.get('code')}:{note.get('as_of_date')}:{note.get('timeframe')}")
        for linked in note.get("linked_objects") or []:
            if not isinstance(linked, dict):
                continue
            links.append({
                "annotation_id": linked.get("annotation_id"),
                "note_id": note_id,
                "paragraph_id": linked.get("paragraph_id"),
                "link_type": linked.get("link_type") or "note_to_object",
                "object_type": linked.get("object_type"),
            })
        for paragraph in note.get("paragraphs") or []:
            if not isinstance(paragraph, dict):
                continue
            paragraph_id = paragraph.get("paragraph_id")
            for linked in paragraph.get("linked_objects") or []:
                if not isinstance(linked, dict):
                    continue
                links.append({
                    "annotation_id": linked.get("annotation_id") or linked.get("anchor_object_id"),
                    "note_id": note_id,
                    "paragraph_id": paragraph_id,
                    "link_type": linked.get("link_type") or "paragraph_to_object",
                    "object_type": linked.get("object_type") or linked.get("anchor_type"),
                    "resolution": linked.get("resolution"),
                    "provisional": bool(linked.get("provisional", False)),
                })
    for annotation in annotations:
        payload = annotation.get("payload") if isinstance(annotation.get("payload"), dict) else {}
        note_id = payload.get("note_id")
        if note_id:
            links.append({
                "annotation_id": annotation.get("id"),
                "note_id": note_id,
                "paragraph_id": payload.get("paragraph_id"),
                "link_type": payload.get("link_type") or "annotation_to_note",
                "object_type": annotation.get("object_type"),
            })
    return links


def get_chart_reading_bundle(
    conn: duckdb.DuckDBPyConnection,
    *,
    code: str,
    as_of_date: Any,
    include_provisional_visual: bool = False,
) -> dict[str, Any]:
    normalized_code = _normalize_code(code)
    if not normalized_code:
        raise ValueError("invalid_code")
    normalized_date = _normalize_as_of_date(as_of_date)
    daily = _fetch_daily_context(conn, normalized_code, normalized_date)
    raw_daily_rows = list(daily.get("_raw_recent_rows", []))
    if include_provisional_visual:
        raw_daily_rows = _append_yahoo_visual_overlay(daily, raw_daily_rows, normalized_code)
    daily.pop("_raw_recent_rows", None)
    monthly = _fetch_monthly_context(conn, normalized_code, normalized_date)
    annotations = _fetch_annotations(conn, normalized_code, normalized_date)
    notes = _fetch_notes(conn, normalized_code, normalized_date)
    timeframe_notes = _build_timeframe_notes(notes)
    # Canonical consumer keys are notes/annotations. Keep the older UI-context
    # keys as aliases for compatibility with existing callers.
    return {
        "schema_version": "chart_reading_bundle_v1",
        "read_only": True,
        "boundary_owner": "MeeMee",
        "research_logic_included": False,
        "code": normalized_code,
        "as_of_date": normalized_date,
        "chart_context": {
            "daily": daily,
            "weekly": _build_weekly_context(raw_daily_rows),
            "monthly": monthly,
        },
        "visual_evaluation": _build_visual_evaluation(raw_daily_rows, requested_date=normalized_date),
        "ma_role_review": build_ma_role_review_payload(raw_daily_rows),
        "selected_bar": daily.get("selected_bar"),
        "annotations": annotations,
        "visible_annotations": annotations,
        "notes": notes,
        "date_notes": notes,
        "timeframe_notes": timeframe_notes,
        "environment_notes": _build_environment_notes(notes, annotations),
        "drawing_objects": _build_drawing_objects(annotations),
        "linked_notes": _build_linked_notes(notes, annotations),
        "linked_tags": _collect_tags(notes, annotations),
        "position_state": _fetch_position_state(conn, normalized_code),
        "no_lookahead": True,
    }
