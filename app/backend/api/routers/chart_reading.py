from __future__ import annotations

import json
from uuid import uuid4

from fastapi import APIRouter, Body, HTTPException
from fastapi.responses import JSONResponse

from app.backend.services.chart_reading_bundle import get_chart_reading_bundle
from app.db.session import is_transient_duckdb_error, try_get_conn
from app.utils.date_utils import jst_now
from app.utils.text_utils import _normalize_code

router = APIRouter()

_ANNOTATION_TYPES = {"bar", "region", "line", "path", "position_action", "scenario"}
_ANNOTATION_TYPES.update({"callout", "indicator", "chart_context"})
_NOTE_TIMEFRAMES = {"daily", "weekly", "monthly", "environment", "mixed", "D", "W", "M"}
_NOTE_PARAGRAPH_TYPES = {
    "monthly_context",
    "weekly_context",
    "daily_bar_reading",
    "region_reading",
    "line_reading",
    "indicator_reading",
    "callout_reading",
    "position_action",
    "scenario_update",
    "risk_note",
    "review",
}
_READ_TIMEOUT_SEC = 3.0
_WRITE_TIMEOUT_SEC = 5.0
_CALLOUT_ANCHOR_TYPES = {"bar", "candle", "indicator", "region", "line"}
_CALLOUT_INDICATOR_TARGETS = {"ma7", "ma20", "ma60", "ma100"}


def _json_array(value: object) -> str:
    if not isinstance(value, list):
        value = []
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _json_object(value: object) -> str:
    if not isinstance(value, dict):
        value = {}
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _parse_json_object(value: object) -> dict:
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(str(value or "{}"))
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_json_array(value: object) -> list:
    if isinstance(value, list):
        return value
    try:
        parsed = json.loads(str(value or "[]"))
    except Exception:
        return []
    return parsed if isinstance(parsed, list) else []


def _annotation_payload(row: tuple) -> dict:
    return {
        "id": row[0],
        "code": row[1],
        "as_of_date": row[2],
        "timeframe": row[3],
        "object_type": row[4],
        "payload": _parse_json_object(row[5]),
        "tags": _parse_json_array(row[6]),
        "no_lookahead": bool(row[7]),
        "created_at": row[8].isoformat() if row[8] else None,
        "updated_at": row[9].isoformat() if row[9] else None,
    }


def _validate_annotation_payload(payload: dict) -> tuple[str, str, str, str, dict, list, bool]:
    code = _normalize_code(payload.get("code"))
    as_of_date = str(payload.get("as_of_date") or "").strip()
    timeframe = str(payload.get("timeframe") or "D").strip() or "D"
    object_type = str(payload.get("object_type") or "").strip()
    if not code or not as_of_date:
        raise HTTPException(status_code=400, detail="code_and_as_of_date_required")
    if object_type not in _ANNOTATION_TYPES:
        raise HTTPException(status_code=400, detail="invalid_annotation_type")
    raw_payload = payload.get("payload")
    body_payload = dict(raw_payload) if isinstance(raw_payload, dict) else {}
    body_payload.setdefault("code", code)
    body_payload.setdefault("as_of_date", as_of_date)
    body_payload.setdefault("timeframe", timeframe)
    body_payload.setdefault("no_lookahead", True)
    if object_type == "callout":
        anchor_type = str(body_payload.get("anchor_type") or "").strip()
        anchor_target = str(body_payload.get("anchor_target") or "").strip()
        label_position = body_payload.get("label_position")
        if anchor_type not in _CALLOUT_ANCHOR_TYPES:
            raise HTTPException(status_code=400, detail="invalid_callout_anchor_type")
        if anchor_type == "indicator" and anchor_target not in _CALLOUT_INDICATOR_TARGETS:
            raise HTTPException(status_code=400, detail="invalid_callout_anchor_target")
        if not isinstance(label_position, dict):
            raise HTTPException(status_code=400, detail="callout_label_position_required")
        if not str(body_payload.get("anchor_date") or body_payload.get("anchor_time") or "").strip():
            raise HTTPException(status_code=400, detail="callout_anchor_date_required")
        try:
            float(body_payload.get("anchor_price"))
            float(label_position.get("price"))
        except Exception as exc:
            raise HTTPException(status_code=400, detail="callout_price_required") from exc
        if not str(label_position.get("date") or label_position.get("time") or "").strip():
            raise HTTPException(status_code=400, detail="callout_label_date_required")
    tags = payload.get("tags") if isinstance(payload.get("tags"), list) else body_payload.get("tags")
    tags = tags if isinstance(tags, list) else []
    no_lookahead = bool(payload.get("no_lookahead", body_payload.get("no_lookahead", True)))
    body_payload["no_lookahead"] = no_lookahead
    return code, as_of_date, timeframe, object_type, body_payload, tags, no_lookahead


def _normalize_linked_object(value: object) -> dict | None:
    if not isinstance(value, dict):
        return None
    item = dict(value)
    object_type = str(item.get("object_type") or item.get("type") or "").strip()
    annotation_id = str(item.get("annotation_id") or item.get("anchor_object_id") or "").strip()
    anchor_type = str(item.get("anchor_type") or "").strip()
    if annotation_id:
        item.setdefault("resolution", "annotation_id")
        item.setdefault("annotation_id", annotation_id)
    elif anchor_type == "indicator" or object_type == "indicator":
        item.setdefault("object_type", "indicator")
        item.setdefault("resolution", item.get("resolution") or "indicator_anchor")
        if not str(item.get("anchor_target") or "").strip():
            raise HTTPException(status_code=400, detail="indicator_link_anchor_target_required")
    else:
        item.setdefault("resolution", item.get("resolution") or "payload_fallback")
        item.setdefault("provisional", True)
    if object_type:
        item.setdefault("object_type", object_type)
    return item


def _normalize_paragraphs(value: object) -> list:
    if not isinstance(value, list):
        return []
    normalized: list[dict] = []
    for index, raw in enumerate(value):
        if not isinstance(raw, dict):
            continue
        text = str(raw.get("text") or "").strip()
        paragraph_id = str(raw.get("paragraph_id") or f"p{index + 1}").strip()
        comment_type = str(raw.get("comment_type") or "review").strip()
        if comment_type not in _NOTE_PARAGRAPH_TYPES:
            raise HTTPException(status_code=400, detail="invalid_paragraph_comment_type")
        linked_objects = [
            item
            for item in (_normalize_linked_object(linked) for linked in raw.get("linked_objects", []))
            if item is not None
        ]
        normalized.append(
            {
                "paragraph_id": paragraph_id,
                "order": int(raw.get("order") or index + 1),
                "text": text,
                "comment_type": comment_type,
                "linked_objects": linked_objects,
                "reason_tags": raw.get("reason_tags") if isinstance(raw.get("reason_tags"), list) else [],
                "action_label": raw.get("action_label") or None,
                "position_before": raw.get("position_before") or None,
                "position_after": raw.get("position_after") or None,
                "no_lookahead": bool(raw.get("no_lookahead", True)),
            }
        )
    return normalized


def _normalize_note_payload(payload: dict) -> tuple[str, str, str, str, str, list, list, list, bool]:
    code = _normalize_code(payload.get("code") or payload.get("symbol"))
    as_of_date = str(payload.get("as_of_date") or payload.get("date") or "").strip()
    timeframe = str(payload.get("timeframe") or "daily").strip() or "daily"
    title = str(payload.get("title") or "").strip()
    note_text = str(payload.get("note_text") or payload.get("memo") or "").strip()
    if not code or not as_of_date:
        raise HTTPException(status_code=400, detail="code_and_as_of_date_required")
    if timeframe not in _NOTE_TIMEFRAMES:
        raise HTTPException(status_code=400, detail="invalid_timeframe")
    tags = payload.get("tags") if isinstance(payload.get("tags"), list) else []
    linked_objects = [
        item
        for item in (_normalize_linked_object(linked) for linked in payload.get("linked_objects", []))
        if item is not None
    ]
    paragraphs = _normalize_paragraphs(payload.get("paragraphs"))
    if paragraphs and not note_text:
        note_text = "\n".join(paragraph["text"] for paragraph in paragraphs if paragraph.get("text")).strip()
    no_lookahead = bool(payload.get("no_lookahead", True))
    return code, as_of_date, timeframe, title, note_text, paragraphs, tags, linked_objects, no_lookahead


def _annotation_response(
    *,
    annotation_id: str,
    code: str,
    as_of_date: str,
    timeframe: str,
    object_type: str,
    payload: dict,
    tags: list,
    no_lookahead: bool,
    created_at,
    updated_at,
) -> dict:
    return {
        "id": annotation_id,
        "code": code,
        "as_of_date": as_of_date,
        "timeframe": timeframe,
        "object_type": object_type,
        "payload": payload,
        "tags": tags,
        "no_lookahead": no_lookahead,
        "created_at": created_at.isoformat() if created_at else None,
        "updated_at": updated_at.isoformat() if updated_at else None,
    }


def _db_retryable_response(*, error_detail: str | None = None) -> JSONResponse:
    payload: dict[str, object] = {
        "error": "db_unavailable",
        "retryable": True,
        "message": "Database is temporarily unavailable",
    }
    if error_detail:
        payload["error_detail"] = error_detail
    return JSONResponse(status_code=503, content=payload, headers={"Retry-After": "1"})


@router.get("/api/chart-reading/bundle")
def chart_reading_bundle(code: str, as_of_date: str):
    try:
        with try_get_conn(timeout_sec=_READ_TIMEOUT_SEC) as conn:
            if conn is None:
                return _db_retryable_response()
            return get_chart_reading_bundle(conn, code=code, as_of_date=as_of_date)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        if is_transient_duckdb_error(exc):
            return _db_retryable_response(error_detail=str(exc))
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/api/chart-annotations")
def list_chart_annotations(code: str, as_of_date: str | None = None):
    normalized_code = _normalize_code(code)
    if not normalized_code:
        raise HTTPException(status_code=400, detail="invalid_code")
    try:
        with try_get_conn(timeout_sec=_READ_TIMEOUT_SEC) as conn:
            if conn is None:
                return _db_retryable_response()
            if as_of_date:
                rows = conn.execute(
                    """
                    SELECT id, code, as_of_date, timeframe, object_type, payload_json, tags_json, no_lookahead, created_at, updated_at
                    FROM chart_annotations
                    WHERE code = ? AND as_of_date <= ?
                    ORDER BY as_of_date DESC, updated_at DESC
                    """,
                    [normalized_code, as_of_date],
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT id, code, as_of_date, timeframe, object_type, payload_json, tags_json, no_lookahead, created_at, updated_at
                    FROM chart_annotations
                    WHERE code = ?
                    ORDER BY as_of_date DESC, updated_at DESC
                    """,
                    [normalized_code],
                ).fetchall()
        return {"items": [_annotation_payload(row) for row in rows]}
    except Exception as exc:
        if is_transient_duckdb_error(exc):
            return _db_retryable_response(error_detail=str(exc))
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.put("/api/chart-notes")
def upsert_chart_note(payload: dict = Body(...)):
    code, as_of_date, timeframe, title, note_text, paragraphs, tags, linked_objects, no_lookahead = _normalize_note_payload(payload)
    now = jst_now().replace(tzinfo=None)
    try:
        with try_get_conn(timeout_sec=_WRITE_TIMEOUT_SEC) as conn:
            if conn is None:
                return _db_retryable_response()
            if not note_text:
                conn.execute(
                    "DELETE FROM chart_notes WHERE code = ? AND as_of_date = ? AND timeframe = ?",
                    [code, as_of_date, timeframe],
                )
                if timeframe in {"D", "daily"}:
                    conn.execute(
                        "DELETE FROM daily_memos WHERE symbol = ? AND date = ? AND timeframe = ?",
                        [code, as_of_date, "D"],
                    )
                return {"ok": True, "deleted": True, "updated_at": None}
            existed = conn.execute(
                "SELECT 1 FROM chart_notes WHERE code = ? AND as_of_date = ? AND timeframe = ?",
                [code, as_of_date, timeframe],
            ).fetchone()
            if existed:
                conn.execute(
                    """
                    UPDATE chart_notes
                    SET title = ?,
                        note_text = ?,
                        paragraphs_json = ?,
                        tags_json = ?,
                        linked_objects_json = ?,
                        no_lookahead = ?,
                        updated_at = ?
                    WHERE code = ? AND as_of_date = ? AND timeframe = ?
                    """,
                    [
                        title,
                        note_text,
                        _json_array(paragraphs),
                        _json_array(tags),
                        _json_array(linked_objects),
                        no_lookahead,
                        now,
                        code,
                        as_of_date,
                        timeframe,
                    ],
                )
            else:
                conn.execute(
                    """
                    INSERT INTO chart_notes (
                        code, as_of_date, timeframe, title, note_text, paragraphs_json, tags_json, linked_objects_json, no_lookahead, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        code,
                        as_of_date,
                        timeframe,
                        title,
                        note_text,
                        _json_array(paragraphs),
                        _json_array(tags),
                        _json_array(linked_objects),
                        no_lookahead,
                        now,
                        now,
                    ],
                )
        return {
            "ok": True,
            "note": {
                "schema_version": "chart_note_v1",
                "code": code,
                "as_of_date": as_of_date,
                "timeframe": timeframe,
                "title": title,
                "note_text": note_text,
                "paragraphs": paragraphs,
                "tags": tags,
                "linked_objects": linked_objects,
                "no_lookahead": no_lookahead,
                "updated_at": now.isoformat(),
            },
        }
    except Exception as exc:
        if is_transient_duckdb_error(exc):
            return _db_retryable_response(error_detail=str(exc))
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.post("/api/chart-annotations")
def create_chart_annotation(payload: dict = Body(...)):
    code, as_of_date, timeframe, object_type, body_payload, tags, no_lookahead = _validate_annotation_payload(payload)
    annotation_id = str(payload.get("id") or uuid4())
    now = jst_now().replace(tzinfo=None)
    try:
        with try_get_conn(timeout_sec=_WRITE_TIMEOUT_SEC) as conn:
            if conn is None:
                return _db_retryable_response()
            conn.execute(
                """
                INSERT INTO chart_annotations (
                    id, code, as_of_date, timeframe, object_type, payload_json, tags_json, no_lookahead, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    annotation_id,
                    code,
                    as_of_date,
                    timeframe,
                    object_type,
                    _json_object(body_payload),
                    _json_array(tags),
                    no_lookahead,
                    now,
                    now,
                ],
            )
        annotation = _annotation_response(
            annotation_id=annotation_id,
            code=code,
            as_of_date=as_of_date,
            timeframe=timeframe,
            object_type=object_type,
            payload=body_payload,
            tags=tags,
            no_lookahead=no_lookahead,
            created_at=now,
            updated_at=now,
        )
        return {"ok": True, "id": annotation_id, "annotation": annotation, "updated_at": now.isoformat()}
    except Exception as exc:
        if is_transient_duckdb_error(exc):
            return _db_retryable_response(error_detail=str(exc))
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.put("/api/chart-annotations/{annotation_id}")
def update_chart_annotation(annotation_id: str, payload: dict = Body(...)):
    code, as_of_date, timeframe, object_type, body_payload, tags, no_lookahead = _validate_annotation_payload(payload)
    now = jst_now().replace(tzinfo=None)
    try:
        with try_get_conn(timeout_sec=_WRITE_TIMEOUT_SEC) as conn:
            if conn is None:
                return _db_retryable_response()
            cursor = conn.execute(
                """
                UPDATE chart_annotations
                SET code = ?,
                    as_of_date = ?,
                    timeframe = ?,
                    object_type = ?,
                    payload_json = ?,
                    tags_json = ?,
                    no_lookahead = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                [
                    code,
                    as_of_date,
                    timeframe,
                    object_type,
                    _json_object(body_payload),
                    _json_array(tags),
                    no_lookahead,
                    now,
                    annotation_id,
                ],
            )
        annotation = _annotation_response(
            annotation_id=annotation_id,
            code=code,
            as_of_date=as_of_date,
            timeframe=timeframe,
            object_type=object_type,
            payload=body_payload,
            tags=tags,
            no_lookahead=no_lookahead,
            created_at=None,
            updated_at=now,
        )
        return {"ok": True, "id": annotation_id, "updated": cursor.rowcount != 0, "annotation": annotation, "updated_at": now.isoformat()}
    except Exception as exc:
        if is_transient_duckdb_error(exc):
            return _db_retryable_response(error_detail=str(exc))
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.delete("/api/chart-annotations/{annotation_id}")
def delete_chart_annotation(annotation_id: str):
    try:
        with try_get_conn(timeout_sec=_WRITE_TIMEOUT_SEC) as conn:
            if conn is None:
                return _db_retryable_response()
            existed = conn.execute("SELECT 1 FROM chart_annotations WHERE id = ? LIMIT 1", [annotation_id]).fetchone()
            conn.execute("DELETE FROM chart_annotations WHERE id = ?", [annotation_id])
        return {"ok": True, "id": annotation_id, "deleted": existed is not None}
    except Exception as exc:
        if is_transient_duckdb_error(exc):
            return _db_retryable_response(error_detail=str(exc))
        return JSONResponse(status_code=500, content={"error": str(exc)})
