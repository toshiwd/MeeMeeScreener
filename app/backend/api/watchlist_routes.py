from __future__ import annotations

import os
import subprocess
import sys

from fastapi import APIRouter, Body
from fastapi.responses import JSONResponse

from app.backend.services.watchlist import (
    delete_favorites_code,
    delete_practice_sessions,
    delete_ticker_db_rows,
    invalidate_screener_cache,
    load_watchlist_codes,
    normalize_watch_code,
    resolve_watchlist_path,
    restore_watchlist_artifacts,
    trash_watchlist_artifacts,
    update_watchlist_file,
    watchlist_lock,
)

router = APIRouter()


@router.get("/api/watchlist")
def get_watchlist():
    path = resolve_watchlist_path()
    if not os.path.isfile(path):
        return {"codes": [], "path": path, "missing": True}

    with watchlist_lock:
        codes = load_watchlist_codes(path)
    return {"codes": codes, "path": path, "missing": False}


@router.post("/api/watchlist/add")
def watchlist_add(payload: dict = Body(default=None)):
    payload = payload or {}
    code = normalize_watch_code(payload.get("code"))
    if not code:
        return JSONResponse(status_code=400, content={"ok": False, "error": "invalid_code"})

    path = resolve_watchlist_path()
    with watchlist_lock:
        existing = load_watchlist_codes(path) if os.path.isfile(path) else []
        already = code in existing
        update_watchlist_file(path, code, remove=False)
    return {"ok": True, "code": code, "alreadyExisted": already}


@router.post("/api/watchlist/remove")
def watchlist_remove(payload: dict = Body(default=None)):
    payload = payload or {}
    code = normalize_watch_code(payload.get("code"))
    if not code:
        return JSONResponse(status_code=400, content={"ok": False, "error": "invalid_code"})

    delete_artifacts = payload.get("deleteArtifacts", True)
    delete_db = payload.get("deleteDb", False)
    delete_related = payload.get("deleteRelated", False)
    path = resolve_watchlist_path()
    if not os.path.isfile(path):
        return JSONResponse(status_code=400, content={"ok": False, "error": "code_txt_missing"})

    with watchlist_lock:
        removed = update_watchlist_file(path, code, remove=True)
        trash_token = None
        trashed: list[str] = []
        if delete_artifacts:
            trash_token, trashed = trash_watchlist_artifacts(code)

    db_counts: dict[str, int] = {}
    favorites_deleted = 0
    practice_deleted = 0
    if delete_db:
        try:
            db_counts = delete_ticker_db_rows(code)
            invalidate_screener_cache()
        except Exception as exc:
            return JSONResponse(
                status_code=500,
                content={"ok": False, "error": f"db_delete_failed:{exc}", "code": code},
            )
    if delete_related:
        try:
            favorites_deleted = delete_favorites_code(code)
            practice_deleted = delete_practice_sessions(code)
        except Exception as exc:
            return JSONResponse(
                status_code=500,
                content={"ok": False, "error": f"related_delete_failed:{exc}", "code": code},
            )
    return {
        "ok": True,
        "code": code,
        "removed": removed,
        "deleteArtifacts": bool(delete_artifacts),
        "deleteDb": bool(delete_db),
        "deleteRelated": bool(delete_related),
        "dbDeletedCounts": db_counts,
        "dbDeletedTotal": sum(db_counts.values()),
        "favoritesDeleted": favorites_deleted,
        "practiceDeleted": practice_deleted,
        "trashed": trashed,
        "trashToken": trash_token,
    }


@router.post("/api/watchlist/undo_remove")
def watchlist_undo_remove(payload: dict = Body(default=None)):
    payload = payload or {}
    code = normalize_watch_code(payload.get("code"))
    token = payload.get("trashToken") or ""
    if not code:
        return JSONResponse(status_code=400, content={"ok": False, "error": "invalid_code"})

    with watchlist_lock:
        restored = restore_watchlist_artifacts(token)
        update_watchlist_file(resolve_watchlist_path(), code, remove=False)
    return {"ok": True, "code": code, "restored": restored}


@router.post("/api/watchlist/open")
def watchlist_open():
    path = resolve_watchlist_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.isfile(path):
        with open(path, "w", encoding="utf-8") as handle:
            handle.write("")
    try:
        if os.name == "nt":
            os.startfile(path)
        else:
            opener = "open" if sys.platform == "darwin" else "xdg-open"
            subprocess.Popen([opener, path])
    except Exception as exc:
        return JSONResponse(
            status_code=500, content={"ok": False, "error": f"open_failed:{exc}", "path": path}
        )
    return {"ok": True, "path": path}
