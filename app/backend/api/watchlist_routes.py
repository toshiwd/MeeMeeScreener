from fastapi import APIRouter, Body
from fastapi.responses import JSONResponse

router = APIRouter()


def _load_main():
    # 遅延importで循環参照を回避する。
    from app.backend import main as main_module

    return main_module


@router.get("/api/watchlist")
def get_watchlist():
    main_module = _load_main()
    path = main_module._resolve_pan_code_txt_path()
    if not main_module.os.path.isfile(path):
        return {"codes": [], "path": path, "missing": True}
    with main_module._watchlist_lock:
        codes = main_module._load_watchlist_codes(path)
    return {"codes": codes, "path": path, "missing": False}


@router.post("/api/watchlist/add")
def watchlist_add(payload: dict = Body(default=None)):
    main_module = _load_main()
    payload = payload or {}
    code = main_module._normalize_watch_code(payload.get("code"))
    if not code:
        return JSONResponse(status_code=400, content={"ok": False, "error": "invalid_code"})
    path = main_module._resolve_pan_code_txt_path()
    with main_module._watchlist_lock:
        codes = main_module._load_watchlist_codes(path) if main_module.os.path.isfile(path) else []
        already = code in codes
        main_module._update_watchlist_file(path, code, remove=False)
    return {"ok": True, "code": code, "alreadyExisted": already}


@router.post("/api/watchlist/remove")
def watchlist_remove(payload: dict = Body(default=None)):
    main_module = _load_main()
    payload = payload or {}
    code = main_module._normalize_watch_code(payload.get("code"))
    if not code:
        return JSONResponse(status_code=400, content={"ok": False, "error": "invalid_code"})
    delete_artifacts = payload.get("deleteArtifacts", True)
    delete_db = payload.get("deleteDb", False)
    delete_related = payload.get("deleteRelated", False)
    path = main_module._resolve_pan_code_txt_path()
    if not main_module.os.path.isfile(path):
        return JSONResponse(status_code=400, content={"ok": False, "error": "code_txt_missing"})
    with main_module._watchlist_lock:
        removed = main_module._update_watchlist_file(path, code, remove=True)
        trash_token = None
        trashed: list[str] = []
        if delete_artifacts:
            trash_token, trashed = main_module._trash_watchlist_artifacts(code)
    db_counts: dict[str, int] = {}
    favorites_deleted = 0
    practice_deleted = 0
    if delete_db:
        try:
            db_counts = main_module._delete_ticker_db_rows(code)
            main_module._invalidate_screener_cache()
        except Exception as exc:
            return JSONResponse(
                status_code=500,
                content={"ok": False, "error": f"db_delete_failed:{exc}", "code": code}
            )
    if delete_related:
        try:
            favorites_deleted = main_module._delete_favorites_code(code)
            practice_deleted = main_module._delete_practice_sessions(code)
        except Exception as exc:
            return JSONResponse(
                status_code=500,
                content={"ok": False, "error": f"related_delete_failed:{exc}", "code": code}
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
        "trashToken": trash_token
    }


@router.post("/api/watchlist/undo_remove")
def watchlist_undo_remove(payload: dict = Body(default=None)):
    main_module = _load_main()
    payload = payload or {}
    code = main_module._normalize_watch_code(payload.get("code"))
    token = payload.get("trashToken") or ""
    if not code:
        return JSONResponse(status_code=400, content={"ok": False, "error": "invalid_code"})
    with main_module._watchlist_lock:
        restored = main_module._restore_watchlist_artifacts(token)
        main_module._update_watchlist_file(main_module._resolve_pan_code_txt_path(), code, remove=False)
    return {"ok": True, "code": code, "restored": restored}


@router.post("/api/watchlist/open")
def watchlist_open():
    main_module = _load_main()
    path = main_module._resolve_pan_code_txt_path()
    main_module.os.makedirs(main_module.os.path.dirname(path), exist_ok=True)
    if not main_module.os.path.isfile(path):
        with open(path, "w", encoding="utf-8") as handle:
            handle.write("")
    try:
        if main_module.os.name == "nt":
            main_module.os.startfile(path)
        else:
            opener = "open" if main_module.sys.platform == "darwin" else "xdg-open"
            main_module.subprocess.Popen([opener, path])
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": f"open_failed:{exc}", "path": path}
        )
    return {"ok": True, "path": path}