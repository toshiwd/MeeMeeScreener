from __future__ import annotations

import os

from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter()


def _load_main():
    # 遅延importで循環参照を避ける。
    from app.backend import main as main_module

    return main_module


@router.post("/api/txt_update/run")
def txt_update_run():
    main_module = _load_main()
    # Use the job manager so progress/state survives SPA navigation and we can
    # report failures reliably via sys_jobs.
    update_vbs_path = main_module._resolve_update_vbs_path()
    if not main_module.os.path.isfile(update_vbs_path):
        # Ensure the progress file reflects the error so "no change" isn't mistaken for no-op.
        main_module._write_vbs_progress(phase="error", error=f"vbs_not_found:{update_vbs_path}")
        return JSONResponse(status_code=404, content={"ok": False, "error": f"vbs_not_found:{update_vbs_path}"})

    code_candidate = main_module._resolve_pan_code_txt_path()
    code_path = code_candidate if main_module.os.path.isfile(code_candidate) else None
    if not code_path:
        main_module._write_vbs_progress(phase="error", error=f"code_txt_missing:{code_candidate}")
        return JSONResponse(
            status_code=400,
            content={"ok": False, "error": "code_txt_missing", "searched": [code_candidate]},
        )

    job_id = main_module.job_manager.submit("txt_update", unique=True)
    if not job_id:
        main_module._write_vbs_progress(phase="blocked", error="update_in_progress")
        return JSONResponse(status_code=409, content={"ok": False, "error": "update_in_progress"})

    # Reset progress immediately so the UI doesn't reuse stale vbs_progress.json from a prior run.
    main_module._write_vbs_progress(
        phase="queued",
        job_id=job_id,
        current="",
        started=0,
        ok=0,
        err=0,
        split=0,
        error="",
    )

    return {
        "ok": True,
        "started": True,
        "started_at": main_module.datetime.now().isoformat(),
        "total": main_module._count_codes(code_path),
        "job_id": job_id,
    }


@router.get("/api/txt_update/status")
def txt_update_status():
    main_module = _load_main()
    return main_module.txt_update_status()

@router.get("/api/txt_update/diagnostics")
def txt_update_diagnostics():
    main_module = _load_main()
    update_vbs_path = main_module._resolve_update_vbs_path()
    code_txt_path = main_module._resolve_pan_code_txt_path()
    out_dir = main_module._resolve_pan_out_txt_dir()
    progress_path, legacy_path = main_module._resolve_vbs_progress_paths()

    sys_root = os.environ.get("SystemRoot") or "C:\\Windows"
    cscript = os.path.join(sys_root, "SysWOW64", "cscript.exe")
    if not os.path.isfile(cscript):
        cscript = os.path.join(sys_root, "System32", "cscript.exe")

    payload = {
        "update_vbs_path": update_vbs_path,
        "update_vbs_exists": os.path.isfile(update_vbs_path),
        "code_txt_path": code_txt_path,
        "code_txt_exists": os.path.isfile(code_txt_path),
        "pan_out_txt_dir": out_dir,
        "pan_out_txt_exists": os.path.isdir(out_dir),
        "progress_path": progress_path,
        "progress_exists": os.path.isfile(progress_path),
        "legacy_progress_path": legacy_path,
        "legacy_progress_exists": os.path.isfile(legacy_path) if legacy_path else False,
        "cscript_path": cscript,
        "cscript_exists": os.path.isfile(cscript),
    }

    for label, path in (("progress_payload", progress_path), ("legacy_progress_payload", legacy_path)):
        if path and os.path.isfile(path):
            try:
                with open(path, "r", encoding="utf-8", errors="ignore") as handle:
                    payload[label] = handle.read()
            except Exception:
                payload[label] = None

    return payload


@router.get("/api/txt_update/split_suspects")
def txt_update_split_suspects():
    main_module = _load_main()
    if not main_module.os.path.isfile(main_module.SPLIT_SUSPECTS_PATH):
        return {"items": []}
    items = []
    try:
        for line in main_module._read_text_lines(main_module.SPLIT_SUSPECTS_PATH):
            if not line or line.lower().startswith("code,"):
                continue
            parts = [part.strip() for part in line.split(",")]
            if len(parts) < 7:
                continue
            items.append(
                {
                    "code": parts[0],
                    "file_date": parts[1],
                    "file_close": parts[2],
                    "pan_date": parts[3],
                    "pan_close": parts[4],
                    "diff_ratio": parts[5],
                    "reason": parts[6],
                    "detected_at": parts[7] if len(parts) > 7 else "",
                }
            )
        return {"items": items}
    except Exception as exc:
        return JSONResponse(status_code=200, content={"items": [], "error": str(exc)})


@router.post("/api/update_txt")
def update_txt():
    return txt_update_run()
