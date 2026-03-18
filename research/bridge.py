from __future__ import annotations

from datetime import datetime, timezone
import shutil
from pathlib import Path
from typing import Any

import pandas as pd

from research.storage import ResearchPaths, read_csv, read_json, write_json
from research.study_storage import study_paths


PRIOR_SNAPSHOT_FILE = "research_prior_snapshot.json"
ADOPTED_HYPOTHESES_FILE = "adopted_hypotheses.json"
BRIDGE_MANIFEST_FILE = "bridge_manifest.json"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _load_latest_bridge_manifest(paths: ResearchPaths) -> dict[str, Any]:
    manifest_path = paths.bridge_latest_dir / BRIDGE_MANIFEST_FILE
    if not manifest_path.exists():
        return {"generated_at": None, "artifacts": {}}
    try:
        return read_json(manifest_path)
    except Exception:
        return {"generated_at": None, "artifacts": {}}


def _latest_codes_payload(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {"asof": None, "codes": [], "rank_map": {}}
    work = frame.copy()
    if "phase" in work.columns:
        inference = work[work["phase"].astype(str).str.lower() == "inference"].copy()
        if not inference.empty:
            work = inference
    work["asof_date"] = pd.to_datetime(work.get("asof_date"), errors="coerce").dt.strftime("%Y-%m-%d")
    work = work.dropna(subset=["asof_date", "code"]).copy()
    if work.empty:
        return {"asof": None, "codes": [], "rank_map": {}}
    latest_asof = str(work["asof_date"].max())
    work = work[work["asof_date"] == latest_asof].copy()
    if "score" in work.columns:
        work = work.sort_values("score", ascending=False)
    seen: set[str] = set()
    codes: list[str] = []
    for code in work["code"].astype(str):
        code_str = code.strip()
        if not code_str or code_str in seen:
            continue
        seen.add(code_str)
        codes.append(code_str)
    return {
        "asof": latest_asof,
        "codes": codes,
        "rank_map": {code: idx + 1 for idx, code in enumerate(codes)},
    }


def _write_bridge_history(paths: ResearchPaths, *, source_type: str, source_id: str, payloads: dict[str, dict[str, Any]]) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    target = paths.bridge_history_root / f"{stamp}_{source_type}_{source_id}"
    target.mkdir(parents=True, exist_ok=True)
    for name, payload in payloads.items():
        write_json(target / name, payload)
    return target


def _write_bridge_latest(
    paths: ResearchPaths,
    *,
    source_type: str,
    source_id: str,
    payloads: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    latest_dir = paths.bridge_latest_dir
    latest_dir.parent.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    stage_dir = paths.bridge_root / f".latest_stage_{stamp}"
    if latest_dir.exists():
        shutil.copytree(latest_dir, stage_dir)
    else:
        stage_dir.mkdir(parents=True, exist_ok=True)

    manifest = _load_latest_bridge_manifest(paths)
    artifacts = manifest.get("artifacts") if isinstance(manifest.get("artifacts"), dict) else {}
    for name, payload in payloads.items():
        write_json(stage_dir / name, payload)
        artifacts[name] = {
            "source_type": source_type,
            "source_id": source_id,
            "generated_at": _utc_now_iso(),
            "filename": name,
        }
    manifest["generated_at"] = _utc_now_iso()
    manifest["artifacts"] = artifacts
    write_json(stage_dir / BRIDGE_MANIFEST_FILE, manifest)
    paths.replace_dir_atomically(stage_dir, latest_dir)
    return manifest


def export_bridge_run(paths: ResearchPaths, run_id: str) -> dict[str, Any]:
    run_dir = paths.run_dir(run_id)
    if not run_dir.exists():
        raise FileNotFoundError(f"run not found: {run_id}")
    manifest = read_json(run_dir / "manifest.json")
    long_frame = read_csv(run_dir / "top20_long.csv")
    short_frame = read_csv(run_dir / "top20_short.csv")
    payload = {
        "generated_at": _utc_now_iso(),
        "source_type": "run",
        "source_id": run_id,
        "run_id": run_id,
        "run_manifest_created_at": manifest.get("created_at"),
        "up": _latest_codes_payload(long_frame),
        "down": _latest_codes_payload(short_frame),
    }
    payloads = {PRIOR_SNAPSHOT_FILE: payload}
    history_dir = _write_bridge_history(paths, source_type="run", source_id=run_id, payloads=payloads)
    latest_manifest = _write_bridge_latest(paths, source_type="run", source_id=run_id, payloads=payloads)
    return {
        "ok": True,
        "run_id": run_id,
        "history_dir": str(history_dir),
        "latest_dir": str(paths.bridge_latest_dir),
        "bridge_manifest": latest_manifest,
    }


def export_bridge_study(paths: ResearchPaths, study_id: str) -> dict[str, Any]:
    spaths = study_paths(paths, study_id)
    adopted_path = spaths["adopted_hypotheses"]
    if not adopted_path.exists():
        raise FileNotFoundError(f"adopted hypotheses not found: {adopted_path}")
    payload = read_json(adopted_path)
    payloads = {
        ADOPTED_HYPOTHESES_FILE: payload,
    }
    history_dir = _write_bridge_history(paths, source_type="study", source_id=study_id, payloads=payloads)
    latest_manifest = _write_bridge_latest(paths, source_type="study", source_id=study_id, payloads=payloads)
    return {
        "ok": True,
        "study_id": study_id,
        "history_dir": str(history_dir),
        "latest_dir": str(paths.bridge_latest_dir),
        "bridge_manifest": latest_manifest,
    }
