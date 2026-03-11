from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from research.study_storage import study_paths
from research.storage import ResearchPaths, read_csv, read_json, write_json


def _read_optional_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return read_csv(path)


def run_study_report(paths: ResearchPaths, study_id: str) -> dict[str, Any]:
    spaths = study_paths(paths, study_id)
    manifest = read_json(spaths["manifest"])
    dataset_meta = read_json(spaths["dataset_meta"]) if spaths["dataset_meta"].exists() else {}
    top_payload = read_json(spaths["top_hypotheses"])
    adopted_payload = read_json(spaths["adopted_hypotheses"])

    top_items = top_payload.get("items") if isinstance(top_payload.get("items"), list) else []
    adopted_items = adopted_payload.get("items") if isinstance(adopted_payload.get("items"), list) else []

    top_frame = pd.DataFrame(top_items)
    adopted_frame = pd.DataFrame(adopted_items)
    dist_h = _read_optional_csv(spaths["dist_horizon"])
    dist_c = _read_optional_csv(spaths["dist_cluster"])
    dist_r = _read_optional_csv(spaths["dist_regime"])

    summary = {
        "ok": True,
        "study_id": study_id,
        "snapshot_id": manifest.get("snapshot_id"),
        "status": manifest.get("status"),
        "top_hypotheses_count": int(len(top_frame)),
        "adopted_hypotheses_count": int(len(adopted_frame)),
        "datasets": (dataset_meta.get("datasets") if isinstance(dataset_meta, dict) else {}) or {},
        "top_hypotheses": top_items,
        "adopted_hypotheses": adopted_items,
        "distribution_rows": {
            "horizon": int(len(dist_h)),
            "cluster": int(len(dist_c)),
            "regime": int(len(dist_r)),
        },
    }

    report_path = spaths["root"] / "report.json"
    write_json(report_path, summary)
    return summary
