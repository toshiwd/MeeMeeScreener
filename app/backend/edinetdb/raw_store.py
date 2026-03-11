from __future__ import annotations

import gzip
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def normalize_endpoint_name(endpoint: str) -> str:
    value = str(endpoint or "unknown").strip().strip("/")
    if not value:
        return "unknown"
    value = value.replace("/v1/", "").replace("v1/", "")
    value = value.replace("/", "_")
    value = value.replace("{", "").replace("}", "")
    return value


def write_raw_gzip(
    *,
    raw_root: Path,
    endpoint: str,
    edinet_code: str | None,
    payload: dict[str, Any],
) -> Path:
    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
    endpoint_dir = normalize_endpoint_name(endpoint)
    code_dir = str(edinet_code or "_global").strip() or "_global"
    target_dir = raw_root / endpoint_dir / code_dir
    target_dir.mkdir(parents=True, exist_ok=True)
    out_path = target_dir / f"{ts}.json.gz"
    with gzip.open(out_path, "wt", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, sort_keys=False)
    return out_path
