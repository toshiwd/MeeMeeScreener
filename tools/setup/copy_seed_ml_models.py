from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path


def _extract_version(name: str) -> str | None:
    stem = Path(name).stem
    if "_" not in stem:
        return None
    prefix = stem.split("_", 1)[0]
    if len(prefix) == 14 and prefix.isdigit():
        return prefix
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Copy latest local ML model artifacts into release seed folder.")
    parser.add_argument("--dest", required=True, help="Destination directory")
    args = parser.parse_args()

    local_appdata = os.environ.get("LOCALAPPDATA", "")
    if not local_appdata:
        print("WARN: LOCALAPPDATA is not set. Skip ML seed copy.")
        return 0

    src_dir = Path(local_appdata) / "MeeMeeScreener" / "data" / "models" / "ml"
    if not src_dir.is_dir():
        print(f"WARN: ML model directory not found. skip: {src_dir}")
        return 0

    files = [p for p in src_dir.glob("*.txt") if _extract_version(p.name)]
    if not files:
        print(f"WARN: no versioned ML model files found. skip: {src_dir}")
        return 0

    versions = sorted({_extract_version(p.name) for p in files if _extract_version(p.name)})
    latest = versions[-1]
    selected = sorted([p for p in files if _extract_version(p.name) == latest], key=lambda p: p.name)

    dest_dir = Path(args.dest)
    dest_dir.mkdir(parents=True, exist_ok=True)
    copied: list[str] = []
    for src in selected:
        dst = dest_dir / src.name
        shutil.copy2(src, dst)
        copied.append(src.name)

    manifest = {
        "model_version": latest,
        "source_dir": str(src_dir),
        "files": copied,
    }
    (dest_dir / "seed_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"ML seed copied: version={latest} files={len(copied)} dest={dest_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
