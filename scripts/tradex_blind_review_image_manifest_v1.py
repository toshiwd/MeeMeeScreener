"""Bind MeeMee chart exports to a frozen blind reviewer board."""
import argparse
import hashlib
import json
import shutil
from pathlib import Path

import pandas as pd


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--board", type=Path, required=True)
    parser.add_argument("--capture-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    image_output = args.output / "images"
    image_output.mkdir()

    board = pd.read_parquet(args.board)
    manifests = []
    for path in args.capture_root.rglob("image_manifest.jsonl"):
        manifests.extend(json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip())
    by_key = {(str(row["code"]).zfill(4), str(row["as_of"])): row for row in manifests}

    rows = []
    for item in board.itertuples():
        ymd = str(int(item.ymd))
        as_of = f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:]}"
        source = by_key.get((str(item.code).zfill(4), as_of))
        if source is None:
            raise RuntimeError(f"missing capture for {item.case_id} {item.code} {as_of}")
        if "reviewTimeframes=daily-monthly" not in source["url"] or f"mainAsOf={as_of}" not in source["url"]:
            raise RuntimeError(f"capture contract mismatch for {item.case_id}")
        source_path = Path(source["saved_path"])
        destination = image_output / f"{item.case_id}_{str(item.code).zfill(4)}_{ymd}.png"
        shutil.copy2(source_path, destination)
        rows.append({
            "case_id": item.case_id,
            "code": str(item.code).zfill(4),
            "ymd": int(item.ymd),
            "chart_cutoff_ymd": int(item.chart_cutoff_ymd),
            "review_timeframes": ["monthly", "daily"],
            "weekly_visible": False,
            "future_bars_allowed": False,
            "image_relpath": f"images/{destination.name}",
            "image_sha256": sha(destination),
        })

    expected = len(board)
    if len(rows) != expected or len({row["code"] for row in rows}) != expected:
        raise RuntimeError(f"expected {expected} unique-code images")
    manifest = args.output / "review_image_manifest.jsonl"
    manifest.write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n", encoding="utf-8")
    audit = {
        "schema_version": "tradex_blind_review_image_manifest_v1.audit",
        "review_only": True,
        "image_count": len(rows),
        "unique_codes": len({row["code"] for row in rows}),
        "weekly_visible_count": sum(row["weekly_visible"] for row in rows),
        "future_bars_allowed_count": sum(row["future_bars_allowed"] for row in rows),
        "board_sha256": sha(args.board),
        "manifest_sha256": sha(manifest),
        "capture_owner": "MeeMee",
        "research_owner": "TRADEX",
        "outcome_revealed": False,
    }
    audit_path = args.output / "audit.json"
    audit_path.write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "audit.json", "sha256": sha(audit_path)}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(args.output), **audit}, indent=2))


if __name__ == "__main__":
    main()
