"""Join the monthly review queue to completed MeeMee clean screenshots."""
from __future__ import annotations

import argparse
import json
from pathlib import Path


def read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--queue", type=Path, required=True)
    ap.add_argument("--manifest", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    queue = read_jsonl(args.queue)
    manifest = read_jsonl(args.manifest)
    images = {(str(r["code"]).zfill(4), str(r["as_of"])): r for r in manifest}
    rows = []
    for row in queue:
        key = (str(row["code"]).zfill(4), row["as_of_iso"])
        shot = images.get(key)
        rows.append({
            **row,
            "image_path": None if shot is None else shot["saved_path"],
            "image_clean": bool(shot and shot.get("clean_screenshot")),
            "human_label": {
                "monthly_regime": None,
                "location": None,
                "confidence": None,
                "reason_codes": [],
            },
        })
    payload = {
        "schema_version": "tradex_monthly_environment_review_pack_v1",
        "artifact_role": "human_review_pack_not_labels",
        "rows": rows,
        "label_options": {
            "monthly_regime": ["BOX", "POST_BOX_BREAKOUT_CONSOLIDATION", "UPTREND", "DOWNTREND", "AMBIGUOUS"],
            "location": ["BOX_BOTTOM", "BOX_MIDDLE", "BOX_CEILING", "ABOVE_OLD_BOX", "BELOW_OLD_BOX", "TREND_MIDDLE", "TREND_EXTREME"],
            "reason_codes": ["BOX_TOUCHES", "BOX_BREAKOUT", "BOX_REENTRY", "MA_SLOPE", "HIGH_LOW_SEQUENCE", "CURRENT_MONTH_CHANGES_READ", "OTHER"],
        },
        "not_changed": ["classifier", "trade rules", "ranking", "runtime DB"],
    }
    out = args.output / "review_pack.json"
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "queue_rows": len(queue), "manifest_rows": len(manifest), "joined_images": sum(r["image_path"] is not None for r in rows),
        "missing_images": sum(r["image_path"] is None for r in rows), "human_labels_filled": 0,
        "review_only": True,
    }
    (args.output / "audit.json").write_text(json.dumps(audit, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "review_pack.json"}, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(args.output), "audit": audit}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
