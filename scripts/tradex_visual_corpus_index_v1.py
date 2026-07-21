from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


AXIS_ID = "tradex_visual_corpus_index_v1"
DEFAULT_ROOT = Path(r"G:\Tradex")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\visual_corpus_index_v1")
DEFAULT_TARGET_EVENTS = Path(
    r"G:\Tradex\2201_like_visual_event_dataset_v1\20260705T154230Z-tradex_2201_like_visual_event_dataset_v1\events_all.jsonl"
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _read_jsonl(path: Path, *, max_lines: int | None = None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8-sig", errors="replace") as handle:
        for index, line in enumerate(handle):
            if max_lines is not None and index >= max_lines:
                break
            text = line.strip()
            if not text:
                continue
            try:
                row = json.loads(text)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                rows.append(row)
    return rows


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, ensure_ascii=False, default=str) + "\n" for row in rows), encoding="utf-8")


def _key(row: dict[str, Any]) -> str | None:
    code = row.get("code")
    as_of = row.get("as_of") or row.get("asof") or row.get("date")
    if code is None or as_of in (None, ""):
        return None
    return f"{code}:{as_of}"


def _quality(row: dict[str, Any], manifest_path: Path) -> str:
    if row.get("clean_screenshot") is True and row.get("centered_screenshot") is True:
        return "clean_centered"
    if row.get("clean_screenshot") is True:
        return "clean_uncentered"
    if "multiscale" in str(manifest_path).lower():
        return "legacy_multiscale"
    return "legacy_or_unknown"


def _compact_row(row: dict[str, Any], manifest_path: Path) -> dict[str, Any]:
    quality = _quality(row, manifest_path)
    return {
        "schema_version": f"{AXIS_ID}_row_v1",
        "code": str(row.get("code")) if row.get("code") is not None else None,
        "as_of": row.get("as_of") or row.get("asof") or row.get("date"),
        "key": _key(row),
        "quality": quality,
        "clean_screenshot": row.get("clean_screenshot"),
        "centered_screenshot": row.get("centered_screenshot"),
        "center_lookback_months": row.get("center_lookback_months"),
        "center_lookahead_months": row.get("center_lookahead_months"),
        "viewport": row.get("viewport"),
        "saved_path": row.get("saved_path") or row.get("image_path") or row.get("path"),
        "image_relpath": row.get("image_relpath"),
        "manifest_path": str(manifest_path),
        "dataset_root": str(manifest_path.parent),
        "purpose_hint": _purpose_hint(manifest_path),
    }


def _purpose_hint(path: Path) -> str:
    text = str(path).replace("\\", "/").lower()
    for token in [
        "short_shape_unbiased_holdout",
        "short_shape_labeled",
        "shape_entry_timing",
        "shape_research_decline",
        "short_entry_shape_family",
        "short_watch_to_entry",
        "meemee_multiscale",
        "image_assisted_rerank",
        "2201_like",
    ]:
        if token in text:
            return token
    return "unknown"


def run(*, root: Path, output_root: Path, target_events: Path) -> Path:
    run_dir = output_root / f"{_tag()}-{AXIS_ID}"
    run_dir.mkdir(parents=True, exist_ok=False)
    manifest_paths = sorted(root.rglob("image_manifest.jsonl"), key=lambda p: str(p).lower())
    rows: list[dict[str, Any]] = []
    manifest_summaries: list[dict[str, Any]] = []
    for manifest_path in manifest_paths:
        manifest_rows = _read_jsonl(manifest_path)
        compact = [_compact_row(row, manifest_path) for row in manifest_rows]
        rows.extend(compact)
        quality_counts: dict[str, int] = {}
        for row in compact:
            quality_counts[row["quality"]] = quality_counts.get(row["quality"], 0) + 1
        manifest_summaries.append(
            {
                "manifest_path": str(manifest_path),
                "row_count": len(compact),
                "quality_counts": quality_counts,
                "purpose_hint": _purpose_hint(manifest_path),
            }
        )
    target_keys = {_key(row) for row in _read_jsonl(target_events) if _key(row)}
    by_key: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        key = row.get("key")
        if key:
            by_key.setdefault(key, []).append(row)
    joined_target_rows = []
    for key in sorted(target_keys):
        matches = by_key.get(key, [])
        if not matches:
            continue
        matches_sorted = sorted(
            matches,
            key=lambda row: (
                0 if row.get("quality") == "clean_centered" else 1 if row.get("quality") == "clean_uncentered" else 2,
                str(row.get("manifest_path")),
            ),
        )
        joined_target_rows.append(
            {
                "key": key,
                "target_event_match": True,
                "best_quality": matches_sorted[0].get("quality"),
                "match_count": len(matches_sorted),
                "matches": matches_sorted[:5],
            }
        )
    quality_counts: dict[str, int] = {}
    purpose_counts: dict[str, int] = {}
    for row in rows:
        quality_counts[row["quality"]] = quality_counts.get(row["quality"], 0) + 1
        purpose_counts[row["purpose_hint"]] = purpose_counts.get(row["purpose_hint"], 0) + 1
    duplicate_key_count = sum(1 for key, values in by_key.items() if len(values) > 1)
    _write_jsonl(run_dir / "visual_corpus_index.jsonl", rows)
    _write_jsonl(run_dir / "manifest_summaries.jsonl", manifest_summaries)
    _write_jsonl(run_dir / "target_event_image_matches.jsonl", joined_target_rows)
    audit = {
        "schema_version": f"{AXIS_ID}_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "research_phase": "infrastructure_stabilization",
        "root": str(root),
        "target_events": str(target_events),
        "summary": {
            "manifest_count": len(manifest_paths),
            "image_row_count": len(rows),
            "unique_key_count": len(by_key),
            "duplicate_key_count": duplicate_key_count,
            "target_event_key_count": len(target_keys),
            "target_event_keys_with_image": len(joined_target_rows),
            "target_event_image_coverage": len(joined_target_rows) / len(target_keys) if target_keys else None,
            "quality_counts": quality_counts,
            "purpose_counts": purpose_counts,
        },
        "artifacts": {
            "visual_corpus_index_jsonl": str(run_dir / "visual_corpus_index.jsonl"),
            "manifest_summaries_jsonl": str(run_dir / "manifest_summaries.jsonl"),
            "target_event_image_matches_jsonl": str(run_dir / "target_event_image_matches.jsonl"),
        },
        "decision": {
            "candidate_local_decision": "visual_corpus_index_ready",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "historical screenshot manifests indexed and target event coverage measured",
        },
        "runtime_db_write": False,
        "meemee_unchanged": True,
        "production_ranking_changed": False,
    }
    _write_json(run_dir / "visual_corpus_index_audit.json", audit)
    _write_json(output_root / "latest_visual_corpus_index_audit.json", {"run_root": str(run_dir), **audit})
    return run_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--target-events", type=Path, default=DEFAULT_TARGET_EVENTS)
    args = parser.parse_args()
    print(run(root=args.root, output_root=args.output_root, target_events=args.target_events))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
