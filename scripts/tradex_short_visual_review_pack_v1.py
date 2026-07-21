from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


AXIS_ID = "short_visual_review_pack_v1"
DEFAULT_OPERATIONAL = Path(r"G:\Tradex\short_operational_pipeline_v1\latest_short_operational_pipeline.json")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_visual_review_pack_v1")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, ensure_ascii=False, default=str) + "\n" for row in rows), encoding="utf-8")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def _screenshot_index(manifest_path: Path | None) -> dict[str, dict[str, Any]]:
    if manifest_path is None:
        return {}
    rows = _read_jsonl(manifest_path)
    out = {}
    for row in rows:
        code = row.get("code")
        as_of = row.get("as_of")
        if code and as_of:
            out[f"{code}:{as_of}"] = row
    return out


def _empty_tag_decision(row: dict[str, Any], sample: dict[str, Any]) -> dict[str, Any]:
    schema = row.get("visual_review_tags_schema") or {}
    payload = {
        "review_id": f"{row.get('code')}::{sample.get('stage')}::{sample.get('sample')}",
        "code": row.get("code"),
        "display_name": row.get("display_name"),
        "stage": sample.get("stage"),
        "sample": sample.get("sample"),
        "purpose": sample.get("purpose"),
        "pattern_id": row.get("pattern_id"),
        "as_of": row.get("as_of"),
        "review_decision": "unreviewed",
        "confidence": None,
        "selected_tags": {
            "monthly_context": [],
            "daily_context": [],
            "ma_context": [],
            "entry_quality": [],
        },
        "allowed_tags": schema,
        "manual_notes": "",
        "must_not_change": [
            "production_ranking",
            "runtime_db",
            "MeeMee_display",
        ],
    }
    return payload


def _template_rows(operational: dict[str, Any], screenshot_manifest: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for row in operational.get("current_summary_rows", []):
        for sample in row.get("screenshot_samples", []):
            template = _empty_tag_decision(row, sample)
            manifest_row = screenshot_manifest.get(str(sample.get("sample") or ""))
            if manifest_row:
                template["screenshot"] = {
                    "saved_path": manifest_row.get("saved_path"),
                    "image_relpath": manifest_row.get("image_relpath"),
                    "url": manifest_row.get("url"),
                    "clean_screenshot": manifest_row.get("clean_screenshot"),
                    "centered_screenshot": manifest_row.get("centered_screenshot"),
                }
            rows.append(template)
    return rows


def _aggregate(decisions: list[dict[str, Any]]) -> dict[str, Any]:
    reviewed = [row for row in decisions if row.get("review_decision") and row.get("review_decision") != "unreviewed"]
    by_decision: dict[str, int] = {}
    tag_counts: dict[str, dict[str, int]] = {}
    for row in reviewed:
        decision = str(row.get("review_decision"))
        by_decision[decision] = by_decision.get(decision, 0) + 1
        selected = row.get("selected_tags") or {}
        for family, tags in selected.items():
            family_counts = tag_counts.setdefault(str(family), {})
            for tag in tags or []:
                tag_text = str(tag)
                family_counts[tag_text] = family_counts.get(tag_text, 0) + 1
    return {
        "decision_counts": by_decision,
        "tag_counts": tag_counts,
        "reviewed_count": len(reviewed),
        "unreviewed_count": len(decisions) - len(reviewed),
    }


def _validate_decisions(decisions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    errors = []
    valid_review_decisions = {
        "unreviewed",
        "sell_now_review",
        "wait_rebound_fail",
        "avoid_after_drop",
        "avoid_support_nearby",
        "reject_shape",
    }
    for row in decisions:
        review_id = row.get("review_id")
        decision = row.get("review_decision")
        if decision not in valid_review_decisions:
            errors.append(
                {
                    "review_id": review_id,
                    "error_type": "invalid_review_decision",
                    "value": decision,
                    "allowed": sorted(valid_review_decisions),
                }
            )
        allowed = row.get("allowed_tags") or {}
        selected = row.get("selected_tags") or {}
        for family, tags in selected.items():
            allowed_family = set(allowed.get(family) or [])
            for tag in tags or []:
                if tag not in allowed_family:
                    errors.append(
                        {
                            "review_id": review_id,
                            "error_type": "invalid_selected_tag",
                            "tag_family": family,
                            "value": tag,
                            "allowed": sorted(allowed_family),
                        }
                    )
    return errors


def run(*, operational_path: Path, output_root: Path, decisions_path: Path | None, screenshot_manifest_path: Path | None) -> Path:
    operational = _read_json(operational_path)
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    screenshot_manifest = _screenshot_index(screenshot_manifest_path)
    template_rows = _template_rows(operational, screenshot_manifest)
    if decisions_path is None:
        decisions = template_rows
        source_decisions = "template_unreviewed"
    else:
        decisions = _read_jsonl(decisions_path)
        source_decisions = str(decisions_path)
    validation_errors = _validate_decisions(decisions)
    aggregate = _aggregate(decisions)
    _write_jsonl(output_dir / "visual_review_template.jsonl", template_rows)
    _write_jsonl(output_dir / "visual_review_decisions.jsonl", decisions)
    report = {
        "schema_version": f"{AXIS_ID}_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "source_operational": str(operational_path),
        "source_decisions": source_decisions,
        "source_screenshot_manifest": str(screenshot_manifest_path) if screenshot_manifest_path else None,
        "screenshot_batch_command": operational.get("screenshot_batch_command"),
        "template_row_count": len(template_rows),
        "decision_row_count": len(decisions),
        "aggregate": aggregate,
        "validation": {
            "error_count": len(validation_errors),
            "errors": validation_errors,
            "decision": "pass" if not validation_errors else "fail_invalid_manual_visual_review_tags",
        },
        "screenshot_join": {
            "manifest_rows": len(screenshot_manifest),
            "template_rows_with_screenshot": sum(1 for row in template_rows if row.get("screenshot")),
        },
        "artifacts": {
            "visual_review_template_jsonl": str(output_dir / "visual_review_template.jsonl"),
            "visual_review_decisions_jsonl": str(output_dir / "visual_review_decisions.jsonl"),
        },
        "decision": {
            "candidate_local_decision": "visual_review_template_ready" if template_rows else "no_visual_review_rows",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "manual visual review rows generated for short candidates without mutating MeeMee or runtime DB",
        },
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
    }
    _write_json(output_dir / "short_visual_review_pack.json", report)
    _write_json(output_root / "latest_short_visual_review_pack.json", {"run_root": str(output_dir), **report})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--operational", type=Path, default=DEFAULT_OPERATIONAL)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--decisions", type=Path, default=None)
    parser.add_argument("--screenshot-manifest", type=Path, default=None)
    args = parser.parse_args()
    print(
        run(
            operational_path=args.operational,
            output_root=args.output_root,
            decisions_path=args.decisions,
            screenshot_manifest_path=args.screenshot_manifest,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
