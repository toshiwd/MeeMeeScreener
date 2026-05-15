from __future__ import annotations

import json
import os
from copy import deepcopy
from pathlib import Path
from typing import Any


DEFAULT_READONLY_REFLECTION_ROOT = Path(r"G:\Tradex\sell_failed_followthrough_meemee_readonly_reflection_v1")
DEFAULT_FAMILY_FREEZE_ROOT = Path(r"G:\Tradex\sell_failed_followthrough_family_freeze_v1")
OLD_LOOKAHEAD_CANDIDATE = "sell_failed_followthrough_after_break_same_month_refill_liquidity_guard_v1"
CLEAN_CANDIDATE = "sell_failed_followthrough_after_break_same_month_refill_liquidity_guard_no_lookahead_v1"
MANIFEST_NAME = "meemee_readonly_reflection_manifest.json"
DROP_MANIFEST_NAME = "meemee_drop_status_manifest.json"


class ReadonlyReflectionError(RuntimeError):
    pass


def _resolve_root(root: str | Path | None = None) -> Path:
    raw = root or os.getenv("MEEMEE_TRADEX_READONLY_REFLECTION_ROOT") or DEFAULT_READONLY_REFLECTION_ROOT
    return Path(str(raw)).expanduser().resolve(strict=False)


def _resolve_freeze_root(root: str | Path | None = None) -> Path:
    raw = root or os.getenv("MEEMEE_TRADEX_FAMILY_FREEZE_ROOT") or DEFAULT_FAMILY_FREEZE_ROOT
    return Path(str(raw)).expanduser().resolve(strict=False)


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ReadonlyReflectionError(f"missing reflection artifact: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ReadonlyReflectionError(f"invalid reflection artifact json: {path}") from exc
    if not isinstance(payload, dict):
        raise ReadonlyReflectionError(f"reflection artifact must be an object: {path}")
    return payload


def _latest_run_dir(root: Path, *, manifest_name: str = MANIFEST_NAME) -> Path:
    if root.is_file():
        return root.parent
    if (root / manifest_name).exists():
        return root
    if not root.exists():
        raise ReadonlyReflectionError(f"reflection root missing: {root}")
    candidates = [path for path in root.iterdir() if path.is_dir() and (path / manifest_name).exists()]
    if not candidates:
        raise ReadonlyReflectionError(f"no reflection manifest found under: {root}")
    candidates.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    return candidates[0]


def _latest_manifest_path(root: Path, *, manifest_name: str) -> Path:
    return _latest_run_dir(root, manifest_name=manifest_name) / manifest_name


def _load_current_manifest(root: str | Path | None = None) -> tuple[dict[str, Any], Path, str]:
    if root is not None:
        resolved = Path(str(root)).expanduser().resolve(strict=False)
        if resolved.is_file():
            manifest_name = resolved.name
            return _load_json(resolved), resolved.parent, manifest_name
        if (resolved / DROP_MANIFEST_NAME).exists():
            return _load_json(resolved / DROP_MANIFEST_NAME), resolved, DROP_MANIFEST_NAME
        return _load_json(_latest_manifest_path(resolved, manifest_name=MANIFEST_NAME)), _latest_run_dir(resolved), MANIFEST_NAME

    freeze_root = _resolve_freeze_root()
    try:
        path = _latest_manifest_path(freeze_root, manifest_name=DROP_MANIFEST_NAME)
        return _load_json(path), path.parent, DROP_MANIFEST_NAME
    except ReadonlyReflectionError:
        pass
    readonly_root = _resolve_root()
    path = _latest_manifest_path(readonly_root, manifest_name=MANIFEST_NAME)
    return _load_json(path), path.parent, MANIFEST_NAME


def _require(condition: bool, reason: str, failures: list[str]) -> None:
    if not condition:
        failures.append(reason)


def _normalize_manifest(manifest: dict[str, Any], *, run_dir: Path, manifest_name: str) -> dict[str, Any]:
    failures: list[str] = []
    candidate_name = str(manifest.get("candidate_name") or "")
    old_status = str(manifest.get("old_candidate_status") or "")
    forbidden_usage = list(manifest.get("forbidden_meemee_usage") or [])
    allowed_usage = list(manifest.get("allowed_meemee_usage") or [])
    key_metrics = manifest.get("key_metrics") if isinstance(manifest.get("key_metrics"), dict) else {}
    decision = manifest.get("decision")
    is_drop_manifest = manifest_name == DROP_MANIFEST_NAME or decision == "drop_after_multiyear_replay"

    _require(candidate_name == CLEAN_CANDIDATE, "clean_candidate_not_selected", failures)
    _require(OLD_LOOKAHEAD_CANDIDATE not in candidate_name, "old_lookahead_candidate_selected", failures)
    _require(old_status == "lookahead_contaminated_excluded", "old_candidate_not_excluded", failures)
    if is_drop_manifest:
        _require(decision == "drop_after_multiyear_replay", "decision_not_drop_after_multiyear_replay", failures)
        _require(manifest.get("family_status") == "dropped", "family_status_not_dropped", failures)
        _require(manifest.get("shadow_trade_candidate") is False, "shadow_trade_candidate_not_false", failures)
        _require(manifest.get("meemee_readonly_status") == "dropped_after_multiyear_replay", "readonly_status_not_dropped", failures)
    else:
        _require(decision == "meemee_reflectable_candidate", "decision_not_reflectable_candidate", failures)
    _require(manifest.get("display_level") == "read_only_research_candidate", "display_level_not_read_only", failures)
    _require(manifest.get("side") == "sell", "side_not_sell", failures)
    _require(manifest.get("no_lookahead_pass") is True, "no_lookahead_not_passed", failures)
    _require(manifest.get("production_ranking_changed") is False, "production_ranking_changed", failures)
    _require(manifest.get("active_ranking_changed") is False, "active_ranking_changed", failures)
    _require(manifest.get("publish_run") is False, "publish_run_not_false", failures)
    _require(any("production ranking" in str(item).lower() for item in forbidden_usage), "production_ranking_forbidden_usage_missing", failures)
    _require(any("active ranking" in str(item).lower() for item in allowed_usage + forbidden_usage), "active_ranking_label_missing", failures)

    status = "available" if not failures else "blocked"
    return {
        "candidate_name": candidate_name,
        "candidate_version": manifest.get("candidate_version"),
        "status": status,
        "decision": decision,
        "side": manifest.get("side"),
        "display_level": manifest.get("display_level"),
        "source_run_root": manifest.get("source_run_root"),
        "source_decision_artifact": manifest.get("source_decision_artifact"),
        "source_compare_artifact": manifest.get("source_compare_artifact"),
        "source_contract_artifact": manifest.get("source_contract_artifact"),
        "reflectability_decision_artifact": manifest.get("reflectability_decision_artifact"),
        "no_lookahead_pass": manifest.get("no_lookahead_pass") is True,
        "production_ranking_changed": manifest.get("production_ranking_changed") is True,
        "active_ranking_changed": manifest.get("active_ranking_changed") is True,
        "publish_run": manifest.get("publish_run") is True,
        "old_candidate_status": old_status,
        "allowed_meemee_usage": allowed_usage,
        "forbidden_meemee_usage": forbidden_usage,
        "key_metrics": deepcopy(key_metrics),
        "remaining_risks": list(manifest.get("remaining_risks") or []),
        "visible_warning": manifest.get("visible_warning")
        or (
            "Not shadow trade eligible. Not active ranking. Not a live sell signal."
            if is_drop_manifest
            else "This is not active ranking and not a live sell signal."
        ),
        "family_status": manifest.get("family_status"),
        "meemee_readonly_status": manifest.get("meemee_readonly_status"),
        "shadow_trade_candidate": manifest.get("shadow_trade_candidate"),
        "full_period_result_summary": deepcopy(manifest.get("full_period_result_summary") or {}),
        "max_drawdown_summary": deepcopy(manifest.get("max_drawdown_summary") or {}),
        "added_bad_pick_impact": deepcopy(manifest.get("added_bad_pick_impact") or {}),
        "supersedes_readonly_reflection_root": manifest.get("supersedes_readonly_reflection_root"),
        "authoritative_drop_root": manifest.get("authoritative_drop_root"),
        "supersession_reason": manifest.get("supersession_reason"),
        "drop_reason": manifest.get("drop_reason"),
        "guardrail_failures": failures,
        "manifest_path": str(run_dir / manifest_name),
    }


def load_readonly_reflection_manifest(root: str | Path | None = None) -> dict[str, Any]:
    manifest, run_dir, manifest_name = _load_current_manifest(root)
    normalized = _normalize_manifest(manifest, run_dir=run_dir, manifest_name=manifest_name)
    if normalized["guardrail_failures"]:
        raise ReadonlyReflectionError(
            "reflection manifest failed guardrails: " + ", ".join(normalized["guardrail_failures"])
        )
    return normalized


def build_readonly_reflection_snapshot(root: str | Path | None = None, *, strict: bool = False) -> dict[str, Any]:
    try:
        item = load_readonly_reflection_manifest(root)
    except ReadonlyReflectionError as exc:
        if strict:
            raise
        return {
            "available": False,
            "reason": str(exc),
            "items": [],
            "production_ranking_changed": False,
            "active_ranking_changed": False,
            "publish_run": False,
        }
    return {
        "available": True,
        "reason": None,
        "items": [item],
        "production_ranking_changed": False,
        "active_ranking_changed": False,
        "publish_run": False,
    }
