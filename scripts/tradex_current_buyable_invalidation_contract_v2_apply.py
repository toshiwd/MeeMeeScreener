from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "current_buyable_invalidation_contract_v2_apply"
CONTRACT_VERSION = "current_buyable_invalidation_contract_v2_stop_atr2"
DEFAULT_REPAIR_ROOT = Path(
    r"G:\Tradex\current_buyable_invalidation_contract_repair_v1\20260526T013222Z-current-buyable-invalidation-contract-repair-v1"
)
DEFAULT_VARIANT_ROOT = Path(
    r"G:\Tradex\current_buyable_invalidation_variant_compare_v1\20260526T014644Z-current-buyable-invalidation-variant-compare-v1"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\current_buyable_invalidation_contract_v2_apply")
REQUIRED_ARTIFACTS = (
    "invalidation_contract_v2_summary.json",
    "invalidation_contract_v2_rows.csv",
    "invalidation_contract_v2_definition.json",
    "no_lookahead_audit.json",
    "source_coverage.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "item"):
        return _json_ready(value.item())
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_inputs(repair_root: Path, variant_root: Path) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
    rows_path = repair_root / "invalidation_contract_repair_rows.csv"
    if not rows_path.exists():
        raise FileNotFoundError(rows_path)
    rows = pd.read_csv(rows_path, dtype={"code": str})
    for col in ["entry_reference_close", "atr14"]:
        rows[col] = pd.to_numeric(rows[col], errors="coerce")
    return rows, _load_json(repair_root / "research_decision.json"), _load_json(variant_root / "research_decision.json")


def build_v2_rows(rows: pd.DataFrame) -> pd.DataFrame:
    out = rows.copy()
    out["primary_invalidation_level"] = out["entry_reference_close"] - out["atr14"] * 2.0
    out["invalidation_reason"] = "stop_atr2"
    out["contract_version"] = CONTRACT_VERSION
    return out


def no_lookahead_audit(rows: pd.DataFrame, repair_decision: dict[str, Any], variant_decision: dict[str, Any]) -> dict[str, Any]:
    passed = (
        repair_decision.get("research_decision") == "invalidation_contract_repaired_full_levels_ready"
        and variant_decision.get("research_decision") == "invalidation_contract_variant_ready_for_forward_tracking"
        and bool(rows[["entry_reference_close", "atr14", "primary_invalidation_level"]].notna().all().all())
    )
    return {
        "audit_result": "pass" if passed else "blocked",
        "no_lookahead_pass": bool(passed),
        "stop_level_uses_asof_entry_and_atr14_only": True,
        "candidate_selection_changed": False,
        "future_outcomes_used_to_set_level": False,
        "runtime_db_write": False,
        "research_fallback_used": False,
    }


def run(
    repair_root: Path = DEFAULT_REPAIR_ROOT,
    variant_root: Path = DEFAULT_VARIANT_ROOT,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
) -> Path:
    rows, repair_decision, variant_decision = load_inputs(repair_root, variant_root)
    v2 = build_v2_rows(rows)
    audit = no_lookahead_audit(v2, repair_decision, variant_decision)
    decision = "invalidation_contract_v2_stop_atr2_ready_for_forward_tracking" if audit["no_lookahead_pass"] else "blocked_no_lookahead_violation"
    decision_class = "KEEP" if audit["no_lookahead_pass"] else "BLOCKED"
    reasons = ["stop_atr2_applied_to_current_frozen_candidates"] if audit["no_lookahead_pass"] else ["stop_atr2_contract_application_failed"]

    out = output_root / f"{_now_tag()}-current-buyable-invalidation-contract-v2-apply"
    out.mkdir(parents=True, exist_ok=True)
    v2.to_csv(out / "invalidation_contract_v2_rows.csv", index=False)
    _write_json(
        out / "invalidation_contract_v2_summary.json",
        {
            "axis_id": AXIS_ID,
            "decision": decision,
            "decision_class": decision_class,
            "reason_typed": reasons,
            "candidate_count": int(len(v2)),
            "selected_codes": v2["code"].astype(str).tolist(),
            "contract_version": CONTRACT_VERSION,
            "validated_buy_count": 0,
            "production_ready": False,
        },
    )
    _write_json(
        out / "invalidation_contract_v2_definition.json",
        {
            "axis_id": AXIS_ID,
            "contract_version": CONTRACT_VERSION,
            "rule": "primary_invalidation_level = entry_reference_close - 2 * atr14",
            "source_repair_root": str(repair_root),
            "source_variant_compare_root": str(variant_root),
            "candidate_selection_changed": False,
            "future_outcomes_used_to_set_level": False,
        },
    )
    _write_json(out / "no_lookahead_audit.json", audit)
    _write_json(
        out / "source_coverage.json",
        {
            "axis_id": AXIS_ID,
            "repair_root": str(repair_root),
            "variant_root": str(variant_root),
            "candidate_count": int(len(v2)),
            "atr14_complete": bool(v2["atr14"].notna().all()) if not v2.empty else False,
            "research_fallback_used": False,
        },
    )
    _write_json(
        out / "research_decision.json",
        {
            "axis_id": AXIS_ID,
            "research_decision": decision,
            "decision_class": decision_class,
            "reason_typed": reasons,
            "research_watch_only": True,
            "production_ready": False,
            "meemee_reflectable_candidate": False,
            "runtime_db_write": False,
            "production_ranking_changed": False,
            "production_candidate_generator_changed": False,
            "publish_allowed": False,
            "validated_buy_count": 0,
            "active_gate_created": False,
            "research_fallback_used": False,
        },
    )
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repair-root", type=Path, default=DEFAULT_REPAIR_ROOT)
    parser.add_argument("--variant-root", type=Path, default=DEFAULT_VARIANT_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.repair_root, args.variant_root, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
