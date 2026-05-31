from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))


DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\starter_entry_actionability_meemee_reflection_bundle_v1")
REQUIRED_ARTIFACTS = (
    "reflection_bundle_summary.json",
    "meemee_reflection_contract.json",
    "actionability_top_candidates.csv",
    "actionability_reason_rows.csv",
    "actionability_risk_flags.csv",
    "comparison_to_baseline.json",
    "reflection_readiness_decision.json",
    "_ARTIFACT_COMPLETE.json",
)


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def run(actionability_root: Path, output_root: Path) -> Path:
    out = output_root / f"{_now_tag()}-starter-entry-actionability-meemee-reflection-bundle-v1"
    out.mkdir(parents=True, exist_ok=True)
    decision = json.loads((actionability_root / "research_decision.json").read_text(encoding="utf-8"))
    audit = json.loads((actionability_root / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    rows_path = actionability_root / "candidate_actionability_rows.csv"
    rows = pd.read_csv(rows_path) if rows_path.exists() else pd.DataFrame()
    reflectable = decision.get("research_decision") == "keep_for_formal_challenger_compare" and audit.get("audit_result") == "pass" and not rows.empty
    blockers = []
    if decision.get("research_decision") != "keep_for_formal_challenger_compare":
        blockers.append("actionability model decision is not keep_for_formal_challenger_compare")
    if audit.get("audit_result") != "pass":
        blockers.append("no-lookahead audit did not pass")
    if rows.empty:
        blockers.append("candidate actionability rows are unavailable")
    if reflectable:
        top = rows[rows["actionability_rank"] <= 20].copy()
        top.to_csv(out / "actionability_top_candidates.csv", index=False)
        reason_cols = ["code", "decision_date", "baseline_rank", "actionability_rank", "actionability_reason_components_json", "model_version", "training_period"]
        top[[c for c in reason_cols if c in top]].to_csv(out / "actionability_reason_rows.csv", index=False)
        risk_cols = ["code", "decision_date", "starter_bad", "immediate_adverse_entry", "selected_loser", "mae20", "mfe20"]
        top[[c for c in risk_cols if c in top]].to_csv(out / "actionability_risk_flags.csv", index=False)
    else:
        pd.DataFrame(columns=["code", "decision_date", "baseline_rank", "actionability_rank"]).to_csv(out / "actionability_top_candidates.csv", index=False)
        pd.DataFrame(columns=["code", "decision_date", "actionability_reason_components_json"]).to_csv(out / "actionability_reason_rows.csv", index=False)
        pd.DataFrame(columns=["code", "decision_date", "starter_bad", "immediate_adverse_entry"]).to_csv(out / "actionability_risk_flags.csv", index=False)
    comparison = json.loads((actionability_root / "topk_actionability_comparison_summary.json").read_text(encoding="utf-8"))
    _write_json(out / "reflection_bundle_summary.json", {"actionability_root": str(actionability_root), "rows": int(len(rows)), "meemee_reflectable_candidate": reflectable})
    _write_json(
        out / "meemee_reflection_contract.json",
        {
            "read_only_bundle": True,
            "runtime_db_write": False,
            "ui_change": False,
            "required_fields": [
                "code",
                "decision_date",
                "baseline_rank",
                "baseline_score",
                "actionability_rank",
                "starter_entry_actionability_score",
                "starter_entry_probability",
                "actionability_reason_components_json",
                "model_version",
                "training_period",
            ],
        },
    )
    _write_json(out / "comparison_to_baseline.json", comparison)
    _write_json(out / "reflection_readiness_decision.json", {"meemee_reflectable_candidate": reflectable, "blocker_reasons": blockers})
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Create read-only MeeMee reflection candidate bundle for starter-entry actionability")
    parser.add_argument("actionability_root", type=Path)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.actionability_root, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
