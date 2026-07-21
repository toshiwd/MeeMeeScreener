from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path


AXIS_ID = "tradex_severe_loss_current_regime_rollup_v1"
DEFAULT_SOURCE = Path(r"G:\Tradex\point_in_time_severe_loss_classifier_top3_v1\20260713T062128Z-tradex_point_in_time_severe_loss_classifier_top3_v1")
DEFAULT_OUT = Path(r"G:\Tradex\severe_loss_current_regime_rollup_v1")
REQUIRED_FILES = (
    "compare.json",
    "baseline_fixed_interleave_top3.csv",
    "challenger_severe_loss_top3.csv",
    "candidate_scores.csv",
    "frozen_model.json",
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def current_regime_gates(source: dict) -> dict[str, bool]:
    baseline = source["baseline_fixed_interleave"]["shadow"]
    challenger = source["challenger_severe_loss_classifier"]["shadow"]
    branch = source["branching"]["summary"]["shadow"]
    return {
        "daily_pf_ge_1_30": challenger["daily_profit_factor"] >= 1.30,
        "daily_pf_delta_ge_0_10": challenger["daily_profit_factor"] - baseline["daily_profit_factor"] >= 0.10,
        "calendar_expectancy_improves": challenger["calendar_expectancy"] > baseline["calendar_expectancy"],
        "frequency_ge_one_signal_day_week": challenger["signals_per_week"] >= 1.0,
        "cvar_non_degrade": challenger["cvar10"] >= baseline["cvar10"] - 1e-12,
        "drawdown_non_degrade": challenger["max_drawdown_equal_weight"] >= baseline["max_drawdown_equal_weight"] - 1e-12,
        "branch_ge_20pct": branch["changed_day_rate"] >= 0.20,
    }


def generate(source_dir: Path, out_root: Path) -> Path:
    missing = [name for name in REQUIRED_FILES if not (source_dir / name).is_file()]
    if missing:
        raise ValueError(f"SOURCE_ARTIFACT_MISSING: {missing}")
    compare_path = source_dir / "compare.json"
    source = json.loads(compare_path.read_text(encoding="utf-8"))
    if source.get("decision", {}).get("candidate_local_decision") != "hold":
        raise ValueError("ROBUSTNESS_DECISION_NOT_HOLD")
    if source.get("fixed_evaluation_conditions", {}).get("train") != "2024 only":
        raise ValueError("MODEL_TRAIN_PERIOD_NOT_FROZEN_2024")
    if source.get("fixed_evaluation_conditions", {}).get("splits", {}).get("shadow") != "2026 untouched":
        raise ValueError("SHADOW_NOT_DECLARED_UNTOUCHED")
    if source.get("shadow_tuning_used") is not False or source.get("threshold_search_used") is not False:
        raise ValueError("MODEL_OR_THRESHOLD_TUNING_DETECTED")
    frozen_file = json.loads((source_dir / "frozen_model.json").read_text(encoding="utf-8"))
    if frozen_file != source.get("frozen_model"):
        raise ValueError("FROZEN_MODEL_EMBEDDED_DOCUMENT_MISMATCH")

    gates = current_regime_gates(source)
    display_decision = "current_regime_display_only_keep" if all(gates.values()) else "hold"
    shadow = {
        "baseline": source["baseline_fixed_interleave"]["shadow"],
        "challenger": source["challenger_severe_loss_classifier"]["shadow"],
        "branching": source["branching"]["summary"]["shadow"],
    }
    payload = {
        "schema_version": f"{AXIS_ID}.rollup.v1",
        "artifact_role": "authoritative",
        "axis_id": AXIS_ID,
        "research_phase": "effectiveness_judgment",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_artifacts": [
            {"path": str(source_dir / name), "sha256": sha256(source_dir / name)} for name in REQUIRED_FILES
        ],
        "fixed_evaluation_conditions": {
            "comparison": "existing baseline and challenger ledgers; no recomputation or new tuning",
            "model": "frozen DecisionTreeClassifier trained on 2024 only",
            "validation_robustness": "2025 decision remains hold",
            "current_regime_forward_shadow": "2026 untouched",
            "execution_and_candidate_generation": "unchanged from source artifact",
        },
        "model_freeze_evidence": {
            "train_contract": source["fixed_evaluation_conditions"]["train"],
            "shadow_contract": source["fixed_evaluation_conditions"]["splits"]["shadow"],
            "shadow_tuning_used": source["shadow_tuning_used"],
            "threshold_search_used": source["threshold_search_used"],
            "frozen_model_file_matches_embedded_model": True,
            "model_parameters": source["frozen_model"]["parameters"],
            "features": source["frozen_model"]["features"],
            "statement": "2026 outcomes were not used to select features, fit the model, or choose a threshold.",
        },
        "robustness_2025": {
            "decision": "hold",
            "source_validation_keep_gates": source["validation_keep_gates"],
            "baseline": source["baseline_fixed_interleave"]["validation"],
            "challenger": source["challenger_severe_loss_classifier"]["validation"],
        },
        "current_regime_forward_shadow_2026": {**shadow, "gates": gates},
        "decision": {
            "candidate_local_decision": "hold",
            "session_aggregate_decision": "hold_robustness_keep_current_regime_display_only" if display_decision == "current_regime_display_only_keep" else "hold",
            "authoritative_rollup_decision": display_decision,
            "reason_type": "frozen_2024_model_2026_forward_shadow_display_scope_audit",
        },
        "meemee_reflection": {
            "allowed": display_decision == "current_regime_display_only_keep",
            "allowed_scope": ["direction display", "priority display", "reason display", "avoid-reason display"],
            "forbidden_scope": ["production ranking mutation", "automatic trading", "holding decisions", "capital allocation"],
            "implementation_in_this_task": False,
        },
        "new_logic_added": False,
        "shadow_tuning_used": False,
        "runtime_db_write": False,
        "production_ranking_changed": False,
        "meemee_changed": False,
        "silent_fallback_used": False,
    }
    root = out_root / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    root.mkdir(parents=True, exist_ok=False)
    path = root / "session_leaderboard_rollup.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()
    print(generate(args.source, args.out))


if __name__ == "__main__":
    main()
