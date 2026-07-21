from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def safe_ratio(numerator: int, denominator: int) -> float | None:
    return float(numerator / denominator) if denominator else None


def compare(human_path: Path, sealed_path: Path, output: Path) -> dict:
    output.mkdir(parents=True, exist_ok=False)
    human = pd.read_parquet(human_path)
    machine = pd.read_parquet(sealed_path)
    if bool(machine["outcome_joined"].any()):
        raise ValueError("sealed labels unexpectedly contain joined outcomes")
    joined = human.merge(machine, on=["case_id", "code", "ymd"], how="outer", validate="one_to_one", indicator=True)
    if len(joined) != len(human) or len(joined) != len(machine) or set(joined["_merge"]) != {"both"}:
        raise ValueError("human and sealed case sets do not match")
    joined = joined.drop(columns="_merge")
    decided = joined[joined["sideways_decision"] != "BORDERLINE"].copy()
    decided["human_sideways"] = decided["sideways_decision"].eq("SIDEWAYS")
    decided["machine_sideways"] = decided["sideways_state"].astype(bool)
    decided["agrees"] = decided["human_sideways"] == decided["machine_sideways"]
    tp = int((decided["human_sideways"] & decided["machine_sideways"]).sum())
    tn = int((~decided["human_sideways"] & ~decided["machine_sideways"]).sum())
    fp = int((~decided["human_sideways"] & decided["machine_sideways"]).sum())
    fn = int((decided["human_sideways"] & ~decided["machine_sideways"]).sum())

    by_group = []
    for group, frame in joined.groupby("sample_group", sort=True):
        by_group.append({
            "sample_group": str(group),
            "rows": int(len(frame)),
            "human_sideways": int(frame["sideways_decision"].eq("SIDEWAYS").sum()),
            "human_not_sideways": int(frame["sideways_decision"].eq("NOT_SIDEWAYS").sum()),
            "human_borderline": int(frame["sideways_decision"].eq("BORDERLINE").sum()),
        })
    by_year = []
    for year, frame in decided.groupby("year", sort=True):
        by_year.append({"year": int(year), "decided_rows": int(len(frame)), "accuracy": float(frame["agrees"].mean())})
    high = decided[decided["confidence"] == "HIGH"]
    metrics = {
        "rows": int(len(joined)),
        "decided_rows": int(len(decided)),
        "borderline_rows": int(joined["sideways_decision"].eq("BORDERLINE").sum()),
        "confusion_human_reference": {"true_positive": tp, "true_negative": tn, "false_positive": fp, "false_negative": fn},
        "accuracy": safe_ratio(tp + tn, len(decided)),
        "precision": safe_ratio(tp, tp + fp),
        "recall": safe_ratio(tp, tp + fn),
        "specificity": safe_ratio(tn, tn + fp),
        "balanced_accuracy": None,
        "high_confidence_decided_rows": int(len(high)),
        "high_confidence_accuracy": float(high["agrees"].mean()) if len(high) else None,
    }
    if metrics["recall"] is not None and metrics["specificity"] is not None:
        metrics["balanced_accuracy"] = float((metrics["recall"] + metrics["specificity"]) / 2)
    judgment = {
        "decision": "drop_current_thresholds_as_human_sideways_identifier",
        "reason": "blind human agreement is too weak and human-sideways recall is low",
        "evidence": {
            "accuracy": metrics["accuracy"],
            "recall": metrics["recall"],
            "detector_positive_human_sideways": next(x["human_sideways"] for x in by_group if x["sample_group"] == "DETECTOR_POSITIVE"),
            "detector_positive_rows": next(x["rows"] for x in by_group if x["sample_group"] == "DETECTOR_POSITIVE"),
        },
    }
    result = {
        "schema_version": "tradex_sideways_blind_reveal_compare_v1",
        "artifact_role": "authoritative_blind_human_machine_sideways_comparison",
        "review_only": True,
        "fixed_conditions": {
            "universe": "frozen_sideways120_v2",
            "human_freeze_sha256": sha256(human_path),
            "sealed_labels_sha256": sha256(sealed_path),
            "outcomes_loaded": False,
            "borderline_policy": "excluded_from_binary_metrics_and_reported_separately",
        },
        "metrics": metrics,
        "by_sample_group": by_group,
        "by_year": by_year,
        "judgment": judgment,
        "not_changed": ["sideways detector", "MeeMee", "ranking", "runtime DB", "trade rules"],
    }
    compare_path = output / "compare.json"
    compare_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    disagreement = joined[
        (joined["sideways_decision"] == "BORDERLINE")
        | ((joined["sideways_decision"] == "SIDEWAYS") != joined["sideways_state"].astype(bool))
    ].copy()
    disagreement.to_parquet(output / "disagreements.parquet", index=False)
    (output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha256(compare_path)}, indent=2) + "\n",
        encoding="utf-8",
    )
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--human", type=Path, required=True)
    parser.add_argument("--sealed", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(compare(args.human, args.sealed, args.output), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
