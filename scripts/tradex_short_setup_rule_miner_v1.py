from __future__ import annotations

import argparse
import itertools
import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any


AXIS_ID = "short_setup_rule_miner_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_setup_rule_miner_v1")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.write_text("".join(json.dumps(row, ensure_ascii=False, default=str) + "\n" for row in rows), encoding="utf-8")


def _round(value: float | None, digits: int = 6) -> float | None:
    return None if value is None or not math.isfinite(value) else round(value, digits)


def _split(as_of: int) -> str:
    if as_of <= 20231229:
        return "train"
    if as_of <= 20251230:
        return "validation"
    return "test"


def _ge(feature: dict[str, Any], key: str, value: float) -> bool:
    raw = feature.get(key)
    return isinstance(raw, (int, float)) and raw >= value


def _le(feature: dict[str, Any], key: str, value: float) -> bool:
    raw = feature.get(key)
    return isinstance(raw, (int, float)) and raw <= value


def _atoms(row: dict[str, Any]) -> set[str]:
    feature = row.get("feature") or {}
    atoms = {f"tag:{tag}" for tag in row.get("tags") or []}

    for key in ("bearish", "failed_high20", "near_high60", "ma_stack_bull"):
        if feature.get(key):
            atoms.add(f"{key}=true")
    for period in (7, 20, 60, 100, 200):
        if feature.get(f"below_ma{period}"):
            atoms.add(f"below_ma{period}=true")
        if feature.get(f"cross_down_ma{period}"):
            atoms.add(f"cross_down_ma{period}=true")

    for threshold in (0.75, 0.85, 0.92):
        if _ge(feature, "close_position_60", threshold):
            atoms.add(f"close_position_60>={threshold}")
    for threshold in (0.35, 0.45, 0.55):
        if _ge(feature, "upper_wick_ratio", threshold):
            atoms.add(f"upper_wick_ratio>={threshold}")
    for threshold in (0.10, 0.20, 0.30):
        if _le(feature, "lower_wick_ratio", threshold):
            atoms.add(f"lower_wick_ratio<={threshold}")
    for threshold in (0.45, 0.55, 0.65):
        if _ge(feature, "body_ratio", threshold):
            atoms.add(f"body_ratio>={threshold}")
    for threshold in (1.2, 1.5, 2.0):
        if _ge(feature, "volume_ratio20", threshold):
            atoms.add(f"volume_ratio20>={threshold}")

    for period, thresholds in {
        7: (-0.02, 0.00, 0.03, 0.08),
        20: (-0.03, 0.00, 0.05, 0.10),
        60: (-0.05, 0.00, 0.10, 0.18),
        100: (-0.05, 0.00, 0.10),
        200: (-0.05, 0.00, 0.10),
    }.items():
        for threshold in thresholds:
            if _ge(feature, f"dist_ma{period}", threshold):
                atoms.add(f"dist_ma{period}>={threshold}")
            if _le(feature, f"dist_ma{period}", threshold):
                atoms.add(f"dist_ma{period}<={threshold}")
    for period in (7, 20, 60, 100, 200):
        if _le(feature, f"ma{period}_slope5", 0.0):
            atoms.add(f"ma{period}_slope5<=0")
        if _ge(feature, f"ma{period}_slope5", 0.02):
            atoms.add(f"ma{period}_slope5>=0.02")
    return atoms


def _summarize(indices: set[int], rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not indices:
        return {"count": 0}
    ret = [float(rows[index]["ret20_short"]) for index in indices]
    mfe = [float(rows[index]["mfe20_short"]) for index in indices]
    mae = [float(rows[index]["mae20_short"]) for index in indices]
    return {
        "count": len(indices),
        "unique_code_count": len({rows[index]["code"] for index in indices}),
        "ret20_short_mean": _round(mean(ret)),
        "ret20_short_median": _round(median(ret)),
        "ret20_short_positive_rate": _round(sum(1 for value in ret if value > 0) / len(ret)),
        "mfe20_short_ge_8pct_rate": _round(sum(1 for value in mfe if value >= 0.08) / len(mfe)),
        "mae20_short_le_minus5pct_rate": _round(sum(1 for value in mae if value <= -0.05) / len(mae)),
    }


def _candidate_key(stats: dict[str, Any]) -> tuple[float, float, int]:
    validation = stats["by_split"].get("validation", {})
    train = stats["by_split"].get("train", {})
    return (
        float(validation.get("ret20_short_positive_rate") or 0.0),
        float(train.get("ret20_short_positive_rate") or 0.0),
        int(validation.get("count") or 0),
    )


def _empty_accumulator() -> dict[str, Any]:
    return {
        "count": 0,
        "codes": set(),
        "ret": [],
        "mfe": [],
        "mae": [],
    }


def _add(accumulator: dict[str, Any], row: dict[str, Any]) -> None:
    accumulator["count"] += 1
    accumulator["codes"].add(row["code"])
    accumulator["ret"].append(row["ret20_short"])
    accumulator["mfe"].append(row["mfe20_short"])
    accumulator["mae"].append(row["mae20_short"])


def _summarize_accumulator(accumulator: dict[str, Any]) -> dict[str, Any]:
    count = int(accumulator["count"])
    if count == 0:
        return {"count": 0}
    ret = accumulator["ret"]
    mfe = accumulator["mfe"]
    mae = accumulator["mae"]
    return {
        "count": count,
        "unique_code_count": len(accumulator["codes"]),
        "ret20_short_mean": _round(mean(ret)),
        "ret20_short_median": _round(median(ret)),
        "ret20_short_positive_rate": _round(sum(1 for value in ret if value > 0) / count),
        "mfe20_short_ge_8pct_rate": _round(sum(1 for value in mfe if value >= 0.08) / count),
        "mae20_short_le_minus5pct_rate": _round(sum(1 for value in mae if value <= -0.05) / count),
    }


def run(
    *,
    rows_path: Path,
    output_root: Path,
    min_train_count: int,
    min_validation_count: int,
    min_test_count: int,
    target_win_rate: float,
    min_train_win_rate: float,
    max_rule_size: int,
    beam_width: int,
    max_atoms: int,
    max_active_atoms_per_row: int,
) -> Path:
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)

    rows: list[dict[str, Any]] = []
    single_stats: dict[str, dict[str, dict[str, Any]]] = defaultdict(lambda: defaultdict(_empty_accumulator))
    split_counts: dict[str, int] = defaultdict(int)

    with rows_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            raw = json.loads(line)
            split = _split(int(raw["as_of"]))
            row = {
                "code": str(raw["code"]),
                "as_of": int(raw["as_of"]),
                "split": split,
                "tags": raw.get("tags") or [],
                "ret20_short": float(raw["ret20_short"]),
                "mfe20_short": float(raw["mfe20_short"]),
                "mae20_short": float(raw["mae20_short"]),
            }
            atoms = _atoms(raw)
            row["atoms"] = sorted(atoms)
            rows.append(row)
            split_counts[split] += 1
            for atom in atoms:
                _add(single_stats[atom][split], row)

    ranked_atoms: list[tuple[str, float, int]] = []
    for atom, by_split_acc in single_stats.items():
        train_summary = _summarize_accumulator(by_split_acc.get("train", _empty_accumulator()))
        validation_summary = _summarize_accumulator(by_split_acc.get("validation", _empty_accumulator()))
        train_count = int(train_summary.get("count") or 0)
        validation_count = int(validation_summary.get("count") or 0)
        if train_count < min_train_count or validation_count < min_validation_count:
            continue
        score = max(
            float(train_summary.get("ret20_short_positive_rate") or 0.0),
            float(validation_summary.get("ret20_short_positive_rate") or 0.0),
        )
        ranked_atoms.append((atom, score, validation_count))
    ranked_atoms.sort(key=lambda item: (item[1], item[2]), reverse=True)
    vocabulary = [atom for atom, _score, _count in ranked_atoms[:max_atoms]]
    atom_rank = {atom: index for index, atom in enumerate(vocabulary)}

    aggregate: dict[tuple[str, ...], dict[str, dict[str, Any]]] = defaultdict(lambda: defaultdict(_empty_accumulator))
    for row in rows:
        active = [atom for atom in row["atoms"] if atom in atom_rank]
        active.sort(key=lambda atom: atom_rank[atom])
        active = active[:max_active_atoms_per_row]
        for size in range(1, min(max_rule_size, len(active)) + 1):
            for combo in itertools.combinations(sorted(active), size):
                _add(aggregate[combo][row["split"]], row)

    candidates: list[dict[str, Any]] = []
    for rule, by_split_acc in aggregate.items():
        by_split = {
            split: _summarize_accumulator(accumulator)
            for split, accumulator in sorted(by_split_acc.items())
        }
        train_count = int(by_split.get("train", {}).get("count") or 0)
        validation_count = int(by_split.get("validation", {}).get("count") or 0)
        test_count = int(by_split.get("test", {}).get("count") or 0)
        if train_count < min_train_count or validation_count < min_validation_count:
            continue
        all_acc = _empty_accumulator()
        for accumulator in by_split_acc.values():
            all_acc["count"] += accumulator["count"]
            all_acc["codes"].update(accumulator["codes"])
            all_acc["ret"].extend(accumulator["ret"])
            all_acc["mfe"].extend(accumulator["mfe"])
            all_acc["mae"].extend(accumulator["mae"])
        candidates.append({
            "rule_atoms": list(rule),
            "rule_size": len(rule),
            "by_split": by_split,
            "all": _summarize_accumulator(all_acc),
            "passes_target_validation": (by_split["validation"]["ret20_short_positive_rate"] or 0.0) >= target_win_rate,
            "passes_min_train_win_rate": (by_split["train"]["ret20_short_positive_rate"] or 0.0) >= min_train_win_rate,
            "passes_target_test": test_count >= min_test_count
            and (by_split.get("test", {}).get("ret20_short_positive_rate") or 0.0) >= target_win_rate,
        })

    candidates.sort(key=_candidate_key, reverse=True)
    candidates = candidates[:beam_width]
    target_candidates = [
        candidate for candidate in candidates
        if candidate["passes_target_validation"] and candidate["passes_min_train_win_rate"]
    ]
    stable_target_candidates = [
        candidate for candidate in target_candidates
        if candidate["passes_target_test"]
    ]
    _write_jsonl(output_dir / "rule_candidates_top.jsonl", candidates[:500])
    _write_jsonl(output_dir / "rule_candidates_target_validation.jsonl", target_candidates[:200])
    _write_jsonl(output_dir / "rule_candidates_target_stable.jsonl", stable_target_candidates[:200])
    audit = {
        "schema_version": "tradex_short_setup_rule_miner_v1_audit",
        "generated_at": _utc_now(),
        "boundary_owner": "TRADEX",
        "axis_id": AXIS_ID,
        "source_rows_path": str(rows_path),
        "row_count": len(rows),
        "atom_count": len(vocabulary),
        "selected_atoms": vocabulary,
        "split_counts": dict(sorted(split_counts.items())),
        "fixed_evaluation_conditions": {
            "target": "ret20_short_positive_rate",
            "target_win_rate": target_win_rate,
            "min_train_win_rate": min_train_win_rate,
            "train": "as_of <= 2023-12-29",
            "validation": "2024-01-01 <= as_of <= 2025-12-30",
            "test": "as_of >= 2026-01-01",
            "min_train_count": min_train_count,
            "min_validation_count": min_validation_count,
            "min_test_count": min_test_count,
            "max_rule_size": max_rule_size,
            "beam_width": beam_width,
            "max_atoms": max_atoms,
            "max_active_atoms_per_row": max_active_atoms_per_row,
            "cost_slippage": "not_applied_same_as_source_backtest",
        },
        "top_candidate": candidates[0] if candidates else None,
        "target_validation_candidate_count": len(target_candidates),
        "stable_target_candidate_count": len(stable_target_candidates),
        "authoritative_rollup_decision": "keep" if stable_target_candidates else "hold_no_stable_70pct_rule",
        "runtime_db_write": False,
        "production_ranking_changed": False,
        "non_scope": ["MeeMee reflection", "production ranking mutation", "runtime DB write"],
    }
    _write_json(output_dir / "rule_miner_audit.json", audit)
    _write_json(output_root / "latest_rule_miner_audit.json", {"run_root": str(output_dir), **audit})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows-path", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--min-train-count", type=int, default=120)
    parser.add_argument("--min-validation-count", type=int, default=60)
    parser.add_argument("--min-test-count", type=int, default=20)
    parser.add_argument("--target-win-rate", type=float, default=0.70)
    parser.add_argument("--min-train-win-rate", type=float, default=0.60)
    parser.add_argument("--max-rule-size", type=int, default=4)
    parser.add_argument("--beam-width", type=int, default=500)
    parser.add_argument("--max-atoms", type=int, default=36)
    parser.add_argument("--max-active-atoms-per-row", type=int, default=10)
    args = parser.parse_args()
    print(run(
        rows_path=args.rows_path,
        output_root=args.output_root,
        min_train_count=args.min_train_count,
        min_validation_count=args.min_validation_count,
        min_test_count=args.min_test_count,
        target_win_rate=args.target_win_rate,
        min_train_win_rate=args.min_train_win_rate,
        max_rule_size=args.max_rule_size,
        beam_width=args.beam_width,
        max_atoms=args.max_atoms,
        max_active_atoms_per_row=args.max_active_atoms_per_row,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
