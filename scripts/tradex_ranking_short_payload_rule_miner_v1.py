from __future__ import annotations

import argparse
import itertools
import json
import math
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any

import duckdb

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path


AXIS_ID = "ranking_short_payload_rule_miner_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\ranking_short_payload_rule_miner_v1")


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


def _split(dt: int) -> str:
    if dt <= 20241230:
        return "train"
    if dt <= 20251230:
        return "validation"
    return "test"


def _num(item: dict[str, Any], key: str) -> float | None:
    value = item.get(key)
    return float(value) if isinstance(value, (int, float)) and math.isfinite(float(value)) else None


def _ge(item: dict[str, Any], key: str, threshold: float) -> bool:
    value = _num(item, key)
    return value is not None and value >= threshold


def _le(item: dict[str, Any], key: str, threshold: float) -> bool:
    value = _num(item, key)
    return value is not None and value <= threshold


def _atoms(row: dict[str, Any]) -> set[str]:
    item = row["item"]
    atoms = {
        f"setupType={row.get('setup_type') or 'missing'}",
        f"entryQualified={bool(row.get('entry_qualified'))}",
    }
    rank = int(row["rank"])
    for threshold in (3, 5, 10, 20, 30):
        if rank <= threshold:
            atoms.add(f"rank<={threshold}")
    for key in ("marketRiskOff", "marketRiskOn", "monthlyBoxWild", "shootingStarLike", "threeBlackCrows", "swingQualified"):
        value = item.get(key)
        if isinstance(value, bool):
            atoms.add(f"{key}={value}")
        elif isinstance(value, (int, float)) and float(value) >= 1.0:
            atoms.add(f"{key}=true")
    for key in ("marketRegime", "monthlyBoxState", "riskMode", "swingSide"):
        value = item.get(key)
        if value is not None:
            atoms.add(f"{key}={value}")
    numeric_thresholds = {
        "changePct": [(-0.08, "<="), (-0.05, "<="), (-0.03, "<="), (0.0, "<="), (0.03, ">=")],
        "liquidity20d": [(100000, ">="), (300000, ">="), (1000000, ">="), (3000000, ">=")],
        "candleBodyRatio": [(0.25, ">="), (0.45, ">="), (0.60, ">=")],
        "candleUpperWickRatio": [(0.35, ">="), (0.50, ">="), (0.65, ">=")],
        "candleLowerWickRatio": [(0.10, "<="), (0.20, "<="), (0.30, "<=")],
        "weeklyBreakoutDownProb": [(0.50, ">="), (0.70, ">="), (0.90, ">=")],
        "weeklyRangeProb": [(0.30, ">="), (0.50, ">="), (0.70, ">=")],
        "monthlyBreakoutDownProb": [(0.40, ">="), (0.60, ">="), (0.80, ">=")],
        "monthlyRangeProb": [(0.40, ">="), (0.60, ">="), (0.80, ">=")],
        "monthlyRangePos": [(0.30, "<="), (0.50, "<="), (0.70, "<="), (0.70, ">="), (0.85, ">=")],
        "monthlyBoxPos": [(0.30, "<="), (0.50, "<="), (0.70, "<="), (0.70, ">="), (0.85, ">=")],
        "entryScore": [(0.50, ">="), (0.65, ">="), (0.80, ">=")],
        "probSide": [(0.50, ">="), (0.70, ">="), (0.85, ">=")],
        "tradePriorityScore": [(0.50, ">="), (0.70, ">="), (0.85, ">=")],
        "tradePriorityHitScore": [(0.50, ">="), (0.70, ">="), (0.85, ">=")],
        "tradePrioritySafetyScore": [(0.50, ">="), (0.70, ">="), (0.85, ">=")],
        "marketBreadthAdvRatio": [(0.40, "<="), (0.55, "<="), (0.60, ">=")],
    }
    for key, specs in numeric_thresholds.items():
        for threshold, direction in specs:
            if direction == ">=" and _ge(item, key, threshold):
                atoms.add(f"{key}>={threshold}")
            if direction == "<=" and _le(item, key, threshold):
                atoms.add(f"{key}<={threshold}")
    return atoms


def _empty() -> dict[str, Any]:
    return {"count": 0, "codes": set(), "ret": [], "mfe": [], "mae": []}


def _add(acc: dict[str, Any], row: dict[str, Any]) -> None:
    acc["count"] += 1
    acc["codes"].add(row["code"])
    acc["ret"].append(row["return_30d"])
    acc["mfe"].append(row["max_favorable_30"])
    acc["mae"].append(row["max_adverse_30"])


def _summary(acc: dict[str, Any]) -> dict[str, Any]:
    count = int(acc["count"])
    if count == 0:
        return {"count": 0}
    ret = acc["ret"]
    mfe = acc["mfe"]
    mae = acc["mae"]
    return {
        "count": count,
        "unique_code_count": len(acc["codes"]),
        "return_30d_mean": _round(mean(ret)),
        "return_30d_median": _round(median(ret)),
        "return_30d_positive_rate": _round(sum(1 for value in ret if value > 0) / count),
        "max_favorable_30_ge_8pct_rate": _round(sum(1 for value in mfe if value >= 0.08) / count),
        "max_adverse_30_le_minus5pct_rate": _round(sum(1 for value in mae if value <= -0.05) / count),
    }


def _candidate_key(candidate: dict[str, Any]) -> tuple[float, float, float, int]:
    validation = candidate["by_split"].get("validation", {})
    train = candidate["by_split"].get("train", {})
    test = candidate["by_split"].get("test", {})
    return (
        float(validation.get("return_30d_positive_rate") or 0.0),
        float(train.get("return_30d_positive_rate") or 0.0),
        float(test.get("return_30d_positive_rate") or 0.0),
        int(validation.get("count") or 0),
    )


def _load_rows(conn: duckdb.DuckDBPyConnection, *, logic_version: str, top_k: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT dt, rank, code, name, ranking_logic_version, entry_qualified_at_appearance,
               setup_type_at_appearance, return_30d, max_favorable_30, max_adverse_30, payload_json
        FROM ranking_appearance_daily
        WHERE dir = 'down'
          AND ranking_logic_version = ?
          AND rank <= ?
          AND return_30d IS NOT NULL
          AND payload_json IS NOT NULL
          AND COALESCE(name, '') NOT ILIKE '%ETF%'
          AND COALESCE(name, '') NOT ILIKE '%ETN%'
          AND COALESCE(name, '') NOT ILIKE '%REIT%'
          AND COALESCE(name, '') NOT ILIKE '%(投)%'
          AND COALESCE(name, '') NOT ILIKE '%投資法人%'
          AND COALESCE(name, '') NOT ILIKE '%NEXT%'
        ORDER BY dt, rank, code
        """,
        [logic_version, top_k],
    ).fetchall()
    out: list[dict[str, Any]] = []
    for dt, rank, code, name, logic, entry_qualified, setup_type, ret, mfe, mae, payload_json in rows:
        payload = json.loads(payload_json)
        item = payload.get("ranking_item") or {}
        out.append({
            "dt": int(dt),
            "split": _split(int(dt)),
            "rank": int(rank),
            "code": str(code),
            "name": name,
            "ranking_logic_version": logic,
            "entry_qualified": bool(entry_qualified),
            "setup_type": setup_type,
            "return_30d": float(ret),
            "max_favorable_30": float(mfe),
            "max_adverse_30": float(mae),
            "item": item,
        })
    return out


def run(
    *,
    db_path: Path,
    output_root: Path,
    logic_version: str,
    top_k: int,
    min_train_count: int,
    min_validation_count: int,
    min_test_count: int,
    target_win_rate: float,
    min_train_win_rate: float,
    max_rule_size: int,
    max_atoms: int,
    max_active_atoms_per_row: int,
) -> Path:
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = _load_rows(conn, logic_version=logic_version, top_k=top_k)
        runtime_status = inspect_runtime_stock_db(runtime_db_path=db_path)
    finally:
        conn.close()

    single: dict[str, dict[str, dict[str, Any]]] = defaultdict(lambda: defaultdict(_empty))
    split_counts: dict[str, int] = defaultdict(int)
    for row in rows:
        atoms = _atoms(row)
        row["atoms"] = sorted(atoms)
        split_counts[row["split"]] += 1
        for atom in atoms:
            _add(single[atom][row["split"]], row)

    ranked_atoms: list[tuple[str, float, int]] = []
    for atom, by_split in single.items():
        train = _summary(by_split.get("train", _empty()))
        validation = _summary(by_split.get("validation", _empty()))
        if int(train.get("count") or 0) < min_train_count or int(validation.get("count") or 0) < min_validation_count:
            continue
        score = max(float(train.get("return_30d_positive_rate") or 0), float(validation.get("return_30d_positive_rate") or 0))
        ranked_atoms.append((atom, score, int(validation.get("count") or 0)))
    ranked_atoms.sort(key=lambda item: (item[1], item[2]), reverse=True)
    vocabulary = [atom for atom, _score, _count in ranked_atoms[:max_atoms]]
    atom_rank = {atom: index for index, atom in enumerate(vocabulary)}

    aggregate: dict[tuple[str, ...], dict[str, dict[str, Any]]] = defaultdict(lambda: defaultdict(_empty))
    for row in rows:
        active = [atom for atom in row["atoms"] if atom in atom_rank]
        active.sort(key=lambda atom: atom_rank[atom])
        active = active[:max_active_atoms_per_row]
        for size in range(1, min(max_rule_size, len(active)) + 1):
            for combo in itertools.combinations(sorted(active), size):
                _add(aggregate[combo][row["split"]], row)

    candidates: list[dict[str, Any]] = []
    for rule, by_split_acc in aggregate.items():
        by_split = {split: _summary(acc) for split, acc in sorted(by_split_acc.items())}
        if int(by_split.get("train", {}).get("count") or 0) < min_train_count:
            continue
        if int(by_split.get("validation", {}).get("count") or 0) < min_validation_count:
            continue
        test_count = int(by_split.get("test", {}).get("count") or 0)
        all_acc = _empty()
        for acc in by_split_acc.values():
            all_acc["count"] += acc["count"]
            all_acc["codes"].update(acc["codes"])
            all_acc["ret"].extend(acc["ret"])
            all_acc["mfe"].extend(acc["mfe"])
            all_acc["mae"].extend(acc["mae"])
        candidates.append({
            "rule_atoms": list(rule),
            "rule_size": len(rule),
            "by_split": by_split,
            "all": _summary(all_acc),
            "passes_target_validation": (by_split["validation"]["return_30d_positive_rate"] or 0) >= target_win_rate,
            "passes_min_train_win_rate": (by_split["train"]["return_30d_positive_rate"] or 0) >= min_train_win_rate,
            "passes_target_test": test_count >= min_test_count
            and (by_split.get("test", {}).get("return_30d_positive_rate") or 0) >= target_win_rate,
        })
    candidates.sort(key=_candidate_key, reverse=True)
    target_validation = [c for c in candidates if c["passes_target_validation"] and c["passes_min_train_win_rate"]]
    stable = [c for c in target_validation if c["passes_target_test"]]
    _write_jsonl(output_dir / "ranking_short_rule_candidates_top.jsonl", candidates[:500])
    _write_jsonl(output_dir / "ranking_short_rule_candidates_target_validation.jsonl", target_validation[:200])
    _write_jsonl(output_dir / "ranking_short_rule_candidates_target_stable.jsonl", stable[:200])
    audit = {
        "schema_version": "tradex_ranking_short_payload_rule_miner_v1_audit",
        "generated_at": _utc_now(),
        "boundary_owner": "TRADEX",
        "axis_id": AXIS_ID,
        "db_path": str(db_path),
        "runtime_stock_db_status": runtime_status,
        "source_surface": "runtime_duckdb.ranking_appearance_daily",
        "fixed_evaluation_conditions": {
            "dir": "down",
            "ranking_logic_version": logic_version,
            "top_k": top_k,
            "instrument_filter": "exclude ETF/ETN/REIT/investment-corporation/NEXT-like names",
            "target": "return_30d_positive_rate",
            "target_win_rate": target_win_rate,
            "min_train_win_rate": min_train_win_rate,
            "train": "dt <= 2024-12-30",
            "validation": "2025-01-01 <= dt <= 2025-12-30",
            "test": "dt >= 2026-01-01",
            "min_train_count": min_train_count,
            "min_validation_count": min_validation_count,
            "min_test_count": min_test_count,
            "cost_slippage": "source ranking_appearance_daily values, no added cost",
        },
        "row_count": len(rows),
        "split_counts": dict(sorted(split_counts.items())),
        "selected_atoms": vocabulary,
        "top_candidate": candidates[0] if candidates else None,
        "target_validation_candidate_count": len(target_validation),
        "stable_target_candidate_count": len(stable),
        "authoritative_rollup_decision": "keep" if stable else "hold_no_stable_70pct_rule",
        "runtime_db_write": False,
        "production_ranking_changed": False,
        "non_scope": ["MeeMee reflection", "production ranking mutation", "runtime DB write"],
    }
    _write_json(output_dir / "ranking_short_rule_miner_audit.json", audit)
    _write_json(output_root / "latest_ranking_short_rule_miner_audit.json", {"run_root": str(output_dir), **audit})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=None)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--logic-version", default="ranking:trade:top50:v1")
    parser.add_argument("--top-k", type=int, default=20)
    parser.add_argument("--min-train-count", type=int, default=80)
    parser.add_argument("--min-validation-count", type=int, default=50)
    parser.add_argument("--min-test-count", type=int, default=20)
    parser.add_argument("--target-win-rate", type=float, default=0.70)
    parser.add_argument("--min-train-win-rate", type=float, default=0.60)
    parser.add_argument("--max-rule-size", type=int, default=4)
    parser.add_argument("--max-atoms", type=int, default=45)
    parser.add_argument("--max-active-atoms-per-row", type=int, default=10)
    args = parser.parse_args()
    print(run(
        db_path=args.db_path or resolve_runtime_stock_db_path(),
        output_root=args.output_root,
        logic_version=args.logic_version,
        top_k=args.top_k,
        min_train_count=args.min_train_count,
        min_validation_count=args.min_validation_count,
        min_test_count=args.min_test_count,
        target_win_rate=args.target_win_rate,
        min_train_win_rate=args.min_train_win_rate,
        max_rule_size=args.max_rule_size,
        max_atoms=args.max_atoms,
        max_active_atoms_per_row=args.max_active_atoms_per_row,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
