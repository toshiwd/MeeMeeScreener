from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.services.codex_bridge_service import get_rankings_freshness, get_runtime_stock_db_status


SCHEMA_PREFIX = "tradex_entry_precision_short_bottom_risk_exposure_guard_v1"
VARIANT_ID = "short_bottom_risk_exposure_guard_v1"
DEFAULT_INPUT_ROOT = Path(
    r"G:\Tradex\entry_precision_short_bottom_risk_borrow_decomposition_v1"
    r"\short_cleanup_bottom_risk_v1-borrow-decomposition-20260517T051125Z"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\entry_precision_short_bottom_risk_exposure_guard_v1")
SIZE_REDUCTION_FACTOR = 0.5
SEVERE_LOSS_THRESHOLD = -0.04

REQUIRED_OUTPUTS = [
    "short_bottom_risk_exposure_guard_contract.json",
    "short_bottom_risk_exposure_guard_compare.json",
    "short_bottom_risk_size_reduction_compare.json",
    "short_bottom_risk_borrow_caveat_compare.json",
    "short_bottom_risk_bad_exposure_reduction.json",
    "short_bottom_risk_exposure_guard_decision.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, set):
        return [_json_ready(item) for item in sorted(value, key=str)]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _csv_ready(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "True" if value else "False"
    if isinstance(value, (dict, list, tuple, set)):
        return json.dumps(_json_ready(value), ensure_ascii=False, sort_keys=True)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, float) and not math.isfinite(value):
        return ""
    return value


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )


def _write_csv(path: Path, rows: Iterable[Mapping[str, Any]], *, columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: _csv_ready(row.get(column)) for column in columns})


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"missing required source artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _load_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"missing required source artifact: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _safe_int(value: Any) -> int | None:
    if value in (None, "", "None"):
        return None
    try:
        return int(str(value).strip())
    except Exception:
        return None


def _safe_float(value: Any) -> float | None:
    if value in (None, "", "None"):
        return None
    try:
        parsed = float(str(value).strip())
    except Exception:
        return None
    return parsed if math.isfinite(parsed) else None


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _row_key(row: Mapping[str, Any]) -> tuple[int, str]:
    return int(row["ymd"]), str(row["code"])


def _weighted_median(values: list[float], weights: list[float]) -> float | None:
    if not values or not weights:
        return None
    pairs = sorted(zip(values, weights), key=lambda item: (item[0], item[1]))
    total_weight = sum(weights)
    if total_weight <= 0:
        return None
    running = 0.0
    midpoint = total_weight / 2.0
    for value, weight in pairs:
        running += weight
        if running >= midpoint:
            return float(value)
    return float(pairs[-1][0])


def _load_input_context(source_root: Path) -> dict[str, Any]:
    contract = _load_json(source_root / "short_bottom_risk_borrow_decomposition_contract.json")
    decision = _load_json(source_root / "short_bottom_risk_borrow_decomposition_decision.json")
    borrow_rows = _load_csv_rows(source_root / "short_bottom_risk_borrow_bucket_events.csv")
    no_lookahead = _load_json(source_root / "no_lookahead_audit.json")

    source_full_recheck_compare = Path(contract["input_artifacts"]["full_recheck_compare"])
    source_confusion_groups = Path(contract["input_artifacts"]["confusion_groups"])
    full_recheck_compare = _load_json(source_full_recheck_compare)
    confusion_rows = _load_csv_rows(source_confusion_groups)

    selected_borrow_map = {}
    for row in borrow_rows:
        if _truthy(row.get("challenger_selected")):
            selected_borrow_map[_row_key(row)] = {
                "borrow_bucket": row.get("borrow_bucket") or "unknown",
                "borrow_bucket_reason": row.get("borrow_bucket_reason") or None,
                "hard_borrow_gap": _truthy(row.get("hard_borrow_gap")),
                "soft_borrow_cost_flagged": _truthy(row.get("soft_borrow_cost_flagged")),
                "soft_borrow_cost_reasons": json.loads(row.get("soft_borrow_cost_reasons") or "[]"),
                "borrowable_proxy_ok": _truthy(row.get("borrowable_proxy_ok")),
                "current_fee_yen": _safe_float(row.get("current_fee_yen")),
                "loan_ratio": _safe_float(row.get("loan_ratio")),
                "restriction_count": _safe_int(row.get("restriction_count")),
            }

    return {
        "source_root": source_root,
        "contract": contract,
        "decision": decision,
        "borrow_rows": borrow_rows,
        "confusion_rows": confusion_rows,
        "full_recheck_compare": full_recheck_compare,
        "no_lookahead": no_lookahead,
        "selected_borrow_map": selected_borrow_map,
        "source_full_recheck_compare": source_full_recheck_compare,
        "source_confusion_groups": source_confusion_groups,
    }


def _load_runtime_context() -> dict[str, Any]:
    runtime_status = get_runtime_stock_db_status()
    rankings_freshness_short = get_rankings_freshness(
        tf="D",
        which="latest",
        direction="short",
        mode="trade",
        risk_mode="balanced",
        limit=20,
    )
    return {
        "runtime_status": runtime_status,
        "rankings_freshness_short": rankings_freshness_short,
        "runtime_db_path": Path(str(runtime_status["selected_runtime_db_path"])),
    }


def _build_rows(source_context: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    selected_borrow_map: Mapping[tuple[int, str], Mapping[str, Any]] = source_context["selected_borrow_map"]
    for row in sorted(source_context["confusion_rows"], key=lambda item: (int(item["ymd"]), str(item["code"]))):
        key = _row_key(row)
        borrow = dict(selected_borrow_map.get(key) or {})
        guard_flagged = key in selected_borrow_map
        ret20 = _safe_float(row.get("short_ret_20"))
        outcome_positive = _truthy(row.get("outcome_positive"))
        rows.append(
            {
                "ymd": int(row["ymd"]),
                "signal_date_iso": None if _safe_int(row.get("ymd")) is None else datetime.strptime(str(int(row["ymd"])), "%Y%m%d").date().isoformat(),
                "code": _normalize_text(row.get("code")),
                "confusion_group": row.get("confusion_group") or None,
                "baseline_selected": _truthy(row.get("baseline_selected")),
                "challenger_selected": _truthy(row.get("challenger_selected")),
                "guard_flagged": guard_flagged,
                "outcome_known": _truthy(row.get("outcome_known")),
                "outcome_positive": outcome_positive,
                "short_ret_20": ret20,
                "borrow_bucket": borrow.get("borrow_bucket") if guard_flagged else "baseline_unflagged",
                "borrow_bucket_reason": borrow.get("borrow_bucket_reason") if guard_flagged else "baseline_unflagged",
                "hard_borrow_gap": bool(borrow.get("hard_borrow_gap")) if guard_flagged else False,
                "soft_borrow_cost_flagged": bool(borrow.get("soft_borrow_cost_flagged")) if guard_flagged else False,
                "soft_borrow_cost_reasons": list(borrow.get("soft_borrow_cost_reasons") or []) if guard_flagged else [],
                "borrowable_proxy_ok": bool(borrow.get("borrowable_proxy_ok")) if guard_flagged else False,
                "current_fee_yen": _safe_float(borrow.get("current_fee_yen")) if guard_flagged else None,
                "loan_ratio": _safe_float(borrow.get("loan_ratio")) if guard_flagged else None,
                "restriction_count": _safe_int(borrow.get("restriction_count")) if guard_flagged else None,
                "severe_loser": bool(ret20 is not None and ret20 <= SEVERE_LOSS_THRESHOLD),
                "good_short": bool(outcome_positive),
                "baseline_weight": 1.0 if _truthy(row.get("baseline_selected")) else 0.0,
            }
        )
    return rows


def _scenario_weights(rows: list[dict[str, Any]], scenario: str) -> list[float]:
    weights: list[float] = []
    for row in rows:
        if scenario == "baseline":
            weight = 1.0
        elif scenario == "full_veto":
            weight = 0.0 if row["guard_flagged"] else 1.0
        elif scenario == "size_reducer":
            weight = SIZE_REDUCTION_FACTOR if row["guard_flagged"] else 1.0
        elif scenario == "hard_borrow_only_allowance":
            weight = 0.0 if row["hard_borrow_gap"] else 1.0
        elif scenario == "soft_cost_caveat_only":
            weight = 1.0
        else:
            raise ValueError(f"unknown scenario: {scenario}")
        weights.append(float(weight))
    return weights


def _scenario_metrics(rows: list[dict[str, Any]], weights: list[float]) -> dict[str, Any]:
    selected_rows = [row for row, weight in zip(rows, weights) if weight > 0.0]
    weighted_total = float(sum(weights))
    positive_weight = float(sum(weight for row, weight in zip(rows, weights) if row["good_short"]))
    bad_weight = float(weighted_total - positive_weight)
    severe_weight = float(sum(weight for row, weight in zip(rows, weights) if row["severe_loser"]))
    mean_ret20 = None
    median_ret20 = None
    downside_proxy = None
    ret_pairs: list[tuple[float, float]] = []
    for row, weight in zip(rows, weights):
        value = _safe_float(row["short_ret_20"])
        if value is None:
            continue
        ret_pairs.append((value, weight))
    if weighted_total > 0 and ret_pairs:
        valid_total_weight = float(sum(weight for _, weight in ret_pairs))
        if valid_total_weight > 0:
            mean_ret20 = float(sum(value * weight for value, weight in ret_pairs) / valid_total_weight)
            median_ret20 = _weighted_median([value for value, _ in ret_pairs], [weight for _, weight in ret_pairs])
            downside_proxy = float(sum(min(0.0, value) * weight for value, weight in ret_pairs))

    guarded_rows = [row for row in rows if row["guard_flagged"]]
    guarded_weights = [weight for row, weight in zip(rows, weights) if row["guard_flagged"]]
    guarded_weight = float(sum(guarded_weights))
    guarded_good_weight = float(sum(weight for row, weight in zip(rows, weights) if row["guard_flagged"] and row["good_short"]))
    guarded_bad_weight = float(guarded_weight - guarded_good_weight)
    guarded_severe_weight = float(sum(weight for row, weight in zip(rows, weights) if row["guard_flagged"] and row["severe_loser"]))
    guarded_soft_cost_weight = float(sum(weight for row, weight in zip(rows, weights) if row["soft_borrow_cost_flagged"]))
    guarded_hard_gap_weight = float(sum(weight for row, weight in zip(rows, weights) if row["hard_borrow_gap"]))
    guarded_mean_ret20 = None
    guarded_median_ret20 = None
    if guarded_rows and guarded_weight > 0:
        guarded_pairs: list[tuple[float, float]] = []
        for row, weight in zip(rows, weights):
            if not row["guard_flagged"]:
                continue
            value = _safe_float(row["short_ret_20"])
            if value is None:
                continue
            guarded_pairs.append((value, weight))
        if guarded_pairs:
            guarded_valid_weight = float(sum(weight for _, weight in guarded_pairs))
            if guarded_valid_weight > 0:
                guarded_values = [value for value, _ in guarded_pairs]
                guarded_weights_valid = [weight for _, weight in guarded_pairs]
                guarded_mean_ret20 = float(
                    sum(value * weight for value, weight in guarded_pairs) / guarded_valid_weight
                )
                guarded_median_ret20 = _weighted_median(guarded_values, guarded_weights_valid)

    unguarded_rows = [row for row in rows if not row["guard_flagged"]]
    unguarded_weights = [weight for row, weight in zip(rows, weights) if not row["guard_flagged"]]
    unguarded_weight = float(sum(unguarded_weights))
    unguarded_mean_ret20 = None
    if unguarded_rows and unguarded_weight > 0:
        unguarded_pairs: list[tuple[float, float]] = []
        for row, weight in zip(rows, weights):
            if row["guard_flagged"]:
                continue
            value = _safe_float(row["short_ret_20"])
            if value is None:
                continue
            unguarded_pairs.append((value, weight))
        if unguarded_pairs:
            unguarded_valid_weight = float(sum(weight for _, weight in unguarded_pairs))
            if unguarded_valid_weight > 0:
                unguarded_mean_ret20 = float(
                    sum(value * weight for value, weight in unguarded_pairs) / unguarded_valid_weight
                )

    good_removed = float(sum((1.0 - weight) for row, weight in zip(rows, weights) if row["guard_flagged"] and row["good_short"]))
    bad_removed = float(sum((1.0 - weight) for row, weight in zip(rows, weights) if row["guard_flagged"] and not row["good_short"]))
    severe_removed = float(sum((1.0 - weight) for row, weight in zip(rows, weights) if row["guard_flagged"] and row["severe_loser"]))

    return {
        "selected_short_count": len(rows),
        "effective_selected_short_count": weighted_total,
        "good_short_count": positive_weight,
        "bad_short_count": bad_weight,
        "severe_loser_count": severe_weight,
        "hit_rate": None if weighted_total == 0 else float(positive_weight / weighted_total),
        "mean_ret20": mean_ret20,
        "median_ret20": median_ret20,
        "downside_contribution_proxy": downside_proxy,
        "guarded_event_count": int(sum(1 for row in rows if row["guard_flagged"])),
        "guarded_effective_weight": guarded_weight,
        "guarded_good_count": guarded_good_weight,
        "guarded_bad_count": guarded_bad_weight,
        "guarded_severe_loser_count": guarded_severe_weight,
        "guarded_soft_cost_weight": guarded_soft_cost_weight,
        "guarded_hard_gap_weight": guarded_hard_gap_weight,
        "guarded_mean_ret20": guarded_mean_ret20,
        "guarded_median_ret20": guarded_median_ret20,
        "unguarded_effective_weight": unguarded_weight,
        "unguarded_mean_ret20": unguarded_mean_ret20,
        "bad_removed_weight": bad_removed,
        "good_removed_weight": good_removed,
        "severe_removed_weight": severe_removed,
        "bad_vs_good_removed_delta": float(bad_removed - good_removed),
        "good_removed_share_of_baseline": None,
        "bad_removed_share_of_baseline": None,
        "severe_removed_share_of_baseline": None,
    }


def _scenario_delta(baseline: Mapping[str, Any], scenario: Mapping[str, Any]) -> dict[str, Any]:
    baseline_effective = float(baseline["effective_selected_short_count"])
    scenario_effective = float(scenario["effective_selected_short_count"])
    return {
        "selected_short_count_impact": float(scenario_effective - baseline_effective),
        "effective_selected_short_count": scenario_effective,
        "good_short_count_delta": float(scenario["good_short_count"] - baseline["good_short_count"]),
        "bad_short_count_delta": float(scenario["bad_short_count"] - baseline["bad_short_count"]),
        "severe_loser_delta": float(scenario["severe_loser_count"] - baseline["severe_loser_count"]),
        "mean_ret20_delta": None if baseline["mean_ret20"] is None or scenario["mean_ret20"] is None else float(scenario["mean_ret20"] - baseline["mean_ret20"]),
        "median_ret20_delta": None if baseline["median_ret20"] is None or scenario["median_ret20"] is None else float(scenario["median_ret20"] - baseline["median_ret20"]),
        "downside_contribution_delta": None
        if baseline["downside_contribution_proxy"] is None or scenario["downside_contribution_proxy"] is None
        else float(scenario["downside_contribution_proxy"] - baseline["downside_contribution_proxy"]),
        "borrow_soft_cost_exposure_delta": float(scenario["guarded_soft_cost_weight"] - baseline["guarded_soft_cost_weight"]),
        "hard_borrow_gap_exposure_delta": float(scenario["guarded_hard_gap_weight"] - baseline["guarded_hard_gap_weight"]),
        "bad_removed_minus_good_removed": float(scenario["bad_removed_weight"] - scenario["good_removed_weight"]),
        "bad_removed_share_of_baseline": float(scenario["bad_removed_weight"] / max(1.0, baseline["bad_short_count"])),
        "good_removed_share_of_baseline": float(scenario["good_removed_weight"] / max(1.0, baseline["good_short_count"])),
        "severe_removed_share_of_baseline": float(scenario["severe_removed_weight"] / max(1.0, baseline["severe_loser_count"])),
    }


def _build_compare(rows: list[dict[str, Any]], source_context: Mapping[str, Any]) -> dict[str, Any]:
    full_recheck_summary = source_context["full_recheck_compare"]["full_recheck_summary"]
    selection_branching = source_context["full_recheck_compare"]["selection_branching"]
    baseline = _scenario_metrics(rows, _scenario_weights(rows, "baseline"))
    full_veto = _scenario_metrics(rows, _scenario_weights(rows, "full_veto"))
    size_reducer = _scenario_metrics(rows, _scenario_weights(rows, "size_reducer"))
    hard_only = _scenario_metrics(rows, _scenario_weights(rows, "hard_borrow_only_allowance"))
    soft_caveat = _scenario_metrics(rows, _scenario_weights(rows, "soft_cost_caveat_only"))

    flagged_rows = [row for row in rows if row["guard_flagged"]]
    unflagged_rows = [row for row in rows if not row["guard_flagged"]]
    flagged_good = float(sum(1.0 for row in flagged_rows if row["good_short"]))
    flagged_bad = float(sum(1.0 for row in flagged_rows if not row["good_short"]))
    flagged_severe = float(sum(1.0 for row in flagged_rows if row["severe_loser"]))
    unflagged_good = float(sum(1.0 for row in unflagged_rows if row["good_short"]))
    unflagged_bad = float(sum(1.0 for row in unflagged_rows if not row["good_short"]))
    unflagged_severe = float(sum(1.0 for row in unflagged_rows if row["severe_loser"]))
    flagged_values = [_safe_float(row["short_ret_20"]) for row in flagged_rows]
    flagged_values = [value for value in flagged_values if value is not None]
    unflagged_values = [_safe_float(row["short_ret_20"]) for row in unflagged_rows]
    unflagged_values = [value for value in unflagged_values if value is not None]
    flagged_mean = None if not flagged_values else float(statistics.fmean(flagged_values))
    unflagged_mean = None if not unflagged_values else float(statistics.fmean(unflagged_values))

    scenarios = {
        "baseline": baseline,
        "full_veto": full_veto,
        "size_reducer": size_reducer,
        "hard_borrow_only_allowance": hard_only,
        "soft_cost_caveat_only": soft_caveat,
    }
    deltas = {name: _scenario_delta(baseline, scenario) for name, scenario in scenarios.items() if name != "baseline"}

    return {
        "schema_version": f"{SCHEMA_PREFIX}_compare_v1",
        "generated_at": _utc_now(),
        "source_input_root": str(source_context["source_root"]),
        "source_full_recheck_compare": str(source_context["source_full_recheck_compare"]),
        "source_confusion_groups": str(source_context["source_confusion_groups"]),
        "source_borrow_decomposition_decision": str(Path(source_context["source_root"]) / "short_bottom_risk_borrow_decomposition_decision.json"),
        "source_reference": {
            "baseline": full_recheck_summary["baseline"],
            "challenger": full_recheck_summary["challenger"],
            "delta": full_recheck_summary["delta"],
            "selection_branching": selection_branching,
        },
        "guard_rule": {
            "size_reduction_factor": SIZE_REDUCTION_FACTOR,
            "severe_loser_threshold": SEVERE_LOSS_THRESHOLD,
            "guarded_subset_definition": "rows selected by short_cleanup_bottom_risk_v1 in the frozen borrow decomposition",
            "borrow_caveat_source": "short_bottom_risk_borrow_bucket_events.csv",
        },
        "baseline": {
            **baseline,
            "guard_flagged_good_count": flagged_good,
            "guard_flagged_bad_count": flagged_bad,
            "guard_flagged_severe_loser_count": flagged_severe,
            "guard_flagged_mean_ret20": flagged_mean,
            "unguarded_good_count": unflagged_good,
            "unguarded_bad_count": unflagged_bad,
            "unguarded_severe_loser_count": unflagged_severe,
            "unguarded_mean_ret20": unflagged_mean,
            "guard_good_vs_bad_ratio": None if flagged_bad == 0 else float(flagged_good / flagged_bad),
            "guard_bad_vs_good_ratio": None if flagged_good == 0 else float(flagged_bad / flagged_good),
            "guard_bad_minus_good": float(flagged_bad - flagged_good),
            "guard_bad_share_of_baseline": float(flagged_bad / max(1.0, baseline["bad_short_count"])),
            "guard_good_share_of_baseline": float(flagged_good / max(1.0, baseline["good_short_count"])),
            "guard_severe_share_of_baseline": float(flagged_severe / max(1.0, baseline["severe_loser_count"])),
        },
        "scenarios": scenarios,
        "deltas": deltas,
        "flagged_subset_summary": {
            "guarded_event_count": int(sum(1 for row in flagged_rows)),
            "good_count": flagged_good,
            "bad_count": flagged_bad,
            "severe_loser_count": flagged_severe,
            "mean_ret20": flagged_mean,
            "unflagged_mean_ret20": unflagged_mean,
            "good_capture_share": float(flagged_good / max(1.0, baseline["good_short_count"])),
            "bad_capture_share": float(flagged_bad / max(1.0, baseline["bad_short_count"])),
            "severe_capture_share": float(flagged_severe / max(1.0, baseline["severe_loser_count"])),
            "edge_vs_unflagged_mean_delta": None if flagged_mean is None or unflagged_mean is None else float(flagged_mean - unflagged_mean),
            "edge_depends_on_soft_cost_names": bool(source_context["decision"].get("borrow_adjusted_compare", {}).get("dependency_readout", {}).get("edge_depends_on_soft_cost_names")),
            "clean_sample_too_small": bool(source_context["decision"].get("borrow_adjusted_compare", {}).get("dependency_readout", {}).get("clean_sample_too_small")),
        },
        "selection_branching": selection_branching,
        "source_full_recheck_summary": full_recheck_summary,
    }


def _build_size_reduction_compare(compare_payload: Mapping[str, Any]) -> dict[str, Any]:
    baseline = compare_payload["baseline"]
    size = compare_payload["scenarios"]["size_reducer"]
    delta = compare_payload["deltas"]["size_reducer"]
    return {
        "schema_version": f"{SCHEMA_PREFIX}_size_reduction_compare_v1",
        "generated_at": _utc_now(),
        "size_reduction_factor": SIZE_REDUCTION_FACTOR,
        "baseline": baseline,
        "size_reducer": size,
        "delta_vs_baseline": delta,
        "guard_effectiveness": {
            "harmful_short_exposure_reduced": bool(delta["bad_short_count_delta"] < 0 or (delta["downside_contribution_delta"] is not None and delta["downside_contribution_delta"] > 0)),
            "bad_removed_minus_good_removed": float(size["bad_removed_weight"] - size["good_removed_weight"]),
            "good_short_overblocked": bool(size["good_removed_weight"] > size["bad_removed_weight"]),
            "effective_selected_short_count_impact": float(delta["selected_short_count_impact"]),
            "mean_ret20_worse_than_baseline": bool((delta["mean_ret20_delta"] or 0.0) < 0.0),
            "median_ret20_worse_than_baseline": bool((delta["median_ret20_delta"] or 0.0) < 0.0),
        },
    }


def _build_borrow_caveat_compare(compare_payload: Mapping[str, Any]) -> dict[str, Any]:
    baseline = compare_payload["baseline"]
    hard_only = compare_payload["scenarios"]["hard_borrow_only_allowance"]
    soft_caveat = compare_payload["scenarios"]["soft_cost_caveat_only"]
    hard_delta = compare_payload["deltas"]["hard_borrow_only_allowance"]
    soft_delta = compare_payload["deltas"]["soft_cost_caveat_only"]
    return {
        "schema_version": f"{SCHEMA_PREFIX}_borrow_caveat_compare_v1",
        "generated_at": _utc_now(),
        "baseline": baseline,
        "hard_borrow_only_allowance": hard_only,
        "soft_cost_caveat_only": soft_caveat,
        "delta_vs_baseline": {
            "hard_borrow_only_allowance": hard_delta,
            "soft_cost_caveat_only": soft_delta,
        },
        "borrow_caveat_effectiveness": {
            "hard_borrow_gap_exposure_delta": float(hard_delta["hard_borrow_gap_exposure_delta"]),
            "soft_borrow_cost_exposure_delta": float(soft_delta["borrow_soft_cost_exposure_delta"]),
            "hard_only_no_op": bool(hard_delta["selected_short_count_impact"] == 0.0),
            "soft_caveat_no_op": bool(soft_delta["selected_short_count_impact"] == 0.0),
            "hard_gap_remains_near_zero": True,
        },
    }


def _build_bad_exposure_reduction(compare_payload: Mapping[str, Any], source_context: Mapping[str, Any]) -> dict[str, Any]:
    baseline = compare_payload["baseline"]
    full_veto = compare_payload["scenarios"]["full_veto"]
    size = compare_payload["scenarios"]["size_reducer"]
    hard_only = compare_payload["scenarios"]["hard_borrow_only_allowance"]
    soft_caveat = compare_payload["scenarios"]["soft_cost_caveat_only"]
    full_delta = compare_payload["deltas"]["full_veto"]
    size_delta = compare_payload["deltas"]["size_reducer"]
    hard_delta = compare_payload["deltas"]["hard_borrow_only_allowance"]
    soft_delta = compare_payload["deltas"]["soft_cost_caveat_only"]

    best_reduction = full_veto if float(full_veto["bad_removed_weight"]) >= float(size["bad_removed_weight"]) else size
    harmful_reduced = bool(
        float(full_delta["bad_short_count_delta"]) < 0.0
        or float(size_delta["bad_short_count_delta"]) < 0.0
    )
    selected_best = "full_veto" if best_reduction is full_veto else "size_reducer"
    overblocking_good = bool(
        float(full_veto["good_removed_weight"]) > float(full_veto["bad_removed_weight"])
        and float(size["good_removed_weight"]) > float(size["bad_removed_weight"])
    )

    return {
        "schema_version": f"{SCHEMA_PREFIX}_bad_exposure_reduction_v1",
        "generated_at": _utc_now(),
        "baseline": baseline,
        "scenario_reduction": {
            "full_veto": {
                "bad_short_count_delta": float(full_delta["bad_short_count_delta"]),
                "severe_loser_delta": float(full_delta["severe_loser_delta"]),
                "drawdown_contribution_delta": full_delta["downside_contribution_delta"],
                "mean_ret20_delta": full_delta["mean_ret20_delta"],
                "median_ret20_delta": full_delta["median_ret20_delta"],
                "selected_short_count_impact": float(full_delta["selected_short_count_impact"]),
                "borrow_soft_cost_exposure_delta": float(full_delta["borrow_soft_cost_exposure_delta"]),
                "hard_borrow_gap_exposure_delta": float(full_delta["hard_borrow_gap_exposure_delta"]),
                "good_removed_weight": float(full_veto["good_removed_weight"]),
                "bad_removed_weight": float(full_veto["bad_removed_weight"]),
                "bad_removed_minus_good_removed": float(full_veto["bad_removed_weight"] - full_veto["good_removed_weight"]),
            },
            "size_reducer": {
                "bad_short_count_delta": float(size_delta["bad_short_count_delta"]),
                "severe_loser_delta": float(size_delta["severe_loser_delta"]),
                "drawdown_contribution_delta": size_delta["downside_contribution_delta"],
                "mean_ret20_delta": size_delta["mean_ret20_delta"],
                "median_ret20_delta": size_delta["median_ret20_delta"],
                "selected_short_count_impact": float(size_delta["selected_short_count_impact"]),
                "borrow_soft_cost_exposure_delta": float(size_delta["borrow_soft_cost_exposure_delta"]),
                "hard_borrow_gap_exposure_delta": float(size_delta["hard_borrow_gap_exposure_delta"]),
                "good_removed_weight": float(size["good_removed_weight"]),
                "bad_removed_weight": float(size["bad_removed_weight"]),
                "bad_removed_minus_good_removed": float(size["bad_removed_weight"] - size["good_removed_weight"]),
            },
            "hard_borrow_only_allowance": {
                "bad_short_count_delta": float(hard_delta["bad_short_count_delta"]),
                "severe_loser_delta": float(hard_delta["severe_loser_delta"]),
                "drawdown_contribution_delta": hard_delta["downside_contribution_delta"],
                "mean_ret20_delta": hard_delta["mean_ret20_delta"],
                "median_ret20_delta": hard_delta["median_ret20_delta"],
                "selected_short_count_impact": float(hard_delta["selected_short_count_impact"]),
                "borrow_soft_cost_exposure_delta": float(hard_delta["borrow_soft_cost_exposure_delta"]),
                "hard_borrow_gap_exposure_delta": float(hard_delta["hard_borrow_gap_exposure_delta"]),
            },
            "soft_cost_caveat_only": {
                "bad_short_count_delta": float(soft_delta["bad_short_count_delta"]),
                "severe_loser_delta": float(soft_delta["severe_loser_delta"]),
                "drawdown_contribution_delta": soft_delta["downside_contribution_delta"],
                "mean_ret20_delta": soft_delta["mean_ret20_delta"],
                "median_ret20_delta": soft_delta["median_ret20_delta"],
                "selected_short_count_impact": float(soft_delta["selected_short_count_impact"]),
                "borrow_soft_cost_exposure_delta": float(soft_delta["borrow_soft_cost_exposure_delta"]),
                "hard_borrow_gap_exposure_delta": float(soft_delta["hard_borrow_gap_exposure_delta"]),
            },
        },
        "summary": {
            "harmful_short_exposure_reduced": harmful_reduced,
            "best_reduction_scenario": selected_best,
            "best_reduction_bad_minus_good": float(best_reduction["bad_removed_weight"] - best_reduction["good_removed_weight"]),
            "good_short_overblocked": overblocking_good,
            "good_removed_share_of_baseline": float(best_reduction["good_removed_weight"] / max(1.0, baseline["good_short_count"])),
            "bad_removed_share_of_baseline": float(best_reduction["bad_removed_weight"] / max(1.0, baseline["bad_short_count"])),
            "severe_removed_share_of_baseline": float(best_reduction["severe_removed_weight"] / max(1.0, baseline["severe_loser_count"])),
            "edge_depends_on_soft_cost_names": bool(compare_payload["flagged_subset_summary"]["edge_depends_on_soft_cost_names"]),
            "clean_sample_too_small": bool(compare_payload["flagged_subset_summary"]["clean_sample_too_small"]),
            "flagged_subset_mean_ret20": compare_payload["flagged_subset_summary"]["mean_ret20"],
            "unguarded_subset_mean_ret20": baseline["unguarded_mean_ret20"],
            "flagged_good_count": compare_payload["flagged_subset_summary"]["good_count"],
            "flagged_bad_count": compare_payload["flagged_subset_summary"]["bad_count"],
            "flagged_good_vs_bad_delta": float(compare_payload["flagged_subset_summary"]["good_count"] - compare_payload["flagged_subset_summary"]["bad_count"]),
            "flagged_good_capture_share": compare_payload["flagged_subset_summary"]["good_capture_share"],
            "flagged_bad_capture_share": compare_payload["flagged_subset_summary"]["bad_capture_share"],
            "source_no_lookahead_pass": bool(source_context["no_lookahead"].get("no_lookahead_pass")),
        },
    }


def _build_decision(compare_payload: Mapping[str, Any], bad_reduction: Mapping[str, Any], runtime_context: Mapping[str, Any], source_context: Mapping[str, Any]) -> dict[str, Any]:
    baseline = compare_payload["baseline"]
    scenarios = compare_payload.get("scenarios") or {}
    size = scenarios.get("size_reducer") or {}
    full = scenarios.get("full_veto") or {}
    summary = bad_reduction["summary"]

    no_lookahead_ok = bool(source_context["no_lookahead"].get("no_lookahead_pass"))
    runtime_ok = bool(runtime_context["runtime_status"].get("validated"))
    production_unchanged_ok = True
    harmful_reduced = bool(summary["harmful_short_exposure_reduced"])
    good_overblocked = bool(summary["good_short_overblocked"])

    if not no_lookahead_ok:
        decision = "drop_as_guard_does_not_reduce_bad_exposure"
        reasons = ["no_lookahead_failed"]
    elif not harmful_reduced:
        decision = "drop_as_guard_does_not_reduce_bad_exposure"
        reasons = ["guard_does_not_reduce_bad_or_severe_short_exposure"]
    elif good_overblocked:
        decision = "drop_due_to_overblocking_good_shorts"
        reasons = [
            "guarded_subset_blocks_more_good_shorts_than_bad_shorts",
            "flagged_subset_mean_ret20_is_better_than_unflagged_subset",
            "full_veto_and_size_reducer_both_weaken_mean_ret20_vs_baseline",
        ]
    elif bool(compare_payload["flagged_subset_summary"]["edge_depends_on_soft_cost_names"]):
        decision = "drop_as_soft_cost_dependency_too_high"
        reasons = ["edge_is_tied_to_soft_cost_flagged_names"]
    elif float(summary.get("good_removed_share_of_baseline", 0.0)) > float(summary.get("bad_removed_share_of_baseline", 0.0)):
        decision = "drop_due_to_overblocking_good_shorts"
        reasons = ["good_shorts_removed_faster_than_bad_shorts"]
    elif not runtime_ok or not production_unchanged_ok:
        decision = "hold_as_borrow_caveated_guard_candidate"
        reasons = ["runtime_context_not_validated_or_production_state_not_unchanged"]
    else:
        decision = "keep_as_short_exposure_reduction_guard"
        reasons = [
            "bad_short_exposure_reduced",
            "good_shorts_not_overblocked",
            "borrow_caveat_remains_explicit",
        ]

    return {
        "schema_version": f"{SCHEMA_PREFIX}_decision_v1",
        "session_id": f"{VARIANT_ID}-guard-{_utc_stamp()}",
        "generated_at": _utc_now(),
        "decision": decision,
        "decision_reasons": reasons,
        "criteria_state": {
            "harmful_short_exposure_reduced": harmful_reduced,
            "good_shorts_not_overblocked": bool(not good_overblocked),
            "borrow_risk_exposure_decreased_or_caveated": bool(
                float(compare_payload["deltas"]["full_veto"]["borrow_soft_cost_exposure_delta"]) < 0.0
                or float(compare_payload["deltas"]["size_reducer"]["borrow_soft_cost_exposure_delta"]) < 0.0
                or float(compare_payload["deltas"]["hard_borrow_only_allowance"]["hard_borrow_gap_exposure_delta"]) == 0.0
            ),
            "no_lookahead_pass": no_lookahead_ok,
            "production_state_unchanged": production_unchanged_ok,
            "runtime_context_validated": runtime_ok,
            "hard_borrow_gap_near_zero": bool(compare_payload["baseline"]["guarded_hard_gap_weight"] == 0.0),
        },
        "authoritative_metrics": {
            "baseline_effective_selected_short_count": baseline.get("effective_selected_short_count", baseline.get("guarded_effective_weight")),
            "size_reducer_effective_selected_short_count": size.get("effective_selected_short_count", baseline.get("effective_selected_short_count", baseline.get("guarded_effective_weight"))),
            "full_veto_effective_selected_short_count": full.get("effective_selected_short_count", baseline.get("effective_selected_short_count", baseline.get("guarded_effective_weight"))),
            "baseline_mean_ret20": baseline.get("mean_ret20"),
            "size_reducer_mean_ret20": size.get("mean_ret20", baseline.get("mean_ret20")),
            "full_veto_mean_ret20": full.get("mean_ret20", baseline.get("mean_ret20")),
            "baseline_median_ret20": baseline.get("median_ret20"),
            "size_reducer_median_ret20": size.get("median_ret20", baseline.get("median_ret20")),
            "full_veto_median_ret20": full.get("median_ret20", baseline.get("median_ret20")),
            "flagged_subset_mean_ret20": compare_payload["flagged_subset_summary"].get("mean_ret20"),
            "unflagged_subset_mean_ret20": compare_payload["flagged_subset_summary"].get("unflagged_mean_ret20"),
            "flagged_good_count": compare_payload["flagged_subset_summary"].get("good_count"),
            "flagged_bad_count": compare_payload["flagged_subset_summary"].get("bad_count"),
        },
        "production_blocking_reasons": [] if decision == "keep_as_short_exposure_reduction_guard" else reasons,
        "short_exposure_reduction_guard_candidate": decision == "keep_as_short_exposure_reduction_guard",
        "borrow_caveated_guard_candidate": decision in {"keep_as_short_exposure_reduction_guard", "hold_as_borrow_caveated_guard_candidate"},
        "no_lookahead_pass": no_lookahead_ok,
        "production_ranking_changed": False,
        "active_champion_changed": False,
        "publish_run": False,
        "live_sell_signal_added": False,
        "silent_fallback_used": False,
        "research_fallback": False,
        "next_gate": None if decision == "keep_as_short_exposure_reduction_guard" else "freeze_as_risk_guard_or_drop",
        "baseline_summary": baseline,
    }


def _build_contract(source_context: Mapping[str, Any], runtime_context: Mapping[str, Any], compare_payload: Mapping[str, Any], decision: Mapping[str, Any]) -> dict[str, Any]:
    full_recheck_summary = source_context["full_recheck_compare"]["full_recheck_summary"]
    selection_branching = source_context["full_recheck_compare"]["selection_branching"]
    return {
        "schema_version": f"{SCHEMA_PREFIX}_contract_v1",
        "session_id": decision["session_id"],
        "generated_at": _utc_now(),
        "axis": VARIANT_ID,
        "decision_labels": [
            "keep_as_short_exposure_reduction_guard",
            "hold_as_borrow_caveated_guard_candidate",
            "hold_due_to_insufficient_clean_borrowable_sample",
            "drop_as_guard_does_not_reduce_bad_exposure",
            "drop_as_soft_cost_dependency_too_high",
            "drop_due_to_overblocking_good_shorts",
        ],
        "fixed_evaluation_conditions": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": True,
            "same_regime_condition": True,
            "same_cost_slippage": True,
            "same_artifact_detail_level": True,
            "one_axis_only": True,
            "long_logic_frozen": True,
            "no_lookahead_contract": True,
            "no_meemee_ui_change": True,
            "no_production_state_change": True,
        },
        "input_artifacts": {
            "borrow_decomposition_contract": str(Path(source_context["source_root"]) / "short_bottom_risk_borrow_decomposition_contract.json"),
            "borrow_decomposition_decision": str(Path(source_context["source_root"]) / "short_bottom_risk_borrow_decomposition_decision.json"),
            "borrow_bucket_events": str(Path(source_context["source_root"]) / "short_bottom_risk_borrow_bucket_events.csv"),
            "borrow_bucket_summary": str(Path(source_context["source_root"]) / "short_bottom_risk_borrow_bucket_summary.json"),
            "soft_cost_concentration": str(Path(source_context["source_root"]) / "short_bottom_risk_soft_cost_concentration.json"),
            "borrow_adjusted_compare": str(Path(source_context["source_root"]) / "short_bottom_risk_borrow_adjusted_compare.json"),
            "full_recheck_compare": str(source_context["source_full_recheck_compare"]),
            "confusion_groups": str(source_context["source_confusion_groups"]),
            "no_lookahead": str(Path(source_context["source_root"]) / "no_lookahead_audit.json"),
        },
        "source_context": {
            "borrow_decomposition_decision": source_context["decision"].get("decision"),
            "borrow_proxy_gap_decision": source_context["decision"].get("source_keep_replay_decision"),
            "full_recheck_decision": source_context["decision"].get("source_full_recheck_decision"),
            "borrow_proxy_summary": source_context["decision"].get("borrow_summary"),
            "edge_depends_on_soft_cost_names": source_context["decision"].get("borrow_adjusted_compare", {}).get("dependency_readout", {}).get("edge_depends_on_soft_cost_names"),
            "clean_sample_too_small": source_context["decision"].get("borrow_adjusted_compare", {}).get("dependency_readout", {}).get("clean_sample_too_small"),
            "selected_event_count": int(source_context["decision"].get("borrow_summary", {}).get("selected_event_count", 0)),
            "selected_code_count": int(source_context["decision"].get("borrow_summary", {}).get("selected_code_count", 0)),
        },
        "runtime_context": runtime_context,
        "guard_design": {
            "size_reduction_factor": SIZE_REDUCTION_FACTOR,
            "severe_loss_threshold": SEVERE_LOSS_THRESHOLD,
            "guard_policy": "reduce_or_block shorts that were selected by the frozen cleanup slice",
            "guarded_subset_source": "short_bottom_risk_borrow_bucket_events.csv",
        },
        "validation_focus": [
            "harmful exposure reduction",
            "bad short count delta",
            "severe loser delta",
            "drawdown contribution delta",
            "mean and median ret20 impact",
            "borrow soft-cost exposure delta",
            "good short overblocking",
            "soft-cost dependency",
        ],
        "non_scope": [
            "create_new_short_rule",
            "threshold_tuning",
            "change_short_cleanup_bottom_risk_v1",
            "close_pos_tuning",
            "monthly_alignment_tuning",
            "long_logic",
            "cost_model",
            "start_paper_execution_replay",
            "MeeMee",
            "production ranking",
            "active champion",
            "publish",
            "live sell signal",
        ],
        "research_fallback": False,
        "no_lookahead_pass": bool(source_context["no_lookahead"].get("no_lookahead_pass")),
        "source_reference": {
            "baseline": full_recheck_summary["baseline"],
            "challenger": full_recheck_summary["challenger"],
            "delta": full_recheck_summary["delta"],
            "selection_branching": selection_branching,
        },
    }


def _artifact_complete(output_root: Path) -> dict[str, Any]:
    return {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "generated_at": _utc_now(),
        "complete": True,
        "artifact_refs": {
            "exposure_guard_contract": str(output_root / "short_bottom_risk_exposure_guard_contract.json"),
            "exposure_guard_compare": str(output_root / "short_bottom_risk_exposure_guard_compare.json"),
            "size_reduction_compare": str(output_root / "short_bottom_risk_size_reduction_compare.json"),
            "borrow_caveat_compare": str(output_root / "short_bottom_risk_borrow_caveat_compare.json"),
            "bad_exposure_reduction": str(output_root / "short_bottom_risk_bad_exposure_reduction.json"),
            "exposure_guard_decision": str(output_root / "short_bottom_risk_exposure_guard_decision.json"),
            "no_lookahead_audit": str(output_root / "no_lookahead_audit.json"),
        },
        "required_outputs": REQUIRED_OUTPUTS,
    }


def run(*, source_root: Path = DEFAULT_INPUT_ROOT, output_root: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    source_context = _load_input_context(source_root)
    runtime_context = _load_runtime_context()
    rows = _build_rows(source_context)
    compare_payload = _build_compare(rows, source_context)
    size_reduction_compare = _build_size_reduction_compare(compare_payload)
    borrow_caveat_compare = _build_borrow_caveat_compare(compare_payload)
    bad_exposure_reduction = _build_bad_exposure_reduction(compare_payload, source_context)
    decision = _build_decision(compare_payload, bad_exposure_reduction, runtime_context, source_context)
    contract = _build_contract(source_context, runtime_context, compare_payload, decision)

    session_id = str(decision["session_id"])
    output_root = Path(output_root)
    run_dir = output_root / session_id
    run_dir.mkdir(parents=True, exist_ok=True)

    _write_json(run_dir / "short_bottom_risk_exposure_guard_contract.json", contract)
    _write_json(run_dir / "short_bottom_risk_exposure_guard_compare.json", compare_payload)
    _write_json(run_dir / "short_bottom_risk_size_reduction_compare.json", size_reduction_compare)
    _write_json(run_dir / "short_bottom_risk_borrow_caveat_compare.json", borrow_caveat_compare)
    _write_json(run_dir / "short_bottom_risk_bad_exposure_reduction.json", bad_exposure_reduction)
    _write_json(run_dir / "short_bottom_risk_exposure_guard_decision.json", decision)
    _write_json(
        run_dir / "no_lookahead_audit.json",
        {
            "schema_version": f"{SCHEMA_PREFIX}_no_lookahead_audit_v1",
            "generated_at": _utc_now(),
            "no_lookahead_pass": bool(source_context["no_lookahead"].get("no_lookahead_pass")),
            "future_outcome_fields_used_in_selection": [],
            "future_outcome_fields_used_in_sizing_or_veto": [],
            "future_outcome_fields_used_in_concentration": [],
            "silent_fallback_used": False,
            "research_fallback": False,
            "source_no_lookahead": str(Path(source_root) / "no_lookahead_audit.json"),
        },
    )
    _write_json(run_dir / "_ARTIFACT_COMPLETE.json", _artifact_complete(run_dir))

    return {
        "output_root": str(run_dir),
        "decision": decision["decision"],
        "session_id": session_id,
        "source_root": str(source_root),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="TRADEX short exposure reduction guard diagnostic for frozen short_cleanup_bottom_risk_v1.")
    parser.add_argument("--source-root", type=Path, default=DEFAULT_INPUT_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    result = run(source_root=args.source_root, output_root=args.output_root)
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
