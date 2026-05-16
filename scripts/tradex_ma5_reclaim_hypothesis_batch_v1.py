from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.backend.services import tradex_research_contracts as contracts


AXIS_ID = "ma5_reclaim_hypothesis_batch_v1"
SOURCE_AXIS_ID = "ma5_reclaim_ma20_exit_probe_v1"
SCHEMA_PREFIX = "tradex_ma5_reclaim_hypothesis_batch_v1"
DEFAULT_SOURCE_TRADE_LEDGER = Path(
    r"G:\Tradex\ma5_reclaim_ma20_exit_probe_v1\20260512T000000Z-ma5-reclaim-ma20-exit-probe-v1-ma5_reclaim_ma20_exit_probe_v1\trade_ledger.jsonl"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\ma5_reclaim_hypothesis_batch_v1")

MIN_TRADES = 500
EXCELLENT_AVG_RET = 0.008
PROMISING_AVG_RET = 0.004
EXCELLENT_PROFIT_FACTOR = 1.35
PROMISING_PROFIT_FACTOR = 1.20
MAX_SEVERE_LOSS_RATE = 0.05

SIGNAL_FEATURE_COLUMNS = {
    "ma_stack",
    "price_vs_ma20",
    "price_vs_ma60",
    "ma20_vs_ma60",
    "ma20_slope_state",
    "ma60_slope_state",
}
LABEL_COLUMNS = {"ret", "mfe", "mae", "win", "severe_loss", "exit_reason", "exit_date"}

REQUIRED_ARTIFACTS = (
    "evaluation_contract.json",
    "run_manifest.json",
    "feature_availability_audit.json",
    "hypothesis_ledger.jsonl",
    "hypothesis_leaderboard.json",
    "condition_contrast.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)


@dataclass(frozen=True)
class HypothesisSpec:
    hypothesis_id: str
    thesis: str
    predicate: Callable[[pd.DataFrame], pd.Series]
    hypothesis_family: str = "ma_condition_filter"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    try:
        import numpy as np

        if isinstance(value, np.generic):
            return _json_ready(value.item())
    except Exception:
        pass
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _json_text(payload: Any) -> str:
    return json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, default=str)


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_json_text(payload) + "\n", encoding="utf-8")
    return path


def _write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(_json_text(row) + "\n" for row in rows), encoding="utf-8")
    return path


def _stable_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(_json_text(payload).encode("utf-8")).hexdigest()


def _safe_path(value: str | Path | None, default: Path) -> Path:
    if value is None or not str(value).strip():
        return default.resolve()
    return Path(str(value)).expanduser().resolve()


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _is_in(column: str, values: set[str]) -> Callable[[pd.DataFrame], pd.Series]:
    return lambda frame: frame[column].astype(str).isin(values)


def _is_not_in(column: str, values: set[str]) -> Callable[[pd.DataFrame], pd.Series]:
    return lambda frame: ~frame[column].astype(str).isin(values)


def _all(*predicates: Callable[[pd.DataFrame], pd.Series]) -> Callable[[pd.DataFrame], pd.Series]:
    def _predicate(frame: pd.DataFrame) -> pd.Series:
        out = pd.Series(True, index=frame.index)
        for predicate in predicates:
            out &= predicate(frame)
        return out

    return _predicate


def _any(*predicates: Callable[[pd.DataFrame], pd.Series]) -> Callable[[pd.DataFrame], pd.Series]:
    def _predicate(frame: pd.DataFrame) -> pd.Series:
        out = pd.Series(False, index=frame.index)
        for predicate in predicates:
            out |= predicate(frame)
        return out

    return _predicate


def hypothesis_specs() -> list[HypothesisSpec]:
    bull = {"bull_stack_5_20_60"}
    near_bull = {"ma5_above_20_below_60"}
    pullback = {"pullback_in_ma20_above_60"}
    bear = {"bear_stack_5_20_60"}
    return [
        HypothesisSpec("h00_base_all", "No MA-environment filter; previous MA5 reclaim / MA20 exit baseline.", lambda frame: pd.Series(True, index=frame.index), "baseline"),
        HypothesisSpec("h01_price_above_ma60", "Only take MA5 reclaim when price is above MA60.", _is_in("price_vs_ma60", {"price_above_ma60"})),
        HypothesisSpec("h02_price_below_ma60", "Only take MA5 reclaim when price is below MA60.", _is_in("price_vs_ma60", {"price_below_ma60"})),
        HypothesisSpec("h03_ma20_above_ma60", "Favor mid-term trend confirmation: MA20 above MA60.", _is_in("ma20_vs_ma60", {"ma20_above_ma60"})),
        HypothesisSpec("h04_ma20_below_ma60", "Test early recovery before MA20 crosses above MA60.", _is_in("ma20_vs_ma60", {"ma20_below_ma60"})),
        HypothesisSpec("h05_ma60_rising", "Require the long MA to be rising.", _is_in("ma60_slope_state", {"ma60_rising"})),
        HypothesisSpec("h06_ma60_not_falling", "Avoid falling MA60 but allow flat/rising.", _is_not_in("ma60_slope_state", {"ma60_falling"})),
        HypothesisSpec("h07_ma60_falling", "Stress-test falling MA60.", _is_in("ma60_slope_state", {"ma60_falling"}), "negative_control"),
        HypothesisSpec("h08_ma20_rising", "Require MA20 rising.", _is_in("ma20_slope_state", {"ma20_rising"})),
        HypothesisSpec("h09_bull_stack", "Require MA5 > MA20 > MA60.", _is_in("ma_stack", bull)),
        HypothesisSpec("h10_bull_stack_ma60_rising", "Bull stack plus rising MA60.", _all(_is_in("ma_stack", bull), _is_in("ma60_slope_state", {"ma60_rising"}))),
        HypothesisSpec("h11_bull_stack_ma60_not_falling", "Bull stack while avoiding falling MA60.", _all(_is_in("ma_stack", bull), _is_not_in("ma60_slope_state", {"ma60_falling"}))),
        HypothesisSpec("h12_near_bull_ma60_rising", "MA5 reclaimed above MA20 while MA20 is still below MA60, but MA60 rising.", _all(_is_in("ma_stack", near_bull), _is_in("ma60_slope_state", {"ma60_rising"}))),
        HypothesisSpec("h13_bull_or_near_bull_ma60_rising", "Bull or near-bull stack with MA60 rising.", _all(_is_in("ma_stack", bull | near_bull), _is_in("ma60_slope_state", {"ma60_rising"}))),
        HypothesisSpec("h14_bull_or_near_bull_ma60_not_falling", "Bull or near-bull stack while avoiding falling MA60.", _all(_is_in("ma_stack", bull | near_bull), _is_not_in("ma60_slope_state", {"ma60_falling"}))),
        HypothesisSpec("h15_pullback_price_below_ma60", "Known-risk control: pullback in MA20-above-MA60 but price below MA60.", _all(_is_in("ma_stack", pullback), _is_in("price_vs_ma60", {"price_below_ma60"})), "negative_control"),
        HypothesisSpec("h16_pullback_price_above_ma60", "Pullback in MA20-above-MA60 while price still above MA60.", _all(_is_in("ma_stack", pullback), _is_in("price_vs_ma60", {"price_above_ma60"}))),
        HypothesisSpec("h17_bear_stack_ma60_rising", "Early reversal from bear stack only when MA60 is rising.", _all(_is_in("ma_stack", bear), _is_in("ma60_slope_state", {"ma60_rising"}))),
    ]


def load_trade_ledger(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"source trade ledger not found: {path}")
    frame = pd.read_json(path, lines=True)
    required = {
        "symbol",
        "entry_date",
        "ret",
        "mfe",
        "mae",
        "win",
        "severe_loss",
        *SIGNAL_FEATURE_COLUMNS,
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"trade ledger missing required columns: {missing}")
    for column in ("ret", "mfe", "mae", "holding_days"):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    for column in ("win", "severe_loss"):
        frame[column] = frame[column].fillna(False).astype(bool)
    for column in SIGNAL_FEATURE_COLUMNS:
        frame[column] = frame[column].fillna("unknown").astype(str)
    return frame


def _profit_factor(frame: pd.DataFrame) -> float | None:
    ret = pd.to_numeric(frame["ret"], errors="coerce")
    gains = float(ret[ret > 0.0].sum())
    losses = float(ret[ret < 0.0].sum())
    if losses == 0.0:
        return None if gains == 0.0 else 999.0
    return float(gains / abs(losses))


def _metrics(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {
            "trade_count": 0,
            "symbol_count": 0,
            "avg_ret": None,
            "median_ret": None,
            "win_rate": None,
            "profit_factor": None,
            "avg_mfe": None,
            "avg_mae": None,
            "severe_loss_rate": None,
            "avg_holding_days": None,
        }
    return {
        "trade_count": int(len(frame)),
        "symbol_count": int(frame["symbol"].astype(str).nunique()),
        "avg_ret": _safe_float(frame["ret"].mean()),
        "median_ret": _safe_float(frame["ret"].median()),
        "win_rate": _safe_float(frame["win"].astype(float).mean()),
        "profit_factor": _profit_factor(frame),
        "avg_mfe": _safe_float(frame["mfe"].mean()),
        "avg_mae": _safe_float(frame["mae"].mean()),
        "severe_loss_rate": _safe_float(frame["severe_loss"].astype(float).mean()),
        "avg_holding_days": _safe_float(frame["holding_days"].mean()) if "holding_days" in frame.columns else None,
    }


def _classify(metrics: dict[str, Any]) -> str:
    count = int(metrics.get("trade_count") or 0)
    avg_ret = metrics.get("avg_ret")
    pf = metrics.get("profit_factor")
    severe = metrics.get("severe_loss_rate")
    if count < MIN_TRADES:
        return "insufficient_sample"
    if avg_ret is None or pf is None or severe is None:
        return "insufficient_metrics"
    if avg_ret >= EXCELLENT_AVG_RET and pf >= EXCELLENT_PROFIT_FACTOR and severe <= MAX_SEVERE_LOSS_RATE:
        return "excellent"
    if avg_ret >= PROMISING_AVG_RET and pf >= PROMISING_PROFIT_FACTOR and severe <= MAX_SEVERE_LOSS_RATE:
        return "promising"
    if avg_ret > 0.0 and pf >= 1.05:
        return "weak_positive"
    if avg_ret <= 0.0 or pf < 1.0:
        return "drop"
    return "mixed"


def evaluate_hypotheses(trades: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    baseline_metrics: dict[str, Any] | None = None
    for spec in hypothesis_specs():
        mask = spec.predicate(trades).fillna(False).astype(bool)
        selected = trades[mask].copy()
        metrics = _metrics(selected)
        if spec.hypothesis_id == "h00_base_all":
            baseline_metrics = metrics
        avg_ret_delta = None
        pf_delta = None
        severe_delta = None
        if baseline_metrics:
            if metrics["avg_ret"] is not None and baseline_metrics["avg_ret"] is not None:
                avg_ret_delta = float(metrics["avg_ret"] - baseline_metrics["avg_ret"])
            if metrics["profit_factor"] is not None and baseline_metrics["profit_factor"] is not None:
                pf_delta = float(metrics["profit_factor"] - baseline_metrics["profit_factor"])
            if metrics["severe_loss_rate"] is not None and baseline_metrics["severe_loss_rate"] is not None:
                severe_delta = float(metrics["severe_loss_rate"] - baseline_metrics["severe_loss_rate"])
        rows.append(
            {
                "hypothesis_id": spec.hypothesis_id,
                "hypothesis_family": spec.hypothesis_family,
                "thesis": spec.thesis,
                **metrics,
                "avg_ret_delta_vs_base": avg_ret_delta,
                "profit_factor_delta_vs_base": pf_delta,
                "severe_loss_rate_delta_vs_base": severe_delta,
                "hypothesis_decision": _classify(metrics),
                "reentry_expansion_modeled": False,
                "screening_scope": "filters previous base-rule trade ledger; skipped trades do not open replacement entries",
            }
        )
    return sorted(
        rows,
        key=lambda row: (
            {"excellent": 0, "promising": 1, "weak_positive": 2, "mixed": 3, "drop": 4, "insufficient_sample": 5}.get(
                row["hypothesis_decision"],
                9,
            ),
            -(row.get("avg_ret") or -999.0),
            -(row.get("trade_count") or 0),
        ),
    )


def build_feature_availability_audit(trades: pd.DataFrame) -> dict[str, Any]:
    feature_rows = []
    for column in sorted(SIGNAL_FEATURE_COLUMNS):
        present = column in trades.columns
        non_null = int(trades[column].notna().sum()) if present else 0
        feature_rows.append(
            {
                "column": column,
                "present": present,
                "non_null_count": non_null,
                "non_null_rate": None if len(trades) == 0 else float(non_null / len(trades)),
            }
        )
    overlap = sorted(SIGNAL_FEATURE_COLUMNS & LABEL_COLUMNS)
    return {
        "schema_version": f"{SCHEMA_PREFIX}_feature_availability_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "source_axis_id": SOURCE_AXIS_ID,
        "trade_rows": int(len(trades)),
        "feature_rows": feature_rows,
        "signal_feature_columns": sorted(SIGNAL_FEATURE_COLUMNS),
        "label_columns_excluded_from_hypothesis_filters": sorted(LABEL_COLUMNS),
        "signal_label_overlap": overlap,
        "used_future_labels_in_hypothesis_filters": bool(overlap),
        "silent_fallback_used": False,
    }


def build_evaluation_contract(source_trade_ledger: Path) -> dict[str, Any]:
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_evaluation_contract_v1",
        "axis_id": AXIS_ID,
        "source_axis_id": SOURCE_AXIS_ID,
        "boundary": "TRADEX-only",
        "research_phase": "hypothesis screening batch",
        "source_trade_ledger": str(source_trade_ledger),
        "hypothesis_count": len(hypothesis_specs()),
        "same_condition_controls": {
            "same_base_entry_rule": "MA5 reclaim confirmed for 4 bars, next open entry",
            "same_base_exit_rule": "close below MA20 or max 40 trading days",
            "same_trade_ledger": True,
            "same_cost_slippage": contracts.TRADEX_DEFAULT_COST_MODEL,
            "artifact_detail_level": contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        },
        "limitations": {
            "reentry_expansion_modeled": False,
            "notes": "This is a fast screening pass over the prior base trade ledger. Full hypothesis-specific replay is required before candidate use.",
        },
        "future_label_policy": {
            "future_labels_used_for_hypothesis_filters": False,
            "future_labels_used_for_evaluation": True,
        },
        "candidate_scoring_created": False,
        "meemee_reflection_allowed": False,
        "publish_bundle_allowed": False,
        "silent_fallback_used": False,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_leaderboard(rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts: dict[str, int] = {}
    for row in rows:
        counts[row["hypothesis_decision"]] = counts.get(row["hypothesis_decision"], 0) + 1
    return {
        "schema_version": f"{SCHEMA_PREFIX}_hypothesis_leaderboard_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "source_axis_id": SOURCE_AXIS_ID,
        "overview": {
            "hypothesis_count": len(rows),
            "decision_counts": counts,
            "excellent_count": counts.get("excellent", 0),
            "promising_count": counts.get("promising", 0),
        },
        "rows": rows,
    }


def build_condition_contrast(rows: list[dict[str, Any]]) -> dict[str, Any]:
    positive = [row for row in rows if row["hypothesis_decision"] in {"excellent", "promising", "weak_positive"}]
    negative = [row for row in rows if row["hypothesis_decision"] == "drop"]
    return {
        "schema_version": f"{SCHEMA_PREFIX}_condition_contrast_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "positive_patterns": positive[:20],
        "negative_patterns": negative[:20],
        "contrast_summary": {
            "best_positive_hypothesis": positive[0]["hypothesis_id"] if positive else None,
            "worst_negative_hypothesis": negative[0]["hypothesis_id"] if negative else None,
        },
    }


def build_research_decision(rows: list[dict[str, Any]], output_dir: Path) -> dict[str, Any]:
    excellent = [row for row in rows if row["hypothesis_decision"] == "excellent"]
    promising = [row for row in rows if row["hypothesis_decision"] == "promising"]
    if excellent:
        decision = "excellent_hypothesis_found"
    elif promising:
        decision = "promising_hypothesis_found"
    else:
        decision = "no_promising_hypothesis"
    return {
        "schema_version": f"{SCHEMA_PREFIX}_research_decision_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "artifact_root": str(output_dir),
        "decision": decision,
        "authoritative_research_decision": decision,
        "excellent_hypotheses": excellent,
        "promising_hypotheses": promising,
        "top_hypotheses": rows[:10],
        "decision_reasons": [
            {"code": "excellent_count", "value": len(excellent)},
            {"code": "promising_count", "value": len(promising)},
            {"code": "hypothesis_count", "value": len(rows)},
            {"code": "reentry_expansion_modeled", "value": False},
        ],
        "candidate_scoring_created": False,
        "meemee_reflectable": False,
        "publish_bundle_created": False,
        "silent_fallback_used": False,
    }


def _artifact_complete(output_dir: Path, paths: dict[str, str], decision: dict[str, Any]) -> dict[str, Any]:
    existing = {name: Path(path).exists() for name, path in paths.items()}
    required_existing = {name: (output_dir / name).exists() for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"}
    return {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "artifact_root": str(output_dir),
        "required_artifacts": list(REQUIRED_ARTIFACTS),
        "existing_artifacts": {**existing, **required_existing},
        "complete": all(existing.values()) and all(required_existing.values()),
        "authoritative_research_decision": decision["authoritative_research_decision"],
        "silent_fallback_used": False,
        "candidate_scoring_created": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
    }


def run_ma5_reclaim_hypothesis_batch_v1(
    *,
    source_trade_ledger: str | Path = DEFAULT_SOURCE_TRADE_LEDGER,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
) -> dict[str, Any]:
    source_path = _safe_path(source_trade_ledger, DEFAULT_SOURCE_TRADE_LEDGER)
    output_base = _safe_path(output_root, DEFAULT_OUTPUT_ROOT)
    run_name = run_id.strip() if run_id else _default_run_id()
    if not run_name.endswith(AXIS_ID):
        run_name = f"{run_name}-{AXIS_ID}"
    output_dir = output_base / run_name
    output_dir.mkdir(parents=True, exist_ok=True)

    trades = load_trade_ledger(source_path)
    rows = evaluate_hypotheses(trades)
    evaluation_contract = build_evaluation_contract(source_path)
    run_manifest = contracts.build_run_manifest(
        session_id=run_name,
        seed=0,
        random_seed=0,
        input_artifacts=[
            {"name": "source_trade_ledger", "path": str(source_path)},
            {"name": "evaluation_contract", "contract_hash": evaluation_contract["contract_hash"]},
        ],
        asof=_utc_now(),
        config={
            "axis_id": AXIS_ID,
            "source_axis_id": SOURCE_AXIS_ID,
            "hypothesis_count": len(hypothesis_specs()),
            "reentry_expansion_modeled": False,
            "candidate_scoring_created": False,
        },
        universe=sorted(trades["symbol"].astype(str).unique().tolist()),
        period={
            "source": "inherited_from_source_trade_ledger",
            "label": "screening_over_prior_ma5_reclaim_trade_ledger",
        },
        horizon="inherited_ma20_exit_or_40d",
        artifact_detail_level=contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        fallback_status=contracts.TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
        cost_model=contracts.TRADEX_DEFAULT_COST_MODEL,
    )
    contracts.validate_run_manifest(run_manifest)
    feature_audit = build_feature_availability_audit(trades)
    leaderboard = build_leaderboard(rows)
    contrast = build_condition_contrast(rows)
    decision = build_research_decision(rows, output_dir)

    paths: dict[str, str] = {}
    for name, payload in {
        "evaluation_contract.json": evaluation_contract,
        "run_manifest.json": run_manifest,
        "feature_availability_audit.json": feature_audit,
        "hypothesis_leaderboard.json": leaderboard,
        "condition_contrast.json": contrast,
        "research_decision.json": decision,
    }.items():
        paths[name] = str(_write_json(output_dir / name, payload))
    paths["hypothesis_ledger.jsonl"] = str(_write_jsonl(output_dir / "hypothesis_ledger.jsonl", rows))
    complete = _artifact_complete(output_dir, paths, decision)
    paths["_ARTIFACT_COMPLETE.json"] = str(_write_json(output_dir / "_ARTIFACT_COMPLETE.json", complete))

    return {
        "output_dir": str(output_dir),
        "paths": paths,
        "authoritative_research_decision": decision["authoritative_research_decision"],
        "top_hypotheses": decision["top_hypotheses"][:8],
        "silent_fallback_used": False,
        "candidate_scoring_created": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-trade-ledger", default=str(DEFAULT_SOURCE_TRADE_LEDGER))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)
    result = run_ma5_reclaim_hypothesis_batch_v1(
        source_trade_ledger=args.source_trade_ledger,
        output_root=args.output_root,
        run_id=args.run_id.strip() or None,
    )
    print(_json_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
