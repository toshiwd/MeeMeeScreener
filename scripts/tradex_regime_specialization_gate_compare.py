from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_chart_first_replay import DEFAULT_SOURCE_DB_PATH, run_chart_first_replay  # noqa: E402

DEFAULT_OUTPUT_DIR = Path(r"G:\Tradex\sample_replays\tradex_regime_specialization_gate")
DEFAULT_START_DATE = "2025-05-01"
DEFAULT_END_DATE = "2026-04-23"
DEFAULT_FREEZE_DATE = "2026-04-22"
DEFAULT_SNAPSHOT_AS_OF = 20260422

BASELINE_SYMBOLS: tuple[str, ...] = (
    "1545",
    "1655",
    "9041",
    "6367",
    "9989",
    "7593",
    "7867",
    "3481",
    "2806",
    "8957",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, Path):
        return str(value)
    return value


def _json_text(payload: Any) -> str:
    return json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, default=str)


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_json_text(payload) + "\n", encoding="utf-8")
    return path


def _slug(value: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in str(value)).strip("_").lower()


def _sym_path(symbol: str) -> str:
    return f"tradex_chart_first_{symbol}"


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    return out if math.isfinite(out) else float(default)


def _safe_bool(value: Any) -> bool:
    return bool(value) if value is not None else False


@dataclass(frozen=True)
class GateLabel:
    label: str
    reason: str


def _basis_lookup(*, source_db_path: Path, as_of: int, symbols: list[str]) -> dict[str, dict[str, Any]]:
    if not symbols:
        return {}
    placeholders = ",".join(["?"] * len(symbols))
    query = f"""
        SELECT code, dt, name, basis_version, source_as_of, basis_payload_json
        FROM signal_basis_daily
        WHERE dt <= ? AND code IN ({placeholders})
        ORDER BY code, dt DESC
    """
    out: dict[str, dict[str, Any]] = {}
    with duckdb.connect(str(source_db_path), read_only=True) as conn:
        rows = conn.execute(query, [as_of, *symbols]).fetchall()
    for code, dt, name, basis_version, source_as_of, payload in rows:
        code = str(code or "")
        if not code or code in out:
            continue
        basis = json.loads(payload) if payload else {}
        if not isinstance(basis, dict):
            basis = {}
        basis = dict(basis)
        basis["code"] = code
        basis["name"] = name
        basis["basis_version"] = basis_version
        basis["basis_dt"] = dt
        basis["source_as_of"] = source_as_of
        out[code] = basis
    return out


def _long_tradable_label(item: dict[str, Any], *, strict: bool) -> GateLabel:
    weekly_up = _safe_float(item.get("weeklyBreakoutUpProb"))
    monthly_up = _safe_float(item.get("monthlyBreakoutUpProb"))
    weekly_down = _safe_float(item.get("weeklyBreakoutDownProb"))
    monthly_down = _safe_float(item.get("monthlyBreakoutDownProb"))
    monthly_range = _safe_float(item.get("monthlyRangeProb"))
    box_state = str(item.get("monthlyBoxState") or "")
    monthly_wild = _safe_bool(item.get("monthlyBoxWild"))
    strong_long = max(weekly_up, monthly_up)
    strong_short = max(weekly_down, monthly_down)
    if box_state == "box_upper" and weekly_up >= 0.60 and monthly_up >= 0.45 and not monthly_wild:
        return GateLabel("long_tradable", "box_upper_breakout")
    if monthly_up >= (0.62 if strict else 0.60) and weekly_up >= 0.55 and monthly_down <= (0.45 if strict else 0.50) and monthly_range <= 0.65 and box_state in {"box_upper", "box_mid"} and not monthly_wild:
        return GateLabel("long_tradable", "trend_up_dominance")
    if monthly_up >= (0.62 if strict else 0.60) and weekly_up >= 0.52 and monthly_range >= (0.60 if strict else 0.60) and box_state in {"box_upper", "box_mid"} and not monthly_wild:
        return GateLabel("long_tradable", "clean_range_breakout")
    if not strict and strong_long >= 0.58 and strong_long >= strong_short - 0.05 and box_state in {"box_upper", "box_mid"} and not monthly_wild:
        return GateLabel("long_tradable", "broad_long_bias")
    return GateLabel("no_trade", "long_trend_not_strong_enough")


def _short_tradable_label(item: dict[str, Any], *, strict: bool) -> GateLabel:
    weekly_up = _safe_float(item.get("weeklyBreakoutUpProb"))
    monthly_up = _safe_float(item.get("monthlyBreakoutUpProb"))
    weekly_down = _safe_float(item.get("weeklyBreakoutDownProb"))
    monthly_down = _safe_float(item.get("monthlyBreakoutDownProb"))
    monthly_range = _safe_float(item.get("monthlyRangeProb"))
    box_state = str(item.get("monthlyBoxState") or "")
    monthly_wild = _safe_bool(item.get("monthlyBoxWild"))
    strong_short = max(weekly_down, monthly_down)
    strong_long = max(weekly_up, monthly_up)
    if strict:
        short_ok = (
            monthly_down >= 0.60
            and weekly_down >= 0.65
            and monthly_up <= 0.45
            and monthly_range <= 0.60
            and box_state in {"box_lower", "box_mid"}
            and not monthly_wild
        )
    else:
        short_ok = (
            strong_short >= 0.55
            and box_state in {"box_lower", "box_mid"}
            and not monthly_wild
            and strong_short >= strong_long - 0.05
        )
    if short_ok:
        return GateLabel("short_tradable", "downtrend_specialized" if strict else "broad_downtrend")
    return GateLabel("no_trade", "short_trend_not_clean_enough")


def _baseline_label(item: dict[str, Any]) -> GateLabel:
    long_label = _long_tradable_label(item, strict=False)
    if long_label.label == "long_tradable":
        return GateLabel("long_tradable", "baseline_broad_long_path")
    short_label = _short_tradable_label(item, strict=False)
    if short_label.label == "short_tradable":
        return GateLabel("short_tradable", "baseline_broad_short_path")
    return GateLabel("no_trade", "baseline_broad_filter")


def _specialized_label(item: dict[str, Any]) -> GateLabel:
    long_label = _long_tradable_label(item, strict=True)
    if long_label.label == "long_tradable":
        return GateLabel("long_tradable", long_label.reason)
    short_label = _short_tradable_label(item, strict=True)
    if short_label.label == "short_tradable":
        return GateLabel("short_tradable", short_label.reason)
    return GateLabel("no_trade", "regime_mismatch_or_noise")


def _symbol_record(symbol: str, *, gate: GateLabel, baseline: GateLabel, replay: dict[str, Any], snapshot: dict[str, Any]) -> dict[str, Any]:
    aggregate = replay["aggregate"]
    summary = replay["roundtrip_summary"]
    return {
        "symbol": symbol,
        "name": summary.get("name") or summary.get("symbol"),
        "snapshot": {
            "weeklyBreakoutUpProb": snapshot.get("weeklyBreakoutUpProb"),
            "monthlyBreakoutUpProb": snapshot.get("monthlyBreakoutUpProb"),
            "weeklyBreakoutDownProb": snapshot.get("weeklyBreakoutDownProb"),
            "monthlyBreakoutDownProb": snapshot.get("monthlyBreakoutDownProb"),
            "monthlyRangeProb": snapshot.get("monthlyRangeProb"),
            "monthlyBoxState": snapshot.get("monthlyBoxState"),
            "monthlyBoxPos": snapshot.get("monthlyBoxPos"),
            "monthlyBoxMonths": snapshot.get("monthlyBoxMonths"),
            "monthlyBoxRangePct": snapshot.get("monthlyBoxRangePct"),
            "monthlyBoxWild": snapshot.get("monthlyBoxWild"),
            "reclaim60": snapshot.get("reclaim60"),
            "v60Core": snapshot.get("v60Core"),
            "v60Strong": snapshot.get("v60Strong"),
            "cnt60Up": snapshot.get("cnt60Up"),
            "cnt100Up": snapshot.get("cnt100Up"),
            "marketRegime": snapshot.get("marketRegime"),
            "basis_dt": snapshot.get("basis_dt"),
            "basis_version": snapshot.get("basis_version"),
            "source_as_of": snapshot.get("source_as_of"),
        },
        "baseline_gate": baseline.label,
        "baseline_gate_reason": baseline.reason,
        "specialized_gate": gate.label,
        "specialized_gate_reason": gate.reason,
        "replay": {
            "roundtrip_count": aggregate.get("roundtrip_count"),
            "entry_count": aggregate.get("entry_count"),
            "exit_count": aggregate.get("exit_count"),
            "hedge_count": aggregate.get("hedge_count"),
            "stay_count": aggregate.get("stay_count"),
            "net_realized_pnl": aggregate.get("net_realized_pnl"),
            "average_capture_ratio": aggregate.get("average_capture_ratio"),
            "max_drawdown_during_holding": aggregate.get("max_drawdown_during_holding"),
            "exits_early_or_late": aggregate.get("exits_early_or_late"),
            "final_position": aggregate.get("final_position"),
        },
        "paths": replay["paths"],
    }


def _aggregate_rows(rows: list[dict[str, Any]], *, predicate: str) -> dict[str, Any]:
    selected = [row for row in rows if row[predicate] != "no_trade"]
    no_trade = [row for row in rows if row[predicate] == "no_trade"]
    return {
        "selected_count": len(selected),
        "no_trade_count": len(no_trade),
        "selected_symbols": [row["symbol"] for row in selected],
        "no_trade_symbols": [row["symbol"] for row in no_trade],
        "roundtrip_count": sum(_safe_float(row["replay"]["roundtrip_count"]) for row in selected),
        "entry_count": sum(_safe_float(row["replay"]["entry_count"]) for row in selected),
        "exit_count": sum(_safe_float(row["replay"]["exit_count"]) for row in selected),
        "hedge_count": sum(_safe_float(row["replay"]["hedge_count"]) for row in selected),
        "stay_count": sum(_safe_float(row["replay"]["stay_count"]) for row in selected),
        "net_realized_pnl": sum(_safe_float(row["replay"]["net_realized_pnl"]) for row in selected),
        "average_capture_ratio_mean": (
            sum(_safe_float(row["replay"]["average_capture_ratio"]) for row in selected) / len(selected) if selected else None
        ),
        "worst_drawdown": min((_safe_float(row["replay"]["max_drawdown_during_holding"]) for row in selected), default=None),
        "trend_up_preserved_count": sum(
            1
            for row in selected
            if _safe_float(row["snapshot"].get("monthlyBreakoutUpProb")) >= 0.55
            and str(row["snapshot"].get("monthlyBoxState") or "") in {"box_upper", "box_mid"}
        ),
        "trend_down_selected_count": sum(
            1
            for row in selected
            if _safe_float(row["snapshot"].get("monthlyBreakoutDownProb")) >= 0.60
            and str(row["snapshot"].get("monthlyBoxState") or "") in {"box_lower", "box_mid"}
        ),
    }


def _group_rows(rows: list[dict[str, Any]], *, symbol_order: list[str]) -> list[dict[str, Any]]:
    by_symbol = {row["symbol"]: row for row in rows}
    grouped: list[dict[str, Any]] = []
    for symbol in symbol_order:
        row = by_symbol[symbol]
        grouped.append(
            {
                "symbol": symbol,
                "baseline_gate": row["baseline_gate"],
                "specialized_gate": row["specialized_gate"],
                "baseline_gate_reason": row["baseline_gate_reason"],
                "specialized_gate_reason": row["specialized_gate_reason"],
                "net_realized_pnl": row["replay"]["net_realized_pnl"],
                "max_drawdown_during_holding": row["replay"]["max_drawdown_during_holding"],
                "roundtrip_count": row["replay"]["roundtrip_count"],
                "monthly_box_state": row["snapshot"].get("monthlyBoxState"),
                "weekly_breakout_up_prob": row["snapshot"].get("weeklyBreakoutUpProb"),
                "monthly_breakout_up_prob": row["snapshot"].get("monthlyBreakoutUpProb"),
                "weekly_breakout_down_prob": row["snapshot"].get("weeklyBreakoutDownProb"),
                "monthly_breakout_down_prob": row["snapshot"].get("monthlyBreakoutDownProb"),
            }
        )
    return grouped


def _symbols_with_gate(rows: list[dict[str, Any]], symbols: list[str], predicate: str) -> int:
    symbol_set = set(symbols)
    return sum(1 for row in rows if row["symbol"] in symbol_set and row[predicate] != "no_trade")


def run_gate_compare(
    *,
    source_db_path: Path = DEFAULT_SOURCE_DB_PATH,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    symbols: list[str] | None = None,
    start_date: str = DEFAULT_START_DATE,
    end_date: str = DEFAULT_END_DATE,
    freeze_date: str = DEFAULT_FREEZE_DATE,
    snapshot_as_of: int = DEFAULT_SNAPSHOT_AS_OF,
) -> dict[str, Any]:
    candidate_symbols = list(dict.fromkeys(symbols or list(BASELINE_SYMBOLS)))
    basis_lookup = _basis_lookup(source_db_path=source_db_path, as_of=snapshot_as_of, symbols=candidate_symbols)

    runs: list[dict[str, Any]] = []
    for symbol in candidate_symbols:
        replay_output_dir = output_dir / _sym_path(symbol)
        replay = run_chart_first_replay(
            source_db_path=source_db_path,
            output_dir=replay_output_dir,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            freeze_date=freeze_date,
        )
        snapshot = basis_lookup.get(symbol, {})
        baseline = _baseline_label(snapshot)
        specialized = _specialized_label(snapshot)
        runs.append(
            _symbol_record(
                symbol,
                gate=specialized,
                baseline=baseline,
                replay=replay,
                snapshot=snapshot,
            )
        )

    baseline_aggregate = _aggregate_rows(runs, predicate="baseline_gate")
    specialized_aggregate = _aggregate_rows(runs, predicate="specialized_gate")

    current_snapshot_rows = [row for row in runs if row["symbol"] in set(BASELINE_SYMBOLS)]
    gate_changed_rows = [row for row in current_snapshot_rows if row["baseline_gate"] != row["specialized_gate"]]
    group_symbol_sets = {
        "trend_up": ["1545", "1655", "9041"],
        "trend_down": ["9989", "7593", "7867", "3481", "2806", "8957"],
        "mixed_strong_select": ["1545", "1655", "6367", "9989", "3481", "9041"],
    }
    group_rows = {
        name: _group_rows(runs, symbol_order=[sym for sym in order if sym in {row["symbol"] for row in runs}])
        for name, order in group_symbol_sets.items()
    }

    compare_payload = {
        "schema_version": "tradex_regime_specialization_gate_compare_v1",
        "generated_at": _utc_now(),
        "same_condition_contract": {
            "same_universe": True,
            "same_period": True,
            "same_source_db": str(source_db_path),
            "same_snapshot_as_of": snapshot_as_of,
            "same_replay_engine": "tradex_chart_first_replay",
            "same_execution_assumption": "next_trading_day_open",
            "same_artifact_detail_level": "daily_ledger + roundtrip_summary + postmortem",
            "snapshot_source": "signal_basis_daily_direct_query",
        },
        "baseline_selector": {
            "name": "broad_snapshot_selector",
            "selected_count": baseline_aggregate["selected_count"],
            "no_trade_count": baseline_aggregate["no_trade_count"],
            "selected_symbols": baseline_aggregate["selected_symbols"],
        },
        "specialized_selector": {
            "name": "downtrend_specialization_gate",
            "selected_count": specialized_aggregate["selected_count"],
            "no_trade_count": specialized_aggregate["no_trade_count"],
            "selected_symbols": specialized_aggregate["selected_symbols"],
        },
        "compare_metrics": {
            "baseline": baseline_aggregate,
            "specialized": specialized_aggregate,
            "delta": {
                "selected_count": specialized_aggregate["selected_count"] - baseline_aggregate["selected_count"],
                "no_trade_count": specialized_aggregate["no_trade_count"] - baseline_aggregate["no_trade_count"],
                "net_realized_pnl": specialized_aggregate["net_realized_pnl"] - baseline_aggregate["net_realized_pnl"],
                "worst_drawdown": (
                    (specialized_aggregate["worst_drawdown"] - baseline_aggregate["worst_drawdown"])
                    if specialized_aggregate["worst_drawdown"] is not None and baseline_aggregate["worst_drawdown"] is not None
                    else None
                ),
            },
        },
        "group_rows": group_rows,
        "symbol_rows": runs,
        "selection_divergence_reason": "signal_basis_baseline_selection_vs_downtrend_specialization_gate",
        "branching_metrics": {
            "changed_label_count": len(gate_changed_rows),
            "baseline_selected_count": baseline_aggregate["selected_count"],
            "specialized_selected_count": specialized_aggregate["selected_count"],
            "trend_up_preserved_count": _symbols_with_gate(runs, group_symbol_sets["trend_up"], "specialized_gate"),
            "trend_down_selected_count": _symbols_with_gate(runs, group_symbol_sets["trend_down"], "specialized_gate"),
        },
        "supplemental_calibration": {
            "short_side_prior_artifact": str(
                Path(r"G:\Tradex\sample_replays\tradex_sample_2413_short_compare\tradex_sample_2413_short_compare.json")
            ),
            "short_side_prior_decision": "keep",
            "note": "The 3-way gate branch is specialized for current snapshot names; the clean short-side calibration remains validated by the prior 2413 short compare.",
        },
    }

    no_trade_reasons = {}
    for row in runs:
        key = row["specialized_gate_reason"]
        no_trade_reasons[key] = no_trade_reasons.get(key, 0) + 1
    reason_rollup = {
        "schema_version": "tradex_regime_specialization_gate_reason_rollup_v1",
        "generated_at": _utc_now(),
        "baseline_reason_rollup": {
            "long_tradable": sum(1 for row in runs if row["baseline_gate"] == "long_tradable"),
            "short_tradable": sum(1 for row in runs if row["baseline_gate"] == "short_tradable"),
            "no_trade": sum(1 for row in runs if row["baseline_gate"] == "no_trade"),
        },
        "specialized_reason_rollup": {
            "long_tradable": sum(1 for row in runs if row["specialized_gate"] == "long_tradable"),
            "short_tradable": sum(1 for row in runs if row["specialized_gate"] == "short_tradable"),
            "no_trade": sum(1 for row in runs if row["specialized_gate"] == "no_trade"),
        },
        "specialized_no_trade_reasons": no_trade_reasons,
        "symbol_reason_rows": [
            {
                "symbol": row["symbol"],
                "baseline_gate": row["baseline_gate"],
                "baseline_gate_reason": row["baseline_gate_reason"],
                "specialized_gate": row["specialized_gate"],
                "specialized_gate_reason": row["specialized_gate_reason"],
            }
            for row in runs
        ],
    }

    decision_reasons = []
    if specialized_aggregate["selected_count"] < baseline_aggregate["selected_count"]:
        decision_reasons.append("selection_became_more_specialized")
    if _symbols_with_gate(runs, group_symbol_sets["trend_up"], "specialized_gate") >= 3:
        decision_reasons.append("trend_up_preserved")
    if specialized_aggregate["net_realized_pnl"] > baseline_aggregate["net_realized_pnl"]:
        decision_reasons.append("selected_pool_pnl_improved")
    if len([row for row in runs if row["specialized_gate"] == "short_tradable"]) >= 2:
        decision_reasons.append("short_bucket_retained")

    decision = "keep" if (
        specialized_aggregate["trend_up_preserved_count"] >= 3
        and len([row for row in runs if row["specialized_gate"] == "no_trade"]) >= 4
        and specialized_aggregate["net_realized_pnl"] >= baseline_aggregate["net_realized_pnl"]
    ) else "hold"
    if not decision_reasons:
        decision_reasons.append("insufficient_separation_for_keep")

    decision_payload = {
        "schema_version": "tradex_regime_specialization_gate_decision_v1",
        "generated_at": _utc_now(),
        "candidate_local_decision": decision,
        "session_aggregate_decision": decision,
        "authoritative_rollup_decision": decision,
        "decision_reasons": decision_reasons,
        "same_condition_contract": compare_payload["same_condition_contract"],
        "compare_metrics": compare_payload["compare_metrics"],
        "authoritative_sources": {
            "compare_json": "tradex_regime_specialization_gate_compare.json",
            "reason_rollup_json": "tradex_regime_specialization_gate_reason_rollup.json",
        },
        "remaining_risks": [
            "The short side is still only partially specialized in the current snapshot set.",
            "The comparison is based on the frozen replay contract, so the snapshot gate should still be validated on a larger calibration slice.",
            "A supplemental short calibration artifact confirms 2413, but that validation is external to the current snapshot pool.",
        ],
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "compare_json": _write_json(output_dir / "tradex_regime_specialization_gate_compare.json", compare_payload),
        "decision_json": _write_json(output_dir / "tradex_regime_specialization_gate_decision.json", decision_payload),
        "reason_rollup_json": _write_json(output_dir / "tradex_regime_specialization_gate_reason_rollup.json", reason_rollup),
    }
    return {
        "ok": True,
        "output_dir": str(output_dir),
        "paths": {key: str(value) for key, value in paths.items()},
        "compare": compare_payload,
        "decision": decision_payload,
        "reason_rollup": reason_rollup,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build the TRADEX downtrend specialization gate compare.")
    parser.add_argument("--source-db-path", default=str(DEFAULT_SOURCE_DB_PATH))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", default=DEFAULT_END_DATE)
    parser.add_argument("--freeze-date", default=DEFAULT_FREEZE_DATE)
    parser.add_argument("--snapshot-as-of", type=int, default=DEFAULT_SNAPSHOT_AS_OF)
    parser.add_argument(
        "--symbols",
        default="",
        help="Optional comma-separated symbol list. Defaults to the fixed 10-symbol branch pool.",
    )
    args = parser.parse_args(argv)

    symbols = [symbol.strip() for symbol in args.symbols.split(",") if symbol.strip()] or None
    payload = run_gate_compare(
        source_db_path=Path(args.source_db_path).expanduser().resolve(),
        output_dir=Path(args.output_dir).expanduser().resolve(),
        symbols=symbols,
        start_date=str(args.start_date),
        end_date=str(args.end_date),
        freeze_date=str(args.freeze_date),
        snapshot_as_of=int(args.snapshot_as_of),
    )
    print(_json_text(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
