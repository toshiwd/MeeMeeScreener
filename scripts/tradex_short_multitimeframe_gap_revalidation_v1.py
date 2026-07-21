from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from external_analysis.ma_hierarchical_labels import _build_daily_context_for_code, _build_monthly_context_for_code, _build_weekly_context_for_code


DEFAULT_DB = Path.home() / "AppData/Local/MeeMeeScreener/data/stocks.duckdb"
DEFAULT_OUT = Path(r"G:\Tradex\research_knowledge_registry_v1")
DEFAULT_CACHE = DEFAULT_OUT / "context_cache_v1"
CUTOFF = 20260710
STATE_SCHEMA = "ma_hierarchical_labels_point_in_time_completed_week_month_v2"
DEFINITION_FILE = ROOT / "external_analysis" / "ma_hierarchical_labels.py"
LOOKBACK_START = 20190101
NORMALIZED_DATE_SQL = """case
 when try_cast(date as bigint) between 19000101 and 29991231 then try_cast(date as bigint)
 when try_cast(date as bigint) between 0 and 4102444800 then try_cast(strftime(to_timestamp(try_cast(date as bigint)), '%Y%m%d') as bigint)
 else null end"""


def _date_int(series: pd.Series) -> pd.Series:
    raw = pd.to_numeric(series, errors="coerce")
    out = raw.copy()
    epoch = raw > 100_000_000
    out.loc[epoch] = pd.to_datetime(raw.loc[epoch], unit="s").dt.strftime("%Y%m%d").astype(float)
    return out.astype("Int64")


def states_at_signal(bars: pd.DataFrame, signal_date: int) -> dict[str, object]:
    """Point-in-time states: future rows are removed before feature creation."""
    frame = bars.copy()
    frame["date"] = _date_int(frame["date"])
    frame = frame.loc[frame["date"] <= signal_date].dropna(subset=["date"]).copy()
    if frame.empty:
        return {"daily_main_state": "unknown", "weekly_main_state": "unknown", "source_max_date": None}
    frame["date"] = frame["date"].astype(int)
    frame["date_ts"] = pd.to_datetime(frame["date"].astype(str), format="%Y%m%d")
    daily = _build_daily_context_for_code(frame)
    weekly = _build_weekly_context_for_code(frame)
    return {
        "daily_main_state": str(daily.iloc[-1]["daily_main_state"]),
        "weekly_main_state": str(weekly.iloc[-1]["weekly_main_state"]),
        "source_max_date": int(frame["date"].max()),
    }


def _prepare_context_input(frame: pd.DataFrame) -> pd.DataFrame:
    x = frame.copy()
    x["date"] = _date_int(x["date"]); x = x.dropna(subset=["date"]); x["date"] = x["date"].astype(int)
    x["date_ts"] = pd.to_datetime(x["date"].astype(str), format="%Y%m%d")
    return x.sort_values("date")


def build_context_lookup(bars: pd.DataFrame, signal_dates: list[int]) -> pd.DataFrame:
    """Generate contexts once and point-in-time join signals to them."""
    frame = _prepare_context_input(bars)
    daily = _build_daily_context_for_code(frame)[["date", "daily_main_state"]].copy()
    weekly = _build_weekly_context_for_code(frame)[["week_end_date", "weekly_main_state"]].copy()
    monthly = _build_monthly_context_for_code(frame)[["month_end_date", "monthly_main_state"]].copy()
    signals = pd.DataFrame({"signal_date": sorted(set(int(x) for x in signal_dates))})
    out = signals.merge(daily, left_on="signal_date", right_on="date", how="left").drop(columns=["date"])
    out = pd.merge_asof(out.sort_values("signal_date"), weekly.sort_values("week_end_date"),
                        left_on="signal_date", right_on="week_end_date", direction="backward")
    out = pd.merge_asof(out.sort_values("signal_date"), monthly.sort_values("month_end_date"),
                        left_on="signal_date", right_on="month_end_date", direction="backward")
    out["daily_source_date"] = out["signal_date"]
    out["weekly_source_date"] = out["week_end_date"].astype("Int64")
    out["monthly_source_date"] = out["month_end_date"].astype("Int64")
    out["source_max_date"] = out[["daily_source_date", "weekly_source_date", "monthly_source_date"]].max(axis=1).astype("Int64")
    return out


def _sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def bars_content_sha(frame: pd.DataFrame) -> str:
    cols = ["code", "date", "o", "h", "l", "c", "v"]
    ordered = frame[cols].sort_values(["code", "date"]).reset_index(drop=True)
    values = pd.util.hash_pandas_object(ordered, index=False).to_numpy(dtype="uint64")
    return hashlib.sha256(values.tobytes()).hexdigest()


def cache_key(db_path: Path, bars_sha: str, codes: list[str]) -> dict[str, object]:
    return {"db_path": str(db_path.resolve()), "bars_content_sha256": bars_sha,
            "code_set_sha256": hashlib.sha256("\n".join(sorted(codes)).encode()).hexdigest(),
            "confirmed_date": CUTOFF, "definition_source": str(DEFINITION_FILE),
            "definition_sha256": _sha(DEFINITION_FILE), "state_schema_version": STATE_SCHEMA}


def _atomic_json(path: Path, payload: dict[str, object]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def _atomic_parquet(path: Path, frame: pd.DataFrame) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    frame.to_parquet(tmp, index=False)
    os.replace(tmp, path)


def build_or_load_cache(db_path: Path, cache_dir: Path, bars: pd.DataFrame,
                        signal_dates_by_code: dict[str, list[int]], bars_sha: str | None = None) -> pd.DataFrame:
    started = time.perf_counter()
    expected = cache_key(db_path, bars_sha or bars_content_sha(bars), sorted(signal_dates_by_code))
    key_hash = hashlib.sha256(json.dumps(expected, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    cache_dir = cache_dir / key_hash[:20]
    cache_dir.mkdir(parents=True, exist_ok=True)
    manifest_path, data_path = cache_dir / "manifest.json", cache_dir / "contexts.parquet"
    if manifest_path.exists() or data_path.exists():
        if not (manifest_path.exists() and data_path.exists()):
            raise RuntimeError("stale context cache: manifest/data pair incomplete")
        actual = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
        if actual.get("dataset_key") != expected:
            raise RuntimeError("stale context cache: dataset key mismatch; explicit cache removal/rebuild required")
        frame = pd.read_parquet(data_path)
        if int(actual.get("row_count", -1)) != len(frame):
            raise RuntimeError("stale context cache: row count mismatch")
        print(f"context-cache hit rows={len(frame)}", flush=True)
        return frame
    chunks = []
    codes = sorted(signal_dates_by_code)
    grouped = {str(k): v for k, v in bars.groupby(bars["code"].astype(str))}
    for idx, code in enumerate(codes, 1):
        cb = grouped.get(code)
        if cb is not None:
            x = build_context_lookup(cb, signal_dates_by_code[code]); x.insert(0, "code", code); chunks.append(x)
        if idx == 1 or idx % 25 == 0 or idx == len(codes):
            print(f"context-cache build {idx}/{len(codes)}", flush=True)
    frame = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
    _atomic_parquet(data_path, frame)
    _atomic_json(manifest_path, {"schema_version": "tradex_context_cache_v1", "dataset_key": expected,
                                 "row_count": len(frame), "code_count": len(codes),
                                 "phase_rows": {"signals_requested": sum(len(v) for v in signal_dates_by_code.values()),
                                                "contexts_written": len(frame)},
                                 "phase_timing_seconds": {"context_build_and_write": round(time.perf_counter() - started, 3)},
                                 "generated_at": datetime.now(timezone.utc).isoformat()})
    return frame


def _outcome(bars: pd.DataFrame, signal_date: int) -> dict[str, object] | None:
    future = bars.loc[bars["date"] > signal_date].sort_values("date").head(20)
    if len(future) < 20:
        return None
    entry = float(future.iloc[0]["o"])
    if not np.isfinite(entry) or entry <= 0:
        return None
    lows = future["l"].astype(float).to_numpy()
    highs = future["h"].astype(float).to_numpy()
    closes = future["c"].astype(float).to_numpy()
    mfe = (entry - lows) / entry
    mae = (highs - entry) / entry
    return {
        "entry_date": int(future.iloc[0]["date"]), "entry_open": entry,
        "exit_date_20": int(future.iloc[-1]["date"]), "return_20": float((entry - closes[-1]) / entry),
        "short_mfe_20": float(np.max(mfe)), "short_mae_20": float(np.max(mae)),
        "days_to_short_mfe": int(np.argmax(mfe) + 1),
        "sell_strong": bool(np.max(mfe) >= 0.10 and np.max(mae[: np.argmax(mfe) + 1]) < 0.08),
    }


def _summary(frame: pd.DataFrame, mask: pd.Series) -> dict[str, object]:
    x = frame.loc[mask]
    return {
        "n": int(len(x)), "unique_symbols": int(x["code"].nunique()),
        "sell_strong_rate": float(x["sell_strong"].mean()) if len(x) else None,
        "return_20_mean": float(x["return_20"].mean()) if len(x) else None,
        "short_mfe_20_mean": float(x["short_mfe_20"].mean()) if len(x) else None,
        "short_mae_20_mean": float(x["short_mae_20"].mean()) if len(x) else None,
        "days_to_short_mfe_mean": float(x["days_to_short_mfe"].mean()) if len(x) else None,
    }


def build(db_path: Path, cache_dir: Path = DEFAULT_CACHE, candidate: str = "daily_weekly") -> tuple[pd.DataFrame, dict[str, object]]:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        sig = con.execute("""
          select dt, code, side, logic_version, basis_version
          from signal_decision_daily
          where lower(side) in ('sell','short') and entry_qualified = true
            and dt between 20240301 and 20260710
        """).fetchdf()
        codes = sorted(str(x) for x in sig["code"].unique())
        placeholders = ",".join("?" for _ in codes)
        predicate = f"cast(code as varchar) in ({placeholders}) and normalized_date between ? and ?"
        params = [*codes, LOOKBACK_START, CUTOFF]
        print(f"bars fingerprint start codes={len(codes)}", flush=True); phase = time.perf_counter()
        fp = con.execute(f"""select count(*) n, min(normalized_date) min_date, max(normalized_date) max_date,
                    bit_xor(hash(cast(code as varchar), normalized_date, o, h, l, c, v)) content_xor
          from (select code,o,h,l,c,v,{NORMALIZED_DATE_SQL} normalized_date from daily_bars) q where {predicate}""", params).fetchone()
        bars_sha = hashlib.sha256(json.dumps(list(fp), separators=(",", ":")).encode()).hexdigest()
        print(f"bars fingerprint done rows={fp[0]} seconds={time.perf_counter()-phase:.2f}", flush=True)
        print("bars fetch start", flush=True); phase = time.perf_counter()
        bars = con.execute(f"""select cast(code as varchar) code, normalized_date date,o,h,l,c,v
          from (select code,o,h,l,c,v,{NORMALIZED_DATE_SQL} normalized_date from daily_bars) q
          where {predicate} order by code,normalized_date""", params).fetchdf()
        print(f"bars fetch done rows={len(bars)} seconds={time.perf_counter()-phase:.2f}", flush=True)
    finally:
        con.close()
    bars["date"] = bars["date"].astype(int)
    signal_dates_by_code = {str(code): [int(x) for x in group["dt"]] for code, group in sig.groupby(sig["code"].astype(str))}
    cached = build_or_load_cache(db_path, cache_dir, bars, signal_dates_by_code, bars_sha=bars_sha)
    context_by_code = {str(code): group.set_index("signal_date") for code, group in cached.groupby(cached["code"].astype(str))}
    by_code = {str(k): _prepare_context_input(v) for k, v in bars.groupby(bars["code"].astype(str)) if str(k) in signal_dates_by_code}
    rows = []
    for s in sig.itertuples(index=False):
        code, dt = str(s.code), int(s.dt)
        cb = by_code.get(code)
        if cb is None:
            continue
        context = context_by_code.get(code)
        if context is None or dt not in context.index:
            continue
        ctx = context.loc[dt]
        if isinstance(ctx, pd.DataFrame):
            ctx = ctx.iloc[0]
        state = {"daily_main_state": str(ctx["daily_main_state"]),
                 "weekly_main_state": str(ctx["weekly_main_state"]),
                 "monthly_main_state": str(ctx["monthly_main_state"]),
                 "daily_source_date": int(ctx["daily_source_date"]),
                 "weekly_source_date": int(ctx["weekly_source_date"]),
                 "monthly_source_date": int(ctx["monthly_source_date"]),
                 "source_max_date": int(ctx["source_max_date"])}
        outcome = _outcome(cb, dt)
        if outcome is None:
            continue
        rows.append({"code": code, "signal_date": dt, "side": "short", "logic_version": s.logic_version,
                     "basis_version": s.basis_version, **state, "no_lookahead": bool(state["source_max_date"] <= dt), **outcome})
    ledger = pd.DataFrame(rows)
    if ledger.empty:
        raise RuntimeError("no complete-horizon qualified short signals")
    ledger["period"] = np.where(ledger["signal_date"] <= 20260228, "discovery", "untouched_diagnostic")
    if candidate == "monthly_weekly":
        a = ledger["monthly_main_state"].eq("monthly_down_mid")
        a_name = "A_monthly_only"
        changed_axis = "add monthly_down_mid x weekly_range_late interaction only"
    else:
        a = ledger["daily_main_state"].eq("daily_down_mid")
        a_name = "A_daily_only"
        changed_axis = "add daily_down_mid x weekly_range_late interaction only"
    b = ledger["weekly_main_state"].eq("weekly_range_late")
    comparisons = {}
    for period in ("discovery", "untouched_diagnostic"):
        p = ledger["period"].eq(period)
        comparisons[period] = {a_name: _summary(ledger, p & a), "B_weekly_only": _summary(ledger, p & b),
                               "C_intersection": _summary(ledger, p & a & b)}
    result = {
        "schema_version": "tradex_short_multitimeframe_gap_revalidation_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(), "dataset_version": "runtime_confirmed_20260710_point_in_time_v1",
        "decision": "revalidation_only_not_trade_adoption",
        "changed_axis": changed_axis, "candidate": candidate,
        "entry": "next_session_open", "horizon": 20, "complete_horizon_only": True,
        "state_definition_source": "external_analysis/ma_hierarchical_labels.py",
        "periods": {"discovery": [20240301, 20260228], "untouched_diagnostic": [20260301, 20260710]},
        "row_count": int(len(ledger)), "no_lookahead_all": bool(ledger["no_lookahead"].all()), "comparisons": comparisons,
    }
    return ledger, result


def main() -> int:
    ap = argparse.ArgumentParser(); ap.add_argument("--db", type=Path, default=DEFAULT_DB); ap.add_argument("--output-root", type=Path, default=DEFAULT_OUT); ap.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE); ap.add_argument("--candidate", choices=["daily_weekly", "monthly_weekly"], default="daily_weekly")
    args = ap.parse_args(); stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    phase = "phase3b-monthly-weekly-revalidation" if args.candidate == "monthly_weekly" else "phase3-short-interaction-revalidation"
    out = args.output_root / f"{stamp}-{phase}"; out.mkdir(parents=True)
    ledger, result = build(args.db, args.cache_dir, candidate=args.candidate)
    ledger.to_parquet(out / "point_in_time_short_ledger.parquet", index=False)
    (out / "revalidation_result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(out); return 0


if __name__ == "__main__": raise SystemExit(main())
