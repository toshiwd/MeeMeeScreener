from __future__ import annotations

import argparse
import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


DEFAULT_DB_PATH = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUT_ROOT = Path(r"G:\Tradex\month_start_direction_research_v1")


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _ymd_from_epoch(value: Any) -> int | None:
    if value is None:
        return None
    try:
        raw = int(value)
    except Exception:
        return None
    if 19000101 <= raw <= 20991231:
        return raw
    if raw >= 1_000_000_000_000:
        raw //= 1000
    if raw >= 1_000_000_000:
        return int(datetime.fromtimestamp(raw, tz=timezone.utc).strftime("%Y%m%d"))
    return None


def _epoch_from_ymd(ymd: int) -> int:
    parsed = datetime.strptime(str(int(ymd)), "%Y%m%d").replace(tzinfo=timezone.utc)
    return int(parsed.timestamp())


def _safe_float(value: Any) -> float | None:
    try:
        out = float(value)
    except Exception:
        return None
    if not math.isfinite(out):
        return None
    return out


def _ret(current: float | None, previous: float | None) -> float | None:
    if current is None or previous is None or previous == 0:
        return None
    return current / previous - 1.0


def _bucket_ret(value: float | None) -> str:
    if value is None:
        return "ret_na"
    if value <= -0.12:
        return "ret_strong_down"
    if value <= -0.04:
        return "ret_down"
    if value < 0.04:
        return "ret_flat"
    if value < 0.12:
        return "ret_up"
    return "ret_strong_up"


def _bucket_dist(value: float | None, prefix: str) -> str:
    if value is None:
        return f"{prefix}_na"
    if value <= -0.06:
        return f"{prefix}_far_below"
    if value <= -0.015:
        return f"{prefix}_below"
    if value < 0.015:
        return f"{prefix}_near"
    if value < 0.06:
        return f"{prefix}_above"
    return f"{prefix}_far_above"


def _bucket_pos(value: float | None, prefix: str) -> str:
    if value is None:
        return f"{prefix}_na"
    if value <= 0.20:
        return f"{prefix}_low"
    if value <= 0.45:
        return f"{prefix}_lower_mid"
    if value <= 0.65:
        return f"{prefix}_mid"
    if value <= 0.85:
        return f"{prefix}_upper_mid"
    return f"{prefix}_high"


def _ma_stack(close: float | None, ma7: float | None, ma20: float | None, ma60: float | None) -> str:
    if close is None or ma7 is None or ma20 is None or ma60 is None:
        return "ma_stack_na"
    if close > ma7 > ma20 > ma60:
        return "bull_stack_c_gt_7_gt_20_gt_60"
    if close < ma7 < ma20 < ma60:
        return "bear_stack_c_lt_7_lt_20_lt_60"
    if close > ma20 and ma20 > ma60:
        return "above20_uptrend"
    if close < ma20 and ma20 < ma60:
        return "below20_downtrend"
    if close > ma60:
        return "above60_mixed"
    return "below60_mixed"


def _candle_bucket(row: pd.Series) -> str:
    o = _safe_float(row.get("o"))
    h = _safe_float(row.get("h"))
    l = _safe_float(row.get("l"))
    c = _safe_float(row.get("c"))
    if None in (o, h, l, c) or h == l:
        return "candle_na"
    body = abs(c - o) / max(h - l, 1e-9)
    lower = (min(o, c) - l) / max(h - l, 1e-9)
    upper = (h - max(o, c)) / max(h - l, 1e-9)
    if c > o and body >= 0.55:
        return "strong_bull"
    if c < o and body >= 0.55:
        return "strong_bear"
    if lower >= 0.45 and lower > upper:
        return "lower_wick"
    if upper >= 0.45 and upper > lower:
        return "upper_wick"
    return "small_body"


def _vol_bucket(value: float | None) -> str:
    if value is None:
        return "vol_na"
    if value < 0.75:
        return "vol_light"
    if value < 1.25:
        return "vol_normal"
    if value < 2.0:
        return "vol_expanded"
    return "vol_spike"


def _load_daily(db_path: Path, *, start_ymd: int) -> pd.DataFrame:
    date_expr = """
        CASE
            WHEN b.date BETWEEN 19000101 AND 20991231 THEN CAST(b.date AS INTEGER)
            WHEN b.date >= 1000000000000 THEN CAST(strftime(to_timestamp(b.date / 1000), '%Y%m%d') AS INTEGER)
            WHEN b.date >= 1000000000 THEN CAST(strftime(to_timestamp(b.date), '%Y%m%d') AS INTEGER)
            ELSE NULL
        END
    """
    with duckdb.connect(str(db_path), read_only=True) as conn:
        frame = conn.execute(
            f"""
            SELECT
                b.code,
                COALESCE(t.name, b.code) AS name,
                {date_expr} AS ymd,
                b.o,
                b.h,
                b.l,
                b.c,
                b.v,
                COALESCE(b.source, 'pan') AS source,
                m.ma7,
                m.ma20,
                m.ma60
            FROM daily_bars b
            LEFT JOIN tickers t ON t.code = b.code
            LEFT JOIN daily_ma m ON m.code = b.code AND m.date = b.date
            WHERE {date_expr} IS NOT NULL
              AND COALESCE(b.source, 'pan') <> 'yahoo'
              AND {date_expr} >= ?
            ORDER BY b.code, ymd
            """
            ,
            [int(start_ymd)],
        ).fetchdf()
    frame["ymd"] = pd.to_numeric(frame["ymd"], errors="coerce").astype("Int64")
    frame = frame.dropna(subset=["ymd", "c"]).copy()
    frame["ymd"] = frame["ymd"].astype(int)
    return frame


def _exclude_code_name(code: str, name: str) -> bool:
    text = f"{code} {name}".lower()
    blocked = ("etf", "reit", "etn", "next", "投信", "上場投", "投資法人", "(投", "インデックス", "ダブルインバ")
    return any(token in text for token in blocked)


def _enrich(group: pd.DataFrame) -> pd.DataFrame:
    g = group.sort_values("ymd").reset_index(drop=True).copy()
    g["ret5"] = g["c"].pct_change(5)
    g["ret20"] = g["c"].pct_change(20)
    g["ret60"] = g["c"].pct_change(60)
    g["fwd5"] = g["c"].shift(-5) / g["c"] - 1.0
    g["vol20"] = g["v"].rolling(20).mean()
    g["vol_ratio20"] = g["v"] / g["vol20"]
    g["high20"] = g["h"].rolling(20).max()
    g["low20"] = g["l"].rolling(20).min()
    g["high60"] = g["h"].rolling(60).max()
    g["low60"] = g["l"].rolling(60).min()
    g["pos20"] = (g["c"] - g["low20"]) / (g["high20"] - g["low20"])
    g["pos60"] = (g["c"] - g["low60"]) / (g["high60"] - g["low60"])
    return g


def _add_features(frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, row in frame.iterrows():
        close = _safe_float(row.get("c"))
        ma7 = _safe_float(row.get("ma7"))
        ma20 = _safe_float(row.get("ma20"))
        ma60 = _safe_float(row.get("ma60"))
        out = row.to_dict()
        out["ma_stack"] = _ma_stack(close, ma7, ma20, ma60)
        out["dist_ma7_bucket"] = _bucket_dist(_ret(close, ma7), "dist7")
        out["dist_ma20_bucket"] = _bucket_dist(_ret(close, ma20), "dist20")
        out["dist_ma60_bucket"] = _bucket_dist(_ret(close, ma60), "dist60")
        out["ret20_bucket"] = _bucket_ret(_safe_float(row.get("ret20")))
        out["ret60_bucket"] = _bucket_ret(_safe_float(row.get("ret60")))
        out["pos20_bucket"] = _bucket_pos(_safe_float(row.get("pos20")), "pos20")
        out["pos60_bucket"] = _bucket_pos(_safe_float(row.get("pos60")), "pos60")
        out["candle_bucket"] = _candle_bucket(row)
        out["vol_bucket"] = _vol_bucket(_safe_float(row.get("vol_ratio20")))
        rows.append(out)
    out = pd.DataFrame(rows)
    out["rule_key"] = (
        out["ma_stack"]
        + "|"
        + out["ret20_bucket"]
        + "|"
        + out["pos60_bucket"]
        + "|"
        + out["candle_bucket"]
        + "|"
        + out["vol_bucket"]
    )
    return out


def _month_end_samples(enriched: pd.DataFrame, *, max_asof: int) -> pd.DataFrame:
    base = enriched[enriched["ymd"] <= max_asof].copy()
    base["month"] = (base["ymd"] // 100).astype(int)
    idx = base.groupby(["code", "month"])["ymd"].idxmax()
    samples = base.loc[idx].copy()
    samples = samples.dropna(subset=["fwd5"])
    samples = samples[samples["ymd"] < max_asof]
    return samples


def _stats(samples: pd.DataFrame, side: str, min_n: int) -> dict[str, dict[str, Any]]:
    sign = 1.0 if side == "up" else -1.0
    rows: dict[str, dict[str, Any]] = {}
    for key, group in samples.groupby("rule_key"):
        directional = sign * pd.to_numeric(group["fwd5"], errors="coerce")
        directional = directional.dropna()
        if len(directional) < min_n:
            continue
        rows[str(key)] = {
            "n": int(len(directional)),
            "win_rate": float((directional > 0).mean()),
            "avg_directional_ret5": float(directional.mean()),
            "median_directional_ret5": float(directional.median()),
            "loss_tail_p10": float(directional.quantile(0.10)),
        }
    return rows


def _current_rows(enriched: pd.DataFrame, asof: int) -> pd.DataFrame:
    latest = enriched[enriched["ymd"] <= asof].copy()
    idx = latest.groupby("code")["ymd"].idxmax()
    return latest.loc[idx].copy()


def _candidate_rows(current: pd.DataFrame, stats: dict[str, dict[str, Any]], side: str) -> list[dict[str, Any]]:
    sign = 1.0 if side == "up" else -1.0
    rows: list[dict[str, Any]] = []
    for _, row in current.iterrows():
        code = str(row["code"])
        name = str(row.get("name") or code)
        if _exclude_code_name(code, name):
            continue
        stat = stats.get(str(row.get("rule_key")))
        if not stat:
            continue
        score = (
            float(stat["avg_directional_ret5"]) * 100.0
            + max(0.0, float(stat["win_rate"]) - 0.5) * 4.0
            + min(0.5, math.log10(max(1, int(stat["n"]))) / 10.0)
        )
        rows.append(
            {
                "code": code,
                "name": name,
                "side": side,
                "as_of": int(row["ymd"]),
                "close": _safe_float(row.get("c")),
                "rule_key": str(row.get("rule_key")),
                "historical_n": int(stat["n"]),
                "historical_win_rate": round(float(stat["win_rate"]), 6),
                "historical_avg_directional_ret5": round(float(stat["avg_directional_ret5"]), 6),
                "historical_median_directional_ret5": round(float(stat["median_directional_ret5"]), 6),
                "historical_loss_tail_p10": round(float(stat["loss_tail_p10"]), 6),
                "score": round(score, 6),
                "ret20": _safe_float(row.get("ret20")),
                "ret60": _safe_float(row.get("ret60")),
                "ma7": _safe_float(row.get("ma7")),
                "ma20": _safe_float(row.get("ma20")),
                "ma60": _safe_float(row.get("ma60")),
                "ma_stack": row.get("ma_stack"),
                "pos60_bucket": row.get("pos60_bucket"),
                "candle_bucket": row.get("candle_bucket"),
                "vol_bucket": row.get("vol_bucket"),
                "directional_context_ret5_if_known": None
                if pd.isna(row.get("fwd5"))
                else round(float(sign * row.get("fwd5")), 6),
            }
        )
    return sorted(rows, key=lambda x: (x["score"], x["historical_win_rate"], x["historical_n"]), reverse=True)


def run(db_path: Path, out_dir: Path, asof: int | None, min_n: int, top_n: int) -> dict[str, Any]:
    daily = _load_daily(db_path, start_ymd=20180101)
    if daily.empty:
        raise RuntimeError("daily_bars is empty")
    enriched = pd.concat([_enrich(g) for _, g in daily.groupby("code")], ignore_index=True)
    latest_asof = int(enriched["ymd"].max()) if asof is None else int(asof)
    samples = _add_features(_month_end_samples(enriched, max_asof=latest_asof))
    up_stats = _stats(samples, "up", min_n)
    down_stats = _stats(samples, "down", min_n)
    current = _add_features(_current_rows(enriched, latest_asof))
    up_candidates = _candidate_rows(current, up_stats, "up")[:top_n]
    down_candidates = _candidate_rows(current, down_stats, "down")[:top_n]

    out_dir.mkdir(parents=True, exist_ok=True)
    requested_confirmed_asof = 20260630
    actual_latest_confirmed_asof = int(enriched["ymd"].max())
    payload = {
        "schema_version": "tradex_month_start_direction_research_v1",
        "generated_at": _utc_now(),
        "db_path": str(db_path),
        "research_phase": "effectiveness_judgment",
        "basis": "confirmed_non_yahoo_daily_bars",
        "research_fallback": bool(actual_latest_confirmed_asof < requested_confirmed_asof),
        "requested_confirmed_asof": requested_confirmed_asof,
        "actual_latest_confirmed_asof": actual_latest_confirmed_asof,
        "current_asof_used": latest_asof,
        "fixed_evaluation_conditions": {
            "entry_timing": "month-end confirmed close; evaluate next 5 trading days",
            "directional_return": "up uses fwd5, down uses -fwd5",
            "universe": "runtime tickers excluding ETF/ETN/REIT/index-like names at candidate stage",
            "min_rule_samples": min_n,
            "cost_slippage": "not applied",
            "data_source": "daily_bars where source != yahoo",
        },
        "sample_counts": {
            "daily_rows": int(len(daily)),
            "month_end_samples": int(len(samples)),
            "up_rule_count": int(len(up_stats)),
            "down_rule_count": int(len(down_stats)),
            "current_rows": int(len(current)),
        },
        "decisions": {
            "candidate_local_decision": "hold_review_only_candidates",
            "session_aggregate_decision": "hold_until_20260630_confirmed_source_available"
            if latest_asof < 20260630
            else "review_candidates_available",
            "authoritative_rollup_decision": "review_only_not_meemee_reflection",
        },
        "top_up_candidates": up_candidates,
        "top_down_candidates": down_candidates,
    }
    (out_dir / "month_start_direction_research.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    pd.DataFrame(up_candidates).to_csv(out_dir / "top_up_candidates.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(down_candidates).to_csv(out_dir / "top_down_candidates.csv", index=False, encoding="utf-8-sig")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=Path(os.getenv("STOCKS_DB_PATH") or DEFAULT_DB_PATH))
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--asof", type=int, default=None)
    parser.add_argument("--min-n", type=int, default=24)
    parser.add_argument("--top-n", type=int, default=20)
    args = parser.parse_args()

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = args.out_root / f"{stamp}-month-start-direction-research-v1"
    payload = run(args.db_path, out_dir, args.asof, args.min_n, args.top_n)
    print(json.dumps({"out_dir": str(out_dir), "current_asof_used": payload["current_asof_used"], "sample_counts": payload["sample_counts"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
