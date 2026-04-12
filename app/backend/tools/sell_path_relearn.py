from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from app.backend.tools.swing_gate_relearn import _fmt_num, _fmt_pct, _jsonable, _table_exists
from app.db.session import get_conn

DEFAULT_LOOKBACK_DAYS = 365 * 3
DEFAULT_REPORT_DIR = Path("G:/Tradex/reports")


@dataclass(frozen=True)
class SellPathConfig:
    lookback_days: int = DEFAULT_LOOKBACK_DAYS
    report_dir: Path = DEFAULT_REPORT_DIR
    bullish_candle_prob_min: float = 0.55
    bearish_candle_prob_min: float = 0.55
    weak_breadth_max: float = 0.45


def _load_sell_signal_frame(conn, *, start_ymd: int, end_ymd: int) -> pd.DataFrame:
    if not _table_exists(conn, "signal_decision_daily") or not _table_exists(conn, "ml_feature_daily"):
        return pd.DataFrame()
    frame = conn.execute(
        """
        SELECT
            s.dt AS signal_dt,
            s.code,
            s.side,
            s.setup_type,
            s.entry_qualified,
            s.forward_return_5,
            s.forward_return_10,
            s.forward_return_20,
            s.forward_return_30,
            s.max_favorable_30,
            s.max_adverse_30,
            CAST(json_extract(s.score_snapshot_json, '$.entryScore') AS DOUBLE) AS entry_score,
            f.close,
            f.ma7,
            f.ma20,
            f.ma60,
            f.candle_body_ratio AS body_ratio,
            f.candle_upper_wick_ratio AS upper_wick_ratio,
            f.candle_lower_wick_ratio AS lower_wick_ratio,
            f.candle_triplet_up_prob,
            f.candle_triplet_down_prob,
            f.gap_pct,
            f.close_ret2,
            f.close_ret3,
            f.close_ret20,
            f.close_ret60,
            f.breakout20_down,
            f.rebound60,
            f.drawdown60,
            f.market_ret20,
            f.breadth_above_ma20,
            f.breadth_above_ma60,
            f.sector_ret20,
            f.rel_sector_ret20,
            r.regime_id
        FROM signal_decision_daily s
        LEFT JOIN ml_feature_daily f
            ON f.dt = epoch(strptime(cast(s.dt AS VARCHAR), '%Y%m%d'))::BIGINT
           AND f.code = s.code
        LEFT JOIN market_regime_daily r
            ON r.dt = s.dt
        WHERE s.entry_qualified = TRUE
          AND s.side = 'sell'
          AND s.dt BETWEEN ? AND ?
        ORDER BY s.dt ASC, s.code ASC
        """,
        [int(start_ymd), int(end_ymd)],
    ).df()
    if frame.empty:
        return frame
    frame["code"] = frame["code"].astype(str).str.strip()
    frame["side"] = frame["side"].astype(str).str.strip().str.lower()
    frame["setup_type"] = frame["setup_type"].astype(str).str.strip().str.lower()
    frame["signal_dt"] = pd.to_numeric(frame["signal_dt"], errors="coerce").astype("Int64")
    frame = frame.dropna(subset=["signal_dt", "code", "side"]).copy()
    frame["signal_dt"] = frame["signal_dt"].astype(int)
    for col in [
        "forward_return_5",
        "forward_return_10",
        "forward_return_20",
        "forward_return_30",
        "max_favorable_30",
        "max_adverse_30",
        "entry_score",
        "close",
        "ma7",
        "ma20",
        "ma60",
        "body_ratio",
        "upper_wick_ratio",
        "lower_wick_ratio",
        "candle_triplet_up_prob",
        "candle_triplet_down_prob",
        "gap_pct",
        "close_ret2",
        "close_ret3",
        "close_ret20",
        "close_ret60",
        "breakout20_down",
        "rebound60",
        "drawdown60",
        "market_ret20",
        "breadth_above_ma20",
        "breadth_above_ma60",
        "sector_ret20",
        "rel_sector_ret20",
    ]:
        if col in frame.columns:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
    return frame


def _augment_features(frame: pd.DataFrame, *, bullish_candle_prob_min: float, bearish_candle_prob_min: float) -> pd.DataFrame:
    out = frame.copy()
    out["gap7"] = out["close"] / out["ma7"] - 1.0
    out["gap20"] = out["close"] / out["ma20"] - 1.0
    out["gap60"] = out["close"] / out["ma60"] - 1.0
    out["bullish_candle_proxy"] = (
        (out["candle_triplet_up_prob"].fillna(0.0) >= float(bullish_candle_prob_min))
        & (out["candle_triplet_up_prob"].fillna(0.0) >= out["candle_triplet_down_prob"].fillna(0.0))
    )
    out["bearish_candle_proxy"] = (
        (out["candle_triplet_down_prob"].fillna(0.0) >= float(bearish_candle_prob_min))
        & (out["candle_triplet_down_prob"].fillna(0.0) >= out["candle_triplet_up_prob"].fillna(0.0))
    )
    out["risk_off_proxy"] = out["regime_id"].isin(["risk_off_trend", "high_vol_chaos"])
    out["market_weak_proxy"] = (out["market_ret20"] < 0.0) | (out["breadth_above_ma20"] <= 0.45) | out["risk_off_proxy"]
    out["setup_type"] = out["setup_type"].astype(str).str.strip().str.lower()
    return out


def _short_profit(frame: pd.DataFrame, horizon: int) -> pd.Series:
    return -pd.to_numeric(frame[f"forward_return_{int(horizon)}"], errors="coerce")


def _path_tag(row: pd.Series) -> str:
    short20 = float(-row.get("forward_return_20") if pd.notna(row.get("forward_return_20")) else 0.0)
    short5 = float(-row.get("forward_return_5") if pd.notna(row.get("forward_return_5")) else 0.0)
    short10 = float(-row.get("forward_return_10") if pd.notna(row.get("forward_return_10")) else 0.0)
    bullish = bool(row.get("bullish_candle_proxy"))
    bearish = bool(row.get("bearish_candle_proxy"))
    setup = str(row.get("setup_type") or "")
    if short20 > 0 and bullish:
        return "bullish_candle_contrarian"
    if short20 > 0 and setup == "breakdown" and short5 > 0 and short10 > 0:
        return "breakdown_continuation"
    if short20 > 0 and setup in {"breakdown", "pressure"} and short5 <= 0 < short20:
        return "failed_retest"
    if short20 <= 0 and (bullish or setup in {"pressure", "breakdown"}):
        return "squeeze_loss"
    if short20 > 0 and bearish:
        return "bearish_candle_short_win"
    return "other"


def _summarize_subset(frame: pd.DataFrame, *, label: str) -> dict[str, Any]:
    if frame.empty:
        return {
            "label": label,
            "count": 0,
            "mean5": None,
            "mean10": None,
            "mean20": None,
            "win20": None,
            "median20": None,
            "mean_gap20": None,
            "mean_market_ret20": None,
            "mean_breadth_ma20": None,
            "mean_bullish_candle_prob": None,
            "mean_bearish_candle_prob": None,
            "mean_max_favorable_30": None,
            "mean_max_adverse_30": None,
        }
    short5 = _short_profit(frame, 5)
    short10 = _short_profit(frame, 10)
    short20 = _short_profit(frame, 20)
    short30 = _short_profit(frame, 30)
    return {
        "label": label,
        "count": int(len(frame)),
        "mean5": float(short5.mean()),
        "mean10": float(short10.mean()),
        "mean20": float(short20.mean()),
        "win20": float((short20 > 0).mean()),
        "median20": float(short20.median()),
        "mean30": float(short30.mean()),
        "mean_gap20": float(frame["gap20"].mean()),
        "mean_market_ret20": float(frame["market_ret20"].mean()) if "market_ret20" in frame.columns else None,
        "mean_breadth_ma20": float(frame["breadth_above_ma20"].mean()) if "breadth_above_ma20" in frame.columns else None,
        "mean_bullish_candle_prob": float(frame["candle_triplet_up_prob"].mean()) if "candle_triplet_up_prob" in frame.columns else None,
        "mean_bearish_candle_prob": float(frame["candle_triplet_down_prob"].mean()) if "candle_triplet_down_prob" in frame.columns else None,
        "mean_max_favorable_30": float(frame["max_favorable_30"].mean()) if "max_favorable_30" in frame.columns else None,
        "mean_max_adverse_30": float(frame["max_adverse_30"].mean()) if "max_adverse_30" in frame.columns else None,
    }


def run_sell_path_relearn(*, config: SellPathConfig = SellPathConfig()) -> dict[str, Any]:
    with get_conn() as conn:
        if not _table_exists(conn, "signal_decision_daily") or not _table_exists(conn, "ml_feature_daily"):
            return {"ok": False, "reason": "required_tables_missing"}
        latest_row = conn.execute("SELECT MAX(dt) FROM signal_decision_daily").fetchone()
        if not latest_row or latest_row[0] is None:
            return {"ok": False, "reason": "signal_frame_empty"}
        latest_ymd = int(latest_row[0])
        start_dt = datetime.strptime(str(latest_ymd), "%Y%m%d")
        start_ymd = int((start_dt - pd.Timedelta(days=max(1, int(config.lookback_days)))).strftime("%Y%m%d"))
        frame = _load_sell_signal_frame(conn, start_ymd=start_ymd, end_ymd=latest_ymd)
    if frame.empty:
        return {"ok": False, "reason": "signal_frame_empty"}
    frame = _augment_features(
        frame,
        bullish_candle_prob_min=config.bullish_candle_prob_min,
        bearish_candle_prob_min=config.bearish_candle_prob_min,
    )
    frame["short_profit_5"] = _short_profit(frame, 5)
    frame["short_profit_10"] = _short_profit(frame, 10)
    frame["short_profit_20"] = _short_profit(frame, 20)
    frame["short_profit_30"] = _short_profit(frame, 30)
    frame["path_tag"] = frame.apply(_path_tag, axis=1)
    frame["has_bullish_candle"] = frame["bullish_candle_proxy"].astype(bool)
    frame["has_bearish_candle"] = frame["bearish_candle_proxy"].astype(bool)

    tag_order = [
        "breakdown_continuation",
        "failed_retest",
        "bullish_candle_contrarian",
        "bearish_candle_short_win",
        "squeeze_loss",
        "other",
    ]
    breakdown = [
        {**_summarize_subset(frame[frame["path_tag"] == tag], label=tag), "path_tag": tag}
        for tag in tag_order
    ]
    contrarian = frame[(frame["path_tag"] == "bullish_candle_contrarian") & (frame["short_profit_20"] > 0)].copy()
    breakdown_short = frame[frame["path_tag"] == "breakdown_continuation"].copy()
    failed_retest = frame[frame["path_tag"] == "failed_retest"].copy()
    squeeze_loss = frame[frame["path_tag"] == "squeeze_loss"].copy()
    bullish_all = frame[frame["bullish_candle_proxy"]].copy()
    bearish_all = frame[frame["bearish_candle_proxy"]].copy()

    summary = {
        "ok": True,
        "as_of_ymd": latest_ymd,
        "period_start_ymd": int(frame["signal_dt"].min()),
        "period_end_ymd": int(frame["signal_dt"].max()),
        "lookback_days": int(config.lookback_days),
        "row_count": int(len(frame)),
        "overall": _summarize_subset(frame, label="all_sell_entries"),
        "bullish_candle": {
            "count": int(len(bullish_all)),
            "win20": float((bullish_all["short_profit_20"] > 0).mean()) if len(bullish_all) else None,
            "mean20": float(bullish_all["short_profit_20"].mean()) if len(bullish_all) else None,
            "median20": float(bullish_all["short_profit_20"].median()) if len(bullish_all) else None,
            "mean5": float(bullish_all["short_profit_5"].mean()) if len(bullish_all) else None,
            "mean10": float(bullish_all["short_profit_10"].mean()) if len(bullish_all) else None,
        },
        "bearish_candle": {
            "count": int(len(bearish_all)),
            "win20": float((bearish_all["short_profit_20"] > 0).mean()) if len(bearish_all) else None,
            "mean20": float(bearish_all["short_profit_20"].mean()) if len(bearish_all) else None,
            "median20": float(bearish_all["short_profit_20"].median()) if len(bearish_all) else None,
            "mean5": float(bearish_all["short_profit_5"].mean()) if len(bearish_all) else None,
            "mean10": float(bearish_all["short_profit_10"].mean()) if len(bearish_all) else None,
        },
        "path_breakdown": breakdown,
        "contrarian_bullish": _summarize_subset(contrarian, label="bullish_candle_contrarian"),
        "failed_retest": _summarize_subset(failed_retest, label="failed_retest"),
        "breakdown_continuation": _summarize_subset(breakdown_short, label="breakdown_continuation"),
        "squeeze_loss": _summarize_subset(squeeze_loss, label="squeeze_loss"),
    }
    summary["path_summary"] = {
        row["path_tag"]: {
            "count": row["count"],
            "mean20": row["mean20"],
            "win20": row["win20"],
        }
        for row in breakdown
    }
    return summary


def _write_markdown_report(result: dict[str, Any], output_path: Path) -> None:
    lines = [
        "# Sell Path Relearn",
        "",
        f"- ok: `{result.get('ok')}`",
        f"- as_of_ymd: `{result.get('as_of_ymd')}`",
        f"- period_start_ymd: `{result.get('period_start_ymd')}`",
        f"- period_end_ymd: `{result.get('period_end_ymd')}`",
        f"- row_count: `{result.get('row_count')}`",
        "",
        "## Overall",
        "",
        "| count | mean5 | mean10 | mean20 | win20 | median20 | market_ret20 | breadth_ma20 | bullish_prob | bearish_prob |",
        "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    overall = result.get("overall") or {}
    lines.append(
        "| {count} | {mean5} | {mean10} | {mean20} | {win20} | {median20} | {market} | {breadth} | {bullish} | {bearish} |".format(
            count=overall.get("count"),
            mean5=_fmt_pct(overall.get("mean5")),
            mean10=_fmt_pct(overall.get("mean10")),
            mean20=_fmt_pct(overall.get("mean20")),
            win20=_fmt_pct(overall.get("win20")),
            median20=_fmt_pct(overall.get("median20")),
            market=_fmt_pct(overall.get("mean_market_ret20")),
            breadth=_fmt_pct(overall.get("mean_breadth_ma20")),
            bullish=_fmt_pct(overall.get("mean_bullish_candle_prob")),
            bearish=_fmt_pct(overall.get("mean_bearish_candle_prob")),
        )
    )
    lines += [
        "",
        "## Path Breakdown",
        "",
        "| path_tag | count | mean5 | mean10 | mean20 | win20 | median20 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in result.get("path_breakdown") or []:
        lines.append(
            f"| {row.get('path_tag')} | {row.get('count')} | {_fmt_pct(row.get('mean5'))} | {_fmt_pct(row.get('mean10'))} | {_fmt_pct(row.get('mean20'))} | {_fmt_pct(row.get('win20'))} | {_fmt_pct(row.get('median20'))} |"
        )
    lines += [
        "",
        "## Bullish Candle Contrarian",
        "",
        "| count | mean5 | mean10 | mean20 | win20 | median20 |",
        "| ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    bullish = result.get("bullish_candle") or {}
    lines.append(
        "| {count} | {mean5} | {mean10} | {mean20} | {win20} | {median20} |".format(
            count=bullish.get("count"),
            mean5=_fmt_pct(bullish.get("mean5")),
            mean10=_fmt_pct(bullish.get("mean10")),
            mean20=_fmt_pct(bullish.get("mean20")),
            win20=_fmt_pct(bullish.get("win20")),
            median20=_fmt_pct(bullish.get("median20")),
        )
    )
    lines += [
        "",
        "## Contrarian / Continuation",
        "",
        "| subset | count | mean5 | mean10 | mean20 | win20 |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for key in ("contrarian_bullish", "breakdown_continuation", "failed_retest", "squeeze_loss"):
        subset = result.get(key) or {}
        lines.append(
            f"| {key} | {subset.get('count')} | {_fmt_pct(subset.get('mean5'))} | {_fmt_pct(subset.get('mean10'))} | {_fmt_pct(subset.get('mean20'))} | {_fmt_pct(subset.get('win20'))} |"
        )
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_json_report(result: dict[str, Any], output_path: Path) -> None:
    output_path.write_text(json.dumps(_jsonable(result), ensure_ascii=False, indent=2), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Relearn sell-side path structures and contrarian bullish setups")
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--bullish-candle-prob-min", type=float, default=0.55)
    parser.add_argument("--bearish-candle-prob-min", type=float, default=0.55)
    parser.add_argument("--weak-breadth-max", type=float, default=0.45)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument("--prefix", default="sell_path_relearn")
    args = parser.parse_args(argv)
    result = run_sell_path_relearn(
        config=SellPathConfig(
            lookback_days=int(args.lookback_days),
            report_dir=Path(args.report_dir),
            bullish_candle_prob_min=float(args.bullish_candle_prob_min),
            bearish_candle_prob_min=float(args.bearish_candle_prob_min),
            weak_breadth_max=float(args.weak_breadth_max),
        )
    )
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d%H%M%S")
    json_path = report_dir / f"{args.prefix}_{stamp}.json"
    md_path = report_dir / f"{args.prefix}_{stamp}.md"
    _write_json_report(result, json_path)
    _write_markdown_report(result, md_path)
    print(json.dumps({"ok": result.get("ok"), "json": str(json_path), "markdown": str(md_path)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
