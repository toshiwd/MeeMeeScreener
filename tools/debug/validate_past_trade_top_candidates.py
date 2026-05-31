from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


@contextmanager
def _temporary_env(updates: dict[str, str]):
    previous = {key: os.environ.get(key) for key in updates}
    try:
        os.environ.update(updates)
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _safe_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except Exception:
        return None
    return parsed if parsed == parsed else None


def _ymd_to_iso(value: int | None) -> str | None:
    if value is None:
        return None
    text = f"{int(value):08d}"
    return f"{text[:4]}-{text[4:6]}-{text[6:8]}"


def _date_expr(column: str = "date") -> str:
    return f"""
        CASE
            WHEN {column} BETWEEN 19000101 AND 20991231 THEN CAST({column} AS INTEGER)
            WHEN {column} >= 1000000000 THEN CAST(strftime(to_timestamp({column}), '%Y%m%d') AS INTEGER)
            ELSE NULL
        END
    """


def _fetch_eval_dates(conn: duckdb.DuckDBPyConnection, *, lookback_dates: int, eval_step: int, horizon: int) -> list[int]:
    rows = conn.execute(
        f"""
        SELECT DISTINCT {_date_expr("date")} AS ymd
        FROM daily_bars
        WHERE COALESCE(source, 'pan') <> 'yahoo'
          AND {_date_expr("date")} IS NOT NULL
        ORDER BY ymd DESC
        LIMIT ?
        """,
        [int(lookback_dates) * int(eval_step) + int(horizon) + 20],
    ).fetchall()
    desc_dates = [int(row[0]) for row in rows if row and row[0] is not None]
    eligible = desc_dates[int(horizon) :]
    return sorted(eligible[:: max(1, int(eval_step))][: max(1, int(lookback_dates))])


def _bars_for_code(conn: duckdb.DuckDBPyConnection, code: str, *, as_of: int, before: int, after: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        f"""
        WITH ranked AS (
            SELECT
                code,
                {_date_expr("date")} AS ymd,
                CAST(o AS DOUBLE) AS o,
                CAST(h AS DOUBLE) AS h,
                CAST(l AS DOUBLE) AS l,
                CAST(c AS DOUBLE) AS c,
                CAST(v AS BIGINT) AS v,
                ROW_NUMBER() OVER (PARTITION BY code ORDER BY {_date_expr("date")}) AS rn
            FROM daily_bars
            WHERE COALESCE(source, 'pan') <> 'yahoo'
              AND {_date_expr("date")} IS NOT NULL
              AND code = ?
        ),
        anchor AS (
            SELECT rn
            FROM ranked
            WHERE ymd <= ?
            ORDER BY ymd DESC
            LIMIT 1
        )
        SELECT r.ymd, r.o, r.h, r.l, r.c, r.v, r.rn - a.rn AS rel
        FROM ranked r
        CROSS JOIN anchor a
        WHERE r.rn BETWEEN a.rn - ? AND a.rn + ?
        ORDER BY r.rn
        """,
        [str(code), int(as_of), int(before), int(after)],
    ).fetchall()
    return [
        {
            "date": int(row[0]),
            "open": _safe_float(row[1]),
            "high": _safe_float(row[2]),
            "low": _safe_float(row[3]),
            "close": _safe_float(row[4]),
            "volume": int(row[5] or 0),
            "rel": int(row[6]),
        }
        for row in rows
    ]


def _moving_average(values: list[float | None], window: int) -> list[float | None]:
    out: list[float | None] = []
    for idx in range(len(values)):
        if idx + 1 < window:
            out.append(None)
            continue
        sample = [value for value in values[idx + 1 - window : idx + 1] if value is not None]
        out.append(sum(sample) / len(sample) if len(sample) == window else None)
    return out


def _outcome_from_bars(bars: list[dict[str, Any]], *, side: str, horizon: int) -> dict[str, Any] | None:
    anchor = next((bar for bar in bars if bar["rel"] == 0), None)
    future = [bar for bar in bars if 0 < int(bar["rel"]) <= int(horizon)]
    if not anchor or len(future) < int(horizon):
        return None
    anchor_close = _safe_float(anchor.get("close"))
    final_close = _safe_float(future[-1].get("close"))
    lows = [_safe_float(bar.get("low")) for bar in future]
    highs = [_safe_float(bar.get("high")) for bar in future]
    lows = [value for value in lows if value is not None]
    highs = [value for value in highs if value is not None]
    if not anchor_close or final_close is None or not lows or not highs:
        return None
    raw_forward = (final_close - anchor_close) / anchor_close
    min_path = (min(lows) - anchor_close) / anchor_close
    max_path = (max(highs) - anchor_close) / anchor_close
    side_forward = raw_forward if side == "buy" else -raw_forward
    side_adverse = min_path if side == "buy" else -max_path
    side_favorable = max_path if side == "buy" else -min_path
    if side_forward <= -0.03 or side_adverse <= -0.05:
        prognosis = "bad"
    elif side_forward >= 0.03 or side_favorable >= 0.05:
        prognosis = "good"
    else:
        prognosis = "neutral"
    return {
        "anchor_close": anchor_close,
        "forward_close": final_close,
        "raw_forward_return": raw_forward,
        "side_forward_return": side_forward,
        "side_adverse_path_return": side_adverse,
        "side_favorable_path_return": side_favorable,
        "prognosis": prognosis,
    }


def _render_chart(path: Path, *, code: str, name: str, side: str, as_of: int, bars: list[dict[str, Any]], outcome: dict[str, Any]) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.patches import Rectangle

    path.parent.mkdir(parents=True, exist_ok=True)
    closes = [_safe_float(bar.get("close")) for bar in bars]
    ma7 = _moving_average(closes, 7)
    ma20 = _moving_average(closes, 20)
    ma60 = _moving_average(closes, 60)
    xs = list(range(len(bars)))
    anchor_idx = next((idx for idx, bar in enumerate(bars) if bar["rel"] == 0), None)
    fig, ax = plt.subplots(figsize=(13, 6), dpi=140)
    for idx, bar in enumerate(bars):
        o = _safe_float(bar.get("open"))
        h = _safe_float(bar.get("high"))
        l = _safe_float(bar.get("low"))
        c = _safe_float(bar.get("close"))
        if o is None or h is None or l is None or c is None:
            continue
        color = "#16a34a" if c >= o else "#dc2626"
        ax.vlines(idx, l, h, color=color, linewidth=1)
        body_low = min(o, c)
        body_height = max(abs(c - o), max(c, o) * 0.001)
        ax.add_patch(Rectangle((idx - 0.32, body_low), 0.64, body_height, facecolor=color, edgecolor=color, linewidth=0.8))
    for values, label, color in [(ma7, "MA7", "#ef4444"), (ma20, "MA20", "#22c55e"), (ma60, "MA60", "#3b82f6")]:
        ax.plot(xs, values, label=label, color=color, linewidth=1.2)
    if anchor_idx is not None:
        ax.axvline(anchor_idx, color="#111827", linewidth=1.2, linestyle="--")
        ax.axvspan(anchor_idx + 0.5, min(len(bars) - 1, anchor_idx + 10) + 0.5, color="#f59e0b", alpha=0.12)
    tick_step = max(1, len(bars) // 8)
    ax.set_xticks(xs[::tick_step])
    ax.set_xticklabels([_ymd_to_iso(bars[idx]["date"])[5:] for idx in xs[::tick_step]], rotation=0)
    ax.set_title(
        f"{side.upper()} {code} as_of={_ymd_to_iso(as_of)} "
        f"10d={outcome['side_forward_return']:.2%} adverse={outcome['side_adverse_path_return']:.2%} {outcome['prognosis']}"
    )
    ax.grid(True, color="#e5e7eb", linewidth=0.6)
    ax.legend(loc="upper left")
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def _compact_item(item: dict[str, Any], *, rank: int, side: str, as_of: int) -> dict[str, Any]:
    keys = [
        "code",
        "name",
        "tradePriorityScore",
        "tradeEntryClass",
        "setupType",
        "changePct",
        "candleUpperWickRatio",
        "candleLowerWickRatio",
        "distMa20Signed",
        "diff20_pct",
        "breakout20_up",
        "momentumFollowThroughScore",
        "monthlyBoxState",
        "monthlyBoxPos",
        "cnt_7_above",
        "cnt_20_above",
        "tradeEntryBlockReasons",
    ]
    return {"as_of": int(as_of), "as_of_iso": _ymd_to_iso(as_of), "side": side, "rank": rank, **{key: item.get(key) for key in keys}}


def run(args: argparse.Namespace) -> dict[str, Any]:
    db_path = Path(args.db_path).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    chart_dir = output_dir / "charts"
    with _temporary_env({"STOCKS_DB_PATH": str(db_path)}):
        from app.backend.services.ml import rankings_cache

        with duckdb.connect(str(db_path), read_only=True) as conn:
            eval_dates = [int(value) for value in args.dates.split(",") if value.strip()] if args.dates else _fetch_eval_dates(
                conn,
                lookback_dates=int(args.lookback_dates),
                eval_step=int(args.eval_step),
                horizon=int(args.horizon),
            )

        observations: list[dict[str, Any]] = []
        for as_of in eval_dates:
            for direction, side in [("up", "buy"), ("down", "short")]:
                payload = rankings_cache.get_rankings_asof(
                    "D",
                    "latest",
                    direction,
                    int(args.top_k),
                    as_of=int(as_of),
                    mode="trade",
                    risk_mode="balanced",
                )
                for idx, item in enumerate(list(payload.get("items") or [])[: int(args.top_k)], start=1):
                    row = _compact_item(item, rank=idx, side=side, as_of=int(as_of))
                    with duckdb.connect(str(db_path), read_only=True) as conn:
                        bars = _bars_for_code(conn, str(row["code"]), as_of=int(as_of), before=int(args.before_bars), after=int(args.horizon))
                    outcome = _outcome_from_bars(bars, side=side, horizon=int(args.horizon))
                    if not outcome:
                        row["missing_outcome_reason"] = "insufficient_future_or_anchor_bars"
                    else:
                        row.update(outcome)
                    observations.append(row)

        bad = [row for row in observations if row.get("prognosis") == "bad"]
        good = [row for row in observations if row.get("prognosis") == "good"]
        bad_sorted = sorted(bad, key=lambda row: (float(row.get("side_forward_return") or 0.0), float(row.get("side_adverse_path_return") or 0.0)))[: int(args.chart_examples)]
        good_sorted = sorted(good, key=lambda row: (-float(row.get("side_forward_return") or 0.0), -float(row.get("side_favorable_path_return") or 0.0)))[: int(args.chart_examples)]
        chart_rows = [("bad", row) for row in bad_sorted] + [("good", row) for row in good_sorted]
        for bucket, row in chart_rows:
            with duckdb.connect(str(db_path), read_only=True) as conn:
                bars = _bars_for_code(conn, str(row["code"]), as_of=int(row["as_of"]), before=int(args.before_bars), after=int(args.horizon))
            chart_path = chart_dir / bucket / f"{row['side']}_{row['as_of']}_{row['rank']:02d}_{row['code']}.png"
            _render_chart(
                chart_path,
                code=str(row["code"]),
                name=str(row.get("name") or ""),
                side=str(row["side"]),
                as_of=int(row["as_of"]),
                bars=bars,
                outcome=row,
            )
            row["chart_path"] = str(chart_path)

    counts = Counter(str(row.get("prognosis") or "missing") for row in observations)
    side_counts: dict[str, dict[str, int]] = {}
    for side in ["buy", "short"]:
        side_counts[side] = dict(Counter(str(row.get("prognosis") or "missing") for row in observations if row.get("side") == side))
    summary = {
        "observation_count": len(observations),
        "eval_dates": eval_dates,
        "top_k": int(args.top_k),
        "horizon_sessions": int(args.horizon),
        "prognosis_distribution": dict(counts),
        "prognosis_distribution_by_side": side_counts,
        "avg_side_forward_return_by_side": {
            side: (
                sum(float(row["side_forward_return"]) for row in observations if row.get("side") == side and row.get("side_forward_return") is not None)
                / max(1, len([row for row in observations if row.get("side") == side and row.get("side_forward_return") is not None]))
            )
            for side in ["buy", "short"]
        },
    }
    artifact = {
        "schema_version": "meemee_past_trade_top_candidate_validation_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_db_path": str(db_path),
        "scope": {
            "ranking_logic_changed": False,
            "runtime_db_mutated": False,
            "tf": "D",
            "mode": "trade",
            "risk_mode": "balanced",
        },
        "fixed_conditions": {
            "top_k": int(args.top_k),
            "horizon_sessions": int(args.horizon),
            "lookback_dates": int(args.lookback_dates),
            "eval_step": int(args.eval_step),
            "explicit_dates": args.dates or None,
        },
        "summary": summary,
        "bad_examples": bad_sorted,
        "good_examples": good_sorted,
        "observations": observations,
    }
    artifact_path = output_dir / "past_trade_top_candidate_validation.json"
    artifact_path.write_text(json.dumps(artifact, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"artifact_path": str(artifact_path), "chart_dir": str(chart_dir), "summary": summary}


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate past MeeMee trade top candidates with forward outcomes and chart PNGs.")
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--output-dir", default="artifacts/actionability/past-trade-top-validation-20260522")
    parser.add_argument("--dates", default="")
    parser.add_argument("--lookback-dates", type=int, default=12)
    parser.add_argument("--eval-step", type=int, default=10)
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--horizon", type=int, default=10)
    parser.add_argument("--before-bars", type=int, default=80)
    parser.add_argument("--chart-examples", type=int, default=8)
    result = run(parser.parse_args())
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
