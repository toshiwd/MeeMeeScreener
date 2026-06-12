from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from shared.runtime_stock_db_contract import resolve_runtime_stock_db_path


AXIS_ID = "short_downside_target_overlay_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_downside_target_overlay_v1")
DEFAULT_SOURCE_BOARD = Path(
    r"G:\Tradex\pre_crash_short_state_review_board_v1"
    r"\20260604T081011Z-pre_crash_short_state_review_board_v1"
    r"\state_review_board.json"
)
INVALIDATION_NOTE = "Review-only downside estimate. No new entry, exit, ranking, score, DB, MeeMee, or production behavior is changed."


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _run_id() -> str:
    return _utc_now().strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(val) for key, val in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value)
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
    except Exception:
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def _load_code_bars(db_path: Path, codes: set[str], min_ymd: int, max_ymd: int) -> dict[str, pd.DataFrame]:
    if not codes:
        return {}
    with duckdb.connect(str(db_path), read_only=True) as conn:
        rows = conn.execute(
            f"""
            WITH bars AS (
              SELECT
                code::VARCHAR AS code,
                CASE
                  WHEN date BETWEEN 19000101 AND 20991231 THEN CAST(date AS INTEGER)
                  WHEN date >= 1000000000000 THEN CAST(strftime(to_timestamp(date / 1000), '%Y%m%d') AS INTEGER)
                  WHEN date >= 315532800 THEN CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)
                  ELSE CAST(date AS INTEGER)
                END AS ymd,
                CAST(o AS DOUBLE) AS o,
                CAST(h AS DOUBLE) AS h,
                CAST(l AS DOUBLE) AS l,
                CAST(c AS DOUBLE) AS c,
                CAST(v AS DOUBLE) AS v,
                lower(coalesce(source, '')) AS source
              FROM daily_bars
              WHERE code IN ({", ".join("?" for _ in codes)})
                AND o IS NOT NULL AND h IS NOT NULL AND l IS NOT NULL AND c IS NOT NULL
                AND lower(coalesce(source, '')) = 'pan'
            )
            SELECT code, ymd, o, h, l, c, v
            FROM bars
            WHERE ymd BETWEEN ? AND ?
            ORDER BY code, ymd
            """,
            [*sorted(codes), int(min_ymd), int(max_ymd)],
        ).df()
    if rows.empty:
        return {}
    return {str(code): frame.reset_index(drop=True) for code, frame in rows.groupby("code", sort=False)}


def _add_context_features(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.sort_values("ymd").reset_index(drop=True).copy()
    c = out["c"].astype(float)
    h = out["h"].astype(float)
    l = out["l"].astype(float)
    o = out["o"].astype(float)
    v = out["v"].replace(0, float("nan")).astype(float)
    for window in (7, 20, 60, 100, 200):
        out[f"ma{window}"] = c.rolling(window).mean()
        out[f"ma{window}_slope5"] = out[f"ma{window}"] / out[f"ma{window}"].shift(5) - 1.0
    for window in (10, 20, 60, 120):
        out[f"prior_low_{window}"] = l.shift(1).rolling(window).min()
        out[f"prior_high_{window}"] = h.shift(1).rolling(window).max()
    span = (h - l).replace(0, float("nan"))
    out["body_ratio"] = (c - o).abs() / span
    out["upper_wick_ratio"] = (h - pd.concat([o, c], axis=1).max(axis=1)) / span
    out["lower_wick_ratio"] = (pd.concat([o, c], axis=1).min(axis=1) - l) / span
    out["close_pos"] = (c - l) / span
    out["is_red"] = c < o
    out["red_streak_5"] = out["is_red"].astype(float).rolling(5).sum()
    out["weak_close_streak_5"] = (out["close_pos"] <= 0.35).astype(float).rolling(5).sum()
    out["vol_ratio_20"] = v / v.rolling(20).mean()
    out["ret_1"] = c / c.shift(1) - 1.0
    out["ret_3"] = c / c.shift(3) - 1.0
    out["ret_5"] = c / c.shift(5) - 1.0
    prev_high = h.shift(1)
    up_gap = l > prev_high * 1.02
    out["up_gap_fill_price"] = prev_high.where(up_gap).ffill()
    out["up_gap_ymd"] = out["ymd"].where(up_gap).ffill()
    return out


def _last_swing_low(frame: pd.DataFrame, idx: int, lookback: int = 80) -> dict[str, Any] | None:
    start = max(1, idx - lookback)
    end = max(start, idx - 1)
    lows = frame["l"].astype(float)
    best: tuple[int, float] | None = None
    for pos in range(start, end):
        if pos + 1 >= len(frame):
            continue
        low = float(lows.iloc[pos])
        if low <= float(lows.iloc[pos - 1]) and low <= float(lows.iloc[pos + 1]):
            if best is None or low < best[1]:
                best = (pos, low)
    if best is None:
        return None
    pos, low = best
    return {"ymd": int(frame.iloc[pos]["ymd"]), "price": float(low)}


def _target_candidates(row: pd.Series, swing_low: dict[str, Any] | None) -> list[dict[str, Any]]:
    close = float(row["c"])
    levels: list[dict[str, Any]] = []
    for key, kind in [
        ("ma7", "moving_average"),
        ("ma20", "moving_average"),
        ("ma60", "moving_average"),
        ("ma100", "moving_average"),
        ("ma200", "moving_average"),
        ("prior_low_10", "prior_low"),
        ("prior_low_20", "prior_low"),
        ("prior_low_60", "prior_low"),
        ("prior_low_120", "prior_low"),
        ("up_gap_fill_price", "gap_fill"),
    ]:
        price = _safe_float(row.get(key))
        if price is None or price <= 0 or price >= close:
            continue
        levels.append(
            {
                "level_id": key,
                "level_type": kind,
                "price": price,
                "downside_pct_from_signal_close": close / price - 1.0,
            }
        )
    if swing_low and swing_low["price"] < close:
        levels.append(
            {
                "level_id": "last_swing_low_80",
                "level_type": "swing_low",
                "price": float(swing_low["price"]),
                "ymd": int(swing_low["ymd"]),
                "downside_pct_from_signal_close": close / float(swing_low["price"]) - 1.0,
            }
        )
    dedup: dict[tuple[str, int], dict[str, Any]] = {}
    for item in levels:
        dedup[(item["level_id"], int(round(float(item["price"]) * 100)))] = item
    return sorted(dedup.values(), key=lambda item: float(item["price"]), reverse=True)


def _momentum_score(row: pd.Series) -> tuple[float, list[str]]:
    score = 0.0
    reasons: list[str] = []
    body = _safe_float(row.get("body_ratio")) or 0.0
    close_pos = _safe_float(row.get("close_pos")) or 0.5
    vol_ratio = _safe_float(row.get("vol_ratio_20")) or 1.0
    ret3 = _safe_float(row.get("ret_3")) or 0.0
    ma20_slope = _safe_float(row.get("ma20_slope5")) or 0.0
    ma60_slope = _safe_float(row.get("ma60_slope5")) or 0.0
    if float(row["c"]) < float(row["o"]):
        score += 1.0
        reasons.append("red_signal_candle")
    if body >= 0.55:
        score += 1.0
        reasons.append("large_body")
    if close_pos <= 0.35:
        score += 1.0
        reasons.append("weak_close")
    if vol_ratio >= 1.5:
        score += 0.75
        reasons.append("high_volume_pressure")
    if ret3 <= -0.05:
        score += 0.75
        reasons.append("three_day_down_momentum")
    if ma20_slope < 0:
        score += 0.5
        reasons.append("ma20_turning_down")
    if ma60_slope < 0:
        score += 0.5
        reasons.append("ma60_turning_down")
    return score, reasons


def _choose_target(levels: list[dict[str, Any]], momentum_score: float) -> tuple[dict[str, Any] | None, str]:
    if not levels:
        return None, "no_lower_reference_level"
    if momentum_score >= 3.0:
        idx = min(2, len(levels) - 1)
        return levels[idx], "strong_momentum_allows_deeper_ladder_level"
    if momentum_score >= 1.5:
        idx = min(1, len(levels) - 1)
        return levels[idx], "moderate_momentum_allows_second_ladder_level"
    return levels[0], "weak_momentum_nearest_support_only"


def _risk_reward(signal_close: float, target: dict[str, Any] | None, stop_price: float | None) -> float | None:
    if target is None or stop_price is None or signal_close <= 0 or stop_price <= signal_close:
        return None
    reward = signal_close - float(target["price"])
    risk = stop_price - signal_close
    if risk <= 0:
        return None
    return reward / risk


def analyze_candidate(candidate: dict[str, Any], frame: pd.DataFrame | None, fallback_rank: int) -> dict[str, Any]:
    base = {
        "code": str(candidate.get("code")),
        "name": candidate.get("name"),
        "signal_ymd": candidate.get("signal_ymd"),
        "original_rank": candidate.get("original_rank") or fallback_rank,
        "original_score": candidate.get("original_score", candidate.get("rank_score")),
        "source_review_state": candidate.get("review_state"),
        "coverage_status": "missing_daily_bars",
        "expected_downside_pct": None,
        "expected_target_price": None,
        "target_quality": "unavailable",
        "target_reason": "missing_daily_bars",
        "target_ladder": [],
        "momentum": {},
        "ma_context": {},
        "support_context": {},
        "risk_reward_to_sl8": None,
        "review_actionability": "NeedsData",
        "invalidation_note": INVALIDATION_NOTE,
    }
    if frame is None or frame.empty:
        return base
    signal_ymd = int(candidate["signal_ymd"])
    enriched = _add_context_features(frame)
    matches = enriched.index[enriched["ymd"].astype(int) == signal_ymd].tolist()
    if not matches:
        base["coverage_status"] = "signal_ymd_not_found"
        base["target_reason"] = "signal_ymd_not_found"
        return base
    idx = matches[-1]
    row = enriched.iloc[idx]
    signal_close = float(row["c"])
    swing_low = _last_swing_low(enriched, idx)
    levels = _target_candidates(row, swing_low)
    momentum, momentum_reasons = _momentum_score(row)
    target, target_reason = _choose_target(levels, momentum)
    stop_price = _safe_float(candidate.get("stop_price_from_signal_close")) or signal_close * 1.08
    rr = _risk_reward(signal_close, target, stop_price)
    expected_downside = None if target is None else signal_close / float(target["price"]) - 1.0
    quality = "NoTarget"
    actionability = "AvoidNoTarget"
    if target is not None:
        if rr is not None and rr >= 1.5 and expected_downside is not None and expected_downside >= 0.08:
            quality = "DeepTarget"
            actionability = "DownsideReviewCandidate"
        elif rr is not None and rr >= 0.8 and expected_downside is not None and expected_downside >= 0.04:
            quality = "ShallowTarget"
            actionability = "ScalpOnlyReview"
        else:
            quality = "PoorReward"
            actionability = "AvoidPoorReward"
    base.update(
        {
            "coverage_status": "ready",
            "signal_close": signal_close,
            "expected_downside_pct": expected_downside,
            "expected_target_price": None if target is None else float(target["price"]),
            "target_quality": quality,
            "target_reason": target_reason,
            "target_ladder": levels,
            "momentum": {
                "score": momentum,
                "reasons": momentum_reasons,
                "body_ratio": _safe_float(row.get("body_ratio")),
                "close_pos": _safe_float(row.get("close_pos")),
                "vol_ratio_20": _safe_float(row.get("vol_ratio_20")),
                "ret_1": _safe_float(row.get("ret_1")),
                "ret_3": _safe_float(row.get("ret_3")),
                "ret_5": _safe_float(row.get("ret_5")),
                "red_streak_5": _safe_float(row.get("red_streak_5")),
                "weak_close_streak_5": _safe_float(row.get("weak_close_streak_5")),
            },
            "ma_context": {
                key: _safe_float(row.get(key))
                for key in (
                    "ma7",
                    "ma20",
                    "ma60",
                    "ma100",
                    "ma200",
                    "ma20_slope5",
                    "ma60_slope5",
                    "ma100_slope5",
                    "ma200_slope5",
                )
            },
            "support_context": {
                key: _safe_float(row.get(key))
                for key in (
                    "prior_low_10",
                    "prior_low_20",
                    "prior_low_60",
                    "prior_low_120",
                    "up_gap_fill_price",
                    "up_gap_ymd",
                )
            }
            | {"last_swing_low_80": swing_low},
            "risk_reward_to_sl8": rr,
            "review_actionability": actionability,
        }
    )
    return base


def _latest_state_board() -> Path:
    root = Path(r"G:\Tradex\pre_crash_short_state_review_board_v1")
    paths = [path for path in root.glob("*\\state_review_board.json") if path.is_file()]
    if not paths:
        return DEFAULT_SOURCE_BOARD
    return max(paths, key=lambda path: path.stat().st_mtime)


def _summary_markdown(payload: dict[str, Any]) -> str:
    counts = payload["counts"]
    lines = [
        "# Short Downside Target Overlay v1",
        "",
        f"- source_board_path: `{payload['source_board_path']}`",
        f"- db_path: `{payload['db_path']}`",
        f"- total_candidates: {counts['total_candidates']}",
        f"- ready_count: {counts['ready_count']}",
        f"- downside_review_candidate_count: {counts['downside_review_candidate_count']}",
        f"- scalp_only_review_count: {counts['scalp_only_review_count']}",
        f"- avoid_poor_reward_count: {counts['avoid_poor_reward_count']}",
        f"- missing_data_count: {counts['missing_data_count']}",
        "",
        "## Top Candidates",
        "",
        "| rank | code | signal | close | target | downside | rr_sl8 | actionability | reason |",
        "|---:|---|---:|---:|---:|---:|---:|---|---|",
    ]
    for item in payload["candidates"][:20]:
        downside = item.get("expected_downside_pct")
        rr = item.get("risk_reward_to_sl8")
        lines.append(
            f"| {item.get('original_rank')} | {item.get('code')} | {item.get('signal_ymd')} | "
            f"{item.get('signal_close')} | {item.get('expected_target_price')} | "
            f"{'' if downside is None else round(float(downside) * 100, 2)}% | "
            f"{'' if rr is None else round(float(rr), 2)} | {item.get('review_actionability')} | {item.get('target_reason')} |"
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- This is a review-only target overlay, not a trade recommendation.",
            "- Ranking, scoring, entry geometry, exit, runtime DB, MeeMee, and production behavior are unchanged.",
            "- Targets are source-backed reference levels: MAs, prior lows, recent swing low, and gap-fill price when available.",
        ]
    )
    return "\n".join(lines) + "\n"


def run(db_path: Path, output_root: Path, source_board_path: Path, code: str | None, signal_ymd: int | None) -> Path:
    run_dir = output_root / _run_id()
    board = _read_json(source_board_path) if source_board_path.exists() else {"candidates": []}
    candidates = list(board.get("candidates", []))
    if code:
        if signal_ymd is None:
            raise ValueError("--signal-ymd is required when --code is provided")
        candidates = [
            {
                "code": str(code),
                "signal_ymd": int(signal_ymd),
                "original_rank": 1,
                "original_score": None,
                "review_state": "ManualChartReview",
            }
        ]
    codes = {str(item["code"]) for item in candidates if item.get("code") is not None}
    signal_dates = [int(item["signal_ymd"]) for item in candidates if item.get("signal_ymd") is not None]
    min_ymd = min(signal_dates) - 20000 if signal_dates else 20150101
    max_ymd = max(signal_dates) if signal_dates else 20991231
    bars_by_code = _load_code_bars(db_path, codes, min_ymd, max_ymd)
    analyzed = [
        analyze_candidate(candidate, bars_by_code.get(str(candidate.get("code"))), idx)
        for idx, candidate in enumerate(candidates, start=1)
    ]
    action_counts = Counter(str(item.get("review_actionability")) for item in analyzed)
    coverage_counts = Counter(str(item.get("coverage_status")) for item in analyzed)
    payload = {
        "run_id": run_dir.name,
        "created_at": _utc_now().isoformat(),
        "source_board_path": str(source_board_path),
        "db_path": str(db_path),
        "axis_id": AXIS_ID,
        "counts": {
            "total_candidates": len(analyzed),
            "ready_count": int(coverage_counts.get("ready", 0)),
            "downside_review_candidate_count": int(action_counts.get("DownsideReviewCandidate", 0)),
            "scalp_only_review_count": int(action_counts.get("ScalpOnlyReview", 0)),
            "avoid_poor_reward_count": int(action_counts.get("AvoidPoorReward", 0)),
            "missing_data_count": len(analyzed) - int(coverage_counts.get("ready", 0)),
        },
        "target_model": {
            "reference_levels": ["ma7", "ma20", "ma60", "ma100", "ma200", "prior_low_10/20/60/120", "last_swing_low_80", "up_gap_fill_price"],
            "momentum_inputs": ["red candle", "body_ratio", "close_pos", "vol_ratio_20", "ret_3", "ma20_slope5", "ma60_slope5"],
            "risk_reference": "sl8 from signal_close",
            "review_only": True,
        },
        "candidates": analyzed,
        "authoritative_decision": "ready_downside_target_overlay_review_only",
        "runtime_db_write": False,
        "meemee_modified": False,
        "production_ranking_modified": False,
    }
    _write_json(run_dir / "short_downside_target_overlay.json", payload)
    (run_dir / "short_downside_target_overlay_summary.md").write_text(_summary_markdown(payload), encoding="utf-8")
    _write_json(
        run_dir / "_ARTIFACT_COMPLETE.json",
        {
            "status": "complete",
            "axis_id": AXIS_ID,
            "generated_at": _utc_now().isoformat(),
            "required_files": [
                "short_downside_target_overlay.json",
                "short_downside_target_overlay_summary.md",
                "_ARTIFACT_COMPLETE.json",
            ],
        },
    )
    return run_dir


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=resolve_runtime_stock_db_path())
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--source-board-path", type=Path, default=_latest_state_board())
    parser.add_argument("--code", default=None)
    parser.add_argument("--signal-ymd", type=int, default=None)
    args = parser.parse_args()
    print(run(args.db_path, args.output_root, args.source_board_path, args.code, args.signal_ymd))


if __name__ == "__main__":
    main()
