from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable
from pathlib import Path
import sys

import duckdb

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.core.config import config


@dataclass
class SignalRow:
    code: str
    dt: int
    p_up: float
    p_down: float
    p_turn_up: float
    p_turn_down: float
    ev20_net: float
    sell_p_down: float
    sell_p_turn_down: float
    trend_down: bool
    trend_down_strict: bool
    short_score: float | None
    dist_ma20_signed: float | None
    ma20_slope: float | None
    ma60_slope: float | None
    short_win_10: bool


def clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return lo if value < lo else hi if value > hi else value


def blended_down_prob(row: SignalRow) -> float:
    ml_down = row.p_down
    return clamp((ml_down * 0.6) + (row.sell_p_down * 0.4))


def blended_turn_down(row: SignalRow) -> float:
    return clamp((row.p_turn_down * 0.5) + (row.sell_p_turn_down * 0.5))


def up_score(row: SignalRow) -> float:
    up_prob = clamp(row.p_up)
    turn_up = clamp(row.p_turn_up)
    ev_bias = clamp(row.ev20_net / 0.06, -1.0, 1.0)
    trend_penalty = 0.08 if row.trend_down_strict else (0.04 if row.trend_down else 0.0)
    return clamp(
        0.5 * up_prob
        + 0.18 * turn_up
        + 0.17 * (0.5 + ev_bias * 0.5)
        + 0.15 * 0.5
        - trend_penalty
    )


def down_score(row: SignalRow) -> float:
    down_prob = blended_down_prob(row)
    turn_down = blended_turn_down(row)
    ev_bias = clamp(row.ev20_net / 0.06, -1.0, 1.0)
    trend_boost = 1.0 if row.trend_down_strict else (0.7 if row.trend_down else 0.3)
    return clamp(
        0.45 * down_prob
        + 0.22 * turn_down
        + 0.18 * (0.5 - ev_bias * 0.5)
        + 0.1 * trend_boost
        + 0.05 * 0.5
    )


def range_score(row: SignalRow) -> float:
    up_prob = clamp(row.p_up)
    down_prob = blended_down_prob(row)
    turn_up = clamp(row.p_turn_up)
    turn_down = blended_turn_down(row)
    ev_bias = clamp(row.ev20_net / 0.06, -1.0, 1.0)
    return clamp(
        0.4 * (1.0 - abs(up_prob - down_prob))
        + 0.3 * min(turn_up, turn_down)
        + 0.3 * (1.0 - abs(ev_bias))
    )


def legacy_down_signal(row: SignalRow) -> bool:
    u = up_score(row)
    d = down_score(row)
    r = range_score(row)
    top_key, top_score = max((("up", u), ("down", d), ("range", r)), key=lambda item: item[1])
    return top_key == "down" and top_score >= 0.56


def tuned_down_signal(row: SignalRow) -> bool:
    down_prob = blended_down_prob(row)
    up_prob = clamp(row.p_up)
    turn_down = blended_turn_down(row)
    u = up_score(row)
    d = down_score(row)
    r = range_score(row)
    top_key, top_score = max((("up", u), ("down", d), ("range", r)), key=lambda item: item[1])
    _ = top_score

    bullish_structure = bool(
        (not row.trend_down)
        and (row.dist_ma20_signed is not None and row.dist_ma20_signed > 0.0)
        and (row.ma20_slope is not None and row.ma20_slope >= 0.0)
        and (row.ma60_slope is not None and row.ma60_slope >= 0.0)
    )
    short_score_norm = clamp((((row.short_score or 70.0) - 70.0) / 90.0), 0.0, 1.0)
    sell_signal_quality = clamp(
        0.38 * down_prob
        + 0.22 * turn_down
        + 0.14 * clamp((-row.ev20_net + 0.005) / 0.04, 0.0, 1.0)
        + 0.16 * (1.0 if row.trend_down_strict else (0.72 if row.trend_down else 0.2))
        + 0.1 * short_score_norm
        - 0.12 * (1.0 if bullish_structure else 0.0)
    )

    force_down_confirm = bool(
        (row.trend_down_strict and down_prob >= 0.58 and turn_down >= 0.56 and row.ev20_net <= 0.0)
        or (down_prob >= 0.68 and turn_down >= 0.64 and row.ev20_net <= -0.01)
    )
    down_confirm = bool(
        row.trend_down
        or row.trend_down_strict
        or (down_prob - up_prob) >= 0.10
        or down_prob >= 0.62
    )
    down_threshold = 0.56

    if force_down_confirm:
        return True
    if top_key != "down":
        return False
    return bool(top_score >= down_threshold and down_confirm and sell_signal_quality >= 0.52)


def metric(rows: Iterable[SignalRow], signal_fn) -> tuple[int, int, int, int, float, float, float]:
    tp = fp = tn = fn = 0
    for row in rows:
        pred = signal_fn(row)
        truth = bool(row.short_win_10)
        if pred and truth:
            tp += 1
        elif pred and not truth:
            fp += 1
        elif (not pred) and truth:
            fn += 1
        else:
            tn += 1
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = (2.0 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
    return tp, fp, tn, fn, precision, recall, f1


def load_rows() -> list[SignalRow]:
    with duckdb.connect(str(config.DB_PATH), read_only=True) as conn:
        raw = conn.execute(
            """
            SELECT
                s.code,
                CAST(s.dt AS INTEGER) AS dt,
                CAST(m.p_up AS DOUBLE) AS p_up,
                CAST(COALESCE(m.p_down, 1.0 - m.p_up) AS DOUBLE) AS p_down,
                CAST(COALESCE(m.p_turn_up, 0.5) AS DOUBLE) AS p_turn_up,
                CAST(COALESCE(m.p_turn_down, 0.5) AS DOUBLE) AS p_turn_down,
                CAST(m.ev20_net AS DOUBLE) AS ev20_net,
                CAST(s.p_down AS DOUBLE) AS sell_p_down,
                CAST(s.p_turn_down AS DOUBLE) AS sell_p_turn_down,
                CAST(COALESCE(s.trend_down, FALSE) AS BOOLEAN) AS trend_down,
                CAST(COALESCE(s.trend_down_strict, FALSE) AS BOOLEAN) AS trend_down_strict,
                CAST(s.short_score AS DOUBLE) AS short_score,
                CAST(s.dist_ma20_signed AS DOUBLE) AS dist_ma20_signed,
                CAST(s.ma20_slope AS DOUBLE) AS ma20_slope,
                CAST(s.ma60_slope AS DOUBLE) AS ma60_slope,
                CAST(s.short_win_10 AS BOOLEAN) AS short_win_10
            FROM sell_analysis_daily s
            JOIN ml_pred_20d m
              ON m.code = s.code
             AND m.dt = s.dt
            WHERE s.short_win_10 IS NOT NULL
              AND s.p_down IS NOT NULL
              AND s.p_turn_down IS NOT NULL
              AND m.p_up IS NOT NULL
              AND m.ev20_net IS NOT NULL
            """
        ).fetchall()
    return [SignalRow(*row) for row in raw]


def print_metrics(label: str, values: tuple[int, int, int, int, float, float, float]) -> None:
    tp, fp, tn, fn, precision, recall, f1 = values
    print(
        f"{label}: tp={tp} fp={fp} tn={tn} fn={fn} "
        f"precision={precision:.4f} recall={recall:.4f} f1={f1:.4f}"
    )


def show_symbol_timeline(rows: list[SignalRow], code: str, dt_from: int) -> None:
    print(f"--- {code} from {dt_from} ---")
    for row in rows:
        if row.code != code or row.dt < dt_from:
            continue
        old_down = legacy_down_signal(row)
        new_down = tuned_down_signal(row)
        if old_down == new_down:
            continue
        print(
            f"{row.dt}: legacy={'down' if old_down else 'neutral'} "
            f"-> tuned={'down' if new_down else 'neutral'} "
            f"(p_down={blended_down_prob(row):.3f}, p_up={row.p_up:.3f}, ev20={row.ev20_net:.3f})"
        )


def main() -> None:
    rows = load_rows()
    print(f"rows={len(rows)}")
    print_metrics("legacy", metric(rows, legacy_down_signal))
    print_metrics("tuned ", metric(rows, tuned_down_signal))
    show_symbol_timeline(rows, code="3110", dt_from=20260101)


if __name__ == "__main__":
    main()
