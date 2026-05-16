from __future__ import annotations

import argparse
import hashlib
import json
import os
import statistics
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import duckdb

from shared.tradex_storage import tradex_keep_path


SCHEMA_VERSION = "meemee_ranking_effectiveness_validation_v1"
DEFAULT_HORIZONS = (5, 10, 20)
DEFAULT_TOP_K = (5, 10, 20)
DEFAULT_TFS = ("D", "W", "M")
DEFAULT_DIRECTIONS = ("up", "down")
DEFAULT_MODE = "trade"
DEFAULT_RISK_MODE = "balanced"
DEFAULT_LOOKBACK_DATES = 120
DEFAULT_EVAL_STEP = 5
DEFAULT_REQUIRED_LATEST = 20260507
DEFAULT_MIN_EVAL_DATES = 20
MOMENTUM_CHALLENGER_ID = "momentum_follow_through_v1"


@dataclass(frozen=True)
class Surface:
    tf: str
    direction: str
    mode: str = DEFAULT_MODE
    risk_mode: str = DEFAULT_RISK_MODE

    @property
    def key(self) -> str:
        return f"{self.tf}_{self.direction}_{self.mode}_{self.risk_mode}"


@contextmanager
def _temporary_env(updates: dict[str, str]):
    previous = {key: os.environ.get(key) for key in updates}
    try:
        for key, value in updates.items():
            os.environ[key] = value
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ymd_to_iso(value: int | None) -> str | None:
    if value is None:
        return None
    text = f"{int(value):08d}"
    return f"{text[:4]}-{text[4:6]}-{text[6:8]}"


def _safe_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except Exception:
        return None
    return parsed if parsed == parsed else None


def _mean(values: Iterable[float]) -> float | None:
    clean = [float(value) for value in values if value == value]
    return float(sum(clean) / len(clean)) if clean else None


def _median(values: Iterable[float]) -> float | None:
    clean = [float(value) for value in values if value == value]
    return float(statistics.median(clean)) if clean else None


def _hash_sample(items: list[dict[str, Any]], *, sample_size: int, seed: str) -> list[dict[str, Any]]:
    return sorted(
        items,
        key=lambda row: hashlib.sha256(f"{seed}:{row.get('code')}".encode("utf-8")).hexdigest(),
    )[:sample_size]


def _normalize_daily_date_expr(column_name: str = "date") -> str:
    return f"""
        CASE
            WHEN {column_name} BETWEEN 19000101 AND 20991231 THEN CAST({column_name} AS INTEGER)
            WHEN {column_name} >= 1000000000000 THEN CAST(strftime(to_timestamp({column_name} / 1000), '%Y%m%d') AS INTEGER)
            WHEN {column_name} >= 1000000000 THEN CAST(strftime(to_timestamp({column_name}), '%Y%m%d') AS INTEGER)
            ELSE NULL
        END
    """


def _fetch_eval_dates(conn: duckdb.DuckDBPyConnection, *, max_horizon: int, lookback_dates: int, eval_step: int) -> list[int]:
    limit = max(1, int(lookback_dates)) * max(1, int(eval_step)) + int(max_horizon) + 20
    rows = conn.execute(
        f"""
        WITH dates AS (
            SELECT DISTINCT {_normalize_daily_date_expr("date")} AS ymd
            FROM daily_bars
            WHERE COALESCE(source, 'pan') <> 'yahoo'
        )
        SELECT ymd
        FROM dates
        WHERE ymd IS NOT NULL
        ORDER BY ymd DESC
        LIMIT ?
        """,
        [limit],
    ).fetchall()
    desc_dates = [int(row[0]) for row in rows if row and row[0] is not None]
    eligible = desc_dates[int(max_horizon) :]
    sampled_desc = eligible[:: max(1, int(eval_step))][: max(1, int(lookback_dates))]
    return sorted(sampled_desc)


def _latest_daily_date(conn: duckdb.DuckDBPyConnection) -> int | None:
    row = conn.execute(
        f"""
        SELECT MAX({_normalize_daily_date_expr("date")})
        FROM daily_bars
        WHERE COALESCE(source, 'pan') <> 'yahoo'
        """
    ).fetchone()
    return int(row[0]) if row and row[0] is not None else None


def _daily_universe_rows(conn: duckdb.DuckDBPyConnection, *, as_of: int, horizon: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        f"""
        WITH normalized AS (
            SELECT
                code,
                {_normalize_daily_date_expr("date")} AS ymd,
                CAST(c AS DOUBLE) AS close,
                CAST(h AS DOUBLE) AS high,
                CAST(l AS DOUBLE) AS low
            FROM daily_bars
            WHERE COALESCE(source, 'pan') <> 'yahoo'
              AND c IS NOT NULL
        ),
        ranked AS (
            SELECT
                code,
                ymd,
                close,
                high,
                low,
                ROW_NUMBER() OVER (PARTITION BY code ORDER BY ymd) AS rn
            FROM normalized
            WHERE ymd IS NOT NULL
        ),
        anchor AS (
            SELECT code, rn AS anchor_rn, close AS anchor_close
            FROM ranked
            WHERE ymd <= ?
            QUALIFY ROW_NUMBER() OVER (PARTITION BY code ORDER BY ymd DESC) = 1
        ),
        future AS (
            SELECT
                a.code,
                a.anchor_close,
                f.close AS forward_close,
                MIN(p.low) AS min_low,
                MAX(p.high) AS max_high
            FROM anchor a
            JOIN ranked f
              ON f.code = a.code AND f.rn = a.anchor_rn + ?
            JOIN ranked p
              ON p.code = a.code AND p.rn > a.anchor_rn AND p.rn <= a.anchor_rn + ?
            WHERE a.anchor_close IS NOT NULL AND a.anchor_close > 0
              AND f.close IS NOT NULL
            GROUP BY a.code, a.anchor_close, f.close
        ),
        trailing_lookup AS (
            SELECT
                a.code,
                p20.close AS trailing_20_close
            FROM anchor a
            LEFT JOIN ranked p20
              ON p20.code = a.code AND p20.rn = a.anchor_rn - 20
        )
        SELECT
            f.code,
            (f.forward_close - f.anchor_close) / f.anchor_close AS forward_return,
            (f.min_low - f.anchor_close) / f.anchor_close AS min_path_return,
            (f.max_high - f.anchor_close) / f.anchor_close AS max_path_return,
            CASE
              WHEN t.trailing_20_close IS NOT NULL AND t.trailing_20_close > 0
              THEN (f.anchor_close - t.trailing_20_close) / t.trailing_20_close
              ELSE NULL
            END AS trailing_20_return
        FROM future f
        LEFT JOIN trailing_lookup t ON t.code = f.code
        """,
        [int(as_of), int(horizon), int(horizon)],
    ).fetchall()
    return [
        {
            "code": str(row[0]),
            "forward_return": float(row[1]),
            "min_path_return": _safe_float(row[2]),
            "max_path_return": _safe_float(row[3]),
            "trailing_20_return": _safe_float(row[4]),
        }
        for row in rows
        if row and row[0] is not None and row[1] is not None
    ]


def _metrics(rows: list[dict[str, Any]], *, direction: str) -> dict[str, Any]:
    if direction == "down":
        favorable = [-float(row["forward_return"]) for row in rows]
        adverse = [float(row.get("max_path_return") or 0.0) for row in rows]
        hit = [float(row["forward_return"]) < 0.0 for row in rows]
    else:
        favorable = [float(row["forward_return"]) for row in rows]
        adverse = [float(row.get("min_path_return") or 0.0) for row in rows]
        hit = [float(row["forward_return"]) > 0.0 for row in rows]
    return {
        "count": len(rows),
        "mean_favorable_return": _mean(favorable),
        "median_favorable_return": _median(favorable),
        "hit_rate": _mean([1.0 if value else 0.0 for value in hit]),
        "bad_pick_rate": _mean([1.0 if value < 0.0 else 0.0 for value in favorable]),
        "mean_raw_forward_return": _mean([float(row["forward_return"]) for row in rows]),
        "median_raw_forward_return": _median([float(row["forward_return"]) for row in rows]),
        "mean_adverse_path_return": _mean(adverse),
    }


def _select_momentum_baseline(universe_rows: list[dict[str, Any]], *, direction: str, k: int) -> list[dict[str, Any]]:
    rows = [row for row in universe_rows if row.get("trailing_20_return") is not None]
    reverse = direction == "up"
    return sorted(rows, key=lambda row: float(row.get("trailing_20_return") or 0.0), reverse=reverse)[:k]


def _momentum_candidate_rows(conn: duckdb.DuckDBPyConnection, *, as_of: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        f"""
        WITH normalized AS (
            SELECT
                code,
                {_normalize_daily_date_expr("date")} AS ymd,
                CAST(c AS DOUBLE) AS close,
                CAST(h AS DOUBLE) AS high,
                CAST(l AS DOUBLE) AS low,
                CAST(v AS DOUBLE) AS volume
            FROM daily_bars
            WHERE COALESCE(source, 'pan') <> 'yahoo'
              AND c IS NOT NULL
        ),
        ranked AS (
            SELECT
                code,
                ymd,
                close,
                high,
                low,
                volume,
                ROW_NUMBER() OVER (PARTITION BY code ORDER BY ymd) AS rn
            FROM normalized
            WHERE ymd IS NOT NULL
        ),
        anchor AS (
            SELECT code, rn AS anchor_rn, close AS anchor_close
            FROM ranked
            WHERE ymd <= ?
            QUALIFY ROW_NUMBER() OVER (PARTITION BY code ORDER BY ymd DESC) = 1
        ),
        features AS (
            SELECT
                a.code,
                a.anchor_close,
                p5.close AS close_5,
                p10.close AS close_10,
                p20.close AS close_20,
                MAX(recent.high) AS high_20,
                MIN(recent.low) AS low_10,
                AVG(recent.volume) AS volume_20,
                AVG(prevvol.volume) AS prev_volume_20
            FROM anchor a
            JOIN ranked cur ON cur.code = a.code AND cur.rn = a.anchor_rn
            LEFT JOIN ranked p5 ON p5.code = a.code AND p5.rn = a.anchor_rn - 5
            LEFT JOIN ranked p10 ON p10.code = a.code AND p10.rn = a.anchor_rn - 10
            LEFT JOIN ranked p20 ON p20.code = a.code AND p20.rn = a.anchor_rn - 20
            LEFT JOIN ranked recent ON recent.code = a.code AND recent.rn > a.anchor_rn - 20 AND recent.rn <= a.anchor_rn
            LEFT JOIN ranked prevvol ON prevvol.code = a.code AND prevvol.rn > a.anchor_rn - 40 AND prevvol.rn <= a.anchor_rn - 20
            WHERE a.anchor_close IS NOT NULL AND a.anchor_close > 0
            GROUP BY a.code, a.anchor_close, p5.close, p10.close, p20.close
        )
        SELECT
            code,
            CASE WHEN close_5 IS NOT NULL AND close_5 > 0 THEN (anchor_close - close_5) / close_5 ELSE NULL END AS ret5,
            CASE WHEN close_10 IS NOT NULL AND close_10 > 0 THEN (anchor_close - close_10) / close_10 ELSE NULL END AS ret10,
            CASE WHEN close_20 IS NOT NULL AND close_20 > 0 THEN (anchor_close - close_20) / close_20 ELSE NULL END AS ret20,
            CASE WHEN high_20 IS NOT NULL AND high_20 > 0 THEN anchor_close / high_20 ELSE NULL END AS high20_pos,
            CASE WHEN low_10 IS NOT NULL AND low_10 > 0 THEN (anchor_close - low_10) / low_10 ELSE NULL END AS low10_lift,
            CASE WHEN prev_volume_20 IS NOT NULL AND prev_volume_20 > 0 THEN volume_20 / prev_volume_20 ELSE NULL END AS volume_ratio
        FROM features
        """,
        [int(as_of)],
    ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        ret5 = _safe_float(row[1])
        ret10 = _safe_float(row[2])
        ret20 = _safe_float(row[3])
        high20_pos = _safe_float(row[4])
        low10_lift = _safe_float(row[5])
        volume_ratio = _safe_float(row[6])
        if ret5 is None or ret10 is None or ret20 is None or high20_pos is None:
            continue
        if ret20 <= 0.03 or ret10 <= 0.01 or ret5 <= -0.02:
            continue
        score = (
            1.40 * ret20
            + 0.90 * ret10
            + 0.55 * ret5
            + 0.08 * max(0.0, min(1.0, (high20_pos - 0.92) / 0.08))
            + 0.03 * max(0.0, min(2.0, (volume_ratio or 1.0) - 1.0))
            - 0.04 * max(0.0, ret5 - 0.12)
            - 0.03 * max(0.0, (low10_lift or 0.0) - 0.24)
        )
        out.append(
            {
                "code": str(row[0]),
                "momentum_score": float(score),
                "ret5": ret5,
                "ret10": ret10,
                "ret20": ret20,
                "high20_pos": high20_pos,
                "volume_ratio": volume_ratio,
            }
        )
    return sorted(out, key=lambda item: (-float(item["momentum_score"]), item["code"]))


def _select_momentum_challenger(
    conn: duckdb.DuckDBPyConnection,
    *,
    as_of: int,
    universe_by_code: dict[str, dict[str, Any]],
    k: int,
) -> list[dict[str, Any]]:
    ranked = _momentum_candidate_rows(conn, as_of=as_of)
    selected: list[dict[str, Any]] = []
    for item in ranked:
        row = universe_by_code.get(str(item["code"]))
        if row is None:
            continue
        merged = dict(row)
        merged["challenger_features"] = item
        selected.append(merged)
        if len(selected) >= k:
            break
    return selected


def _ranking_item_summary(item: dict[str, Any], *, rank: int) -> dict[str, Any]:
    return {
        "rank": rank,
        "code": str(item.get("code") or ""),
        "name": item.get("name"),
        "asOf": item.get("asOf"),
        "changePct": item.get("changePct"),
        "tradePriorityScore": item.get("tradePriorityScore"),
        "tradePriorityProfitScore": item.get("tradePriorityProfitScore"),
        "tradePriorityHitScore": item.get("tradePriorityHitScore"),
        "entryScore": item.get("entryScore"),
        "probSide": item.get("probSide"),
        "setupType": item.get("setupType"),
        "entryQualified": item.get("entryQualified"),
    }


def _surface_inventory_entry(surface: Surface, payload: dict[str, Any]) -> dict[str, Any]:
    items = list(payload.get("items") or [])
    return {
        "surface": surface.key,
        "tf": surface.tf,
        "direction": surface.direction,
        "mode": surface.mode,
        "risk_mode": surface.risk_mode,
        "item_count": len(items),
        "requested_as_of": payload.get("requested_as_of"),
        "snapshot_as_of": payload.get("snapshot_as_of"),
        "freshness_state": payload.get("freshness_state"),
        "candidate_source": payload.get("candidate_source"),
        "top5": [_ranking_item_summary(item, rank=index + 1) for index, item in enumerate(items[:5])],
        "top10_codes": [str(item.get("code") or "") for item in items[:10]],
        "top20_codes": [str(item.get("code") or "") for item in items[:20]],
    }


def _decide_side(compare_payload: dict[str, Any], *, side: str) -> dict[str, Any]:
    k_rows = compare_payload.get("k_summary") if isinstance(compare_payload.get("k_summary"), list) else []
    keep_hits = 0
    weak_hits = 0
    blockers: list[str] = []
    for row in k_rows:
        horizon20 = row.get("horizon_20") if isinstance(row.get("horizon_20"), dict) else {}
        selected = horizon20.get("selected") if isinstance(horizon20.get("selected"), dict) else {}
        universe = horizon20.get("universe") if isinstance(horizon20.get("universe"), dict) else {}
        momentum = horizon20.get("momentum_baseline") if isinstance(horizon20.get("momentum_baseline"), dict) else {}
        selected_mean = selected.get("mean_favorable_return")
        universe_mean = universe.get("mean_favorable_return")
        momentum_mean = momentum.get("mean_favorable_return")
        if selected_mean is None or universe_mean is None or momentum_mean is None:
            blockers.append(f"top{row.get('top_k')}:insufficient_horizon20_metrics")
            continue
        beats_universe = float(selected_mean) > float(universe_mean)
        beats_momentum = float(selected_mean) > float(momentum_mean)
        if beats_universe and beats_momentum:
            keep_hits += 1
        elif beats_universe or beats_momentum:
            weak_hits += 1
    if keep_hits >= 2:
        decision = "keep"
        reason = f"{side}_topk_beats_universe_and_momentum"
    elif keep_hits + weak_hits >= 1:
        decision = "hold"
        reason = f"{side}_partial_or_unstable_edge"
    else:
        decision = "drop"
        reason = f"{side}_does_not_beat_baselines"
    return {
        "decision": decision,
        "reason": reason,
        "topk_keep_hit_count": keep_hits,
        "topk_partial_hit_count": weak_hits,
        "blockers": blockers,
    }


def _decide_challenger(compare_payload: dict[str, Any]) -> dict[str, Any]:
    k_rows = compare_payload.get("k_summary") if isinstance(compare_payload.get("k_summary"), list) else []
    top5 = next((row for row in k_rows if int(row.get("top_k") or 0) == 5), None)
    if not isinstance(top5, dict):
        return {"decision": "not-yet-reportable", "reason": "missing_top5_challenger_metrics"}
    keep_hits = 0
    partial_hits = 0
    blockers: list[str] = []
    for horizon in DEFAULT_HORIZONS:
        row = top5.get(f"horizon_{horizon}") if isinstance(top5.get(f"horizon_{horizon}"), dict) else {}
        selected = row.get("selected") if isinstance(row.get("selected"), dict) else {}
        momentum = row.get("momentum_baseline") if isinstance(row.get("momentum_baseline"), dict) else {}
        challenger = row.get("challenger") if isinstance(row.get("challenger"), dict) else {}
        selected_mean = selected.get("mean_favorable_return")
        momentum_mean = momentum.get("mean_favorable_return")
        challenger_mean = challenger.get("mean_favorable_return")
        if challenger_mean is None or selected_mean is None or momentum_mean is None:
            blockers.append(f"horizon{horizon}:insufficient_challenger_metrics")
            continue
        beats_current = float(challenger_mean) > float(selected_mean)
        beats_momentum = float(challenger_mean) > float(momentum_mean)
        if beats_current and beats_momentum:
            keep_hits += 1
        elif beats_current or beats_momentum:
            partial_hits += 1
    if keep_hits >= 2:
        return {
            "decision": "keep",
            "reason": "momentum_follow_through_top5_beats_current_and_momentum",
            "horizon_keep_hit_count": keep_hits,
            "horizon_partial_hit_count": partial_hits,
            "blockers": blockers,
        }
    if keep_hits + partial_hits >= 1:
        return {
            "decision": "hold",
            "reason": "momentum_follow_through_partial_edge",
            "horizon_keep_hit_count": keep_hits,
            "horizon_partial_hit_count": partial_hits,
            "blockers": blockers,
        }
    return {
        "decision": "drop",
        "reason": "momentum_follow_through_does_not_beat_current_and_momentum",
        "horizon_keep_hit_count": keep_hits,
        "horizon_partial_hit_count": partial_hits,
        "blockers": blockers,
    }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _build_output_dir(output_root: Path | None, *, run_id: str) -> Path:
    root = output_root or tradex_keep_path("ranking_effectiveness")
    out = root / run_id
    out.mkdir(parents=True, exist_ok=True)
    return out


def run_validation(
    *,
    db_path: str | Path,
    output_root: str | Path | None = None,
    required_latest_ymd: int = DEFAULT_REQUIRED_LATEST,
    lookback_dates: int = DEFAULT_LOOKBACK_DATES,
    eval_step: int = DEFAULT_EVAL_STEP,
    min_eval_dates: int = DEFAULT_MIN_EVAL_DATES,
    timeframes: tuple[str, ...] = DEFAULT_TFS,
    directions: tuple[str, ...] = DEFAULT_DIRECTIONS,
    horizons: tuple[int, ...] = DEFAULT_HORIZONS,
    top_k_values: tuple[int, ...] = DEFAULT_TOP_K,
) -> dict[str, Any]:
    resolved_db = Path(db_path).expanduser().resolve(strict=False)
    if not resolved_db.exists():
        raise FileNotFoundError(f"snapshot DB not found: {resolved_db}")

    run_id = f"ranking_effectiveness_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    output_dir = _build_output_dir(Path(output_root).expanduser().resolve() if output_root else None, run_id=run_id)

    with _temporary_env({"STOCKS_DB_PATH": str(resolved_db)}):
        from app.backend.services import codex_bridge_service
        from app.backend.services.ml import rankings_cache

        with duckdb.connect(str(resolved_db), read_only=True) as conn:
            latest_ymd = _latest_daily_date(conn)
            max_horizon = max(horizons)
            eval_dates = _fetch_eval_dates(
                conn,
                max_horizon=max_horizon,
                lookback_dates=lookback_dates,
                eval_step=eval_step,
            )

        runtime_status = codex_bridge_service.get_runtime_stock_db_status()
        freshness_by_surface: dict[str, Any] = {}
        selected_timeframes = tuple(tf.upper() for tf in timeframes if str(tf).upper() in DEFAULT_TFS) or DEFAULT_TFS
        selected_directions = tuple(direction.lower() for direction in directions if str(direction).lower() in DEFAULT_DIRECTIONS) or DEFAULT_DIRECTIONS
        surfaces = [Surface(tf=tf, direction=direction) for tf in selected_timeframes for direction in selected_directions]
        latest_asof = latest_ymd

        not_reportable_reasons: list[str] = []
        if latest_ymd is None:
            not_reportable_reasons.append("snapshot_daily_bars_empty")
        elif int(latest_ymd) < int(required_latest_ymd):
            not_reportable_reasons.append(f"snapshot_latest_date_before_required:{latest_ymd}<{required_latest_ymd}")
        if not eval_dates:
            not_reportable_reasons.append("no_eval_dates_with_forward_horizon")
        elif len(eval_dates) < int(min_eval_dates):
            not_reportable_reasons.append(f"insufficient_eval_dates:{len(eval_dates)}<{int(min_eval_dates)}")

        surface_inventory_items: list[dict[str, Any]] = []
        if latest_asof is not None:
            for surface in surfaces:
                freshness_by_surface[surface.key] = codex_bridge_service.get_rankings_freshness(
                    tf=surface.tf,
                    which="latest",
                    direction=surface.direction,
                    mode=surface.mode,
                    risk_mode=surface.risk_mode,
                    limit=50,
                )
                payload = rankings_cache.get_rankings_asof(
                    surface.tf,
                    "latest",
                    surface.direction,
                    20,
                    as_of=latest_asof,
                    mode=surface.mode,
                    risk_mode=surface.risk_mode,
                )
                surface_inventory_items.append(_surface_inventory_entry(surface, payload))

        surface_inventory = {
            "schema_version": SCHEMA_VERSION,
            "generated_at": _utc_now_iso(),
            "source_db_path": str(resolved_db),
            "runtime_stock_db_status": runtime_status,
            "required_latest_ymd": int(required_latest_ymd),
            "latest_daily_ymd": latest_ymd,
            "latest_daily_iso": _ymd_to_iso(latest_ymd),
            "eval_dates": eval_dates,
            "surfaces": surface_inventory_items,
            "rankings_freshness": freshness_by_surface,
            "not_reportable_reasons": not_reportable_reasons,
        }

        buy_compare = _build_side_compare(
            db_path=resolved_db,
            rankings_cache=rankings_cache,
            eval_dates=eval_dates,
            direction="up",
            timeframes=selected_timeframes,
            horizons=horizons,
            top_k_values=top_k_values,
        )
        sell_compare = _build_side_compare(
            db_path=resolved_db,
            rankings_cache=rankings_cache,
            eval_dates=eval_dates,
            direction="down",
            timeframes=selected_timeframes,
            horizons=horizons,
            top_k_values=top_k_values,
        )
        trend_compare = {
            "schema_version": SCHEMA_VERSION,
            "generated_at": _utc_now_iso(),
            "source_db_path": str(resolved_db),
            "summary": {
                "buy_vs_momentum": buy_compare.get("momentum_baseline_summary"),
                "sell_vs_momentum": sell_compare.get("momentum_baseline_summary"),
            },
        }

        buy_decision = {"decision": "not-yet-reportable", "reason": "freshness_or_horizon_gate_failed"} if not_reportable_reasons else _decide_side(buy_compare, side="buy")
        sell_decision = {"decision": "not-yet-reportable", "reason": "freshness_or_horizon_gate_failed"} if not_reportable_reasons else _decide_side(sell_compare, side="sell")
        challenger_decision = {"decision": "not-yet-reportable", "reason": "freshness_or_horizon_gate_failed"} if not_reportable_reasons else _decide_challenger(buy_compare)
        if not_reportable_reasons:
            overall_decision = "not-yet-reportable"
            overall_reason = "freshness_or_horizon_gate_failed"
        elif challenger_decision["decision"] == "keep":
            overall_decision = "keep"
            overall_reason = "momentum_challenger_keep"
        elif buy_decision["decision"] == "keep" and sell_decision["decision"] == "keep":
            overall_decision = "keep"
            overall_reason = "buy_and_sell_rankings_both_keep"
        elif "drop" == buy_decision["decision"] == sell_decision["decision"]:
            overall_decision = "drop"
            overall_reason = "buy_and_sell_rankings_both_drop"
        else:
            overall_decision = "hold"
            overall_reason = "mixed_or_one_sided_ranking_edge"

        decision_payload = {
            "schema_version": SCHEMA_VERSION,
            "generated_at": _utc_now_iso(),
            "source_db_path": str(resolved_db),
            "authoritative_rollup_decision": overall_decision,
            "decision_reason": overall_reason,
            "candidate_local_decision": {
                "buy": buy_decision,
                "sell": sell_decision,
                "momentum_follow_through_v1": challenger_decision,
            },
            "not_reportable_reasons": not_reportable_reasons,
            "fixed_evaluation_conditions": {
                "mode": DEFAULT_MODE,
                "risk_mode": DEFAULT_RISK_MODE,
                "timeframes": list(DEFAULT_TFS),
                "validated_timeframes": list(selected_timeframes),
                "directions": list(selected_directions),
                "top_k_values": list(top_k_values),
                "horizons": list(horizons),
                "lookback_dates": int(lookback_dates),
                "eval_step": int(eval_step),
                "min_eval_dates": int(min_eval_dates),
                "source_db": "snapshot_only",
                "live_db_queries": False,
            },
            "artifact_refs": {},
            "remaining_risks": [
                "JPX/TDNET listing-status normalization still needed",
                "static operational overrides remain temporary",
                "historical validation depends on available confirmed daily bars in the snapshot",
            ],
        }

    paths = {
        "ranking_surface_inventory": output_dir / "ranking_surface_inventory.json",
        "ranking_effectiveness_buy_compare": output_dir / "ranking_effectiveness_buy_compare.json",
        "ranking_effectiveness_sell_compare": output_dir / "ranking_effectiveness_sell_compare.json",
        "trend_following_baseline_compare": output_dir / "trend_following_baseline_compare.json",
        "authoritative_decision": output_dir / "authoritative_decision.mimimi_ranking_effectiveness.json",
    }
    decision_payload["artifact_refs"] = {key: str(path) for key, path in paths.items()}
    _write_json(paths["ranking_surface_inventory"], surface_inventory)
    _write_json(paths["ranking_effectiveness_buy_compare"], buy_compare)
    _write_json(paths["ranking_effectiveness_sell_compare"], sell_compare)
    _write_json(paths["trend_following_baseline_compare"], trend_compare)
    _write_json(paths["authoritative_decision"], decision_payload)
    return {
        "ok": True,
        "output_dir": str(output_dir),
        "artifact_refs": {key: str(path) for key, path in paths.items()},
        "authoritative_rollup_decision": decision_payload["authoritative_rollup_decision"],
        "decision_reason": decision_payload["decision_reason"],
        "not_reportable_reasons": not_reportable_reasons,
    }


def _build_side_compare(
    *,
    db_path: Path,
    rankings_cache: Any,
    eval_dates: list[int],
    direction: str,
    timeframes: tuple[str, ...],
    horizons: tuple[int, ...],
    top_k_values: tuple[int, ...],
) -> dict[str, Any]:
    surfaces = [Surface(tf=tf, direction=direction) for tf in timeframes]
    observations: list[dict[str, Any]] = []
    aggregate: dict[tuple[int, int], dict[str, list[dict[str, Any]]]] = {
        (top_k, horizon): {"selected": [], "universe": [], "random": [], "momentum": [], "challenger": []}
        for top_k in top_k_values
        for horizon in horizons
    }
    universe_cache: dict[tuple[int, int], list[dict[str, Any]]] = {}
    for as_of in eval_dates:
        payload_by_tf = {
            surface.tf: rankings_cache.get_rankings_asof(
                surface.tf,
                "latest",
                direction,
                max(top_k_values),
                as_of=as_of,
                mode=surface.mode,
                risk_mode=surface.risk_mode,
            )
            for surface in surfaces
        }
        with duckdb.connect(str(db_path), read_only=True) as conn:
            for horizon in horizons:
                universe_rows = universe_cache.setdefault(
                    (as_of, horizon),
                    _daily_universe_rows(conn, as_of=as_of, horizon=horizon),
                )
                universe_by_code = {row["code"]: row for row in universe_rows}
                challenger_cache: dict[int, list[dict[str, Any]]] = {}
                for surface in surfaces:
                    items = list(payload_by_tf[surface.tf].get("items") or [])
                    for top_k in top_k_values:
                        codes = [str(item.get("code") or "") for item in items[:top_k]]
                        selected_rows = [universe_by_code[code] for code in codes if code in universe_by_code]
                        random_rows = _hash_sample(universe_rows, sample_size=len(selected_rows) or top_k, seed=f"{as_of}:{surface.key}:{top_k}:{horizon}")
                        momentum_rows = _select_momentum_baseline(universe_rows, direction=direction, k=len(selected_rows) or top_k)
                        challenger_rows: list[dict[str, Any]] = []
                        if direction == "up" and surface.tf == "D":
                            challenger_rows = challenger_cache.setdefault(
                                top_k,
                                _select_momentum_challenger(
                                    conn,
                                    as_of=as_of,
                                    universe_by_code=universe_by_code,
                                    k=len(selected_rows) or top_k,
                                ),
                            )
                        aggregate[(top_k, horizon)]["selected"].extend(selected_rows)
                        aggregate[(top_k, horizon)]["universe"].extend(universe_rows)
                        aggregate[(top_k, horizon)]["random"].extend(random_rows)
                        aggregate[(top_k, horizon)]["momentum"].extend(momentum_rows)
                        aggregate[(top_k, horizon)]["challenger"].extend(challenger_rows)
                        observations.append(
                            {
                                "as_of": as_of,
                                "as_of_iso": _ymd_to_iso(as_of),
                                "surface": surface.key,
                                "horizon": horizon,
                                "top_k": top_k,
                                "selected_count": len(selected_rows),
                                "ranking_codes": codes,
                                "missing_forward_codes": [code for code in codes if code not in universe_by_code],
                                "selected": _metrics(selected_rows, direction=direction),
                                "universe": _metrics(universe_rows, direction=direction),
                                "random_baseline": _metrics(random_rows, direction=direction),
                                "momentum_baseline": _metrics(momentum_rows, direction=direction),
                                "challenger": _metrics(challenger_rows, direction=direction) if challenger_rows else None,
                                "challenger_id": MOMENTUM_CHALLENGER_ID if challenger_rows else None,
                            }
                        )

    k_summary: list[dict[str, Any]] = []
    momentum_summary: list[dict[str, Any]] = []
    for top_k in top_k_values:
        row: dict[str, Any] = {"top_k": top_k}
        for horizon in horizons:
            bucket = aggregate[(top_k, horizon)]
            horizon_payload = {
                "selected": _metrics(bucket["selected"], direction=direction),
                "universe": _metrics(bucket["universe"], direction=direction),
                "random_baseline": _metrics(bucket["random"], direction=direction),
                "momentum_baseline": _metrics(bucket["momentum"], direction=direction),
                "challenger": _metrics(bucket["challenger"], direction=direction),
                "challenger_id": MOMENTUM_CHALLENGER_ID if bucket["challenger"] else None,
            }
            row[f"horizon_{horizon}"] = horizon_payload
            momentum_summary.append(
                {
                    "top_k": top_k,
                    "horizon": horizon,
                    "selected_minus_momentum_mean_favorable_return": _delta(
                        horizon_payload["selected"].get("mean_favorable_return"),
                        horizon_payload["momentum_baseline"].get("mean_favorable_return"),
                    ),
                    "selected_minus_universe_mean_favorable_return": _delta(
                        horizon_payload["selected"].get("mean_favorable_return"),
                        horizon_payload["universe"].get("mean_favorable_return"),
                    ),
                }
            )
        k_summary.append(row)

    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _utc_now_iso(),
        "source_db_path": str(db_path),
        "direction": direction,
        "mode": DEFAULT_MODE,
        "risk_mode": DEFAULT_RISK_MODE,
        "timeframes": list(DEFAULT_TFS),
        "validated_timeframes": list(timeframes),
        "eval_date_count": len(eval_dates),
        "eval_dates": eval_dates,
        "top_k_values": list(top_k_values),
        "horizons": list(horizons),
        "k_summary": k_summary,
        "momentum_baseline_summary": momentum_summary,
        "observations": observations,
    }


def _delta(left: Any, right: Any) -> float | None:
    if left is None or right is None:
        return None
    return float(left) - float(right)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate MeeMee ranking effectiveness from a snapshot DuckDB.")
    parser.add_argument("--db-path", required=True, help="Snapshot stocks.duckdb path. Live DB paths are intentionally not auto-discovered.")
    parser.add_argument("--output-root", default=None, help="Output root. Defaults to G:\\Tradex\\keep\\ranking_effectiveness.")
    parser.add_argument("--required-latest-ymd", type=int, default=DEFAULT_REQUIRED_LATEST)
    parser.add_argument("--lookback-dates", type=int, default=DEFAULT_LOOKBACK_DATES)
    parser.add_argument("--eval-step", type=int, default=DEFAULT_EVAL_STEP)
    parser.add_argument("--min-eval-dates", type=int, default=DEFAULT_MIN_EVAL_DATES)
    parser.add_argument("--timeframes", default=",".join(DEFAULT_TFS), help="Comma-separated timeframes to validate, e.g. D or D,W,M.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(list(sys.argv[1:] if argv is None else argv))
    result = run_validation(
        db_path=args.db_path,
        output_root=args.output_root,
        required_latest_ymd=args.required_latest_ymd,
        lookback_dates=args.lookback_dates,
        eval_step=args.eval_step,
        min_eval_dates=args.min_eval_dates,
        timeframes=tuple(part.strip().upper() for part in str(args.timeframes).split(",") if part.strip()),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
