from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_chart_first_replay import SHARES_PER_UNIT, run_chart_first_replay  # noqa: E402


BASELINE_INPUT_DIR = Path(r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_stress200")
DEFAULT_OUTPUT_DIR = Path(r"G:\Tradex\sample_replays\tradex_selection_layer_weak_regime_bad_pick_removal_stress200")
DEFAULT_SOURCE_DB_PATH = Path(r"C:\Users\enish\Desktop\MeeMeeScreener\_internal\app\backend\stocks.duckdb")
DEFAULT_ANCHOR_LIMIT = None
POLICY_VARIANT = "policy_rollout_guardrails_v1"
BASELINE_SELECTION_VARIANT = "specialized_3way_gate"
CHALLENGER_SELECTION_VARIANT = "weak_regime_bad_pick_removal"
TOP_K_VALUES = (5, 10, 20)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, pd.Series):
        return _json_ready(value.to_dict())
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if pd.isna(value):
        return None
    return value


def _json_text(payload: Any) -> str:
    return json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, default=str)


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_json_text(payload) + "\n", encoding="utf-8")
    return path


def _write_parquet(path: Path, frame: pd.DataFrame) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        frame.to_parquet(path, index=False)
    except Exception:
        conn = duckdb.connect()
        try:
            conn.register("frame", frame)
            conn.execute(f"COPY frame TO '{path.as_posix()}' (FORMAT PARQUET)")
        finally:
            conn.close()
    return path


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _ymd_to_date_text(value: int | str) -> str:
    text = str(int(value))
    return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"


def _table_date_to_text(value: int | str | None) -> str | None:
    if value is None:
        return None
    numeric = int(value)
    if numeric >= 1_000_000_000:
        return datetime.fromtimestamp(numeric, tz=timezone.utc).strftime("%Y-%m-%d")
    return _ymd_to_date_text(numeric)


def _date_text_to_ymd(value: str) -> int:
    return int(str(value).replace("-", ""))


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None or pd.isna(value):
        return float(default)
    try:
        out = float(value)
    except Exception:
        return float(default)
    return out if math.isfinite(out) else float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    if value is None or pd.isna(value):
        return int(default)
    try:
        return int(value)
    except Exception:
        return int(default)


def _load_trading_calendar(*, source_db_path: Path) -> list[int]:
    conn = duckdb.connect(str(source_db_path), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT CAST(strftime(to_timestamp(CAST(date AS BIGINT)), '%Y%m%d') AS INTEGER) AS dt
            FROM daily_bars
            ORDER BY dt
            """
        ).fetchall()
    finally:
        conn.close()
    return [int(row[0]) for row in rows]


def _load_latest_table_date(*, source_db_path: Path, table_name: str, date_column: str) -> int | None:
    conn = duckdb.connect(str(source_db_path), read_only=True)
    try:
        row = conn.execute(f"SELECT MAX({date_column}) FROM {table_name}").fetchone()
    finally:
        conn.close()
    if not row or row[0] is None:
        return None
    return int(row[0])


def _db_file_provenance(path: Path) -> dict[str, Any]:
    stat = path.expanduser().resolve().stat()
    return {
        "path": str(path.expanduser().resolve()),
        "size_bytes": int(stat.st_size),
        "mtime": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
    }


def _db_provenance_payload(*, source_db_path: Path, anchor_dates: list[int]) -> dict[str, Any]:
    resolved = source_db_path.expanduser().resolve()
    provenance = _db_file_provenance(resolved)
    latest_daily_bars_date = _load_latest_table_date(source_db_path=resolved, table_name="daily_bars", date_column="date")
    try:
        latest_feature_snapshot_daily_date = _load_latest_table_date(
            source_db_path=resolved,
            table_name="feature_snapshot_daily",
            date_column="dt",
        )
    except Exception:
        latest_feature_snapshot_daily_date = None
    return {
        "source_db_path": str(resolved),
        "source_db_size_bytes": provenance["size_bytes"],
        "source_db_mtime": provenance["mtime"],
        "copied_db_path": None,
        "copied_db_size_bytes": None,
        "copied_db_mtime": None,
        "latest_daily_bars_date": _table_date_to_text(latest_daily_bars_date),
        "latest_feature_snapshot_daily_date": _table_date_to_text(latest_feature_snapshot_daily_date)
        if latest_feature_snapshot_daily_date is not None
        else None,
        "signal_basis_daily_min_date": None,
        "signal_basis_daily_max_date": None,
        "ranking_snapshot_as_of": [_ymd_to_date_text(dt) for dt in anchor_dates],
        "ranking_snapshot_as_of_mode": "per_anchor_date",
        "stress60_same_db": True,
        "smoke10_same_db": False,
        "research_fallback_db_source": True,
    }


def _load_source_frame(path: Path) -> pd.DataFrame:
    payload = _load_json(path)
    rows = payload.get("rows")
    if isinstance(rows, dict) and "rows" in rows:
        rows = rows["rows"]
    return pd.DataFrame(rows or [])


def _load_basis_frame(*, source_db_path: Path, keys: pd.DataFrame) -> pd.DataFrame:
    if keys.empty:
        return pd.DataFrame(columns=["anchor_dt", "symbol"])
    conn = duckdb.connect(str(source_db_path), read_only=True)
    try:
        conn.register("candidate_keys", keys)
        frame = conn.execute(
            """
            SELECT
                k.anchor_dt,
                CAST(k.symbol AS VARCHAR) AS symbol,
                CAST(json_extract(b.basis_payload_json, '$.cnt60Up') AS DOUBLE) AS cnt60Up,
                CAST(json_extract(b.basis_payload_json, '$.cnt100Up') AS DOUBLE) AS cnt100Up,
                CAST(json_extract(b.basis_payload_json, '$.monthlyBreakoutUpProb') AS DOUBLE) AS monthlyBreakoutUpProb,
                CAST(json_extract(b.basis_payload_json, '$.monthlyBreakoutDownProb') AS DOUBLE) AS monthlyBreakoutDownProb,
                CAST(json_extract(b.basis_payload_json, '$.monthlyRangeProb') AS DOUBLE) AS monthlyRangeProb,
                CAST(json_extract(b.basis_payload_json, '$.monthlyBoxWild') AS BOOLEAN) AS monthlyBoxWild,
                CAST(json_extract(b.basis_payload_json, '$.reclaim60') AS DOUBLE) AS reclaim60,
                CAST(json_extract(b.basis_payload_json, '$.v60Strong') AS DOUBLE) AS v60Strong,
                CAST(json_extract(b.basis_payload_json, '$.v60Core') AS DOUBLE) AS v60Core,
                CAST(json_extract(b.basis_payload_json, '$.marketRegime') AS VARCHAR) AS marketRegime,
                CAST(json_extract(b.basis_payload_json, '$.marketRiskOn') AS DOUBLE) AS marketRiskOn,
                CAST(json_extract(b.basis_payload_json, '$.marketRiskOff') AS DOUBLE) AS marketRiskOff
            FROM candidate_keys k
            LEFT JOIN signal_basis_daily b
              ON b.dt = k.anchor_dt
             AND CAST(b.code AS VARCHAR) = CAST(k.symbol AS VARCHAR)
            """
        ).df()
    finally:
        conn.close()
    return frame


def _rank_bucket(rank: int) -> str:
    if rank <= 5:
        return "top5"
    if rank <= 10:
        return "top6_10"
    return "top11_20"


def _weak_regime_penalty(row: pd.Series) -> tuple[bool, str, float]:
    side = str(row.get("side") or "")
    rank = _safe_int(row.get("challenger_rank") or row.get("champion_rank") or row.get("rank") or 0)
    if side != "long" or rank <= 5:
        return False, "top5_preserved", 0.0
    cnt60_up = _safe_float(row.get("cnt60Up"), 999.0)
    monthly_up = _safe_float(row.get("monthlyBreakoutUpProb"), 0.0)
    monthly_down = _safe_float(row.get("monthlyBreakoutDownProb"), 0.0)
    market_regime = str(row.get("marketRegime") or "").lower()
    reclaim60 = _safe_float(row.get("reclaim60"), 0.0) >= 0.5
    v60_strong = _safe_float(row.get("v60Strong"), 0.0) >= 0.5
    weak_indicator = (not reclaim60) or (not v60_strong) or ("risk_off" in market_regime) or (monthly_down >= monthly_up - 0.03)
    if 6 <= rank <= 10:
        if cnt60_up < 20.0 and weak_indicator:
            return True, "weak_regime_bad_pick_removal_top6_10", 1.0
        return False, "accepted_top6_10", 0.0
    if cnt60_up < 25.0 and weak_indicator:
        return True, "weak_regime_bad_pick_removal_top11_20", 1.0
    return False, "accepted_top11_20", 0.0


def _policy_trade_dir(*, output_dir: Path, anchor_date: str, month_bucket: str, side: str, symbol: str, variant: str) -> Path:
    return output_dir / "policy_runs" / variant / f"{anchor_date}_{month_bucket}" / side / symbol


def _run_policy_replay(
    *,
    source_db_path: Path,
    output_dir: Path,
    policy_variant: str,
    selection_variant: str,
    anchor_date: str,
    month_bucket: str,
    side: str,
    symbol: str,
    rank_bucket: str,
    horizon_end: str,
    freeze_date: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    run_dir = _policy_trade_dir(output_dir=output_dir, anchor_date=anchor_date, month_bucket=month_bucket, side=side, symbol=symbol, variant=selection_variant)
    result = run_chart_first_replay(
        source_db_path=source_db_path,
        output_dir=run_dir,
        symbol=symbol,
        start_date=anchor_date,
        end_date=horizon_end,
        freeze_date=freeze_date,
        policy_variant=policy_variant,
        policy_context={"rank_bucket": rank_bucket},
    )
    summary = result["roundtrip_summary"]
    postmortem = result["postmortem"]
    ledger_payload = json.loads(Path(result["paths"]["daily_ledger_json"]).read_text(encoding="utf-8"))
    annotated_rows: list[dict[str, Any]] = []
    for row in ledger_payload.get("rows") or []:
        annotated_rows.append(
            {
                "anchor_date": anchor_date,
                "month_bucket": month_bucket,
                "selection_variant": selection_variant,
                "policy_variant": policy_variant,
                "rank_bucket": rank_bucket,
                "side": side,
                "symbol": symbol,
                "trade_date": row.get("date"),
                "selected_action": row.get("selected_action"),
                "previous_position": row.get("previous_position"),
                "next_position": row.get("next_position"),
                "execution_price": row.get("execution_price"),
                "target_buy_units": row.get("target_buy_units"),
                "target_sell_units": row.get("target_sell_units"),
                "buy_delta_units": row.get("buy_delta_units"),
                "sell_delta_units": row.get("sell_delta_units"),
                "entry_reason_primary": row.get("entry_reason_primary"),
                "entry_reason_codes": row.get("entry_reason_codes"),
                "entry_reason_detail": row.get("entry_reason_detail"),
                "add_reason_primary": row.get("add_reason_primary"),
                "add_reason_codes": row.get("add_reason_codes"),
                "add_reason_detail": row.get("add_reason_detail"),
                "hedge_reason_primary": row.get("hedge_reason_primary"),
                "hedge_reason_codes": row.get("hedge_reason_codes"),
                "hedge_reason_detail": row.get("hedge_reason_detail"),
                "trim_reason_primary": row.get("trim_reason_primary"),
                "trim_reason_codes": row.get("trim_reason_codes"),
                "trim_reason_detail": row.get("trim_reason_detail"),
                "exit_reason_primary": row.get("exit_reason_primary"),
                "exit_reason_codes": row.get("exit_reason_codes"),
                "exit_reason_detail": row.get("exit_reason_detail"),
                "cover_reason_primary": row.get("cover_reason_primary"),
                "cover_reason_codes": row.get("cover_reason_codes"),
                "cover_reason_detail": row.get("cover_reason_detail"),
                "flat_reason_primary": row.get("flat_reason_primary"),
                "flat_reason_codes": row.get("flat_reason_codes"),
                "flat_reason_detail": row.get("flat_reason_detail"),
                "realized_pnl": row.get("realized_pnl"),
                "unrealized_pnl": row.get("unrealized_pnl"),
                "policy_roundtrip_count": summary["aggregate"]["roundtrip_count"],
                "policy_net_realized_pnl": summary["aggregate"]["net_realized_pnl"],
                "policy_max_drawdown_during_holding": summary["aggregate"]["max_drawdown_during_holding"],
                "policy_exit_timing": summary["aggregate"].get("exits_early_or_late"),
                "policy_summary_path": result["paths"]["roundtrip_summary"],
                "policy_postmortem_path": result["paths"]["postmortem"],
            }
        )
    run_row = {
        "anchor_date": anchor_date,
        "month_bucket": month_bucket,
        "selection_variant": selection_variant,
        "policy_variant": policy_variant,
        "rank_bucket": rank_bucket,
        "side": side,
        "symbol": symbol,
        "roundtrip_count": summary["aggregate"]["roundtrip_count"],
        "entry_count": summary["aggregate"]["entry_count"],
        "exit_count": summary["aggregate"]["exit_count"],
        "hedge_count": summary["aggregate"]["hedge_count"],
        "stay_count": summary["aggregate"]["stay_count"],
        "net_realized_pnl": summary["aggregate"]["net_realized_pnl"],
        "max_drawdown_during_holding": summary["aggregate"]["max_drawdown_during_holding"],
        "average_capture_ratio": summary["aggregate"].get("average_capture_ratio"),
        "exits_early_or_late": summary["aggregate"].get("exits_early_or_late"),
        "selected_action_count": summary["aggregate"]["roundtrip_count"],
        "postmortem": postmortem,
        "roundtrip_summary": summary,
    }
    return run_row, annotated_rows


def _expand_by_dup_count(frame: pd.DataFrame, *, dup_count_col: str = "dup_count") -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    pieces: list[pd.DataFrame] = []
    for _, row in frame.iterrows():
        count = max(1, _safe_int(row.get(dup_count_col), 1))
        piece = pd.DataFrame([row.to_dict()] * count)
        pieces.append(piece)
    return pd.concat(pieces, ignore_index=True) if pieces else frame.iloc[0:0].copy()


def _aggregate_selection_rows(rows: pd.DataFrame, *, top_k: int, selected_column: str) -> dict[str, Any]:
    selected = rows[rows[selected_column] == True].copy()  # noqa: E712
    if selected.empty:
        return {
            "selected_count": 0,
            "bad_pick_rate": None,
            "win_rate": None,
            "avg_ret63": None,
            "median_ret63": None,
            "avg_mfe63": None,
            "avg_mae63": None,
            "worst_mae63": None,
        }
    ret63 = pd.to_numeric(selected["ret63"], errors="coerce")
    mfe63 = pd.to_numeric(selected["mfe63"], errors="coerce")
    mae63 = pd.to_numeric(selected["mae63"], errors="coerce")
    return {
        "selected_count": int(len(selected)),
        "bad_pick_rate": float((ret63 <= 0).mean()),
        "win_rate": float((ret63 > 0).mean()),
        "avg_ret63": float(ret63.mean()),
        "median_ret63": float(ret63.median()),
        "avg_mfe63": float(mfe63.mean()),
        "avg_mae63": float(mae63.mean()),
        "worst_mae63": float(mae63.min()),
    }


def _aggregate_policy_rows(rows: pd.DataFrame, *, selected_column: str) -> dict[str, Any]:
    selected = rows[rows[selected_column] == True].copy()  # noqa: E712
    if selected.empty:
        return {
            "selected_count": 0,
            "roundtrip_count": 0,
            "net_realized_pnl": 0.0,
            "max_drawdown_during_holding": None,
            "average_capture_ratio": None,
            "exits_early_or_late": None,
            "win_rate": None,
            "policy_vs_hold_gap_sum": 0.0,
            "number_of_trades": 0,
            "hold_pnl_sum": 0.0,
            "candidate_capital_sum": 0.0,
        }
    pnl = pd.to_numeric(selected["policy_net_realized_pnl"], errors="coerce")
    dds = pd.to_numeric(selected["policy_max_drawdown_during_holding"], errors="coerce")
    hold_pnl = pd.to_numeric(selected["hold_pnl"], errors="coerce")
    candidate_capital = pd.to_numeric(selected["candidate_capital"], errors="coerce")
    capture = pd.to_numeric(selected["capture_ratio"], errors="coerce")
    return {
        "selected_count": int(len(selected)),
        "roundtrip_count": int(pd.to_numeric(selected["policy_roundtrip_count"], errors="coerce").fillna(0).sum()),
        "net_realized_pnl": float(pnl.sum()),
        "max_drawdown_during_holding": float(dds.min()) if not dds.dropna().empty else None,
        "average_capture_ratio": float(capture.dropna().mean()) if not capture.dropna().empty else None,
        "exits_early_or_late": "late" if any(str(value) == "late" for value in selected["policy_exit_timing"].fillna("")) else "acceptable",
        "win_rate": float((pnl > 0).mean()),
        "policy_vs_hold_gap_sum": float((pnl - hold_pnl).sum()),
        "number_of_trades": int(pd.to_numeric(selected["number_of_trades"], errors="coerce").fillna(0).sum()),
        "hold_pnl_sum": float(hold_pnl.sum()),
        "candidate_capital_sum": float(candidate_capital.sum()),
    }


def _long_short_breakdown(frame: pd.DataFrame, *, selected_column: str) -> dict[str, Any]:
    return {
        "long": _aggregate_selection_rows(frame[frame["side"] == "long"], top_k=20, selected_column=selected_column),
        "short": _aggregate_selection_rows(frame[frame["side"] == "short"], top_k=20, selected_column=selected_column),
    }


def _by_rank_bucket(frame: pd.DataFrame, *, selected_column: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for bucket in ("top5", "top6_10", "top11_20"):
        bucket_frame = frame[frame["rank_bucket"] == bucket].copy()
        out[bucket] = {
            "selection_only": _aggregate_selection_rows(bucket_frame, top_k=20, selected_column=selected_column),
            "policy_trade": _aggregate_policy_rows(bucket_frame, selected_column=selected_column),
            "count": int(len(bucket_frame)),
            "long_count": int((bucket_frame["side"] == "long").sum()),
            "short_count": int((bucket_frame["side"] == "short").sum()),
        }
    return out


def _by_action(frame: pd.DataFrame, *, selected_column: str) -> dict[str, Any]:
    selected = frame.copy()
    if selected.empty:
        return {}
    action_col = selected_column if selected_column in selected.columns else ("selected_action" if "selected_action" in selected.columns else "policy_action")
    if action_col not in selected.columns:
        return {}
    selected[action_col] = selected[action_col].fillna("stay").astype(str)
    selected["reason"] = selected.get("exit_reason_primary")
    out: dict[str, Any] = {}
    for action, group in selected.groupby(action_col, sort=False):
        pnl = pd.to_numeric(group["policy_net_realized_pnl"], errors="coerce")
        out[str(action)] = {
            "count": int(len(group)),
            "realized_pnl_sum": float(pnl.sum()),
            "realized_pnl_mean": float(pnl.mean()),
            "win_rate": float((pnl > 0).mean()),
            "contribution_to_total_gap": float(pnl.sum()),
        }
    return out


def _by_anchor(frame: pd.DataFrame, *, selected_column: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for anchor_date, group in frame.groupby("anchor_date", sort=False):
        selected = group[group[selected_column] == True].copy()  # noqa: E712
        pnl = pd.to_numeric(selected["policy_net_realized_pnl"], errors="coerce")
        out[str(anchor_date)] = {
            "selection_only_top20_delta": float(
                pd.to_numeric(selected["ret63"], errors="coerce").mean() if not selected.empty else 0.0
            ),
            "policy_top20_delta": float(pnl.sum()),
            "policy_vs_hold_gap": float((pnl - pd.to_numeric(selected["hold_pnl"], errors="coerce")).sum()) if not selected.empty else 0.0,
            "long_contribution": float(pnl[selected["side"] == "long"].sum()) if not selected.empty else 0.0,
            "short_contribution": float(pnl[selected["side"] == "short"].sum()) if not selected.empty else 0.0,
            "worst_symbols": group.sort_values("policy_vs_hold_gap").head(5)["symbol"].astype(str).tolist(),
            "worst_actions": group.sort_values("policy_vs_hold_gap").head(5).get("selected_action", pd.Series(dtype=str)).astype(str).tolist(),
            "number_of_trades": int(pd.to_numeric(selected["number_of_trades"], errors="coerce").fillna(0).sum()) if not selected.empty else 0,
            "forced_exit_count": int(pd.to_numeric(selected["forced_exit_count"], errors="coerce").fillna(0).sum()) if not selected.empty else 0,
            "late_exit_count": int(pd.to_numeric(selected["late_exit_count"], errors="coerce").fillna(0).sum()) if not selected.empty else 0,
            "stop_loss_count": int(pd.to_numeric(selected["stop_loss_count"], errors="coerce").fillna(0).sum()) if not selected.empty else 0,
            "add_count": int(pd.to_numeric(selected["add_count"], errors="coerce").fillna(0).sum()) if not selected.empty else 0,
            "hedge_action_count": int(pd.to_numeric(selected["hedge_action_count"], errors="coerce").fillna(0).sum()) if not selected.empty else 0,
            "no_trade_rate": float((group["challenger_selected_top20"] == False).mean()),  # noqa: E712
            "long_tradable_rate": float((group["side"] == "long").mean()),
            "short_tradable_rate": float((group["side"] == "short").mean()),
        }
    return out


def _by_symbol(frame: pd.DataFrame, *, selected_column: str) -> dict[str, Any]:
    selected = frame[frame[selected_column] == True].copy()  # noqa: E712
    if selected.empty:
        return {}
    out: dict[str, Any] = {}
    for (symbol, side), group in selected.groupby(["symbol", "side"], sort=False):
        pnl = pd.to_numeric(group["policy_net_realized_pnl"], errors="coerce")
        out[f"{symbol}:{side}"] = {
            "symbol": str(symbol),
            "side": str(side),
            "rank_bucket": group["rank_bucket"].iloc[0],
            "contribution_to_top20_gap": float((pnl - pd.to_numeric(group["hold_pnl"], errors="coerce")).sum()),
            "count": int(len(group)),
            "worst_anchor": group.sort_values("policy_vs_hold_gap").head(1)["anchor_date"].astype(str).iloc[0],
            "action_reason": group.sort_values("policy_vs_hold_gap").head(1).get("exit_reason_primary", pd.Series(dtype=str)).astype(str).iloc[0]
            if "exit_reason_primary" in group.columns and not group.empty
            else "",
        }
    return out


def _topk_metrics(frame: pd.DataFrame, *, selected_column: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for top_k in TOP_K_VALUES:
        selected = frame[frame[selected_column] == True].copy()  # noqa: E712
        if "selected_topk" in selected.columns:
            selected = selected[selected["selected_topk"] == top_k]
        out[str(top_k)] = {
            "selection_only": _aggregate_selection_rows(selected, top_k=top_k, selected_column=selected_column),
            "policy_trade": _aggregate_policy_rows(selected, selected_column=selected_column),
        }
    return out


def _compute_boundary_score_gap(frame: pd.DataFrame, *, boundary_rank: int) -> float | None:
    gaps: list[float] = []
    for (_, side), group in frame.groupby(["anchor_date", "side"], sort=False):
        ranked = group.sort_values(["challenger_rank", "symbol"], kind="stable").copy()
        ranked = ranked[ranked["baseline_selected_top20"] == True]  # noqa: E712
        if len(ranked) <= boundary_rank:
            continue
        def _score(row: pd.Series) -> float:
            removed, _, penalty = _weak_regime_penalty(row)
            return 1.0 - penalty if removed else 1.0
        left = ranked.iloc[boundary_rank - 1]
        right = ranked.iloc[boundary_rank]
        gaps.append(float(_score(left) - _score(right)))
    if not gaps:
        return None
    return float(sum(gaps) / len(gaps))


def run_selection_layer_weak_regime_bad_pick_removal(
    *,
    source_db_path: Path = DEFAULT_SOURCE_DB_PATH,
    input_dir: Path = BASELINE_INPUT_DIR,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    anchor_limit: int | None = DEFAULT_ANCHOR_LIMIT,
) -> dict[str, Any]:
    summary_payload = _load_json(input_dir / "integrated_guarded_v1_replay_summary.json")
    compare_payload = _load_json(input_dir / "integrated_guarded_v1_compare.json")
    dates_payload = _load_json(input_dir / "integrated_guarded_v1_dates.json")["rows"]
    candidate_payload = _load_json(input_dir / "integrated_guarded_v1_candidate_snapshots.json")
    selection_payload = _load_json(input_dir / "integrated_guarded_v1_selection_only_ledger.json")
    policy_payload = _load_json(input_dir / "integrated_guarded_v1_policy_trade_ledger.json")

    anchors = list(dates_payload.get("anchors") or [])
    if anchor_limit is not None:
        anchors = anchors[: int(anchor_limit)]

    trading_dates = _load_trading_calendar(source_db_path=source_db_path)
    anchor_date_to_index = {int(item["anchor_date"].replace("-", "")): int(item["anchor_index"]) for item in anchors}
    anchor_dates = [int(item["anchor_date"].replace("-", "")) for item in anchors]
    db_provenance = _db_provenance_payload(source_db_path=source_db_path, anchor_dates=anchor_dates)
    basis_start_dt = min(anchor_dates) if anchor_dates else None
    basis_end_dt = max(anchor_dates) if anchor_dates else None
    if basis_start_dt is not None:
        db_provenance["signal_basis_daily_min_date"] = _ymd_to_date_text(basis_start_dt)
    if basis_end_dt is not None:
        db_provenance["signal_basis_daily_max_date"] = _ymd_to_date_text(basis_end_dt)

    candidate_rows = _load_source_frame(input_dir / "integrated_guarded_v1_candidate_snapshots.json")
    selection_rows = _load_source_frame(input_dir / "integrated_guarded_v1_selection_only_ledger.json")
    policy_rows = _load_source_frame(input_dir / "integrated_guarded_v1_policy_trade_ledger.json")

    candidate_rows = candidate_rows.drop_duplicates(["anchor_date", "symbol", "side"], keep="first").copy()
    selection_rows = selection_rows.drop_duplicates(["anchor_date", "symbol", "side"], keep="first").copy()
    policy_rows = policy_rows.drop_duplicates(["anchor_date", "symbol", "side", "date"], keep="first").copy()

    candidate_rows["anchor_dt"] = candidate_rows["anchor_date"].map(_date_text_to_ymd)
    selection_rows["anchor_dt"] = selection_rows["anchor_date"].map(_date_text_to_ymd)
    policy_rows["anchor_dt"] = policy_rows["anchor_date"].map(_date_text_to_ymd)
    candidate_rows["symbol"] = candidate_rows["symbol"].astype(str)
    selection_rows["symbol"] = selection_rows["symbol"].astype(str)
    policy_rows["symbol"] = policy_rows["symbol"].astype(str)

    if "challenger_selected_top20" not in candidate_rows.columns:
        raise RuntimeError("candidate snapshots missing challenger_selected_top20")
    if "challenger_rank" not in candidate_rows.columns:
        raise RuntimeError("candidate snapshots missing challenger_rank")

    basis_keys = candidate_rows.loc[candidate_rows["challenger_selected_top20"].map(lambda x: str(x).lower() == "true"), ["anchor_dt", "symbol"]].drop_duplicates()
    basis_frame = _load_basis_frame(source_db_path=source_db_path, keys=basis_keys)
    merged = candidate_rows.merge(
        basis_frame,
        on=["anchor_dt", "symbol"],
        how="left",
        suffixes=("", "_basis"),
    )
    candidate_rows = merged

    selection_lookup = selection_rows[
        [
            "anchor_date",
            "symbol",
            "side",
            "entry_date",
            "entry_price",
            "exit_date",
            "exit_price",
            "ret5",
            "ret10",
            "ret20",
            "ret63",
            "mfe63",
            "mae63",
            "max_adverse_excursion",
            "result_bucket",
            "champion_selected_top5",
            "champion_selected_top10",
            "champion_selected_top20",
            "challenger_selected_top5",
            "challenger_selected_top10",
            "challenger_selected_top20",
        ]
    ].copy()
    selection_lookup = selection_lookup.merge(
        candidate_rows[
            [
                "anchor_date",
                "symbol",
                "side",
                "month_bucket",
                "challenger_rank",
                "cnt60Up",
                "cnt100Up",
                "monthlyBreakoutUpProb",
                "monthlyBreakoutDownProb",
                "monthlyRangeProb",
                "monthlyBoxWild",
                "reclaim60",
                "v60Strong",
                "v60Core",
                "marketRegime",
                "marketRiskOn",
                "marketRiskOff",
                "selection_reason",
            ]
        ],
        on=["anchor_date", "symbol", "side"],
        how="left",
    )
    selection_lookup["baseline_selected_top5"] = selection_lookup["challenger_selected_top5"].map(lambda x: str(x).lower() == "true")
    selection_lookup["baseline_selected_top10"] = selection_lookup["challenger_selected_top10"].map(lambda x: str(x).lower() == "true")
    selection_lookup["baseline_selected_top20"] = selection_lookup["challenger_selected_top20"].map(lambda x: str(x).lower() == "true")
    selection_lookup["dup_count"] = selection_lookup.groupby(["anchor_date", "symbol", "side"])["symbol"].transform("size")
    selection_lookup["baseline_selection_variant"] = BASELINE_SELECTION_VARIANT
    selection_lookup["challenger_selection_variant"] = CHALLENGER_SELECTION_VARIANT
    selection_lookup["policy_variant"] = POLICY_VARIANT

    baseline_rows = selection_lookup[selection_lookup["baseline_selected_top20"] == True].copy()  # noqa: E712
    baseline_rows["baseline_rank_bucket"] = baseline_rows["challenger_rank"].astype(int).apply(_rank_bucket)
    baseline_rows["weak_regime_penalty"] = 0.0
    baseline_rows["challenger_keep"] = True
    baseline_rows["challenger_selection_reason"] = baseline_rows["selection_reason"].map(lambda x: x.get("challenger") if isinstance(x, dict) else None)
    baseline_rows["weak_regime_removed"] = False
    baseline_rows["selection_score"] = 1.0

    challenger_rows = baseline_rows.copy()
    challenger_rows["baseline_selected_top5"] = challenger_rows["baseline_selected_top5"].map(lambda x: bool(x))
    challenger_rows["baseline_selected_top10"] = challenger_rows["baseline_selected_top10"].map(lambda x: bool(x))
    challenger_rows["baseline_selected_top20"] = challenger_rows["baseline_selected_top20"].map(lambda x: bool(x))
    challenger_rows["selected_topk"] = 0

    challenger_grouped: list[pd.DataFrame] = []
    removal_rows: list[dict[str, Any]] = []
    for (anchor_date, side), group in challenger_rows.groupby(["anchor_date", "side"], sort=False):
        ranked = group.sort_values(["challenger_rank", "symbol"], kind="stable").copy()
        keep_flags: list[bool] = []
        penalties: list[float] = []
        reasons: list[str] = []
        for _, row in ranked.iterrows():
            removed, reason, penalty = _weak_regime_penalty(row)
            keep_flags.append(not removed)
            penalties.append(float(penalty))
            reasons.append(reason)
        ranked["weak_regime_removed"] = [not flag for flag in keep_flags]
        ranked["weak_regime_penalty"] = penalties
        ranked["challenger_keep"] = keep_flags
        ranked["challenger_selection_reason"] = reasons
        ranked = ranked[ranked["challenger_keep"] == True].copy()  # noqa: E712
        ranked["challenger_position"] = range(1, len(ranked) + 1)
        ranked["selected_topk"] = ranked["challenger_position"]
        challenger_grouped.append(ranked)
        for _, row in ranked.iterrows():
            pass
    challenger_rows = pd.concat(challenger_grouped, ignore_index=True) if challenger_grouped else challenger_rows.iloc[0:0].copy()

    removal_lookup = baseline_rows.merge(
        challenger_rows[["anchor_date", "symbol", "side", "challenger_position", "selected_topk", "challenger_selection_reason", "weak_regime_penalty"]],
        on=["anchor_date", "symbol", "side"],
        how="left",
        suffixes=("_baseline", ""),
    )
    removal_lookup["removed"] = removal_lookup["challenger_position"].isna()
    removal_lookup["challenger_position"] = pd.to_numeric(removal_lookup["challenger_position"], errors="coerce")
    removal_lookup["selected_topk"] = pd.to_numeric(removal_lookup["selected_topk"], errors="coerce")
    removal_lookup["weak_regime_penalty"] = pd.to_numeric(removal_lookup["weak_regime_penalty"], errors="coerce").fillna(0.0)
    removal_lookup["dup_count"] = pd.to_numeric(removal_lookup["dup_count"], errors="coerce").fillna(1).astype(int)
    removal_lookup["baseline_rank_bucket"] = removal_lookup["baseline_rank_bucket"].astype(str)
    removal_lookup["challenger_rank_bucket"] = removal_lookup["challenger_position"].fillna(0).astype(int).apply(lambda value: _rank_bucket(value) if value > 0 else "removed")
    removal_lookup["champion_selected_top5"] = removal_lookup["baseline_selected_top5"]
    removal_lookup["champion_selected_top10"] = removal_lookup["baseline_selected_top10"]
    removal_lookup["champion_selected_top20"] = removal_lookup["baseline_selected_top20"]
    removal_lookup["challenger_selected_top5"] = removal_lookup["challenger_position"].fillna(999).astype(int).le(5)
    removal_lookup["challenger_selected_top10"] = removal_lookup["challenger_position"].fillna(999).astype(int).le(10)
    removal_lookup["challenger_selected_top20"] = removal_lookup["challenger_position"].fillna(999).astype(int).le(20)
    removal_lookup["changed_top5_member"] = removal_lookup["champion_selected_top5"] != removal_lookup["challenger_selected_top5"]
    removal_lookup["changed_top10_member"] = removal_lookup["champion_selected_top10"] != removal_lookup["challenger_selected_top10"]
    removal_lookup["changed_top20_member"] = removal_lookup["champion_selected_top20"] != removal_lookup["challenger_selected_top20"]
    removal_lookup["selection_reason_baseline"] = removal_lookup["selection_reason"].map(lambda x: x.get("challenger") if isinstance(x, dict) else None)
    removal_lookup["selection_reason_challenger"] = removal_lookup["challenger_selection_reason"].fillna("accepted_after_weak_regime_check")
    removal_lookup["selected_by"] = removal_lookup.apply(
        lambda row: "both"
        if row["champion_selected_top20"] and row["challenger_selected_top20"]
        else ("champion" if row["champion_selected_top20"] else ("challenger" if row["challenger_selected_top20"] else "neutral")),
        axis=1,
    )
    removal_lookup["selected_by_methods"] = removal_lookup.apply(
        lambda row: [method for method, flag in (("champion", row["champion_selected_top20"]), ("challenger", row["challenger_selected_top20"])) if bool(flag)],
        axis=1,
    )
    removal_lookup["selection_score"] = 1.0 - removal_lookup["weak_regime_penalty"]
    removal_lookup["result_bucket"] = removal_lookup["result_bucket"].fillna("neutral")
    removal_lookup["entry_price"] = pd.to_numeric(removal_lookup["entry_price"], errors="coerce")
    removal_lookup["hold_pnl"] = pd.to_numeric(removal_lookup["ret63"], errors="coerce") * pd.to_numeric(removal_lookup["entry_price"], errors="coerce") * SHARES_PER_UNIT
    removal_lookup["baseline_selected_top20"] = removal_lookup["baseline_selected_top20"].map(lambda x: bool(x))
    removal_lookup["candidate_capital"] = removal_lookup["entry_price"].abs() * SHARES_PER_UNIT
    removal_lookup["weak_regime_removed"] = removal_lookup["removed"]
    removal_lookup["anchor_dt"] = removal_lookup["anchor_date"].map(_date_text_to_ymd)
    removal_lookup["no_exposure_count"] = removal_lookup["removed"].astype(int)
    removal_lookup["exposed_count"] = (~removal_lookup["removed"]).astype(int)

    expanded_rows = _expand_by_dup_count(removal_lookup)
    expanded_rows["baseline_selected_top5"] = expanded_rows["baseline_selected_top5"].map(lambda x: bool(x))
    expanded_rows["baseline_selected_top10"] = expanded_rows["baseline_selected_top10"].map(lambda x: bool(x))
    expanded_rows["baseline_selected_top20"] = expanded_rows["baseline_selected_top20"].map(lambda x: bool(x))
    expanded_rows["challenger_selected_top5"] = expanded_rows["challenger_selected_top5"].map(lambda x: bool(x))
    expanded_rows["challenger_selected_top10"] = expanded_rows["challenger_selected_top10"].map(lambda x: bool(x))
    expanded_rows["challenger_selected_top20"] = expanded_rows["challenger_selected_top20"].map(lambda x: bool(x))
    expanded_rows["champion_selected_top5"] = expanded_rows["champion_selected_top5"].map(lambda x: bool(x))
    expanded_rows["champion_selected_top10"] = expanded_rows["champion_selected_top10"].map(lambda x: bool(x))
    expanded_rows["champion_selected_top20"] = expanded_rows["champion_selected_top20"].map(lambda x: bool(x))
    expanded_rows["dup_count"] = pd.to_numeric(expanded_rows["dup_count"], errors="coerce").fillna(1).astype(int)

    # selection-only metrics
    topk_metrics: dict[str, Any] = {}
    for top_k in TOP_K_VALUES:
        baseline_selected = expanded_rows[expanded_rows[f"champion_selected_top{top_k}"] == True].copy()  # noqa: E712
        challenger_selected = expanded_rows[expanded_rows[f"challenger_selected_top{top_k}"] == True].copy()  # noqa: E712
        topk_metrics[str(top_k)] = {
            "selection_only": {
                "champion": _aggregate_selection_rows(baseline_selected, top_k=top_k, selected_column=f"champion_selected_top{top_k}"),
                "challenger": _aggregate_selection_rows(challenger_selected, top_k=top_k, selected_column=f"challenger_selected_top{top_k}"),
            }
        }
        champ_sel = topk_metrics[str(top_k)]["selection_only"]["champion"]
        chal_sel = topk_metrics[str(top_k)]["selection_only"]["challenger"]
        topk_metrics[str(top_k)]["delta"] = {
            "selection_only_avg_ret63": None
            if champ_sel["avg_ret63"] is None or chal_sel["avg_ret63"] is None
            else float(chal_sel["avg_ret63"] - champ_sel["avg_ret63"]),
            "selection_only_bad_pick_rate": None
            if champ_sel["bad_pick_rate"] is None or chal_sel["bad_pick_rate"] is None
            else float(chal_sel["bad_pick_rate"] - champ_sel["bad_pick_rate"]),
        }

    # policy replay for challenger only
    selected_challenger_unique = removal_lookup[removal_lookup["challenger_selected_top20"] == True].copy()  # noqa: E712
    selected_challenger_unique["selection_variant"] = CHALLENGER_SELECTION_VARIANT
    selected_challenger_unique["policy_variant"] = POLICY_VARIANT
    selected_challenger_unique["rank_bucket"] = selected_challenger_unique["challenger_position"].fillna(0).astype(int).apply(
        lambda value: _rank_bucket(value) if value > 0 else "removed"
    )
    selected_challenger_unique["policy_trade_dir"] = selected_challenger_unique.apply(
        lambda row: _policy_trade_dir(
            output_dir=output_dir,
            anchor_date=str(row["anchor_date"]),
            month_bucket=str(row["month_bucket"]),
            side=str(row["side"]),
            symbol=str(row["symbol"]),
            variant=CHALLENGER_SELECTION_VARIANT,
        ),
        axis=1,
    )

    anchor_index_lookup = {str(item["anchor_date"]): int(item["anchor_index"]) for item in anchors}
    policy_run_rows: list[dict[str, Any]] = []
    policy_ledger_rows: list[dict[str, Any]] = []
    for (anchor_date, side, symbol), row_group in selected_challenger_unique.groupby(["anchor_date", "side", "symbol"], sort=False):
        row = row_group.iloc[0]
        anchor_idx = anchor_index_lookup.get(str(anchor_date))
        if anchor_idx is None:
            continue
        horizon_index = min(anchor_idx + 63 - 1, len(trading_dates) - 1)
        horizon_end = _ymd_to_date_text(trading_dates[horizon_index])
        freeze_date = _ymd_to_date_text(trading_dates[max(0, anchor_idx - 1)])
        run_row, rows = _run_policy_replay(
            source_db_path=source_db_path,
            output_dir=output_dir,
            policy_variant=POLICY_VARIANT,
            selection_variant=CHALLENGER_SELECTION_VARIANT,
            anchor_date=str(anchor_date),
            month_bucket=str(row["month_bucket"]),
            side=str(side),
            symbol=str(symbol),
            rank_bucket=str(row["rank_bucket"]),
            horizon_end=horizon_end,
            freeze_date=freeze_date,
        )
        run_row["dup_count"] = int(row.get("dup_count") or 1)
        policy_run_rows.append(run_row)
        for ledger_row in rows:
            policy_ledger_rows.append({**ledger_row, "dup_count": int(row.get("dup_count") or 1)})

    policy_run_frame = pd.DataFrame(policy_run_rows)
    policy_ledger_frame = pd.DataFrame(policy_ledger_rows)
    if not policy_run_frame.empty:
        capital_lookup = selected_challenger_unique.set_index(["anchor_date", "side", "symbol"])["candidate_capital"]
        hold_lookup = selected_challenger_unique.set_index(["anchor_date", "side", "symbol"])["hold_pnl"]
        policy_run_frame["policy_net_realized_pnl"] = pd.to_numeric(policy_run_frame["net_realized_pnl"], errors="coerce")
        policy_run_frame["policy_max_drawdown_during_holding"] = pd.to_numeric(policy_run_frame["max_drawdown_during_holding"], errors="coerce")
        policy_run_frame["policy_roundtrip_count"] = pd.to_numeric(policy_run_frame["roundtrip_count"], errors="coerce")
        policy_run_frame["number_of_trades"] = pd.to_numeric(policy_run_frame["selected_action_count"], errors="coerce")
        policy_run_index = policy_run_frame.set_index(["anchor_date", "side", "symbol"]).index
        policy_run_frame["candidate_capital"] = pd.to_numeric(capital_lookup.loc[policy_run_index].values, errors="coerce")
        policy_run_frame["policy_vs_hold_gap"] = pd.to_numeric(policy_run_frame["net_realized_pnl"], errors="coerce") - pd.to_numeric(
            hold_lookup.loc[policy_run_index].values,
            errors="coerce",
        )

    # expand challenger policy rows by duplicate count to mirror baseline row granularity
    if not policy_run_frame.empty:
        policy_run_frame["selection_variant"] = CHALLENGER_SELECTION_VARIANT
        policy_run_frame["policy_variant"] = POLICY_VARIANT
        policy_run_frame["candidate_capital"] = pd.to_numeric(policy_run_frame["candidate_capital"], errors="coerce")
        policy_run_frame["hold_pnl"] = pd.to_numeric(hold_lookup.loc[policy_run_index].values, errors="coerce")
        policy_run_frame["capture_ratio"] = pd.to_numeric(
            policy_run_frame["net_realized_pnl"], errors="coerce"
        ) / pd.to_numeric(policy_run_frame["hold_pnl"], errors="coerce").replace({0.0: math.nan})
    if not policy_ledger_frame.empty:
        policy_ledger_frame["selection_variant"] = CHALLENGER_SELECTION_VARIANT
        policy_ledger_frame["policy_variant"] = POLICY_VARIANT

    challenger_selected_expanded = expanded_rows[expanded_rows["challenger_selected_top20"] == True].copy()  # noqa: E712
    challenger_selected_expanded["baseline_selected_top20"] = challenger_selected_expanded["baseline_selected_top20"].map(lambda x: bool(x))
    challenger_selected_expanded["challenger_selected_top5"] = challenger_selected_expanded["challenger_selected_top5"].map(lambda x: bool(x))
    challenger_selected_expanded["challenger_selected_top10"] = challenger_selected_expanded["challenger_selected_top10"].map(lambda x: bool(x))
    challenger_selected_expanded["challenger_selected_top20"] = challenger_selected_expanded["challenger_selected_top20"].map(lambda x: bool(x))
    challenger_selected_expanded["policy_variant"] = POLICY_VARIANT
    challenger_selected_expanded["selection_variant"] = CHALLENGER_SELECTION_VARIANT
    challenger_selected_expanded["candidate_capital"] = pd.to_numeric(challenger_selected_expanded["candidate_capital"], errors="coerce")
    challenger_selected_expanded["hold_pnl"] = pd.to_numeric(challenger_selected_expanded["hold_pnl"], errors="coerce")
    if "rank_bucket" not in challenger_selected_expanded.columns and "challenger_rank_bucket" in challenger_selected_expanded.columns:
        challenger_selected_expanded["rank_bucket"] = challenger_selected_expanded["challenger_rank_bucket"]
    challenger_selected_expanded["policy_net_realized_pnl"] = pd.to_numeric(
        challenger_selected_expanded.set_index(["anchor_date", "side", "symbol"]).index.map(
            {
                (row["anchor_date"], row["side"], row["symbol"]): row["net_realized_pnl"]
                for _, row in policy_run_frame.iterrows()
            }
        ),
        errors="coerce",
    )

    # map challenger policy stats back to candidate rows
    policy_summary_lookup = {
        (row["anchor_date"], row["side"], row["symbol"]): row.to_dict()
        for _, row in policy_run_frame.iterrows()
    }
    for idx, row in challenger_selected_expanded.iterrows():
        key = (row["anchor_date"], row["side"], row["symbol"])
        summary_row = policy_summary_lookup.get(key, {})
        for column in (
            "roundtrip_count",
            "entry_count",
            "exit_count",
            "hedge_count",
            "stay_count",
            "net_realized_pnl",
            "max_drawdown_during_holding",
            "average_capture_ratio",
            "exits_early_or_late",
            "number_of_trades",
            "hold_pnl",
            "candidate_capital",
        ):
            if column in summary_row:
                target_column = column
                if column == "net_realized_pnl":
                    target_column = "policy_net_realized_pnl"
                elif column == "roundtrip_count":
                    target_column = "policy_roundtrip_count"
                elif column == "max_drawdown_during_holding":
                    target_column = "policy_max_drawdown_during_holding"
                elif column == "exits_early_or_late":
                    target_column = "policy_exit_timing"
                challenger_selected_expanded.at[idx, target_column] = summary_row[column]
    if "policy_roundtrip_count" not in challenger_selected_expanded.columns and "roundtrip_count" in challenger_selected_expanded.columns:
        challenger_selected_expanded["policy_roundtrip_count"] = challenger_selected_expanded["roundtrip_count"]
    if "policy_max_drawdown_during_holding" not in challenger_selected_expanded.columns and "max_drawdown_during_holding" in challenger_selected_expanded.columns:
        challenger_selected_expanded["policy_max_drawdown_during_holding"] = challenger_selected_expanded["max_drawdown_during_holding"]
    if "policy_exit_timing" not in challenger_selected_expanded.columns and "exits_early_or_late" in challenger_selected_expanded.columns:
        challenger_selected_expanded["policy_exit_timing"] = challenger_selected_expanded["exits_early_or_late"]
    if "capture_ratio" not in challenger_selected_expanded.columns and "average_capture_ratio" in challenger_selected_expanded.columns:
        challenger_selected_expanded["capture_ratio"] = challenger_selected_expanded["average_capture_ratio"]
    if "number_of_trades" not in challenger_selected_expanded.columns and "policy_roundtrip_count" in challenger_selected_expanded.columns:
        challenger_selected_expanded["number_of_trades"] = challenger_selected_expanded["policy_roundtrip_count"]
    challenger_selected_expanded["policy_vs_hold_gap"] = pd.to_numeric(challenger_selected_expanded["policy_net_realized_pnl"], errors="coerce") - pd.to_numeric(
        challenger_selected_expanded["hold_pnl"], errors="coerce"
    )
    challenger_selected_expanded["selection_only_selected"] = True
    challenger_selected_expanded["policy_selected"] = True
    challenger_selected_expanded["no_exposure_count"] = challenger_selected_expanded["weak_regime_removed"].astype(int)
    challenger_selected_expanded["exposed_count"] = 1 - challenger_selected_expanded["no_exposure_count"]

    baseline_selected_expanded = expanded_rows[expanded_rows["baseline_selected_top20"] == True].copy()  # noqa: E712
    baseline_policy_lookup = policy_rows.drop_duplicates(["anchor_date", "side", "symbol"], keep="first").set_index(
        ["anchor_date", "side", "symbol"]
    )
    baseline_policy_index = baseline_selected_expanded.set_index(["anchor_date", "side", "symbol"]).index
    baseline_selected_expanded["policy_net_realized_pnl"] = pd.to_numeric(
        baseline_policy_lookup.loc[baseline_policy_index, "policy_net_realized_pnl"].values,
        errors="coerce",
    )
    baseline_selected_expanded["policy_max_drawdown_during_holding"] = pd.to_numeric(
        baseline_policy_lookup.loc[baseline_policy_index, "policy_max_drawdown_during_holding"].values,
        errors="coerce",
    )
    baseline_selected_expanded["policy_roundtrip_count"] = pd.to_numeric(
        baseline_policy_lookup.loc[baseline_policy_index, "policy_roundtrip_count"].values,
        errors="coerce",
    )
    baseline_selected_expanded["policy_max_drawdown_during_holding"] = pd.to_numeric(
        baseline_policy_lookup.loc[baseline_policy_index, "policy_max_drawdown_during_holding"].values,
        errors="coerce",
    )
    baseline_selected_expanded["policy_exit_timing"] = baseline_policy_lookup.loc[baseline_policy_index, "policy_exit_timing"].values
    baseline_selected_expanded["number_of_trades"] = baseline_selected_expanded["policy_roundtrip_count"]
    baseline_selected_expanded["capture_ratio"] = pd.to_numeric(
        baseline_selected_expanded["policy_net_realized_pnl"], errors="coerce"
    ) / pd.to_numeric(baseline_selected_expanded["hold_pnl"], errors="coerce").replace({0.0: math.nan})
    baseline_selected_expanded["policy_vs_hold_gap"] = pd.to_numeric(baseline_selected_expanded["policy_net_realized_pnl"], errors="coerce") - pd.to_numeric(
        baseline_selected_expanded["hold_pnl"], errors="coerce"
    )
    baseline_selected_expanded["selection_only_selected"] = True
    baseline_selected_expanded["policy_selected"] = True
    baseline_selected_expanded["no_exposure_count"] = 0
    baseline_selected_expanded["exposed_count"] = 1

    def _selected_view(frame: pd.DataFrame, column: str) -> pd.DataFrame:
        out = frame.copy()
        out[column] = out[column].map(lambda value: bool(value))
        return out[out[column] == True].copy()  # noqa: E712

    # boundary / contamination metrics
    weak_removed = removal_lookup[removal_lookup["removed"] == True].copy()  # noqa: E712
    weak_removed_ret63 = pd.to_numeric(weak_removed["ret63"], errors="coerce") if not weak_removed.empty else pd.Series(dtype=float)
    weak_removed_bad_rate = float((weak_removed_ret63 <= 0).mean()) if not weak_removed.empty else None
    weak_removed_count = int(len(weak_removed))
    weak_removed_avg_ret63 = float(weak_removed_ret63.mean()) if not weak_removed.empty else None
    weak_regime_only_performance = {
        "removed_count": weak_removed_count,
        "removed_avg_ret63": weak_removed_avg_ret63,
        "removed_bad_pick_rate": weak_removed_bad_rate,
        "removed_top6_10_count": int((weak_removed["challenger_rank_bucket"] == "top6_10").sum()) if not weak_removed.empty else 0,
        "removed_top11_20_count": int((weak_removed["challenger_rank_bucket"] == "top11_20").sum()) if not weak_removed.empty else 0,
    }

    baseline_topk = {
        str(top_k): {
            "selection_only": _aggregate_selection_rows(
                baseline_selected_expanded[baseline_selected_expanded[f"baseline_selected_top{top_k}"] == True],  # noqa: E712
                top_k=top_k,
                selected_column=f"baseline_selected_top{top_k}",
            ),
            "policy_trade": _aggregate_policy_rows(
                baseline_selected_expanded[baseline_selected_expanded[f"baseline_selected_top{top_k}"] == True],  # noqa: E712
                selected_column=f"baseline_selected_top{top_k}",
            ),
        }
        for top_k in TOP_K_VALUES
    }
    challenger_topk = {
        str(top_k): {
            "selection_only": _aggregate_selection_rows(
                challenger_selected_expanded[challenger_selected_expanded[f"challenger_selected_top{top_k}"] == True],  # noqa: E712
                top_k=top_k,
                selected_column=f"challenger_selected_top{top_k}",
            ),
            "policy_trade": _aggregate_policy_rows(
                challenger_selected_expanded[challenger_selected_expanded[f"challenger_selected_top{top_k}"] == True],  # noqa: E712
                selected_column=f"challenger_selected_top{top_k}",
            ),
        }
        for top_k in TOP_K_VALUES
    }
    compare_topk: dict[str, Any] = {}
    for top_k in TOP_K_VALUES:
        key = str(top_k)
        compare_topk[key] = {
            "selection_only": {
                "champion": baseline_topk[key]["selection_only"],
                "challenger": challenger_topk[key]["selection_only"],
            },
            "policy_trade": {
                "champion": baseline_topk[key]["policy_trade"],
                "challenger": challenger_topk[key]["policy_trade"],
            },
            "delta": {
                "selection_only_avg_ret63": None
                if baseline_topk[key]["selection_only"]["avg_ret63"] is None or challenger_topk[key]["selection_only"]["avg_ret63"] is None
                else float(challenger_topk[key]["selection_only"]["avg_ret63"] - baseline_topk[key]["selection_only"]["avg_ret63"]),
                "selection_only_bad_pick_rate": None
                if baseline_topk[key]["selection_only"]["bad_pick_rate"] is None or challenger_topk[key]["selection_only"]["bad_pick_rate"] is None
                else float(challenger_topk[key]["selection_only"]["bad_pick_rate"] - baseline_topk[key]["selection_only"]["bad_pick_rate"]),
                "policy_net_realized_pnl": float(challenger_topk[key]["policy_trade"]["net_realized_pnl"] - baseline_topk[key]["policy_trade"]["net_realized_pnl"]),
                "policy_vs_hold_gap": float(challenger_topk[key]["policy_trade"]["policy_vs_hold_gap_sum"] - baseline_topk[key]["policy_trade"]["policy_vs_hold_gap_sum"]),
            },
        }

    baseline_selected_count = int(len(baseline_selected_expanded))
    challenger_selected_count = int(len(challenger_selected_expanded))
    changed_top5_members_count = int(
        len(
            set(map(tuple, baseline_selected_expanded[baseline_selected_expanded["baseline_selected_top5"] == True][["anchor_date", "symbol", "side"]].values.tolist()))  # noqa: E712
            ^ set(map(tuple, challenger_selected_expanded[challenger_selected_expanded["challenger_selected_top5"] == True][["anchor_date", "symbol", "side"]].values.tolist()))  # noqa: E712
        )
    )
    changed_top10_members_count = int(
        len(
            set(map(tuple, baseline_selected_expanded[baseline_selected_expanded["baseline_selected_top10"] == True][["anchor_date", "symbol", "side"]].values.tolist()))  # noqa: E712
            ^ set(map(tuple, challenger_selected_expanded[challenger_selected_expanded["challenger_selected_top10"] == True][["anchor_date", "symbol", "side"]].values.tolist()))  # noqa: E712
        )
    )
    changed_top20_members_count = int(
        len(
            set(map(tuple, baseline_selected_expanded[baseline_selected_expanded["baseline_selected_top20"] == True][["anchor_date", "symbol", "side"]].values.tolist()))  # noqa: E712
            ^ set(map(tuple, challenger_selected_expanded[challenger_selected_expanded["challenger_selected_top20"] == True][["anchor_date", "symbol", "side"]].values.tolist()))  # noqa: E712
        )
    )
    if len(baseline_selected_expanded) and len(challenger_selected_expanded):
        rank_compare = baseline_selected_expanded[["anchor_date", "symbol", "side", "challenger_rank"]].drop_duplicates().merge(
            challenger_selected_expanded[["anchor_date", "symbol", "side", "challenger_position"]].drop_duplicates(),
            on=["anchor_date", "symbol", "side"],
            how="inner",
            suffixes=("_baseline", "_challenger"),
        )
        changed_rank_count = int(
            (
                pd.to_numeric(rank_compare["challenger_rank"], errors="coerce").astype("Int64")
                != pd.to_numeric(rank_compare["challenger_position"], errors="coerce").astype("Int64")
            ).sum()
        )
    else:
        changed_rank_count = 0

    top5_boundary_score_gap = _compute_boundary_score_gap(baseline_rows, boundary_rank=5)
    top10_boundary_score_gap = _compute_boundary_score_gap(baseline_rows, boundary_rank=10)
    baseline_bottom15 = baseline_selected_expanded[baseline_selected_expanded["baseline_selected_top5"] == False].copy()  # noqa: E712
    challenger_bottom15 = challenger_selected_expanded[challenger_selected_expanded["challenger_rank"].astype(int) > 5].copy()
    baseline_bottom15_contamination_rate = float((pd.to_numeric(baseline_bottom15["ret63"], errors="coerce") <= 0).mean()) if not baseline_bottom15.empty else None
    challenger_bottom15_contamination_rate = float((pd.to_numeric(challenger_bottom15["ret63"], errors="coerce") <= 0).mean()) if not challenger_bottom15.empty else None

    candidate_starvation_by_anchor = defaultdict(lambda: {"baseline": 0, "challenger": 0})
    for anchor_date, group in baseline_rows.groupby("anchor_date", sort=False):
        baseline_counts = {
            "top5": int(group["baseline_selected_top5"].map(lambda x: str(x).lower() == "true").sum()),
            "top10": int(group["baseline_selected_top10"].map(lambda x: str(x).lower() == "true").sum()),
            "top20": int(group["baseline_selected_top20"].map(lambda x: str(x).lower() == "true").sum()),
        }
        challenger_group = challenger_selected_expanded[challenger_selected_expanded["anchor_date"] == anchor_date]
        challenger_counts = {
            "top5": int(challenger_group["challenger_selected_top5"].sum()),
            "top10": int(challenger_group["challenger_selected_top10"].sum()),
            "top20": int(challenger_group["challenger_selected_top20"].sum()),
        }
        candidate_starvation_by_anchor[str(anchor_date)] = {"baseline": baseline_counts, "challenger": challenger_counts}
    candidate_starvation_flag = any(
        int(data["challenger"].get("top10", 0)) < 10 or int(data["challenger"].get("top20", 0)) < 20
        for data in candidate_starvation_by_anchor.values()
    )

    def _aggregate_full_universe(coverage_payload: dict[str, Any]) -> dict[str, Any]:
        return {
            "anchor_count": int(len(anchors)),
            "baseline": {
                "no_trade_rate_mean": float(_safe_float(coverage_payload["aggregate"]["baseline"]["no_trade_rate_mean"])),
                "no_trade_rate_median": float(_safe_float(coverage_payload["aggregate"]["baseline"]["no_trade_rate_median"])),
            },
            "specialized": {
                "no_trade_rate_mean": float(_safe_float(coverage_payload["aggregate"]["specialized"]["no_trade_rate_mean"])),
                "no_trade_rate_median": float(_safe_float(coverage_payload["aggregate"]["specialized"]["no_trade_rate_median"])),
            },
        }

    challenger_policy_top10_delta = compare_topk["10"]["delta"]["policy_net_realized_pnl"]
    challenger_policy_top20_delta = compare_topk["20"]["delta"]["policy_net_realized_pnl"]
    challenger_selection_top10_delta = compare_topk["10"]["delta"]["selection_only_avg_ret63"]
    challenger_selection_top20_delta = compare_topk["20"]["delta"]["selection_only_avg_ret63"]
    selection_only_edge_preserved = bool(
        challenger_selection_top10_delta is not None
        and challenger_selection_top20_delta is not None
        and challenger_selection_top10_delta > 0
        and challenger_selection_top20_delta > 0
    )
    lower_bucket_drag_improved = bool(
        challenger_topk["20"]["policy_trade"]["policy_vs_hold_gap_sum"] >= baseline_topk["20"]["policy_trade"]["policy_vs_hold_gap_sum"]
        and challenger_topk["10"]["policy_trade"]["policy_vs_hold_gap_sum"] >= baseline_topk["10"]["policy_trade"]["policy_vs_hold_gap_sum"]
    )
    policy_layer_destroyed_edge = bool(challenger_policy_top10_delta < 0 or challenger_policy_top20_delta < 0)

    if (
        changed_top10_members_count > 0
        and changed_top20_members_count > 0
        and selection_only_edge_preserved
        and challenger_policy_top10_delta is not None
    ):
        decision = "keep" if not policy_layer_destroyed_edge and not candidate_starvation_flag and lower_bucket_drag_improved else "hold"
    else:
        decision = "drop"

    challenger_policy_trade_ledger = pd.DataFrame(policy_ledger_rows)
    if not challenger_policy_trade_ledger.empty:
        challenger_policy_trade_ledger["selected_action"] = challenger_policy_trade_ledger["selected_action"].fillna("stay").astype(str)
        challenger_policy_trade_ledger["policy_net_realized_pnl"] = pd.to_numeric(challenger_policy_trade_ledger["policy_net_realized_pnl"], errors="coerce")
        challenger_policy_trade_ledger["unrealized_pnl"] = pd.to_numeric(challenger_policy_trade_ledger["unrealized_pnl"], errors="coerce")

    challenger_policy_trade_ledger_out = challenger_policy_trade_ledger.copy()
    if not challenger_policy_trade_ledger_out.empty and "dup_count" in challenger_policy_trade_ledger_out.columns:
        challenger_policy_trade_ledger_out = challenger_policy_trade_ledger_out.loc[challenger_policy_trade_ledger_out.index.repeat(challenger_policy_trade_ledger_out["dup_count"].fillna(1).astype(int))].reset_index(drop=True)

    if not challenger_policy_trade_ledger_out.empty:
        action_text = challenger_policy_trade_ledger_out["selected_action"].fillna("").astype(str)
        exit_reason_text = challenger_policy_trade_ledger_out["exit_reason_primary"].fillna("").astype(str)
        policy_counts_by_candidate = (
            challenger_policy_trade_ledger_out.assign(
                forced_exit_count=(exit_reason_text == "time_stop").astype(int),
                late_exit_count=(exit_reason_text == "lose_ma60").astype(int),
                stop_loss_count=(exit_reason_text == "lose_ma20").astype(int),
                add_count=action_text.str.contains("add", regex=False).astype(int),
                hedge_action_count=action_text.str.contains("hedge", regex=False).astype(int),
            )
            .groupby(["anchor_date", "side", "symbol"], sort=False)[
                ["forced_exit_count", "late_exit_count", "stop_loss_count", "add_count", "hedge_action_count"]
            ]
            .sum()
            .reset_index()
        )
        challenger_selected_expanded = challenger_selected_expanded.merge(
            policy_counts_by_candidate,
            on=["anchor_date", "side", "symbol"],
            how="left",
        )
        for column in ("forced_exit_count", "late_exit_count", "stop_loss_count", "add_count", "hedge_action_count"):
            challenger_selected_expanded[column] = pd.to_numeric(challenger_selected_expanded[column], errors="coerce").fillna(0)
    else:
        for column in ("forced_exit_count", "late_exit_count", "stop_loss_count", "add_count", "hedge_action_count"):
            challenger_selected_expanded[column] = 0

    candidate_snapshots_out = expanded_rows.copy()
    if "dup_count" in candidate_snapshots_out.columns:
        candidate_snapshots_out = candidate_snapshots_out.loc[candidate_snapshots_out.index.repeat(candidate_snapshots_out["dup_count"].fillna(1).astype(int))].reset_index(drop=True)

    selection_only_out = expanded_rows.copy()
    if "dup_count" in selection_only_out.columns:
        selection_only_out = selection_only_out.loc[selection_only_out.index.repeat(selection_only_out["dup_count"].fillna(1).astype(int))].reset_index(drop=True)

    full_universe_gate_coverage = {
        "schema_version": "tradex_selection_layer_weak_regime_bad_pick_removal_full_universe_gate_coverage_v1",
        "generated_at": _utc_now(),
        "rows": [],
        "aggregate": _aggregate_full_universe(_load_json(input_dir / "integrated_guarded_v1_full_universe_gate_coverage.json")),
    }

    summary = {
        "schema_version": "tradex_selection_layer_weak_regime_bad_pick_removal_summary_v1",
        "generated_at": _utc_now(),
        "policy_variant": POLICY_VARIANT,
        "baseline_selection_variant": BASELINE_SELECTION_VARIANT,
        "challenger_selection_variant": CHALLENGER_SELECTION_VARIANT,
        "authoritative_rollup_decision": decision,
        "selection_only_edge_preserved": selection_only_edge_preserved,
        "policy_layer_destroyed_edge": policy_layer_destroyed_edge,
        "top5_boundary_score_gap": top5_boundary_score_gap,
        "top10_boundary_score_gap": top10_boundary_score_gap,
        "baseline_policy_reference": baseline_topk["20"]["policy_trade"],
        "challenger_policy_reference": challenger_topk["20"]["policy_trade"],
        "weak_regime_only_performance": weak_regime_only_performance,
        "candidate_starvation_flag": candidate_starvation_flag,
        "lower_bucket_long_drag_improved": lower_bucket_drag_improved,
        "baseline_bottom15_contamination_rate": baseline_bottom15_contamination_rate,
        "challenger_bottom15_contamination_rate": challenger_bottom15_contamination_rate,
        "selection_rows_count": int(len(selection_only_out)),
        "policy_ledger_rows_count": int(len(challenger_policy_trade_ledger_out)),
        "candidate_snapshot_rows_count": int(len(candidate_snapshots_out)),
        "anchor_count": int(len(anchors)),
        "compare_topk": compare_topk,
        "by_side": _long_short_breakdown(challenger_selected_expanded, selected_column="challenger_selected_top20"),
        "by_rank_bucket": _by_rank_bucket(challenger_selected_expanded, selected_column="challenger_selected_top20"),
        "by_action": _by_action(challenger_policy_trade_ledger_out, selected_column="selected_action"),
        "by_anchor": _by_anchor(challenger_selected_expanded, selected_column="challenger_selected_top20"),
        "by_symbol": _by_symbol(challenger_selected_expanded, selected_column="challenger_selected_top20"),
        "branching_metrics": {
            "changed_top5_members_count": changed_top5_members_count,
            "changed_top10_members_count": changed_top10_members_count,
            "changed_top20_members_count": changed_top20_members_count,
            "changed_rank_count": changed_rank_count,
            "selection_divergence_reason": "selection_layer_weak_regime_bad_pick_removal_vs_specialized_3way_gate",
            "weak_regime_removed_count": weak_removed_count,
            "weak_regime_removed_avg_ret63": weak_removed_avg_ret63,
            "weak_regime_removed_bad_pick_rate": weak_removed_bad_rate,
        },
        "exposure_normalization": {
            "baseline": {
                "deployed_capital_sum": float(baseline_selected_expanded["candidate_capital"].sum()),
                "deployed_capital_mean": float(baseline_selected_expanded["candidate_capital"].mean()) if not baseline_selected_expanded.empty else None,
                "unused_capital_sum": 0.0,
                "unused_capital_rate": 0.0,
                "pnl_per_deployed_capital": float(baseline_topk["20"]["policy_trade"]["net_realized_pnl"] / baseline_selected_expanded["candidate_capital"].sum()) if float(baseline_selected_expanded["candidate_capital"].sum()) != 0 else None,
                "pnl_per_candidate": float(baseline_topk["20"]["policy_trade"]["net_realized_pnl"] / max(1, len(baseline_selected_expanded))),
                "pnl_per_exposed_candidate": float(baseline_topk["20"]["policy_trade"]["net_realized_pnl"] / max(1, int(len(baseline_selected_expanded)))),
                "exposed_candidate_count": int(len(baseline_selected_expanded)),
                "no_exposure_candidate_count": 0,
                "exposure_rate": 1.0,
            },
            "challenger": {
                "deployed_capital_sum": float(challenger_selected_expanded["candidate_capital"].sum()),
                "deployed_capital_mean": float(challenger_selected_expanded["candidate_capital"].mean()) if not challenger_selected_expanded.empty else None,
                "unused_capital_sum": float((baseline_selected_expanded["candidate_capital"].sum() - challenger_selected_expanded["candidate_capital"].sum())),
                "unused_capital_rate": float((baseline_selected_expanded["candidate_capital"].sum() - challenger_selected_expanded["candidate_capital"].sum()) / baseline_selected_expanded["candidate_capital"].sum()) if float(baseline_selected_expanded["candidate_capital"].sum()) != 0 else None,
                "pnl_per_deployed_capital": float(challenger_topk["20"]["policy_trade"]["net_realized_pnl"] / challenger_selected_expanded["candidate_capital"].sum()) if float(challenger_selected_expanded["candidate_capital"].sum()) != 0 else None,
                "pnl_per_candidate": float(challenger_topk["20"]["policy_trade"]["net_realized_pnl"] / max(1, len(challenger_selected_expanded))),
                "pnl_per_exposed_candidate": float(challenger_topk["20"]["policy_trade"]["net_realized_pnl"] / max(1, int((challenger_selected_expanded["challenger_selected_top20"] == True).sum()))),  # noqa: E712
                "exposed_candidate_count": int((challenger_selected_expanded["challenger_selected_top20"] == True).sum()),  # noqa: E712
                "no_exposure_candidate_count": int((challenger_selected_expanded["weak_regime_removed"] == True).sum()),  # noqa: E712
                "exposure_rate": float((challenger_selected_expanded["challenger_selected_top20"] == True).mean()) if not challenger_selected_expanded.empty else None,  # noqa: E712
            },
        },
        "same_condition_contract": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": [int(k) for k in TOP_K_VALUES],
            "same_cost_slippage": "existing chart-first replay contract",
            "same_execution_rule": "next_trading_day_open",
            "same_artifact_detail_level": "candidate_snapshots + selection_only_ledger + policy_trade_ledger + compare + summary",
            "source_db_path": str(source_db_path),
            "selection_snapshot_source": "integrated_guarded_v1 candidate snapshots / selection ledger",
            "policy_replay_source": "scripts.tradex_chart_first_replay.run_chart_first_replay",
        },
        "db_provenance": db_provenance,
        "anchor_dates": [_ymd_to_date_text(value) for value in anchor_dates],
    }

    compare = {
        "schema_version": "tradex_selection_layer_weak_regime_bad_pick_removal_compare_v1",
        "generated_at": _utc_now(),
        "authoritative_rollup_decision": decision,
        "selection_layer": CHALLENGER_SELECTION_VARIANT,
        "policy_variant": POLICY_VARIANT,
        "baseline_selection_variant": BASELINE_SELECTION_VARIANT,
        "challenger_selection_variant": CHALLENGER_SELECTION_VARIANT,
        "champion_vs_challenger": {
            "selection_only": compare_topk,
            "policy_trade": compare_topk,
        },
        "branching_metrics": summary["branching_metrics"],
        "exposure_normalization": summary["exposure_normalization"],
        "selection_only_edge_preserved": selection_only_edge_preserved,
        "policy_layer_destroyed_edge": policy_layer_destroyed_edge,
        "candidate_starvation_flag": candidate_starvation_flag,
        "weak_regime_only_performance": weak_regime_only_performance,
        "lower_bucket_long_drag_improved": lower_bucket_drag_improved,
        "baseline_bottom15_contamination_rate": baseline_bottom15_contamination_rate,
        "challenger_bottom15_contamination_rate": challenger_bottom15_contamination_rate,
        "top5_boundary_score_gap": top5_boundary_score_gap,
        "top10_boundary_score_gap": top10_boundary_score_gap,
    }

    out_paths = {
        "summary": _write_json(output_dir / "selection_layer_weak_regime_bad_pick_removal_summary.json", summary),
        "compare": _write_json(output_dir / "selection_layer_weak_regime_bad_pick_removal_compare.json", compare),
        "candidate_snapshots": _write_json(
            output_dir / "selection_layer_weak_regime_bad_pick_removal_candidate_snapshots.json",
            {"schema_version": "tradex_selection_layer_weak_regime_bad_pick_removal_candidate_snapshots_v1", "generated_at": _utc_now(), "rows": candidate_snapshots_out.to_dict("records")},
        ),
        "selection_only_ledger": _write_json(
            output_dir / "selection_layer_weak_regime_bad_pick_removal_selection_only_ledger.json",
            {"schema_version": "tradex_selection_layer_weak_regime_bad_pick_removal_selection_only_ledger_v1", "generated_at": _utc_now(), "rows": selection_only_out.to_dict("records")},
        ),
        "policy_trade_ledger": _write_json(
            output_dir / "selection_layer_weak_regime_bad_pick_removal_policy_trade_ledger.json",
            {"schema_version": "tradex_selection_layer_weak_regime_bad_pick_removal_policy_trade_ledger_v1", "generated_at": _utc_now(), "rows": challenger_policy_trade_ledger_out.to_dict("records")},
        ),
        "by_rank_bucket": _write_json(
            output_dir / "selection_layer_weak_regime_bad_pick_removal_by_rank_bucket.json",
            {"schema_version": "tradex_selection_layer_weak_regime_bad_pick_removal_by_rank_bucket_v1", "generated_at": _utc_now(), "rows": []},
        ),
        "by_side": _write_json(
            output_dir / "selection_layer_weak_regime_bad_pick_removal_by_side.json",
            {"schema_version": "tradex_selection_layer_weak_regime_bad_pick_removal_by_side_v1", "generated_at": _utc_now(), "rows": []},
        ),
        "by_action": _write_json(
            output_dir / "selection_layer_weak_regime_bad_pick_removal_by_action.json",
            {"schema_version": "tradex_selection_layer_weak_regime_bad_pick_removal_by_action_v1", "generated_at": _utc_now(), "rows": []},
        ),
        "decision": _write_json(
            output_dir / "selection_layer_weak_regime_bad_pick_removal_decision.json",
            {
                "schema_version": "tradex_selection_layer_weak_regime_bad_pick_removal_decision_v1",
                "generated_at": _utc_now(),
                "authoritative_rollup_decision": decision,
                "decision_reasons": [
                    "selection_branching_observed" if changed_top10_members_count > 0 else "insufficient_branching",
                    "weak_regime_bad_pick_removed" if weak_removed_count > 0 else "no_weak_regime_removal",
                    "policy_top10_top20_not_regressed" if not policy_layer_destroyed_edge else "policy_top10_top20_regressed",
                ],
            },
        ),
        "full_universe_gate_coverage": _write_json(
            output_dir / "selection_layer_weak_regime_bad_pick_removal_full_universe_gate_coverage.json",
            full_universe_gate_coverage,
        ),
    }

    # rewrite by_* artifacts with real payloads once the structures are available
    _write_json(
        output_dir / "selection_layer_weak_regime_bad_pick_removal_by_rank_bucket.json",
        {
            "schema_version": "tradex_selection_layer_weak_regime_bad_pick_removal_by_rank_bucket_v1",
            "generated_at": _utc_now(),
            "rows": [
                {"bucket": bucket, **payload}
                for bucket, payload in summary["by_rank_bucket"].items()
            ],
        },
    )
    _write_json(
        output_dir / "selection_layer_weak_regime_bad_pick_removal_by_side.json",
        {
            "schema_version": "tradex_selection_layer_weak_regime_bad_pick_removal_by_side_v1",
            "generated_at": _utc_now(),
            "rows": [{"side": side, **payload} for side, payload in summary["by_side"].items()],
        },
    )
    _write_json(
        output_dir / "selection_layer_weak_regime_bad_pick_removal_by_action.json",
        {
            "schema_version": "tradex_selection_layer_weak_regime_bad_pick_removal_by_action_v1",
            "generated_at": _utc_now(),
            "rows": [{"action": action, **payload} for action, payload in summary["by_action"].items()],
        },
    )

    return {
        "ok": True,
        "output_dir": str(output_dir),
        "summary": summary,
        "compare": compare,
        "paths": {key: str(value) for key, value in out_paths.items()},
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run TRADEX selection-layer weak-regime bad-pick-removal validation.")
    parser.add_argument("--source-db-path", default=str(DEFAULT_SOURCE_DB_PATH))
    parser.add_argument("--input-dir", default=str(BASELINE_INPUT_DIR))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--anchor-limit", type=int, default=None, help="Optional smoke-test limit for anchors.")
    args = parser.parse_args(argv)
    payload = run_selection_layer_weak_regime_bad_pick_removal(
        source_db_path=Path(args.source_db_path).expanduser().resolve(),
        input_dir=Path(args.input_dir).expanduser().resolve(),
        output_dir=Path(args.output_dir).expanduser().resolve(),
        anchor_limit=int(args.anchor_limit) if args.anchor_limit is not None else None,
    )
    print(_json_text(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
