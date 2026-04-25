from __future__ import annotations

import argparse
import json
import math
import sys
from collections import defaultdict
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_chart_first_replay import (  # noqa: E402
    DEFAULT_SOURCE_DB_PATH,
    SHARES_PER_UNIT,
    _build_postmortem,
    _build_roundtrip_summary,
    _load_source_frames,
    simulate_chart_first_replay,
)
from scripts.tradex_random_anchor_3m_policy_instability_diagnosis import _annotate_policy_actions  # noqa: E402


DEFAULT_INPUT_DIR = Path(r"G:\Tradex\sample_replays\tradex_random_anchor_3m_stress200")
DEFAULT_OUTPUT_DIR = Path(r"G:\Tradex\sample_replays\tradex_long_late_exit_repair_v1")
POLICY_VARIANT = "long_late_exit_repair_v1"
TOP_K_VALUES = (5, 10, 20)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return path


def _to_number(value: Any) -> float:
    if value is None:
        return float("nan")
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    try:
        return float(value)
    except Exception:
        return float("nan")


def _is_finite(value: Any) -> bool:
    try:
        return math.isfinite(float(value))
    except Exception:
        return False


def _rank_bucket(rank: Any) -> str:
    value = _to_number(rank)
    if not _is_finite(value):
        return "unknown"
    rank_int = int(value)
    if rank_int <= 5:
        return "top5"
    if rank_int <= 10:
        return "top6_10"
    if rank_int <= 20:
        return "top11_20"
    return "other"


def _topk_member(row: dict[str, Any], method: str, top_k: int) -> bool:
    return bool(row.get(f"{method}_selected_top{int(top_k)}"))


def _selection_rows(rows: list[dict[str, Any]], *, method: str, top_k: int) -> list[dict[str, Any]]:
    return [row for row in rows if _topk_member(row, method, top_k)]


def _summarize_selection_rows(rows: list[dict[str, Any]], *, method: str, top_k: int) -> dict[str, Any]:
    selected = _selection_rows(rows, method=method, top_k=top_k)
    if not selected:
        return {
            "selected_count": 0,
            "bad_pick_rate": None,
            "win_rate": None,
            "avg_ret63": None,
            "median_ret63": None,
            "avg_mfe63": None,
            "avg_mae63": None,
            "worst_mae63": None,
            "neutral_rate": None,
        }
    ret63 = [float(row["ret63"]) for row in selected]
    mfe63 = [float(row["mfe63"]) for row in selected]
    mae63 = [float(row["mae63"]) for row in selected]
    return {
        "selected_count": len(selected),
        "bad_pick_rate": float(sum(1 for value in ret63 if value <= 0) / len(ret63)),
        "win_rate": float(sum(1 for value in ret63 if value > 0) / len(ret63)),
        "avg_ret63": float(sum(ret63) / len(ret63)),
        "median_ret63": float(pd.Series(ret63).median()),
        "avg_mfe63": float(sum(mfe63) / len(mfe63)),
        "avg_mae63": float(sum(mae63) / len(mae63)),
        "worst_mae63": float(min(mae63)),
        "neutral_rate": None,
    }


def _summarize_policy_runs(runs: list[dict[str, Any]], *, method: str, top_k: int) -> dict[str, Any]:
    selected = [row for row in runs if _topk_member(row, method, top_k)]
    if not selected:
        return {
            "selected_count": 0,
            "roundtrip_count": 0,
            "net_realized_pnl": 0.0,
            "max_drawdown_during_holding": None,
            "average_capture_ratio": None,
            "exits_early_or_late": None,
            "win_rate": None,
        }
    pnl = [float(row["net_realized_pnl"]) for row in selected]
    dds = [float(row["max_drawdown_during_holding"]) for row in selected]
    capture = [row.get("average_capture_ratio") for row in selected if row.get("average_capture_ratio") is not None]
    return {
        "selected_count": len(selected),
        "roundtrip_count": int(sum(int(row["roundtrip_count"]) for row in selected)),
        "net_realized_pnl": float(sum(pnl)),
        "max_drawdown_during_holding": float(min(dds)),
        "average_capture_ratio": float(sum(float(value) for value in capture) / len(capture)) if capture else None,
        "exits_early_or_late": "late" if any(str(row.get("exits_early_or_late") or "") == "late" for row in selected) else "acceptable",
        "win_rate": float(sum(1 for value in pnl if value > 0) / len(pnl)),
    }


def _aggregate_topk_metrics(rows: list[dict[str, Any]], run_rows: list[dict[str, Any]]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for top_k in TOP_K_VALUES:
        payload[str(top_k)] = {
            "selection_only": {
                "champion": _summarize_selection_rows(rows, method="champion", top_k=top_k),
                "challenger": _summarize_selection_rows(rows, method="challenger", top_k=top_k),
            },
            "policy_trade": {
                "champion": _summarize_policy_runs(run_rows, method="champion", top_k=top_k),
                "challenger": _summarize_policy_runs(run_rows, method="challenger", top_k=top_k),
            },
        }
        champ_sel = payload[str(top_k)]["selection_only"]["champion"]
        chal_sel = payload[str(top_k)]["selection_only"]["challenger"]
        champ_pol = payload[str(top_k)]["policy_trade"]["champion"]
        chal_pol = payload[str(top_k)]["policy_trade"]["challenger"]
        payload[str(top_k)]["delta"] = {
            "selection_only_avg_ret63": None
            if champ_sel["avg_ret63"] is None or chal_sel["avg_ret63"] is None
            else float(chal_sel["avg_ret63"] - champ_sel["avg_ret63"]),
            "selection_only_bad_pick_rate": None
            if champ_sel["bad_pick_rate"] is None or chal_sel["bad_pick_rate"] is None
            else float(chal_sel["bad_pick_rate"] - champ_sel["bad_pick_rate"]),
            "policy_net_realized_pnl": float(chal_pol["net_realized_pnl"] - champ_pol["net_realized_pnl"]),
            "policy_max_drawdown_during_holding": None
            if champ_pol["max_drawdown_during_holding"] is None or chal_pol["max_drawdown_during_holding"] is None
            else float(chal_pol["max_drawdown_during_holding"] - champ_pol["max_drawdown_during_holding"]),
        }
    return payload


def _aggregate_topk_metrics_by_side(rows: list[dict[str, Any]], run_rows: list[dict[str, Any]]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for side in ("long", "short"):
        side_rows = [row for row in rows if row.get("side") == side]
        side_run_rows = [row for row in run_rows if row.get("side") == side]
        payload[side] = _aggregate_topk_metrics(side_rows, side_run_rows)
    return payload


def _policy_repair_action_summary(policy_frame: pd.DataFrame) -> list[dict[str, Any]]:
    annotated = _annotate_policy_actions(policy_frame.copy())
    annotated["action_category"] = annotated["action_category"].astype(str)
    annotated["reason_primary"] = annotated["reason_primary"].astype(str)
    annotated["is_positive"] = annotated["realized_delta"] > 0
    total_gap = float(annotated.groupby(["anchor_date", "symbol", "side"])["realized_delta"].sum().sum())
    if not math.isfinite(total_gap) or total_gap == 0:
        total_gap = 1.0
    rows: list[dict[str, Any]] = []
    for (category, reason), group in annotated[annotated["action_category"].notna()].groupby(["action_category", "reason_primary"], dropna=False):
        rows.append(
            {
                "action_category": category,
                "reason_primary": reason,
                "count": int(len(group)),
                "realized_pnl_sum": float(group["realized_delta"].sum()),
                "realized_pnl_mean": float(group["realized_delta"].mean()),
                "win_rate": float((group["realized_delta"] > 0).mean()),
                "contribution_to_total_gap": float(group["realized_delta"].sum() / total_gap),
            }
        )
    rows.sort(key=lambda row: row["realized_pnl_sum"])
    return rows


def _repair_policy_ledger_rows(policy_frame: pd.DataFrame) -> pd.DataFrame:
    annotated = policy_frame.copy()
    if "trade_date" not in annotated.columns and "date" in annotated.columns:
        annotated["trade_date"] = annotated["date"]
    annotated = _annotate_policy_actions(annotated)
    annotated["realized_pnl"] = _to_number(annotated["realized_pnl"])
    annotated["realized_delta"] = (
        annotated.groupby(["anchor_date", "symbol", "side"])["realized_pnl"].diff().fillna(annotated["realized_pnl"]).fillna(0.0)
    )
    return annotated


def _policy_vs_hold_gap(rows: pd.DataFrame) -> float:
    return float(pd.to_numeric(rows["policy_vs_hold_gap"], errors="coerce").sum()) if not rows.empty else 0.0


def _load_symbol_frame_cache(
    *,
    source_db_path: Path,
    selection_rows: list[dict[str, Any]],
) -> dict[str, tuple[pd.DataFrame, pd.DataFrame]]:
    windows: dict[str, list[str]] = defaultdict(list)
    for row in selection_rows:
        symbol = str(row["symbol"])
        windows.setdefault(symbol, []).append(str(row["anchor_date"]))
        windows[symbol].append(str(row["exit_date"]))
    cache: dict[str, tuple[pd.DataFrame, pd.DataFrame]] = {}
    for symbol, dates in windows.items():
        start_date = min(dates)
        end_date = max(dates)
        cache[symbol] = _load_source_frames(
            source_db_path=source_db_path,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
        )
    return cache


def run_long_late_exit_repair_v1(
    *,
    input_dir: Path = DEFAULT_INPUT_DIR,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    row_limit: int | None = None,
) -> dict[str, Any]:
    summary_path = input_dir / "random_anchor_replay_summary_stress200.json"
    compare_path = input_dir / "champion_vs_challenger_random_anchor_compare_stress200.json"
    selection_path = input_dir / "selection_only_replay_ledger_stress200.json"
    action_gap_baseline_path = Path(r"G:\Tradex\sample_replays\tradex_random_anchor_3m_stress200_policy_instability\stress200_policy_gap_by_action.json")
    side_gap_baseline_path = Path(r"G:\Tradex\sample_replays\tradex_random_anchor_3m_stress200_policy_instability\stress200_policy_gap_by_side.json")
    rank_gap_baseline_path = Path(r"G:\Tradex\sample_replays\tradex_random_anchor_3m_stress200_policy_instability\stress200_policy_gap_by_rank_bucket.json")
    db_provenance_path = input_dir / "random_anchor_db_provenance_stress200.json"
    exclusion_path = input_dir / "random_anchor_exclusion_diagnostics_stress200.json"
    coverage_path = input_dir / "full_universe_gate_coverage_stress200.json"
    diagnosis_path = Path(r"G:\Tradex\sample_replays\tradex_random_anchor_3m_stress200_policy_instability\stress200_policy_instability_diagnosis.json")

    summary_payload = _load_json(summary_path)
    compare_payload = _load_json(compare_path)
    selection_payload = _load_json(selection_path)
    db_provenance = _load_json(db_provenance_path)
    exclusion_payload = _load_json(exclusion_path)
    coverage_payload = _load_json(coverage_path)
    baseline_action_payload = _load_json(action_gap_baseline_path)
    baseline_side_payload = _load_json(side_gap_baseline_path)
    baseline_rank_payload = _load_json(rank_gap_baseline_path)
    diagnosis_payload = _load_json(diagnosis_path)

    source_db_path = Path(db_provenance.get("source_db_path") or db_provenance.get("working_source_db_path") or DEFAULT_SOURCE_DB_PATH).expanduser().resolve()

    selection_rows: list[dict[str, Any]] = list(selection_payload.get("rows") or [])
    if row_limit is not None:
        selection_rows = selection_rows[: int(row_limit)]

    symbol_cache = _load_symbol_frame_cache(source_db_path=source_db_path, selection_rows=selection_rows)

    repair_run_rows: list[dict[str, Any]] = []
    repair_ledger_rows: list[dict[str, Any]] = []
    for idx, row in enumerate(selection_rows, start=1):
        symbol = str(row["symbol"])
        side = str(row["side"])
        start_date = str(row["anchor_date"])
        end_date = str(row["exit_date"])
        rank_bucket = _rank_bucket(row.get("challenger_rank") if row.get("challenger_rank") is not None else row.get("champion_rank"))
        bars_frame, basis_frame = symbol_cache[symbol]
        ledger_frame, config, roundtrip_payload = simulate_chart_first_replay(
            bars_frame=bars_frame,
            basis_frame=basis_frame,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            source_db_path=source_db_path,
            policy_variant=POLICY_VARIANT,
            policy_context={"rank_bucket": rank_bucket},
        )
        run_config = {
            **config,
            "policy_variant": POLICY_VARIANT,
            "symbol": symbol,
            "period": {"start": start_date, "end": end_date},
        }
        roundtrip_summary = _build_roundtrip_summary(
            config=run_config,
            ledger_frame=ledger_frame,
            roundtrip_payload=roundtrip_payload,
            generated_at=_utc_now(),
        )
        postmortem = _build_postmortem(
            config=run_config,
            ledger_frame=ledger_frame,
            roundtrip_payload=roundtrip_payload,
        )
        summary = roundtrip_summary["aggregate"]
        selected_actions = ledger_frame["selected_action"].astype(str)
        exit_reason_primary = ledger_frame["exit_reason_primary"].astype(str)
        trim_reason_primary = ledger_frame["trim_reason_primary"].astype(str)
        repair_run_rows.append(
            {
                **row,
                "rank_bucket": rank_bucket,
                "policy_variant": POLICY_VARIANT,
                "roundtrip_count": summary["roundtrip_count"],
                "entry_count": summary["entry_count"],
                "exit_count": summary["exit_count"],
                "hedge_count": summary["hedge_count"],
                "stay_count": summary["stay_count"],
                "net_realized_pnl": summary["net_realized_pnl"],
                "max_drawdown_during_holding": summary["max_drawdown_during_holding"],
                "average_capture_ratio": summary.get("average_capture_ratio"),
                "exits_early_or_late": summary.get("exits_early_or_late"),
                "policy_roundtrip_count": summary["roundtrip_count"],
                "policy_net_realized_pnl": summary["net_realized_pnl"],
                "policy_total_pnl": summary["net_realized_pnl"],
                "policy_max_drawdown_during_holding": summary["max_drawdown_during_holding"],
                "policy_exit_timing": summary.get("exits_early_or_late"),
                "number_of_trades": int((selected_actions != "stay").sum()),
                "forced_exit_count": int((exit_reason_primary == "time_stop").sum()),
                "late_exit_count": int((exit_reason_primary == "lose_ma60").sum()),
                "stop_loss_count": int(((exit_reason_primary == "lose_ma20") | (trim_reason_primary == "lose_ma20")).sum()),
                "add_count": int(selected_actions.str.contains("add", regex=False).sum()),
                "hedge_action_count": int(selected_actions.str.contains("hedge", regex=False).sum()),
                "roundtrip_summary": roundtrip_summary,
                "postmortem": postmortem,
            }
        )
        for ledger_row in ledger_frame.to_dict(orient="records"):
            repair_ledger_rows.append(
                {
                    **ledger_row,
                    "anchor_date": start_date,
                    "month_bucket": row.get("month_bucket"),
                    "side": side,
                    "symbol": symbol,
                    "selection_method": row.get("selected_by"),
                    "selection_source": row.get("selected_by_methods"),
                    "champion_rank": row.get("champion_rank"),
                    "challenger_rank": row.get("challenger_rank"),
                    "champion_selected_top5": row.get("champion_selected_top5"),
                    "champion_selected_top10": row.get("champion_selected_top10"),
                    "champion_selected_top20": row.get("champion_selected_top20"),
                    "challenger_selected_top5": row.get("challenger_selected_top5"),
                    "challenger_selected_top10": row.get("challenger_selected_top10"),
                    "challenger_selected_top20": row.get("challenger_selected_top20"),
                    "changed_top5_member": row.get("changed_top5_member"),
                    "changed_top10_member": row.get("changed_top10_member"),
                    "changed_top20_member": row.get("changed_top20_member"),
                    "rank_bucket": rank_bucket,
                    "policy_variant": POLICY_VARIANT,
                    "policy_exit_timing": summary.get("exits_early_or_late"),
                    "policy_roundtrip_count": summary["roundtrip_count"],
                    "policy_net_realized_pnl": summary["net_realized_pnl"],
                    "policy_max_drawdown_during_holding": summary["max_drawdown_during_holding"],
                }
            )
        if idx % 250 == 0:
            print(f"[repair] processed {idx}/{len(selection_rows)} rows", flush=True)

    repair_run_rows_frame = pd.DataFrame(repair_run_rows)
    repair_ledger_frame = pd.DataFrame(repair_ledger_rows)
    selection_rows_frame = pd.DataFrame(selection_rows)
    if not repair_run_rows_frame.empty:
        repair_run_rows_frame["policy_vs_hold_gap"] = pd.to_numeric(repair_run_rows_frame["policy_net_realized_pnl"], errors="coerce") - (
            pd.to_numeric(repair_run_rows_frame["ret63"], errors="coerce") * pd.to_numeric(repair_run_rows_frame["entry_price"], errors="coerce") * SHARES_PER_UNIT
        )
    if not repair_ledger_frame.empty:
        repair_ledger_frame = _repair_policy_ledger_rows(repair_ledger_frame)

    # By-anchor aggregation.
    anchor_rows: list[dict[str, Any]] = []
    if not repair_run_rows_frame.empty:
        for anchor_date, anchor_df in repair_run_rows_frame.groupby("anchor_date"):
            long_df = anchor_df[anchor_df["side"] == "long"]
            short_df = anchor_df[anchor_df["side"] == "short"]
            worst = anchor_df.sort_values("policy_vs_hold_gap").head(5)
            anchor_rows.append(
                {
                    "anchor_date": str(anchor_date),
                    "month_bucket": str(anchor_df["month_bucket"].iloc[0]) if not anchor_df.empty else None,
                    "count": int(len(anchor_df)),
                    "policy_vs_hold_gap_sum": float(anchor_df["policy_vs_hold_gap"].sum()),
                    "policy_total_pnl_sum": float(anchor_df["policy_total_pnl"].sum()),
                    "long_contribution": float(long_df["policy_vs_hold_gap"].sum()) if not long_df.empty else 0.0,
                    "short_contribution": float(short_df["policy_vs_hold_gap"].sum()) if not short_df.empty else 0.0,
                    "number_of_trades": int(anchor_df["number_of_trades"].sum()),
                    "late_exit_count": int(anchor_df["late_exit_count"].sum()),
                    "forced_exit_count": int(anchor_df["forced_exit_count"].sum()),
                    "stop_loss_count": int(anchor_df["stop_loss_count"].sum()),
                    "add_count": int(anchor_df["add_count"].sum()),
                    "hedge_action_count": int(anchor_df["hedge_action_count"].sum()),
                    "worst_symbols": worst[["symbol", "side", "policy_vs_hold_gap", "policy_total_pnl", "rank_bucket"]].to_dict(orient="records"),
                }
            )
        anchor_rows.sort(key=lambda row: row["policy_vs_hold_gap_sum"])

    # By-side aggregation.
    side_rows: list[dict[str, Any]] = []
    for side in ("long", "short"):
        side_df = repair_run_rows_frame[repair_run_rows_frame["side"] == side].copy()
        if side_df.empty:
            continue
        side_rows.append(
            {
                "side": side,
                "count": int(len(side_df)),
                "selection_only_ret63_mean": float(pd.to_numeric(side_df["ret63"], errors="coerce").mean()),
                "selection_only_ret63_median": float(pd.to_numeric(side_df["ret63"], errors="coerce").median()),
                "policy_total_pnl_sum": float(side_df["policy_net_realized_pnl"].sum()),
                "policy_vs_hold_gap_sum": float(side_df["policy_vs_hold_gap"].sum()),
                "policy_vs_hold_gap_mean": float(side_df["policy_vs_hold_gap"].mean()),
                "bad_pick_rate": float((pd.to_numeric(side_df["ret63"], errors="coerce") <= 0).mean()),
                "forced_exit_count": int(side_df["forced_exit_count"].sum()),
                "late_exit_count": int(side_df["late_exit_count"].sum()),
                "stop_loss_count": int(side_df["stop_loss_count"].sum()),
                "add_count": int(side_df["add_count"].sum()),
                "hedge_action_count": int(side_df["hedge_action_count"].sum()),
                "top_contributing_anchors": side_df.sort_values("policy_vs_hold_gap").head(10)[["anchor_date", "symbol", "policy_vs_hold_gap"]].to_dict(orient="records"),
            }
        )

    # By-rank aggregation.
    rank_rows: list[dict[str, Any]] = []
    for bucket in ("top5", "top6_10", "top11_20", "other", "unknown"):
        bucket_df = repair_run_rows_frame[repair_run_rows_frame["rank_bucket"] == bucket].copy()
        if bucket_df.empty:
            continue
        rank_rows.append(
            {
                "rank_bucket": bucket,
                "count": int(len(bucket_df)),
                "selection_only_ret63_mean": float(pd.to_numeric(bucket_df["ret63"], errors="coerce").mean()),
                "selection_only_ret63_median": float(pd.to_numeric(bucket_df["ret63"], errors="coerce").median()),
                "policy_total_pnl_sum": float(bucket_df["policy_net_realized_pnl"].sum()),
                "policy_vs_hold_gap_sum": float(bucket_df["policy_vs_hold_gap"].sum()),
                "policy_vs_hold_gap_mean": float(bucket_df["policy_vs_hold_gap"].mean()),
                "bad_pick_rate": float((pd.to_numeric(bucket_df["ret63"], errors="coerce") <= 0).mean()),
                "forced_exit_count": int(bucket_df["forced_exit_count"].sum()),
                "late_exit_count": int(bucket_df["late_exit_count"].sum()),
                "stop_loss_count": int(bucket_df["stop_loss_count"].sum()),
                "add_count": int(bucket_df["add_count"].sum()),
                "hedge_action_count": int(bucket_df["hedge_action_count"].sum()),
                "change_counts": {
                    "changed_top5_members_count": int((bucket_df["changed_top5_member"].astype(str) == "True").sum()) if "changed_top5_member" in bucket_df.columns else 0,
                    "changed_top10_members_count": int((bucket_df["changed_top10_member"].astype(str) == "True").sum()) if "changed_top10_member" in bucket_df.columns else 0,
                    "changed_top20_members_count": int((bucket_df["changed_top20_member"].astype(str) == "True").sum()) if "changed_top20_member" in bucket_df.columns else 0,
                },
                "top_contributing_anchors": bucket_df.sort_values("policy_vs_hold_gap").head(10)[["anchor_date", "symbol", "side", "policy_vs_hold_gap"]].to_dict(orient="records"),
                "top_contributing_symbols": bucket_df.groupby(["symbol", "side"])["policy_vs_hold_gap"].sum().sort_values().head(10).reset_index().to_dict(orient="records"),
            }
        )

    action_rows = _policy_repair_action_summary(repair_ledger_frame) if not repair_ledger_frame.empty else []
    action_by_side: dict[str, list[dict[str, Any]]] = defaultdict(list)
    if not repair_ledger_frame.empty:
        annotated = _annotate_policy_actions(repair_ledger_frame.copy())
        annotated["action_category"] = annotated["action_category"].astype(str)
        annotated["reason_primary"] = annotated["reason_primary"].astype(str)
        for side, side_df in annotated.groupby("side"):
            total_gap = float(repair_run_rows_frame[repair_run_rows_frame["side"] == side]["policy_vs_hold_gap"].sum())
            if not math.isfinite(total_gap) or total_gap == 0:
                total_gap = 1.0
            rows: list[dict[str, Any]] = []
            for (category, reason), grp in side_df[side_df["action_category"].notna()].groupby(["action_category", "reason_primary"], dropna=False):
                rows.append(
                    {
                        "action_category": category,
                        "reason_primary": reason,
                        "count": int(len(grp)),
                        "realized_pnl_sum": float(grp["realized_delta"].sum()),
                        "realized_pnl_mean": float(grp["realized_delta"].mean()),
                        "win_rate": float((grp["realized_delta"] > 0).mean()),
                        "contribution_to_total_gap": float(grp["realized_delta"].sum() / total_gap),
                    }
                )
            rows.sort(key=lambda row: row["realized_pnl_sum"])
            action_by_side[str(side)] = rows

    current_topk = compare_payload["aggregate_metrics"]["topk_metrics"]
    repair_topk = _aggregate_topk_metrics(selection_rows, repair_run_rows)
    repair_side_topk = _aggregate_topk_metrics_by_side(selection_rows, repair_run_rows)

    comparison_topk: dict[str, Any] = {}
    for top_k in TOP_K_VALUES:
        key = str(top_k)
        comparison_topk[key] = {
            "current_policy": current_topk[key]["policy_trade"],
            "repair_policy": repair_topk[key]["policy_trade"],
            "delta": {
                "policy_net_realized_pnl": repair_topk[key]["delta"]["policy_net_realized_pnl"],
                "selection_only_avg_ret63": repair_topk[key]["delta"]["selection_only_avg_ret63"],
                "selection_only_bad_pick_rate": repair_topk[key]["delta"]["selection_only_bad_pick_rate"],
                "policy_max_drawdown_during_holding": repair_topk[key]["delta"]["policy_max_drawdown_during_holding"],
            },
        }

    current_action_rows = baseline_action_payload.get("rows") or []
    current_side_rows = baseline_side_payload.get("rows") or []
    current_rank_rows = baseline_rank_payload.get("rows") or []

    current_late_exit_loss = next(
        (row["realized_pnl_sum"] for row in current_action_rows if row.get("action_category") == "late_exit" and row.get("reason_primary") == "lose_ma60"),
        None,
    )
    repair_late_exit_loss = next(
        (row["realized_pnl_sum"] for row in action_rows if row.get("action_category") == "late_exit" and row.get("reason_primary") == "lose_ma60"),
        None,
    )

    top5_delta = comparison_topk["5"]["delta"]["policy_net_realized_pnl"]
    top10_delta = comparison_topk["10"]["delta"]["policy_net_realized_pnl"]
    top20_delta = comparison_topk["20"]["delta"]["policy_net_realized_pnl"]
    top10_ok = bool(top10_delta > 0)
    top20_ok = bool(top20_delta > 0)
    top5_ok = bool(top5_delta >= -1e-6)
    if top10_ok and top20_ok and top5_ok:
        decision = "keep"
    elif top10_ok or top20_ok:
        decision = "hold"
    else:
        decision = "drop"

    summary = {
        "schema_version": "tradex_long_late_exit_repair_v1_summary_v1",
        "generated_at": _utc_now(),
        "policy_variant": POLICY_VARIANT,
        "diagnosis_decision": decision,
        "current_policy_reference": {
            "stress200_authoritative_rollup_decision": summary_payload.get("authoritative_rollup_decision"),
            "stress200_same_db": db_provenance.get("stress60_same_db"),
            "stress200_source_db_path": db_provenance.get("source_db_path"),
            "stress200_policy_vs_hold_gap_sum": float((compare_payload["aggregate_metrics"]["topk_metrics"]["20"]["policy_trade"]["challenger"]["net_realized_pnl"])),
        },
        "repair_policy_reference": {
            "row_count": int(len(repair_run_rows_frame)),
            "ledger_row_count": int(len(repair_ledger_frame)),
            "policy_vs_hold_gap_sum": float(repair_run_rows_frame["policy_vs_hold_gap"].sum()) if not repair_run_rows_frame.empty else None,
            "late_exit_loss_sum": repair_late_exit_loss,
        },
        "topk_observations": {
            "top5": top5_delta,
            "top10": top10_delta,
            "top20": top20_delta,
        },
        "top6_10_policy_vs_hold_gap_delta": None
        if not current_rank_rows
        else float(next((row["policy_vs_hold_gap_sum"] for row in rank_rows if row["rank_bucket"] == "top6_10"), 0.0) - next((row["policy_vs_hold_gap_sum"] for row in current_rank_rows if row["rank_bucket"] == "top6_10"), 0.0)),
        "top11_20_policy_vs_hold_gap_delta": None
        if not current_rank_rows
        else float(next((row["policy_vs_hold_gap_sum"] for row in rank_rows if row["rank_bucket"] == "top11_20"), 0.0) - next((row["policy_vs_hold_gap_sum"] for row in current_rank_rows if row["rank_bucket"] == "top11_20"), 0.0)),
        "selection_only_edge_preserved": bool(diagnosis_payload.get("selection_only_edge_preserved", False)),
        "policy_layer_destroyed_edge": bool(diagnosis_payload.get("policy_layer_destroyed_edge", False)),
        "late_exit_loss_reduced": None if current_late_exit_loss is None or repair_late_exit_loss is None else float(repair_late_exit_loss - current_late_exit_loss),
        "recommended_next_axis": "short_supply_and_noise_suppression" if decision != "keep" else "policy_rollout_guardrails",
        "input_artifacts": {
            "selection_only_replay_ledger_stress200": str(selection_path),
            "champion_vs_challenger_random_anchor_compare_stress200": str(compare_path),
            "stress200_policy_gap_by_action": str(action_gap_baseline_path),
            "stress200_policy_gap_by_side": str(side_gap_baseline_path),
            "stress200_policy_gap_by_rank_bucket": str(rank_gap_baseline_path),
            "stress200_policy_instability_diagnosis": str(diagnosis_path),
            "random_anchor_db_provenance_stress200": str(db_provenance_path),
            "random_anchor_exclusion_diagnostics_stress200": str(exclusion_path),
            "full_universe_gate_coverage_stress200": str(coverage_path),
        },
        "summary_references": {
            "stress200_anchor_count": summary_payload.get("anchor_count"),
            "stress200_candidate_snapshot_rows_count": summary_payload.get("candidate_snapshot_rows_count"),
            "stress200_selection_only_replay_rows_count": summary_payload.get("selection_only_replay_rows_count"),
            "stress200_policy_trade_run_rows_count": summary_payload.get("policy_trade_run_rows_count"),
            "stress200_policy_trade_ledger_rows_count": summary_payload.get("policy_trade_ledger_rows_count"),
            "basis_row_skip_count": exclusion_payload.get("skipped_symbols_without_basis_row_count"),
            "full_universe_no_trade_rate_mean_specialized": coverage_payload.get("aggregate", {}).get("specialized", {}).get("no_trade_rate_mean"),
        },
        "selection_topk_current_policy": {
            "top5": current_topk["5"]["policy_trade"]["challenger"],
            "top10": current_topk["10"]["policy_trade"]["challenger"],
            "top20": current_topk["20"]["policy_trade"]["challenger"],
        },
        "selection_topk_repair_policy": {
            "top5": repair_topk["5"]["policy_trade"]["challenger"],
            "top10": repair_topk["10"]["policy_trade"]["challenger"],
            "top20": repair_topk["20"]["policy_trade"]["challenger"],
        },
        "comparison_topk": comparison_topk,
        "baseline_action_rows": current_action_rows,
        "repair_action_rows": action_rows,
        "current_side_rows": current_side_rows,
        "repair_side_rows": side_rows,
        "current_rank_rows": current_rank_rows,
        "repair_rank_rows": rank_rows,
        "anchor_count": len({row["anchor_date"] for row in repair_run_rows}),
        "selection_rows_count": len(selection_rows),
        "policy_run_rows_count": len(repair_run_rows),
        "policy_ledger_rows_count": len(repair_ledger_rows),
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "summary_json": _write_json(output_dir / "long_late_exit_repair_v1_summary.json", summary),
        "compare_json": _write_json(
            output_dir / "long_late_exit_repair_v1_compare.json",
            {
                "schema_version": "tradex_long_late_exit_repair_v1_compare_v1",
                "generated_at": _utc_now(),
                "policy_variant": POLICY_VARIANT,
                "same_condition_contract": {
                    "same_anchor_set": True,
                    "same_candidates": True,
                    "same_db_source": db_provenance.get("source_db_path"),
                    "same_execution_rule": "next_trading_day_open",
                    "same_cost_slippage": "existing chart-first replay contract",
                    "same_top_k": [5, 10, 20],
                    "same_period": True,
                },
                "current_policy": {
                    "topk_metrics": current_topk,
                    "side_rows": current_side_rows,
                    "rank_rows": current_rank_rows,
                    "action_rows": current_action_rows,
                },
                "repair_policy": {
                    "topk_metrics": repair_topk,
                    "side_rows": side_rows,
                    "rank_rows": rank_rows,
                    "action_rows": action_rows,
                },
                "delta": {
                    "top5_policy_net_realized_pnl": comparison_topk["5"]["delta"]["policy_net_realized_pnl"],
                    "top10_policy_net_realized_pnl": comparison_topk["10"]["delta"]["policy_net_realized_pnl"],
                    "top20_policy_net_realized_pnl": comparison_topk["20"]["delta"]["policy_net_realized_pnl"],
                    "top5_selection_only_avg_ret63": comparison_topk["5"]["delta"]["selection_only_avg_ret63"],
                    "top10_selection_only_avg_ret63": comparison_topk["10"]["delta"]["selection_only_avg_ret63"],
                    "top20_selection_only_avg_ret63": comparison_topk["20"]["delta"]["selection_only_avg_ret63"],
                },
            },
        ),
        "by_rank_json": _write_json(
            output_dir / "long_late_exit_repair_v1_by_rank_bucket.json",
            {
                "schema_version": "tradex_long_late_exit_repair_v1_by_rank_bucket_v1",
                "generated_at": _utc_now(),
                "baseline": current_rank_rows,
                "repair": rank_rows,
            },
        ),
        "by_side_json": _write_json(
            output_dir / "long_late_exit_repair_v1_by_side.json",
            {
                "schema_version": "tradex_long_late_exit_repair_v1_by_side_v1",
                "generated_at": _utc_now(),
                "baseline": current_side_rows,
                "repair": side_rows,
            },
        ),
        "by_action_json": _write_json(
            output_dir / "long_late_exit_repair_v1_by_action.json",
            {
                "schema_version": "tradex_long_late_exit_repair_v1_by_action_v1",
                "generated_at": _utc_now(),
                "baseline": current_action_rows,
                "repair": action_rows,
            },
        ),
        "trade_ledger_json": _write_json(
            output_dir / "long_late_exit_repair_v1_trade_ledger.json",
            {
                "schema_version": "tradex_long_late_exit_repair_v1_trade_ledger_v1",
                "generated_at": _utc_now(),
                "policy_variant": POLICY_VARIANT,
                "rows": repair_ledger_rows,
            },
        ),
    }

    return {
        "ok": True,
        "output_dir": str(output_dir),
        "paths": {key: str(value) for key, value in paths.items()},
        "summary": summary,
        "compare": _load_json(Path(paths["compare_json"])),
        "by_rank_bucket": _load_json(Path(paths["by_rank_json"])),
        "by_side": _load_json(Path(paths["by_side_json"])),
        "by_action": _load_json(Path(paths["by_action_json"])),
        "trade_ledger": _load_json(Path(paths["trade_ledger_json"])),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Diagnose long late-exit repair v1 on stress200 candidate rows.")
    parser.add_argument("--input-dir", default=str(DEFAULT_INPUT_DIR))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--row-limit", type=int, default=None)
    args = parser.parse_args(argv)

    payload = run_long_late_exit_repair_v1(
        input_dir=Path(args.input_dir).expanduser().resolve(),
        output_dir=Path(args.output_dir).expanduser().resolve(),
        row_limit=args.row_limit,
    )
    print(json.dumps(payload["summary"], ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
