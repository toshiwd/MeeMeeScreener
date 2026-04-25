from __future__ import annotations

import argparse
import copy
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_chart_first_replay import (  # noqa: E402
    SHARES_PER_UNIT,
    _build_postmortem,
    _build_roundtrip_summary,
    simulate_chart_first_replay,
)
from scripts.tradex_long_late_exit_repair_v1 import (  # noqa: E402
    TOP_K_VALUES,
    _aggregate_topk_metrics,
    _aggregate_topk_metrics_by_side,
    _load_json,
    _load_symbol_frame_cache,
    _policy_repair_action_summary,
    _rank_bucket,
    _to_number,
    _utc_now,
    _write_json,
)


DEFAULT_INPUT_DIR = Path(r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_stress200")
DEFAULT_OUTPUT_DIR = Path(r"G:\Tradex\sample_replays\tradex_lower_bucket_long_rollout_v2_stress200")
POLICY_VARIANT = "lower_bucket_long_rollout_v2"
BASELINE_POLICY_VARIANT = "integrated_specialized_gate_guarded_policy_v1"
VARIANT_ORDER = ("A", "B", "C")


def _variant_label(variant: str) -> str:
    return f"{POLICY_VARIANT}_{variant.lower()}"


def _variant_note(variant: str) -> str:
    if variant == "C":
        return "Variant C collapses to A under the current hold-only long semantics."
    return {
        "A": "Suppress long top11-20 only; keep top6-10 at baseline long rollout behavior.",
        "B": "Suppress long top6-20.",
        "C": "Suppress long top11-20 only; fixed-hold interpretation collapses to A here.",
    }.get(variant, "")


def _full_universe_rate_mean(rows: list[dict[str, Any]], side: str, rate_key: str) -> float | None:
    values: list[float] = []
    for row in rows:
        payload = row.get(side)
        if isinstance(payload, dict) and payload.get(rate_key) is not None:
            values.append(float(payload[rate_key]))
    if not values:
        return None
    return float(sum(values) / len(values))


def _selection_rows_by_topk(rows: list[dict[str, Any]], *, method: str, top_k: int) -> list[dict[str, Any]]:
    return [row for row in rows if bool(row.get(f"{method}_selected_top{int(top_k)}"))]


def _selection_summary(rows: list[dict[str, Any]], *, method: str, top_k: int) -> dict[str, Any]:
    selected = _selection_rows_by_topk(rows, method=method, top_k=top_k)
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
    ret63 = pd.to_numeric(pd.Series([row["ret63"] for row in selected]), errors="coerce")
    mfe63 = pd.to_numeric(pd.Series([row["mfe63"] for row in selected]), errors="coerce")
    mae63 = pd.to_numeric(pd.Series([row["mae63"] for row in selected]), errors="coerce")
    return {
        "selected_count": int(len(selected)),
        "bad_pick_rate": float((ret63 <= 0).mean()),
        "win_rate": float((ret63 > 0).mean()),
        "avg_ret63": float(ret63.mean()),
        "median_ret63": float(ret63.median()),
        "avg_mfe63": float(mfe63.mean()),
        "avg_mae63": float(mae63.mean()),
        "worst_mae63": float(mae63.min()),
        "neutral_rate": None,
    }


def _policy_summary(rows: list[dict[str, Any]], *, method: str, top_k: int) -> dict[str, Any]:
    selected = _selection_rows_by_topk(rows, method=method, top_k=top_k)
    if not selected:
        return {
            "selected_count": 0,
            "roundtrip_count": 0,
            "net_realized_pnl": 0.0,
            "max_drawdown_during_holding": None,
            "average_capture_ratio": None,
            "exits_early_or_late": None,
            "win_rate": None,
            "hold_gap_sum": 0.0,
        }
    pnl = pd.to_numeric(pd.Series([row["policy_net_realized_pnl"] for row in selected]), errors="coerce")
    dds = pd.to_numeric(pd.Series([row["policy_max_drawdown_during_holding"] for row in selected]), errors="coerce")
    capture = pd.to_numeric(pd.Series([row.get("average_capture_ratio") for row in selected]), errors="coerce").dropna()
    hold_gap = pd.to_numeric(pd.Series([row.get("policy_vs_hold_gap") for row in selected]), errors="coerce")
    return {
        "selected_count": int(len(selected)),
        "roundtrip_count": int(sum(int(row["roundtrip_count"]) for row in selected)),
        "net_realized_pnl": float(pnl.sum()),
        "max_drawdown_during_holding": float(dds.min()) if not dds.dropna().empty else None,
        "average_capture_ratio": float(capture.mean()) if not capture.empty else None,
        "exits_early_or_late": "late" if any(str(row.get("exits_early_or_late") or "") == "late" for row in selected) else "acceptable",
        "win_rate": float((pnl > 0).mean()),
        "hold_gap_sum": float(hold_gap.sum()) if not hold_gap.empty else 0.0,
    }


def _aggregate_variant_topk(rows: list[dict[str, Any]], run_rows: list[dict[str, Any]]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for top_k in TOP_K_VALUES:
        key = str(top_k)
        payload[key] = {
            "selection_only": {
                "champion": _selection_summary(rows, method="champion", top_k=top_k),
                "challenger": _selection_summary(rows, method="challenger", top_k=top_k),
            },
            "policy_trade": {
                "champion": _policy_summary(run_rows, method="champion", top_k=top_k),
                "challenger": _policy_summary(run_rows, method="challenger", top_k=top_k),
            },
        }
        champ_sel = payload[key]["selection_only"]["champion"]
        chal_sel = payload[key]["selection_only"]["challenger"]
        champ_pol = payload[key]["policy_trade"]["champion"]
        chal_pol = payload[key]["policy_trade"]["challenger"]
        payload[key]["delta"] = {
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
            "policy_hold_gap_sum": float(float(chal_pol.get("hold_gap_sum") or 0.0) - float(champ_pol.get("hold_gap_sum") or 0.0)),
        }
    return payload


def _aggregate_variant_side(rows: list[dict[str, Any]], run_rows: list[dict[str, Any]]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for side in ("long", "short"):
        side_rows = [row for row in rows if row.get("side") == side]
        side_run_rows = [row for row in run_rows if row.get("side") == side]
        payload[side] = _aggregate_variant_topk(side_rows, side_run_rows)
    return payload


def _rows_to_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    for column in ("policy_net_realized_pnl", "policy_max_drawdown_during_holding", "policy_vs_hold_gap", "ret63", "entry_price"):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def _build_rank_rows(run_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    frame = _rows_to_frame(run_rows)
    if frame.empty:
        return []
    rows: list[dict[str, Any]] = []
    for bucket in ("top5", "top6_10", "top11_20", "other", "unknown"):
        bucket_df = frame[frame["rank_bucket"] == bucket].copy()
        if bucket_df.empty:
            continue
        rows.append(
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
                "long_entry_count": int(bucket_df["long_entry_count"].sum()),
                "no_exposure_count": int(bucket_df["no_exposure_count"].sum()),
                "change_counts": {
                    "changed_top5_members_count": int((bucket_df["changed_top5_member"].astype(str) == "True").sum()) if "changed_top5_member" in bucket_df.columns else 0,
                    "changed_top10_members_count": int((bucket_df["changed_top10_member"].astype(str) == "True").sum()) if "changed_top10_member" in bucket_df.columns else 0,
                    "changed_top20_members_count": int((bucket_df["changed_top20_member"].astype(str) == "True").sum()) if "changed_top20_member" in bucket_df.columns else 0,
                },
                "top_contributing_anchors": bucket_df.sort_values("policy_vs_hold_gap").head(10)[["anchor_date", "symbol", "side", "policy_vs_hold_gap"]].to_dict(orient="records"),
                "top_contributing_symbols": bucket_df.groupby(["symbol", "side"])["policy_vs_hold_gap"].sum().sort_values().head(10).reset_index().to_dict(orient="records"),
            }
        )
    return rows


def _build_side_rows(run_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    frame = _rows_to_frame(run_rows)
    if frame.empty:
        return []
    rows: list[dict[str, Any]] = []
    for side in ("long", "short"):
        side_df = frame[frame["side"] == side].copy()
        if side_df.empty:
            continue
        rows.append(
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
                "long_entry_count": int(side_df["long_entry_count"].sum()),
                "no_exposure_count": int(side_df["no_exposure_count"].sum()),
                "top_contributing_anchors": side_df.sort_values("policy_vs_hold_gap").head(10)[["anchor_date", "symbol", "policy_vs_hold_gap"]].to_dict(orient="records"),
            }
        )
    return rows


def _build_anchor_rows(run_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    frame = _rows_to_frame(run_rows)
    if frame.empty:
        return []
    rows: list[dict[str, Any]] = []
    for anchor_date, anchor_df in frame.groupby("anchor_date"):
        long_df = anchor_df[anchor_df["side"] == "long"]
        short_df = anchor_df[anchor_df["side"] == "short"]
        worst = anchor_df.sort_values("policy_vs_hold_gap").head(5)
        rows.append(
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
                "long_entry_count": int(anchor_df["long_entry_count"].sum()),
                "no_exposure_count": int(anchor_df["no_exposure_count"].sum()),
                "worst_symbols": worst[["symbol", "side", "policy_vs_hold_gap", "policy_total_pnl", "rank_bucket"]].to_dict(orient="records"),
            }
        )
    rows.sort(key=lambda row: row["policy_vs_hold_gap_sum"])
    return rows


def _variant_selection_row_summary(rows: list[dict[str, Any]], *, top_k: int) -> dict[str, Any]:
    selected = [row for row in rows if bool(row.get(f"challenger_selected_top{int(top_k)}"))]
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
    ret63 = pd.to_numeric(pd.Series([row["ret63"] for row in selected]), errors="coerce")
    mfe63 = pd.to_numeric(pd.Series([row["mfe63"] for row in selected]), errors="coerce")
    mae63 = pd.to_numeric(pd.Series([row["mae63"] for row in selected]), errors="coerce")
    return {
        "selected_count": int(len(selected)),
        "bad_pick_rate": float((ret63 <= 0).mean()),
        "win_rate": float((ret63 > 0).mean()),
        "avg_ret63": float(ret63.mean()),
        "median_ret63": float(ret63.median()),
        "avg_mfe63": float(mfe63.mean()),
        "avg_mae63": float(mae63.mean()),
        "worst_mae63": float(mae63.min()),
        "neutral_rate": None,
    }


def _variant_policy_run(
    *,
    variant: str,
    selection_rows: list[dict[str, Any]],
    source_db_path: Path,
    symbol_cache: dict[str, tuple[pd.DataFrame, pd.DataFrame]],
    output_dir: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    run_rows: list[dict[str, Any]] = []
    ledger_rows: list[dict[str, Any]] = []
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
            policy_context={"rank_bucket": rank_bucket, "rollout_variant": variant, "selected_by": row.get("selected_by")},
        )
        run_config = {
            **config,
            "policy_variant": POLICY_VARIANT,
            "rollout_variant": variant,
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
        selected_actions = ledger_frame["selected_action"].astype(str) if not ledger_frame.empty else pd.Series(dtype=str)
        exit_reason_primary = ledger_frame["exit_reason_primary"].astype(str) if not ledger_frame.empty else pd.Series(dtype=str)
        trim_reason_primary = ledger_frame["trim_reason_primary"].astype(str) if not ledger_frame.empty else pd.Series(dtype=str)
        long_entry_count = int((selected_actions.str.contains("long_entry", regex=False) | selected_actions.str.contains("long_add", regex=False)).sum()) if not selected_actions.empty else 0
        no_exposure_count = int(long_entry_count == 0 and side == "long")
        hold_pnl = float(_to_number(row.get("ret63")) * _to_number(row.get("entry_price")) * SHARES_PER_UNIT)
        policy_vs_hold_gap = float(summary["net_realized_pnl"] - hold_pnl)
        run_rows.append(
            {
                **row,
                "rank_bucket": rank_bucket,
                "policy_variant": POLICY_VARIANT,
                "rollout_variant": variant,
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
                "policy_vs_hold_gap": policy_vs_hold_gap,
                "number_of_trades": int((selected_actions != "stay").sum()) if not selected_actions.empty else 0,
                "forced_exit_count": int((exit_reason_primary == "time_stop").sum()) if not exit_reason_primary.empty else 0,
                "late_exit_count": int((exit_reason_primary == "lose_ma60").sum()) if not exit_reason_primary.empty else 0,
                "stop_loss_count": int(((exit_reason_primary == "lose_ma20") | (trim_reason_primary == "lose_ma20")).sum()) if not trim_reason_primary.empty else 0,
                "add_count": int(selected_actions.str.contains("add", regex=False).sum()) if not selected_actions.empty else 0,
                "hedge_action_count": int(selected_actions.str.contains("hedge", regex=False).sum()) if not selected_actions.empty else 0,
                "long_entry_count": long_entry_count,
                "no_exposure_count": no_exposure_count,
                "hold_pnl": hold_pnl,
                "roundtrip_summary": roundtrip_summary,
                "postmortem": postmortem,
            }
        )
        for ledger_row in ledger_frame.to_dict(orient="records"):
            ledger_rows.append(
                {
                    **ledger_row,
                    "trade_date": ledger_row.get("trade_date") or ledger_row.get("decision_date") or ledger_row.get("execution_date"),
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
                    "rollout_variant": variant,
                    "policy_exit_timing": summary.get("exits_early_or_late"),
                    "policy_roundtrip_count": summary["roundtrip_count"],
                    "policy_net_realized_pnl": summary["net_realized_pnl"],
                    "policy_max_drawdown_during_holding": summary["max_drawdown_during_holding"],
                    "policy_vs_hold_gap": policy_vs_hold_gap,
                }
            )
        if idx % 250 == 0:
            print(f"[rollout {variant}] processed {idx}/{len(selection_rows)} rows", flush=True)
    return run_rows, ledger_rows


def run_lower_bucket_long_rollout_v2(
    *,
    input_dir: Path = DEFAULT_INPUT_DIR,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    row_limit: int | None = None,
) -> dict[str, Any]:
    integrated_summary = _load_json(input_dir / "integrated_guarded_v1_replay_summary.json")
    integrated_compare = _load_json(input_dir / "integrated_guarded_v1_compare.json")
    integrated_decision = _load_json(input_dir / "integrated_guarded_v1_decision.json")
    selection_payload = _load_json(input_dir / "integrated_guarded_v1_selection_only_ledger.json")
    db_provenance = _load_json(input_dir / "integrated_guarded_v1_db_provenance.json")
    exclusion_payload = _load_json(input_dir / "integrated_guarded_v1_exclusion_diagnostics.json")
    coverage_payload = _load_json(input_dir / "integrated_guarded_v1_full_universe_gate_coverage.json")
    candidate_snapshots = _load_json(input_dir / "integrated_guarded_v1_candidate_snapshots.json")
    baseline_summary_path = Path(r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_stress200\integrated_guarded_v1_replay_summary.json")
    provenance_rows = db_provenance.get("rows", {})
    provenance_db = provenance_rows.get("db_provenance", provenance_rows)
    source_db_path = Path(
        provenance_db.get("source_db_path")
        or provenance_db.get("working_source_db_path")
        or db_provenance.get("source_db_path")
        or db_provenance.get("working_source_db_path")
        or DEFAULT_INPUT_DIR
    ).expanduser().resolve()

    selection_rows: list[dict[str, Any]] = list(selection_payload.get("rows") or [])
    if row_limit is not None:
        selection_rows = selection_rows[: int(row_limit)]
    symbol_cache = _load_symbol_frame_cache(source_db_path=source_db_path, selection_rows=selection_rows)

    variant_results: dict[str, dict[str, Any]] = {}
    all_ledger_rows: list[dict[str, Any]] = []
    for variant in ("A", "B"):
        run_rows, ledger_rows = _variant_policy_run(
            variant=variant,
            selection_rows=selection_rows,
            source_db_path=source_db_path,
            symbol_cache=symbol_cache,
            output_dir=output_dir,
        )
        all_ledger_rows.extend(ledger_rows)
        run_frame = _rows_to_frame(run_rows)
        if not run_frame.empty:
            run_frame["long_entry_count"] = pd.to_numeric(run_frame["long_entry_count"], errors="coerce").fillna(0).astype(int)
            run_frame["no_exposure_count"] = pd.to_numeric(run_frame["no_exposure_count"], errors="coerce").fillna(0).astype(int)
            run_frame["policy_vs_hold_gap"] = pd.to_numeric(run_frame["policy_vs_hold_gap"], errors="coerce")
        topk = _aggregate_variant_topk(selection_rows, run_rows)
        side_rows = _aggregate_variant_side(selection_rows, run_rows)
        rank_rows = _build_rank_rows(run_rows)
        anchor_rows = _build_anchor_rows(run_rows)
        action_rows = _policy_repair_action_summary(pd.DataFrame(ledger_rows)) if ledger_rows else []
        variant_results[variant] = {
            "variant": variant,
            "variant_label": _variant_label(variant),
            "note": _variant_note(variant),
            "run_rows": run_rows,
            "ledger_rows": ledger_rows,
            "topk": topk,
            "rank_rows": rank_rows,
            "side_rows": side_rows,
            "anchor_rows": anchor_rows,
            "action_rows": action_rows,
        }

    # Variant C is intentionally aliased to A under the current hold-only semantics.
    variant_results["C"] = copy.deepcopy(variant_results["A"])
    variant_results["C"]["variant"] = "C"
    variant_results["C"]["variant_label"] = _variant_label("C")
    variant_results["C"]["note"] = _variant_note("C")
    variant_results["C"]["equivalent_to"] = "A"
    variant_results["C"]["alias_only"] = True

    baseline_topk = integrated_summary["selection_topk_repair_policy"]
    baseline_rank_rows = list(integrated_summary.get("repair_rank_rows") or [])
    baseline_side_rows = list(integrated_summary.get("repair_side_rows") or [])
    baseline_action_rows = list(integrated_summary.get("repair_action_rows") or [])

    compare_variants: dict[str, Any] = {}
    for variant in VARIANT_ORDER:
        result = variant_results[variant]
        topk = result["topk"]
        compare_variants[variant] = {
            "variant": variant,
            "variant_label": result["variant_label"],
            "note": result["note"],
            "alias_of": result.get("equivalent_to"),
            "selection_only": {
                "top5": topk["5"]["selection_only"]["challenger"],
                "top10": topk["10"]["selection_only"]["challenger"],
                "top20": topk["20"]["selection_only"]["challenger"],
            },
            "policy_trade": {
                "top5": topk["5"]["policy_trade"]["challenger"],
                "top10": topk["10"]["policy_trade"]["challenger"],
                "top20": topk["20"]["policy_trade"]["challenger"],
            },
            "delta": {
                "top5_selection_only_avg_ret63": topk["5"]["delta"]["selection_only_avg_ret63"],
                "top10_selection_only_avg_ret63": topk["10"]["delta"]["selection_only_avg_ret63"],
                "top20_selection_only_avg_ret63": topk["20"]["delta"]["selection_only_avg_ret63"],
                "top5_policy_net_realized_pnl": topk["5"]["delta"]["policy_net_realized_pnl"],
                "top10_policy_net_realized_pnl": topk["10"]["delta"]["policy_net_realized_pnl"],
                "top20_policy_net_realized_pnl": topk["20"]["delta"]["policy_net_realized_pnl"],
            },
            "rank_rows": result["rank_rows"],
            "side_rows": result["side_rows"],
            "action_rows": result["action_rows"],
            "anchor_rows": result["anchor_rows"],
        }

    def _score_variant(label: str) -> tuple[float, float, float, float]:
        result = compare_variants[label]
        return (
            float(result["delta"]["top10_policy_net_realized_pnl"]),
            float(result["delta"]["top20_policy_net_realized_pnl"]),
            float(result["delta"]["top5_policy_net_realized_pnl"]),
            -float(result["delta"]["top20_policy_net_realized_pnl"] < 0),
        )

    best_variant = max(("A", "B", "C"), key=_score_variant)
    best_result = compare_variants[best_variant]
    top5_delta = float(best_result["delta"]["top5_policy_net_realized_pnl"])
    top10_delta = float(best_result["delta"]["top10_policy_net_realized_pnl"])
    top20_delta = float(best_result["delta"]["top20_policy_net_realized_pnl"])

    baseline_long_gap = float(next((row["policy_vs_hold_gap_sum"] for row in baseline_rank_rows if row["rank_bucket"] == "top6_10"), 0.0))
    baseline_top11_gap = float(next((row["policy_vs_hold_gap_sum"] for row in baseline_rank_rows if row["rank_bucket"] == "top11_20"), 0.0))
    best_long_gap = float(next((row["policy_vs_hold_gap_sum"] for row in best_result["rank_rows"] if row["rank_bucket"] == "top6_10"), 0.0))
    best_top11_gap = float(next((row["policy_vs_hold_gap_sum"] for row in best_result["rank_rows"] if row["rank_bucket"] == "top11_20"), 0.0))
    baseline_lower_late_exit_count = int(
        sum(int(row.get("late_exit_count") or 0) for row in baseline_rank_rows if str(row.get("rank_bucket")) in {"top6_10", "top11_20"})
    )
    best_lower_late_exit_count = int(
        sum(int(row.get("late_exit_count") or 0) for row in best_result["rank_rows"] if str(row.get("rank_bucket")) in {"top6_10", "top11_20"})
    )

    selection_only_same = True
    top5_ok = top5_delta >= 0
    top10_ok = top10_delta >= 0
    top20_ok = top20_delta >= 0
    lower_bucket_drag_reduced = best_top11_gap >= baseline_top11_gap and best_long_gap >= baseline_long_gap
    late_exit_still_suppressed = best_lower_late_exit_count <= baseline_lower_late_exit_count
    if top10_ok and top20_ok and top5_ok and lower_bucket_drag_reduced:
        decision = "keep"
    elif top20_ok or top10_ok:
        decision = "hold"
    else:
        decision = "drop"

    summary = {
        "schema_version": "tradex_lower_bucket_long_rollout_v2_summary_v1",
        "generated_at": _utc_now(),
        "policy_variant": POLICY_VARIANT,
        "baseline_policy_variant": BASELINE_POLICY_VARIANT,
        "rollout_variants": list(VARIANT_ORDER),
        "variant_aliases": {"C": "A"},
        "authoritative_rollup_decision": decision,
        "diagnosis_decision": decision,
        "best_variant": best_variant,
        "best_variant_label": compare_variants[best_variant]["variant_label"],
        "best_variant_note": compare_variants[best_variant]["note"],
        "selection_only_same_as_baseline": selection_only_same,
        "policy_layer_destroyed_edge": bool(not (top10_ok and top20_ok and top5_ok)),
        "topk_observations": {
            "top5": top5_delta,
            "top10": top10_delta,
            "top20": top20_delta,
        },
        "baseline_policy_reference": {
            "policy_variant": BASELINE_POLICY_VARIANT,
            "top5_policy_net_realized_pnl": float(baseline_topk["top5"]["net_realized_pnl"]),
            "top10_policy_net_realized_pnl": float(baseline_topk["top10"]["net_realized_pnl"]),
            "top20_policy_net_realized_pnl": float(baseline_topk["top20"]["net_realized_pnl"]),
            "top6_10_policy_vs_hold_gap_sum": baseline_long_gap,
            "top11_20_policy_vs_hold_gap_sum": baseline_top11_gap,
            "late_exit_loss_count_lower_buckets": baseline_lower_late_exit_count,
        },
        "best_variant_reference": {
            "top6_10_policy_vs_hold_gap_sum": best_long_gap,
            "top11_20_policy_vs_hold_gap_sum": best_top11_gap,
            "late_exit_loss_count_lower_buckets": best_lower_late_exit_count,
        },
        "summary_references": {
            "anchor_count": integrated_summary.get("anchor_count"),
            "selection_rows_count": integrated_summary.get("selection_rows_count"),
            "policy_run_rows_count": integrated_summary.get("policy_run_rows_count"),
            "policy_ledger_rows_count": integrated_summary.get("policy_ledger_rows_count"),
            "basis_row_skip_count": exclusion_payload.get("rows", {}).get("aggregate", {}).get("skipped_symbols_without_basis_row_count"),
            "full_universe_no_trade_rate_mean_specialized": coverage_payload.get("aggregate", {}).get("specialized", {}).get("no_trade_rate_mean"),
            "full_universe_long_tradable_rate_mean_specialized": _full_universe_rate_mean(coverage_payload.get("rows") or [], "specialized", "long_tradable_rate"),
            "full_universe_short_tradable_rate_mean_specialized": _full_universe_rate_mean(coverage_payload.get("rows") or [], "specialized", "short_tradable_rate"),
        },
        "input_artifacts": {
            "integrated_guarded_v1_replay_summary": str(input_dir / "integrated_guarded_v1_replay_summary.json"),
            "integrated_guarded_v1_compare": str(input_dir / "integrated_guarded_v1_compare.json"),
            "integrated_guarded_v1_decision": str(input_dir / "integrated_guarded_v1_decision.json"),
            "integrated_guarded_v1_selection_only_ledger": str(input_dir / "integrated_guarded_v1_selection_only_ledger.json"),
            "integrated_guarded_v1_policy_trade_ledger": str(input_dir / "integrated_guarded_v1_policy_trade_ledger.json"),
            "integrated_guarded_v1_candidate_snapshots": str(input_dir / "integrated_guarded_v1_candidate_snapshots.json"),
            "integrated_guarded_v1_full_universe_gate_coverage": str(input_dir / "integrated_guarded_v1_full_universe_gate_coverage.json"),
            "integrated_guarded_v1_db_provenance": str(input_dir / "integrated_guarded_v1_db_provenance.json"),
            "integrated_guarded_v1_exclusion_diagnostics": str(input_dir / "integrated_guarded_v1_exclusion_diagnostics.json"),
        },
    }

    compare = {
        "schema_version": "tradex_lower_bucket_long_rollout_v2_compare_v1",
        "generated_at": _utc_now(),
        "policy_variant": POLICY_VARIANT,
        "same_condition_contract": {
            "same_anchor_set": True,
            "same_candidates": True,
            "same_db_source": db_provenance.get("rows", {}).get("source_db_path") if isinstance(db_provenance, dict) else None,
            "same_execution_rule": "next_trading_day_open",
            "same_cost_slippage": "existing chart-first replay contract",
            "same_top_k": [5, 10, 20],
            "same_period": True,
        },
        "baseline": {
            "policy_variant": BASELINE_POLICY_VARIANT,
            "topk_metrics": baseline_topk,
            "side_rows": baseline_side_rows,
            "rank_rows": baseline_rank_rows,
            "action_rows": baseline_action_rows,
        },
        "variants": compare_variants,
        "delta_vs_baseline": {
            variant: compare_variants[variant]["delta"] for variant in VARIANT_ORDER
        },
        "best_variant": best_variant,
        "best_variant_label": compare_variants[best_variant]["variant_label"],
        "best_variant_note": compare_variants[best_variant]["note"],
        "selection_only_same_as_baseline": selection_only_same,
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "summary_json": _write_json(output_dir / "lower_bucket_long_rollout_v2_summary.json", summary),
        "compare_json": _write_json(output_dir / "lower_bucket_long_rollout_v2_compare.json", compare),
        "by_variant_json": _write_json(
            output_dir / "lower_bucket_long_rollout_v2_by_variant.json",
            {
                "schema_version": "tradex_lower_bucket_long_rollout_v2_by_variant_v1",
                "generated_at": _utc_now(),
                "baseline": {
                    "policy_variant": BASELINE_POLICY_VARIANT,
                    "topk_metrics": baseline_topk,
                    "side_rows": baseline_side_rows,
                    "rank_rows": baseline_rank_rows,
                    "action_rows": baseline_action_rows,
                },
                "variants": compare_variants,
            },
        ),
        "by_rank_bucket_json": _write_json(
            output_dir / "lower_bucket_long_rollout_v2_by_rank_bucket.json",
            {
                "schema_version": "tradex_lower_bucket_long_rollout_v2_by_rank_bucket_v1",
                "generated_at": _utc_now(),
                "baseline": baseline_rank_rows,
                "variants": {variant: compare_variants[variant]["rank_rows"] for variant in VARIANT_ORDER},
            },
        ),
        "by_side_json": _write_json(
            output_dir / "lower_bucket_long_rollout_v2_by_side.json",
            {
                "schema_version": "tradex_lower_bucket_long_rollout_v2_by_side_v1",
                "generated_at": _utc_now(),
                "baseline": baseline_side_rows,
                "variants": {variant: compare_variants[variant]["side_rows"] for variant in VARIANT_ORDER},
            },
        ),
        "by_action_json": _write_json(
            output_dir / "lower_bucket_long_rollout_v2_by_action.json",
            {
                "schema_version": "tradex_lower_bucket_long_rollout_v2_by_action_v1",
                "generated_at": _utc_now(),
                "baseline": baseline_action_rows,
                "variants": {variant: compare_variants[variant]["action_rows"] for variant in VARIANT_ORDER},
            },
        ),
        "trade_ledger_json": _write_json(
            output_dir / "lower_bucket_long_rollout_v2_trade_ledger.json",
            {
                "schema_version": "tradex_lower_bucket_long_rollout_v2_trade_ledger_v1",
                "generated_at": _utc_now(),
                "policy_variant": POLICY_VARIANT,
                "rows": all_ledger_rows,
            },
        ),
        "decision_json": _write_json(
            output_dir / "lower_bucket_long_rollout_v2_decision.json",
            {
                "schema_version": "tradex_lower_bucket_long_rollout_v2_decision_v1",
                "generated_at": _utc_now(),
                "policy_variant": POLICY_VARIANT,
                "decision": decision,
                "best_variant": best_variant,
                "best_variant_label": compare_variants[best_variant]["variant_label"],
                "best_variant_note": compare_variants[best_variant]["note"],
                "selection_only_same_as_baseline": selection_only_same,
                "policy_layer_destroyed_edge": bool(not (top10_ok and top20_ok and top5_ok)),
            },
        ),
    }

    return {
        "ok": True,
        "output_dir": str(output_dir),
        "paths": {key: str(value) for key, value in paths.items()},
        "summary": summary,
        "compare": compare,
        "variant_results": variant_results,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Evaluate lower-bucket long rollout v2 against the integrated guarded baseline.")
    parser.add_argument("--input-dir", default=str(DEFAULT_INPUT_DIR))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--row-limit", type=int, default=None)
    args = parser.parse_args(argv)
    payload = run_lower_bucket_long_rollout_v2(
        input_dir=Path(args.input_dir).expanduser().resolve(),
        output_dir=Path(args.output_dir).expanduser().resolve(),
        row_limit=args.row_limit,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
