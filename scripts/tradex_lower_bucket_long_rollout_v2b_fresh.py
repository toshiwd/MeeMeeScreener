from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_chart_first_replay import (  # noqa: E402
    SHARES_PER_UNIT,
)
from scripts.tradex_lower_bucket_long_rollout_v2 import (  # noqa: E402
    DEFAULT_INPUT_DIR,
    _aggregate_variant_side,
    _aggregate_variant_topk,
    _build_anchor_rows,
    _build_rank_rows,
    _load_json,
    _load_symbol_frame_cache,
    _policy_repair_action_summary,
    _rank_bucket,
    _rows_to_frame,
    _to_number,
    _utc_now,
    _variant_policy_run,
    _write_json,
)
from scripts.tradex_long_late_exit_repair_v1 import (  # noqa: E402
    _policy_vs_hold_gap,
)


DEFAULT_OUTPUT_DIR = Path(r"G:\Tradex\sample_replays\tradex_lower_bucket_long_rollout_v2B_fresh")
BASELINE_POLICY_VARIANT = "integrated_specialized_gate_guarded_policy_v1"
ENGINE_POLICY_VARIANT = "lower_bucket_long_rollout_v2"
ARTIFACT_POLICY_VARIANT = "lower_bucket_long_rollout_v2B_fresh"
ROLLUP_VARIANT = "B"
TRANSFORM_REFERENCE_DIR = Path(r"G:\Tradex\sample_replays\tradex_lower_bucket_long_rollout_v2_stress200")
TRANSFORM_REFERENCE_COMPARE = TRANSFORM_REFERENCE_DIR / "lower_bucket_long_rollout_v2_compare.json"
TRANSFORM_REFERENCE_SUMMARY = TRANSFORM_REFERENCE_DIR / "lower_bucket_long_rollout_v2_summary.json"
EXPOSURE_STARVATION_THRESHOLD = 0.25


def _variant_label() -> str:
    return ARTIFACT_POLICY_VARIANT


def _variant_note() -> str:
    return "Fresh replay of the lower-bucket long rollout with long top6-20 no-exposure."


def _load_df(path: Path) -> pd.DataFrame:
    payload = _load_json(path)
    return pd.DataFrame(payload.get("rows") or [])


def _normalize_selection_frame(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    if result.empty:
        return result
    result["challenger_rank"] = pd.to_numeric(result["challenger_rank"], errors="coerce")
    result["champion_rank"] = pd.to_numeric(result["champion_rank"], errors="coerce")
    result["rank_bucket"] = result["challenger_rank"].apply(_rank_bucket)
    for column in ("entry_price", "ret63", "ret10", "ret20", "ret5", "mfe63", "mae63"):
        if column in result.columns:
            result[column] = pd.to_numeric(result[column], errors="coerce")
    result["hold_pnl"] = result["ret63"] * result["entry_price"] * SHARES_PER_UNIT
    return result


def _aggregate_policy_candidate_rows(policy_frame: pd.DataFrame) -> pd.DataFrame:
    if policy_frame.empty:
        return policy_frame.copy()
    frame = policy_frame.copy()
    for column in ("policy_net_realized_pnl", "policy_max_drawdown_during_holding", "policy_roundtrip_count", "realized_pnl", "unrealized_pnl"):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    if "selected_action" in frame.columns:
        frame["selected_action"] = frame["selected_action"].fillna("stay").astype(str)
    if "exit_reason_primary" in frame.columns:
        frame["exit_reason_primary"] = frame["exit_reason_primary"].fillna("").astype(str)
    if "trim_reason_primary" in frame.columns:
        frame["trim_reason_primary"] = frame["trim_reason_primary"].fillna("").astype(str)

    def _agg(group: pd.DataFrame) -> pd.Series:
        last = group.iloc[-1]
        selected_actions = group["selected_action"].astype(str) if "selected_action" in group.columns else pd.Series(dtype=str)
        exit_reason = group["exit_reason_primary"].astype(str) if "exit_reason_primary" in group.columns else pd.Series(dtype=str)
        trim_reason = group["trim_reason_primary"].astype(str) if "trim_reason_primary" in group.columns else pd.Series(dtype=str)
        return pd.Series(
            {
                "policy_net_realized_pnl": float(last["policy_net_realized_pnl"]),
                "policy_max_drawdown_during_holding": float(last["policy_max_drawdown_during_holding"]),
                "policy_roundtrip_count": int(last["policy_roundtrip_count"]),
                "number_of_trades": int((selected_actions != "stay").sum()) if not selected_actions.empty else 0,
                "forced_exit_count": int((exit_reason == "time_stop").sum()) if not exit_reason.empty else 0,
                "late_exit_count": int((exit_reason == "lose_ma60").sum()) if not exit_reason.empty else 0,
                "stop_loss_count": int(((exit_reason == "lose_ma20") | (trim_reason == "lose_ma20")).sum()) if not trim_reason.empty else 0,
                "add_count": int(selected_actions.str.contains("add", regex=False).sum()) if not selected_actions.empty else 0,
                "hedge_action_count": int(selected_actions.str.contains("hedge", regex=False).sum()) if not selected_actions.empty else 0,
                "long_entry_count": int((selected_actions.str.contains("long_entry", regex=False) | selected_actions.str.contains("long_add", regex=False)).sum()) if not selected_actions.empty else 0,
                "entry_count": int(selected_actions.str.contains("entry", regex=False).sum()) if not selected_actions.empty else 0,
                "exit_count": int(selected_actions.str.contains("exit", regex=False).sum()) if not selected_actions.empty else 0,
                "stay_count": int((selected_actions == "stay").sum()) if not selected_actions.empty else 0,
            }
        )

    grouped = frame.groupby(["anchor_date", "symbol", "side"], sort=False).apply(_agg).reset_index()
    return grouped


def _prepare_baseline_candidate_frame(selection_rows: pd.DataFrame, candidate_rows: pd.DataFrame) -> pd.DataFrame:
    sel = _normalize_selection_frame(selection_rows)
    cand = _aggregate_policy_candidate_rows(candidate_rows)
    frame = sel.merge(cand, on=["anchor_date", "symbol", "side"], how="left", suffixes=("", "_policy"))
    if frame.empty:
        return frame
    frame["candidate_capital"] = frame["entry_price"].abs() * SHARES_PER_UNIT
    frame["no_exposure_count"] = 0
    frame["policy_net_realized_pnl"] = pd.to_numeric(frame["policy_net_realized_pnl"], errors="coerce")
    frame["policy_total_pnl"] = frame["policy_net_realized_pnl"]
    frame["policy_max_drawdown_during_holding"] = pd.to_numeric(frame["policy_max_drawdown_during_holding"], errors="coerce")
    frame["policy_roundtrip_count"] = pd.to_numeric(frame["policy_roundtrip_count"], errors="coerce")
    frame["roundtrip_count"] = frame["policy_roundtrip_count"]
    frame["number_of_trades"] = pd.to_numeric(frame["number_of_trades"], errors="coerce")
    frame["hold_pnl"] = pd.to_numeric(frame["hold_pnl"], errors="coerce")
    frame["policy_vs_hold_gap"] = frame["policy_net_realized_pnl"] - frame["hold_pnl"]
    frame["late_exit_count"] = pd.to_numeric(frame["late_exit_count"], errors="coerce").fillna(0).astype(int)
    frame["stop_loss_count"] = pd.to_numeric(frame["stop_loss_count"], errors="coerce").fillna(0).astype(int)
    frame["forced_exit_count"] = pd.to_numeric(frame["forced_exit_count"], errors="coerce").fillna(0).astype(int)
    frame["add_count"] = pd.to_numeric(frame["add_count"], errors="coerce").fillna(0).astype(int)
    frame["hedge_action_count"] = pd.to_numeric(frame["hedge_action_count"], errors="coerce").fillna(0).astype(int)
    frame["entry_count"] = pd.to_numeric(frame["entry_count"], errors="coerce").fillna(0).astype(int)
    frame["exit_count"] = pd.to_numeric(frame["exit_count"], errors="coerce").fillna(0).astype(int)
    frame["stay_count"] = pd.to_numeric(frame["stay_count"], errors="coerce").fillna(0).astype(int)
    return frame


def _run_selection_summary(rows: pd.DataFrame, *, top_k: int) -> dict[str, Any]:
    selected = rows[rows[f"challenger_selected_top{top_k}"] == True].copy()  # noqa: E712
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
            "neutral_rate": None,
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
        "neutral_rate": None,
    }


def _run_policy_summary(rows: pd.DataFrame, *, top_k: int) -> dict[str, Any]:
    selected = rows[rows[f"challenger_selected_top{top_k}"] == True].copy()  # noqa: E712
    if selected.empty:
        return {
            "selected_count": 0,
            "roundtrip_count": 0,
            "net_realized_pnl": 0.0,
            "max_drawdown_during_holding": None,
            "average_capture_ratio": None,
            "exits_early_or_late": None,
            "win_rate": None,
            "hold_gap_sum": 0.0,
            "number_of_trades": 0,
            "no_exposure_count": 0,
        }
    pnl = pd.to_numeric(selected["policy_net_realized_pnl"], errors="coerce")
    dds = pd.to_numeric(selected["policy_max_drawdown_during_holding"], errors="coerce")
    hold_gap = pd.to_numeric(selected["policy_vs_hold_gap"], errors="coerce")
    exposure_ratio = selected["candidate_capital"].sum()
    capture = pd.to_numeric(
        pd.Series([float(p) / float(h) if pd.notna(p) and pd.notna(h) and float(h) != 0.0 else math.nan for p, h in zip(pnl, selected["hold_pnl"])]),
        errors="coerce",
    ).dropna()
    return {
        "selected_count": int(len(selected)),
        "roundtrip_count": int(pd.to_numeric(selected["policy_roundtrip_count"], errors="coerce").fillna(0).sum()),
        "net_realized_pnl": float(pnl.sum()),
        "max_drawdown_during_holding": float(dds.min()) if not dds.dropna().empty else None,
        "average_capture_ratio": float(capture.mean()) if not capture.empty else None,
        "exits_early_or_late": "late" if any(int(x) > 0 for x in pd.to_numeric(selected["late_exit_count"], errors="coerce").fillna(0)) else "acceptable",
        "win_rate": float((pnl > 0).mean()),
        "hold_gap_sum": float(hold_gap.sum()),
        "number_of_trades": int(pd.to_numeric(selected["number_of_trades"], errors="coerce").fillna(0).sum()),
        "no_exposure_count": int(pd.to_numeric(selected["no_exposure_count"], errors="coerce").fillna(0).sum()),
        "deployed_capital_sum": float(selected.loc[selected["no_exposure_count"] == 0, "candidate_capital"].sum()),
        "deployed_capital_mean": float(selected.loc[selected["no_exposure_count"] == 0, "candidate_capital"].mean()) if (selected["no_exposure_count"] == 0).any() else None,
        "unused_capital_sum": float(selected.loc[selected["no_exposure_count"] > 0, "candidate_capital"].sum()),
        "unused_capital_rate": float(selected.loc[selected["no_exposure_count"] > 0, "candidate_capital"].sum() / exposure_ratio) if exposure_ratio else None,
        "pnl_per_deployed_capital": float(pnl.sum() / selected.loc[selected["no_exposure_count"] == 0, "candidate_capital"].sum()) if float(selected.loc[selected["no_exposure_count"] == 0, "candidate_capital"].sum()) != 0 else None,
        "pnl_per_candidate": float(pnl.sum() / len(selected)),
        "pnl_per_exposed_candidate": float(pnl.sum() / max(1, int((selected["no_exposure_count"] == 0).sum()))),
        "exposed_candidate_count": int((selected["no_exposure_count"] == 0).sum()),
        "no_exposure_candidate_count": int((selected["no_exposure_count"] > 0).sum()),
        "exposure_rate": float((selected["no_exposure_count"] == 0).mean()),
        "candidate_starvation_flag": bool((selected["no_exposure_count"] == 0).mean() < EXPOSURE_STARVATION_THRESHOLD),
    }


def _exposure_normalization(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {"overall": {}, "by_topk": {}}
    overall = {
        "selected_count": int(len(frame)),
        "exposed_candidate_count": int((frame["no_exposure_count"] == 0).sum()),
        "no_exposure_candidate_count": int((frame["no_exposure_count"] > 0).sum()),
        "exposure_rate": float((frame["no_exposure_count"] == 0).mean()),
        "deployed_capital_sum": float(frame.loc[frame["no_exposure_count"] == 0, "candidate_capital"].sum()),
        "deployed_capital_mean": float(frame.loc[frame["no_exposure_count"] == 0, "candidate_capital"].mean()) if (frame["no_exposure_count"] == 0).any() else None,
        "unused_capital_sum": float(frame.loc[frame["no_exposure_count"] > 0, "candidate_capital"].sum()),
        "unused_capital_rate": float(frame.loc[frame["no_exposure_count"] > 0, "candidate_capital"].sum() / frame["candidate_capital"].sum()) if float(frame["candidate_capital"].sum()) else None,
        "pnl_per_deployed_capital": float(frame["policy_net_realized_pnl"].sum() / frame.loc[frame["no_exposure_count"] == 0, "candidate_capital"].sum()) if float(frame.loc[frame["no_exposure_count"] == 0, "candidate_capital"].sum()) else None,
        "pnl_per_candidate": float(frame["policy_net_realized_pnl"].sum() / len(frame)),
        "pnl_per_exposed_candidate": float(frame["policy_net_realized_pnl"].sum() / max(1, int((frame["no_exposure_count"] == 0).sum()))),
        "long_exposed_count": int(((frame["side"] == "long") & (frame["no_exposure_count"] == 0)).sum()),
        "short_exposed_count": int(((frame["side"] == "short") & (frame["no_exposure_count"] == 0)).sum()),
        "long_no_exposure_count": int(((frame["side"] == "long") & (frame["no_exposure_count"] > 0)).sum()),
        "short_no_exposure_count": int(((frame["side"] == "short") & (frame["no_exposure_count"] > 0)).sum()),
        "candidate_starvation_flag": bool((frame["no_exposure_count"] == 0).mean() < EXPOSURE_STARVATION_THRESHOLD),
        "candidate_starvation_threshold": EXPOSURE_STARVATION_THRESHOLD,
    }
    by_topk: dict[str, Any] = {}
    for top_k in (5, 10, 20):
        selected = frame[frame[f"challenger_selected_top{top_k}"] == True].copy()  # noqa: E712
        if selected.empty:
            by_topk[str(top_k)] = {
                "selected_count": 0,
                "exposed_candidate_count": 0,
                "no_exposure_candidate_count": 0,
                "exposure_rate": None,
                "deployed_capital_sum": 0.0,
                "deployed_capital_mean": None,
                "unused_capital_sum": 0.0,
                "unused_capital_rate": None,
                "pnl_per_deployed_capital": None,
                "pnl_per_candidate": None,
                "pnl_per_exposed_candidate": None,
                "long_exposed_count": 0,
                "short_exposed_count": 0,
                "long_no_exposure_count": 0,
                "short_no_exposure_count": 0,
                "candidate_starvation_flag": True,
            }
            continue
        deployed = selected.loc[selected["no_exposure_count"] == 0, "candidate_capital"]
        unused = selected.loc[selected["no_exposure_count"] > 0, "candidate_capital"]
        by_topk[str(top_k)] = {
            "selected_count": int(len(selected)),
            "exposed_candidate_count": int((selected["no_exposure_count"] == 0).sum()),
            "no_exposure_candidate_count": int((selected["no_exposure_count"] > 0).sum()),
            "exposure_rate": float((selected["no_exposure_count"] == 0).mean()),
            "deployed_capital_sum": float(deployed.sum()),
            "deployed_capital_mean": float(deployed.mean()) if not deployed.empty else None,
            "unused_capital_sum": float(unused.sum()),
            "unused_capital_rate": float(unused.sum() / selected["candidate_capital"].sum()) if float(selected["candidate_capital"].sum()) else None,
            "pnl_per_deployed_capital": float(selected["policy_net_realized_pnl"].sum() / deployed.sum()) if float(deployed.sum()) else None,
            "pnl_per_candidate": float(selected["policy_net_realized_pnl"].sum() / len(selected)),
            "pnl_per_exposed_candidate": float(selected["policy_net_realized_pnl"].sum() / max(1, int((selected["no_exposure_count"] == 0).sum()))),
            "long_exposed_count": int(((selected["side"] == "long") & (selected["no_exposure_count"] == 0)).sum()),
            "short_exposed_count": int(((selected["side"] == "short") & (selected["no_exposure_count"] == 0)).sum()),
            "long_no_exposure_count": int(((selected["side"] == "long") & (selected["no_exposure_count"] > 0)).sum()),
            "short_no_exposure_count": int(((selected["side"] == "short") & (selected["no_exposure_count"] > 0)).sum()),
            "candidate_starvation_flag": bool((selected["no_exposure_count"] == 0).mean() < EXPOSURE_STARVATION_THRESHOLD),
        }
    return {"overall": overall, "by_topk": by_topk}


def _rank_row_map(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(row.get("rank_bucket")): row for row in rows if row.get("rank_bucket") is not None}


def _build_candidate_level_frame(
    *,
    selection_rows: pd.DataFrame,
    policy_rows: pd.DataFrame,
) -> pd.DataFrame:
    frame = _prepare_baseline_candidate_frame(selection_rows, policy_rows)
    if frame.empty:
        return frame
    frame["policy_variant"] = ARTIFACT_POLICY_VARIANT
    frame["engine_policy_variant"] = ENGINE_POLICY_VARIANT
    frame["rollout_variant"] = ROLLUP_VARIANT
    frame["variant_no_exposure"] = 0
    frame["variant_policy_net_realized_pnl"] = frame["policy_net_realized_pnl"]
    frame["variant_policy_total_pnl"] = frame["policy_net_realized_pnl"]
    frame["variant_policy_vs_hold_gap"] = frame["policy_vs_hold_gap"]
    frame["variant_roundtrip_count"] = frame["policy_roundtrip_count"]
    frame["roundtrip_count"] = frame["policy_roundtrip_count"]
    frame["variant_policy_max_drawdown_during_holding"] = frame["policy_max_drawdown_during_holding"]
    frame["variant_number_of_trades"] = frame["number_of_trades"]
    frame["variant_forced_exit_count"] = frame["forced_exit_count"]
    frame["variant_late_exit_count"] = frame["late_exit_count"]
    frame["variant_stop_loss_count"] = frame["stop_loss_count"]
    frame["variant_add_count"] = frame["add_count"]
    frame["variant_hedge_action_count"] = frame["hedge_action_count"]
    frame["variant_entry_count"] = frame["entry_count"]
    frame["variant_exit_count"] = frame["exit_count"]
    return frame


def run_lower_bucket_long_rollout_v2b_fresh(
    *,
    input_dir: Path = DEFAULT_INPUT_DIR,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    row_limit: int | None = None,
) -> dict[str, Any]:
    integrated_summary = _load_json(input_dir / "integrated_guarded_v1_replay_summary.json")
    integrated_compare = _load_json(input_dir / "integrated_guarded_v1_compare.json")
    integrated_decision = _load_json(input_dir / "integrated_guarded_v1_decision.json")
    selection_payload = _load_json(input_dir / "integrated_guarded_v1_selection_only_ledger.json")
    policy_payload = _load_json(input_dir / "integrated_guarded_v1_policy_trade_ledger.json")
    db_provenance = _load_json(input_dir / "integrated_guarded_v1_db_provenance.json")
    exclusion_payload = _load_json(input_dir / "integrated_guarded_v1_exclusion_diagnostics.json")
    coverage_payload = _load_json(input_dir / "integrated_guarded_v1_full_universe_gate_coverage.json")
    candidate_snapshots = _load_json(input_dir / "integrated_guarded_v1_candidate_snapshots.json")

    provenance_rows = db_provenance.get("rows", {})
    provenance_db = provenance_rows.get("db_provenance", provenance_rows)
    source_db_path = Path(
        provenance_db.get("source_db_path")
        or provenance_db.get("working_source_db_path")
        or db_provenance.get("source_db_path")
        or db_provenance.get("working_source_db_path")
        or DEFAULT_INPUT_DIR
    ).expanduser().resolve()

    selection_rows = pd.DataFrame(selection_payload.get("rows") or [])
    if row_limit is not None:
        selection_rows = selection_rows.head(int(row_limit))
    selection_rows = _normalize_selection_frame(selection_rows)

    baseline_policy_rows = pd.DataFrame(policy_payload.get("rows") or [])
    baseline_candidate_frame = _build_candidate_level_frame(
        selection_rows=selection_rows,
        policy_rows=baseline_policy_rows,
    )

    symbol_cache = _load_symbol_frame_cache(source_db_path=source_db_path, selection_rows=selection_rows.to_dict(orient="records"))
    challenger_run_rows, challenger_ledger_rows = _variant_policy_run(
        variant=ROLLUP_VARIANT,
        selection_rows=selection_rows.to_dict(orient="records"),
        source_db_path=source_db_path,
        symbol_cache=symbol_cache,
        output_dir=output_dir,
    )
    challenger_run_frame = pd.DataFrame(challenger_run_rows).copy()
    challenger_run_frame = _normalize_selection_frame(challenger_run_frame)
    if not challenger_run_frame.empty:
        challenger_run_frame["policy_variant"] = ARTIFACT_POLICY_VARIANT
        challenger_run_frame["engine_policy_variant"] = ENGINE_POLICY_VARIANT
        challenger_run_frame["rollout_variant"] = ROLLUP_VARIANT
        for column in (
            "policy_net_realized_pnl",
            "policy_total_pnl",
            "policy_max_drawdown_during_holding",
            "policy_roundtrip_count",
            "number_of_trades",
            "forced_exit_count",
            "late_exit_count",
            "stop_loss_count",
            "add_count",
            "hedge_action_count",
            "entry_count",
            "exit_count",
            "stay_count",
            "no_exposure_count",
            "variant_policy_net_realized_pnl",
            "variant_policy_total_pnl",
            "variant_policy_vs_hold_gap",
            "variant_roundtrip_count",
            "variant_policy_max_drawdown_during_holding",
            "variant_number_of_trades",
            "variant_forced_exit_count",
            "variant_late_exit_count",
            "variant_stop_loss_count",
            "variant_add_count",
            "variant_hedge_action_count",
            "variant_entry_count",
            "variant_exit_count",
        ):
            if column not in challenger_run_frame.columns:
                challenger_run_frame[column] = 0
        challenger_run_frame["candidate_capital"] = challenger_run_frame["entry_price"].abs() * SHARES_PER_UNIT
        challenger_run_frame["no_exposure_count"] = pd.to_numeric(challenger_run_frame["no_exposure_count"], errors="coerce").fillna(0).astype(int)
        challenger_run_frame["variant_no_exposure"] = challenger_run_frame["no_exposure_count"]
        challenger_run_frame["variant_policy_net_realized_pnl"] = pd.to_numeric(challenger_run_frame["variant_policy_net_realized_pnl"], errors="coerce")
        challenger_run_frame["variant_policy_total_pnl"] = pd.to_numeric(challenger_run_frame["variant_policy_total_pnl"], errors="coerce")
        challenger_run_frame["variant_policy_vs_hold_gap"] = pd.to_numeric(challenger_run_frame["variant_policy_vs_hold_gap"], errors="coerce")
        challenger_run_frame["variant_roundtrip_count"] = pd.to_numeric(challenger_run_frame["variant_roundtrip_count"], errors="coerce")
        challenger_run_frame["roundtrip_count"] = challenger_run_frame["variant_roundtrip_count"]
        challenger_run_frame["variant_policy_max_drawdown_during_holding"] = pd.to_numeric(challenger_run_frame["variant_policy_max_drawdown_during_holding"], errors="coerce")
        challenger_run_frame["variant_number_of_trades"] = pd.to_numeric(challenger_run_frame["variant_number_of_trades"], errors="coerce")
        challenger_run_frame["variant_forced_exit_count"] = pd.to_numeric(challenger_run_frame["variant_forced_exit_count"], errors="coerce")
        challenger_run_frame["variant_late_exit_count"] = pd.to_numeric(challenger_run_frame["variant_late_exit_count"], errors="coerce")
        challenger_run_frame["variant_stop_loss_count"] = pd.to_numeric(challenger_run_frame["variant_stop_loss_count"], errors="coerce")
        challenger_run_frame["variant_add_count"] = pd.to_numeric(challenger_run_frame["variant_add_count"], errors="coerce")
        challenger_run_frame["variant_hedge_action_count"] = pd.to_numeric(challenger_run_frame["variant_hedge_action_count"], errors="coerce")
        challenger_run_frame["variant_entry_count"] = pd.to_numeric(challenger_run_frame["variant_entry_count"], errors="coerce")
        challenger_run_frame["variant_exit_count"] = pd.to_numeric(challenger_run_frame["variant_exit_count"], errors="coerce")
        challenger_run_frame["candidate_capital"] = pd.to_numeric(challenger_run_frame["candidate_capital"], errors="coerce")
        challenger_run_frame["hold_pnl"] = pd.to_numeric(challenger_run_frame["hold_pnl"], errors="coerce")

    baseline_rows = baseline_candidate_frame.to_dict(orient="records")
    challenger_rows = challenger_run_frame.to_dict(orient="records")
    selection_rows_records = selection_rows.to_dict(orient="records")

    baseline_topk = _aggregate_variant_topk(selection_rows_records, baseline_rows)
    challenger_topk = _aggregate_variant_topk(selection_rows_records, challenger_rows)
    baseline_rank_rows = _build_rank_rows(baseline_rows)
    challenger_rank_rows = _build_rank_rows(challenger_rows)
    baseline_side_rows = _aggregate_variant_side(selection_rows_records, baseline_rows)
    challenger_side_rows = _aggregate_variant_side(selection_rows_records, challenger_rows)
    baseline_anchor_rows = _build_anchor_rows(baseline_rows)
    challenger_anchor_rows = _build_anchor_rows(challenger_rows)
    baseline_action_rows = _policy_repair_action_summary(pd.DataFrame(policy_payload.get("rows") or []))
    challenger_action_rows = _policy_repair_action_summary(pd.DataFrame(challenger_ledger_rows))

    transform_reference = _load_json(TRANSFORM_REFERENCE_COMPARE)
    transform_best = transform_reference["variants"]["B"]
    transform_topk = {
        "top5": float(transform_best["policy_trade"]["5"]["net_realized_pnl"]),
        "top10": float(transform_best["policy_trade"]["10"]["net_realized_pnl"]),
        "top20": float(transform_best["policy_trade"]["20"]["net_realized_pnl"]),
    }

    exposure_baseline = _exposure_normalization(baseline_candidate_frame)
    exposure_challenger = _exposure_normalization(challenger_run_frame)
    baseline_rank_map = _rank_row_map(baseline_rank_rows)
    challenger_rank_map = _rank_row_map(challenger_rank_rows)
    exposure_normalization = {
        "schema_version": "tradex_lower_bucket_long_rollout_v2B_fresh_exposure_normalization_v1",
        "generated_at": _utc_now(),
        "policy_variant": ARTIFACT_POLICY_VARIANT,
        "baseline_policy_variant": BASELINE_POLICY_VARIANT,
        "challenger_policy_variant": ARTIFACT_POLICY_VARIANT,
        "baseline": exposure_baseline,
        "challenger": exposure_challenger,
    }

    def _topk_pnl(metric: dict[str, Any], top_k: str) -> float:
        return float(metric[top_k]["policy_trade"]["challenger"]["net_realized_pnl"])

    top5_delta = float(challenger_topk["5"]["delta"]["policy_net_realized_pnl"])
    top10_delta = float(challenger_topk["10"]["delta"]["policy_net_realized_pnl"])
    top20_delta = float(challenger_topk["20"]["delta"]["policy_net_realized_pnl"])
    top5_ok = top5_delta >= 0
    top10_ok = top10_delta >= 0
    top20_ok = top20_delta >= 0
    lower_bucket_drag_reduced = (
        float(challenger_rank_map.get("top6_10", {}).get("policy_vs_hold_gap_sum", 0.0))
        >= float(baseline_rank_map.get("top6_10", {}).get("policy_vs_hold_gap_sum", 0.0))
        and float(challenger_rank_map.get("top11_20", {}).get("policy_vs_hold_gap_sum", 0.0))
        >= float(baseline_rank_map.get("top11_20", {}).get("policy_vs_hold_gap_sum", 0.0))
    )
    late_exit_still_suppressed = (
        int(challenger_rank_map.get("top6_10", {}).get("late_exit_count", 0))
        <= int(baseline_rank_map.get("top6_10", {}).get("late_exit_count", 0))
        and int(challenger_rank_map.get("top11_20", {}).get("late_exit_count", 0))
        <= int(baseline_rank_map.get("top11_20", {}).get("late_exit_count", 0))
    )
    starvation_flag = bool(exposure_challenger["overall"]["candidate_starvation_flag"])
    ppc_top10_ok = (
        float(exposure_challenger["by_topk"]["10"]["pnl_per_deployed_capital"]) >= float(exposure_baseline["by_topk"]["10"]["pnl_per_deployed_capital"])
    )
    ppc_top20_ok = (
        float(exposure_challenger["by_topk"]["20"]["pnl_per_deployed_capital"]) >= float(exposure_baseline["by_topk"]["20"]["pnl_per_deployed_capital"])
    )

    if top5_ok and top10_ok and top20_ok and lower_bucket_drag_reduced and not starvation_flag:
        decision = "keep"
    elif top10_ok or top20_ok or ppc_top10_ok or ppc_top20_ok:
        decision = "hold"
    else:
        decision = "drop"

    summary = {
        "schema_version": "tradex_lower_bucket_long_rollout_v2B_fresh_summary_v1",
        "generated_at": _utc_now(),
        "policy_variant": ARTIFACT_POLICY_VARIANT,
        "engine_policy_variant": ENGINE_POLICY_VARIANT,
        "rollout_variant": ROLLUP_VARIANT,
        "baseline_policy_variant": BASELINE_POLICY_VARIANT,
        "authoritative_rollup_decision": decision,
        "diagnosis_decision": decision,
        "best_variant": ROLLUP_VARIANT,
        "best_variant_label": _variant_label(),
        "best_variant_note": _variant_note(),
        "selection_only_same_as_baseline": True,
        "policy_layer_destroyed_edge": bool(not (top5_ok and top10_ok and top20_ok)),
        "topk_observations": {
            "top5": top5_delta,
            "top10": top10_delta,
            "top20": top20_delta,
        },
        "baseline_policy_reference": {
            "policy_variant": BASELINE_POLICY_VARIANT,
            "top5_policy_net_realized_pnl": float(baseline_topk["5"]["policy_trade"]["challenger"]["net_realized_pnl"]),
            "top10_policy_net_realized_pnl": float(baseline_topk["10"]["policy_trade"]["challenger"]["net_realized_pnl"]),
            "top20_policy_net_realized_pnl": float(baseline_topk["20"]["policy_trade"]["challenger"]["net_realized_pnl"]),
            "top5_policy_vs_hold_gap_sum": float(baseline_topk["5"]["policy_trade"]["challenger"]["hold_gap_sum"]),
            "top10_policy_vs_hold_gap_sum": float(baseline_topk["10"]["policy_trade"]["challenger"]["hold_gap_sum"]),
            "top20_policy_vs_hold_gap_sum": float(baseline_topk["20"]["policy_trade"]["challenger"]["hold_gap_sum"]),
            "lower_bucket_long_exposure_count": int(((baseline_candidate_frame["side"] == "long") & (baseline_candidate_frame["rank_bucket"].isin({"top6_10", "top11_20"}))).sum()),
            "lower_bucket_long_no_exposure_count": 0,
        },
        "challenger_policy_reference": {
            "policy_variant": ARTIFACT_POLICY_VARIANT,
            "top5_policy_net_realized_pnl": float(challenger_topk["5"]["policy_trade"]["challenger"]["net_realized_pnl"]),
            "top10_policy_net_realized_pnl": float(challenger_topk["10"]["policy_trade"]["challenger"]["net_realized_pnl"]),
            "top20_policy_net_realized_pnl": float(challenger_topk["20"]["policy_trade"]["challenger"]["net_realized_pnl"]),
            "top5_policy_vs_hold_gap_sum": float(challenger_topk["5"]["policy_trade"]["challenger"]["hold_gap_sum"]),
            "top10_policy_vs_hold_gap_sum": float(challenger_topk["10"]["policy_trade"]["challenger"]["hold_gap_sum"]),
            "top20_policy_vs_hold_gap_sum": float(challenger_topk["20"]["policy_trade"]["challenger"]["hold_gap_sum"]),
            "lower_bucket_long_exposure_count": int(((challenger_run_frame["side"] == "long") & (challenger_run_frame["rank_bucket"].isin({"top6_10", "top11_20"})) & (challenger_run_frame["no_exposure_count"] == 0)).sum()),
            "lower_bucket_long_no_exposure_count": int(((challenger_run_frame["side"] == "long") & (challenger_run_frame["rank_bucket"].isin({"top6_10", "top11_20"})) & (challenger_run_frame["no_exposure_count"] > 0)).sum()),
        },
        "transform_reference": {
            "policy_variant": "lower_bucket_long_rollout_v2",
            "best_variant": "B",
            "top5_policy_net_realized_pnl": transform_topk["top5"],
            "top10_policy_net_realized_pnl": transform_topk["top10"],
            "top20_policy_net_realized_pnl": transform_topk["top20"],
        },
        "fresh_vs_transform_delta": {
            "top5_policy_net_realized_pnl": float(challenger_topk["5"]["policy_trade"]["challenger"]["net_realized_pnl"] - transform_topk["top5"]),
            "top10_policy_net_realized_pnl": float(challenger_topk["10"]["policy_trade"]["challenger"]["net_realized_pnl"] - transform_topk["top10"]),
            "top20_policy_net_realized_pnl": float(challenger_topk["20"]["policy_trade"]["challenger"]["net_realized_pnl"] - transform_topk["top20"]),
        },
        "exposure_normalization_reference": exposure_normalization,
        "summary_references": {
            "anchor_count": integrated_summary.get("anchor_count"),
            "selection_rows_count": int(len(selection_rows)),
            "policy_run_rows_count": int(len(challenger_run_rows)),
            "policy_ledger_rows_count": int(len(challenger_ledger_rows)),
            "basis_row_skip_count": exclusion_payload.get("rows", {}).get("aggregate", {}).get("skipped_symbols_without_basis_row_count"),
            "full_universe_no_trade_rate_mean_specialized": coverage_payload.get("aggregate", {}).get("specialized", {}).get("no_trade_rate_mean"),
            "full_universe_long_tradable_rate_mean_specialized": float(sum(float(r["specialized"]["long_tradable_rate"]) for r in coverage_payload.get("rows") or [] if r.get("specialized") and r["specialized"].get("long_tradable_rate") is not None) / max(1, sum(1 for r in coverage_payload.get("rows") or [] if r.get("specialized") and r["specialized"].get("long_tradable_rate") is not None))),
            "full_universe_short_tradable_rate_mean_specialized": float(sum(float(r["specialized"]["short_tradable_rate"]) for r in coverage_payload.get("rows") or [] if r.get("specialized") and r["specialized"].get("short_tradable_rate") is not None) / max(1, sum(1 for r in coverage_payload.get("rows") or [] if r.get("specialized") and r["specialized"].get("short_tradable_rate") is not None))),
            "candidate_starvation_flag": starvation_flag,
            "exposure_rate": exposure_challenger["overall"]["exposure_rate"],
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
            "transform_reference_compare": str(TRANSFORM_REFERENCE_COMPARE),
            "transform_reference_summary": str(TRANSFORM_REFERENCE_SUMMARY),
        },
    }

    compare = {
        "schema_version": "tradex_lower_bucket_long_rollout_v2B_fresh_compare_v1",
        "generated_at": _utc_now(),
        "policy_variant": ARTIFACT_POLICY_VARIANT,
        "same_condition_contract": {
            "same_anchor_set": True,
            "same_candidates": True,
            "same_db_source": _load_json(input_dir / "integrated_guarded_v1_db_provenance.json").get("rows", {}).get("db_provenance", {}).get("source_db_path"),
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
        "challenger": {
            "policy_variant": ARTIFACT_POLICY_VARIANT,
            "topk_metrics": challenger_topk,
            "side_rows": challenger_side_rows,
            "rank_rows": challenger_rank_rows,
            "action_rows": challenger_action_rows,
        },
        "delta_vs_baseline": {
            "top5_selection_only_avg_ret63": challenger_topk["5"]["delta"]["selection_only_avg_ret63"],
            "top10_selection_only_avg_ret63": challenger_topk["10"]["delta"]["selection_only_avg_ret63"],
            "top20_selection_only_avg_ret63": challenger_topk["20"]["delta"]["selection_only_avg_ret63"],
            "top5_policy_net_realized_pnl": challenger_topk["5"]["delta"]["policy_net_realized_pnl"],
            "top10_policy_net_realized_pnl": challenger_topk["10"]["delta"]["policy_net_realized_pnl"],
            "top20_policy_net_realized_pnl": challenger_topk["20"]["delta"]["policy_net_realized_pnl"],
        },
        "deterministic_transform_reference": {
            "policy_variant": "lower_bucket_long_rollout_v2",
            "best_variant": "B",
            "top5_policy_net_realized_pnl": transform_topk["top5"],
            "top10_policy_net_realized_pnl": transform_topk["top10"],
            "top20_policy_net_realized_pnl": transform_topk["top20"],
        },
        "fresh_vs_transform_delta": {
            "top5_policy_net_realized_pnl": float(challenger_topk["5"]["policy_trade"]["challenger"]["net_realized_pnl"] - transform_topk["top5"]),
            "top10_policy_net_realized_pnl": float(challenger_topk["10"]["policy_trade"]["challenger"]["net_realized_pnl"] - transform_topk["top10"]),
            "top20_policy_net_realized_pnl": float(challenger_topk["20"]["policy_trade"]["challenger"]["net_realized_pnl"] - transform_topk["top20"]),
        },
        "exposure_normalization": exposure_normalization,
        "best_variant": ROLLUP_VARIANT,
        "best_variant_label": _variant_label(),
        "best_variant_note": _variant_note(),
        "selection_only_same_as_baseline": True,
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "summary_json": _write_json(output_dir / "lower_bucket_long_rollout_v2B_fresh_summary.json", summary),
        "compare_json": _write_json(output_dir / "lower_bucket_long_rollout_v2B_fresh_compare.json", compare),
        "by_rank_bucket_json": _write_json(
            output_dir / "lower_bucket_long_rollout_v2B_fresh_by_rank_bucket.json",
            {
                "schema_version": "tradex_lower_bucket_long_rollout_v2B_fresh_by_rank_bucket_v1",
                "generated_at": _utc_now(),
                "baseline": baseline_rank_rows,
                "challenger": challenger_rank_rows,
            },
        ),
        "by_side_json": _write_json(
            output_dir / "lower_bucket_long_rollout_v2B_fresh_by_side.json",
            {
                "schema_version": "tradex_lower_bucket_long_rollout_v2B_fresh_by_side_v1",
                "generated_at": _utc_now(),
                "baseline": baseline_side_rows,
                "challenger": challenger_side_rows,
            },
        ),
        "by_action_json": _write_json(
            output_dir / "lower_bucket_long_rollout_v2B_fresh_by_action.json",
            {
                "schema_version": "tradex_lower_bucket_long_rollout_v2B_fresh_by_action_v1",
                "generated_at": _utc_now(),
                "baseline": baseline_action_rows,
                "challenger": challenger_action_rows,
            },
        ),
        "exposure_normalization_json": _write_json(
            output_dir / "lower_bucket_long_rollout_v2B_fresh_exposure_normalization.json",
            exposure_normalization,
        ),
        "trade_ledger_json": _write_json(
            output_dir / "lower_bucket_long_rollout_v2B_fresh_trade_ledger.json",
            {
                "schema_version": "tradex_lower_bucket_long_rollout_v2B_fresh_trade_ledger_v1",
                "generated_at": _utc_now(),
                "policy_variant": ARTIFACT_POLICY_VARIANT,
                "rows": challenger_ledger_rows,
            },
        ),
        "decision_json": _write_json(
            output_dir / "lower_bucket_long_rollout_v2B_fresh_decision.json",
            {
                "schema_version": "tradex_lower_bucket_long_rollout_v2B_fresh_decision_v1",
                "generated_at": _utc_now(),
                "policy_variant": ARTIFACT_POLICY_VARIANT,
                "decision": decision,
                "best_variant": ROLLUP_VARIANT,
                "best_variant_label": _variant_label(),
                "best_variant_note": _variant_note(),
                "selection_only_same_as_baseline": True,
                "policy_layer_destroyed_edge": summary["policy_layer_destroyed_edge"],
                "candidate_starvation_flag": starvation_flag,
            },
        ),
    }

    return {
        "ok": True,
        "output_dir": str(output_dir),
        "paths": {key: str(value) for key, value in paths.items()},
        "summary": summary,
        "compare": compare,
        "exposure_normalization": exposure_normalization,
        "variant_results": {
            "B": {
                "rank_rows": challenger_rank_rows,
                "side_rows": challenger_side_rows,
                "action_rows": challenger_action_rows,
            }
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fresh replay for lower-bucket long rollout v2B.")
    parser.add_argument("--input-dir", default=str(DEFAULT_INPUT_DIR))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--row-limit", type=int, default=None)
    args = parser.parse_args(argv)
    payload = run_lower_bucket_long_rollout_v2b_fresh(
        input_dir=Path(args.input_dir).expanduser().resolve(),
        output_dir=Path(args.output_dir).expanduser().resolve(),
        row_limit=args.row_limit,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
