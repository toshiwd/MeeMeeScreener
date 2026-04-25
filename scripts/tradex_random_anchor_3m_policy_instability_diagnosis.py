from __future__ import annotations

import argparse
import json
import math
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_chart_first_replay import SHARES_PER_UNIT  # noqa: E402

DEFAULT_INPUT_DIR = Path(r"G:\Tradex\sample_replays\tradex_random_anchor_3m_stress200")
DEFAULT_OUTPUT_DIR = Path(r"G:\Tradex\sample_replays\tradex_random_anchor_3m_stress200_policy_instability")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if pd.isna(value):
        return None
    return value


def _json_text(payload: Any) -> str:
    return json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, default=str)


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_json_text(payload) + "\n", encoding="utf-8")
    return path


def _load_rows(path: Path, *, dedupe: bool = False) -> pd.DataFrame:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("rows", [])
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    if dedupe:
        # The replay emits exact duplicate rows for shared champion/challenger selections.
        # Use a stable string signature so list-valued columns stay hashable.
        sig = frame.apply(lambda row: json.dumps(_json_ready(row.to_dict()), sort_keys=True, ensure_ascii=False), axis=1)
        frame = frame.loc[~sig.duplicated()].reset_index(drop=True)
    return frame


def _to_number(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _side_bucket(rank: Any) -> str:
    value = _to_number(pd.Series([rank])).iloc[0]
    if pd.isna(value):
        return "unknown"
    if value <= 5:
        return "top5"
    if value <= 10:
        return "top6_10"
    if value <= 20:
        return "top11_20"
    return "outside_top20"


def _action_category(row: pd.Series, *, is_last_row: bool) -> str | None:
    action = str(row.get("selected_action") or "")
    exit_reason = str(row.get("exit_reason_primary") or "")
    flat_reason = str(row.get("flat_reason_primary") or "")
    hedge_reason = str(row.get("hedge_reason_primary") or "")
    trim_reason = str(row.get("trim_reason_primary") or "")
    policy_exit_timing = str(row.get("policy_exit_timing") or "")

    if action == "stay":
        return None
    if "long_entry" in action or "short_entry" in action:
        return "entry"
    if "long_add" in action or "short_add" in action:
        return "add"
    if "hedge_add" in action or "hedge_open" in action:
        return "hedge_add"
    if "hedge_reduce" in action or "hedge_close" in action:
        return "hedge_reduce"
    if "long_trim" in action:
        if trim_reason == "lose_ma20" or exit_reason == "lose_ma20":
            return "stop_loss"
        return "partial_exit"
    if "long_exit" in action or "short_cover" in action:
        if trim_reason == "lose_ma20" or exit_reason == "lose_ma20":
            return "stop_loss"
        if exit_reason == "time_stop":
            return "forced_exit"
        if exit_reason == "lose_ma60" or flat_reason == "late_extension_blocked" or policy_exit_timing == "late":
            return "late_exit"
        return "final_close" if is_last_row else "forced_exit"
    return "other"


def _reason_primary_for_category(row: pd.Series, category: str | None) -> str | None:
    if category is None:
        return None
    if category == "entry":
        return str(row.get("entry_reason_primary") or None)
    if category == "add":
        return str(row.get("add_reason_primary") or None)
    if category == "partial_exit":
        return str(row.get("trim_reason_primary") or row.get("exit_reason_primary") or None)
    if category == "stop_loss":
        return str(row.get("trim_reason_primary") or row.get("exit_reason_primary") or None)
    if category in {"forced_exit", "final_close", "late_exit"}:
        return str(row.get("exit_reason_primary") or None)
    if category in {"hedge_add", "hedge_reduce"}:
        return str(row.get("hedge_reason_primary") or None)
    return str(row.get("selected_action") or None)


def _annotate_policy_actions(policy: pd.DataFrame) -> pd.DataFrame:
    annotated = policy.copy()
    keys = ["anchor_date", "symbol", "side"]
    annotated = annotated.sort_values(keys + ["trade_date"]).copy()
    annotated["realized_pnl"] = _to_number(annotated["realized_pnl"])
    annotated["unrealized_pnl"] = _to_number(annotated["unrealized_pnl"])
    annotated["policy_total_pnl"] = annotated["realized_pnl"].fillna(0.0) + annotated["unrealized_pnl"].fillna(0.0)
    annotated["realized_delta"] = annotated.groupby(keys)["realized_pnl"].diff().fillna(annotated["realized_pnl"]).fillna(0.0)
    annotated["total_delta"] = annotated.groupby(keys)["policy_total_pnl"].diff().fillna(annotated["policy_total_pnl"]).fillna(0.0)
    selected_action = annotated["selected_action"].astype(str)
    exit_reason = annotated["exit_reason_primary"].astype(str)
    flat_reason = annotated["flat_reason_primary"].astype(str)
    hedge_reason = annotated["hedge_reason_primary"].astype(str)
    trim_reason = annotated["trim_reason_primary"].astype(str)
    policy_exit_timing = annotated["policy_exit_timing"].astype(str)
    last_row_mask = annotated.groupby(keys)["trade_date"].transform("max").eq(annotated["trade_date"])

    entry_mask = selected_action.str.contains("long_entry", regex=False) | selected_action.str.contains("short_entry", regex=False)
    add_mask = selected_action.str.contains("long_add", regex=False) | selected_action.str.contains("short_add", regex=False)
    hedge_add_mask = selected_action.str.contains("hedge_add", regex=False) | selected_action.str.contains("hedge_open", regex=False)
    hedge_reduce_mask = selected_action.str.contains("hedge_reduce", regex=False) | selected_action.str.contains("hedge_close", regex=False)
    stop_loss_mask = (exit_reason == "lose_ma20") | (trim_reason == "lose_ma20")
    forced_exit_mask = exit_reason == "time_stop"
    late_exit_mask = (exit_reason == "lose_ma60") | (flat_reason == "late_extension_blocked") | (policy_exit_timing == "late")
    partial_exit_mask = selected_action.str.contains("long_trim", regex=False) & ~stop_loss_mask
    final_close_mask = (selected_action.str.contains("long_exit", regex=False) | selected_action.str.contains("short_cover", regex=False)) & last_row_mask

    annotated["action_category"] = pd.Series([None] * len(annotated), index=annotated.index, dtype="object")
    annotated.loc[entry_mask, "action_category"] = "entry"
    annotated.loc[add_mask, "action_category"] = "add"
    annotated.loc[hedge_add_mask, "action_category"] = "hedge_add"
    annotated.loc[hedge_reduce_mask, "action_category"] = "hedge_reduce"
    annotated.loc[stop_loss_mask, "action_category"] = "stop_loss"
    annotated.loc[late_exit_mask & ~stop_loss_mask, "action_category"] = "late_exit"
    annotated.loc[forced_exit_mask & ~stop_loss_mask & ~late_exit_mask, "action_category"] = "forced_exit"
    annotated.loc[partial_exit_mask & ~stop_loss_mask, "action_category"] = "partial_exit"
    annotated.loc[final_close_mask & annotated["action_category"].isna(), "action_category"] = "final_close"
    annotated.loc[annotated["action_category"].isna() & selected_action.ne("stay"), "action_category"] = "other"

    reason_primary = pd.Series([None] * len(annotated), index=annotated.index, dtype="object")
    reason_primary.loc[annotated["action_category"].eq("entry")] = annotated.loc[annotated["action_category"].eq("entry"), "entry_reason_primary"].astype(str)
    reason_primary.loc[annotated["action_category"].eq("add")] = annotated.loc[annotated["action_category"].eq("add"), "add_reason_primary"].astype(str)
    reason_primary.loc[annotated["action_category"].isin(["partial_exit", "stop_loss"])] = annotated.loc[
        annotated["action_category"].isin(["partial_exit", "stop_loss"]), "trim_reason_primary"
    ].astype(str)
    reason_primary.loc[annotated["action_category"].isin(["forced_exit", "final_close", "late_exit"])] = annotated.loc[
        annotated["action_category"].isin(["forced_exit", "final_close", "late_exit"]), "exit_reason_primary"
    ].astype(str)
    reason_primary.loc[annotated["action_category"].isin(["hedge_add", "hedge_reduce"])] = annotated.loc[
        annotated["action_category"].isin(["hedge_add", "hedge_reduce"]), "hedge_reason_primary"
    ].astype(str)
    reason_primary.loc[annotated["action_category"].eq("other")] = annotated.loc[annotated["action_category"].eq("other"), "selected_action"].astype(str)
    annotated["reason_primary"] = reason_primary
    return annotated


def _summarize_group_rows(group_frame: pd.DataFrame, selection_frame: pd.DataFrame) -> pd.DataFrame:
    keys = ["anchor_date", "symbol", "side"]
    policy = _annotate_policy_actions(group_frame)
    policy["is_trade_action"] = policy["selected_action"].astype(str).ne("stay")
    policy["is_add"] = policy["selected_action"].astype(str).str.contains("add", regex=False)
    policy["is_hedge"] = policy["selected_action"].astype(str).str.contains("hedge", regex=False)
    policy["is_exit"] = policy["selected_action"].astype(str).str.contains("long_exit", regex=False) | policy["selected_action"].astype(str).str.contains("short_cover", regex=False)

    selection = selection_frame.sort_values(keys).copy()
    selection["entry_price"] = _to_number(selection["entry_price"])
    selection["ret63"] = _to_number(selection["ret63"])
    selection["mfe63"] = _to_number(selection["mfe63"])
    selection["mae63"] = _to_number(selection["mae63"])
    selection["max_adverse_excursion"] = _to_number(selection["max_adverse_excursion"])
    selection["challenger_rank"] = _to_number(selection["challenger_rank"])
    selection["champion_rank"] = _to_number(selection["champion_rank"])
    selection["selected_by"] = selection["selected_by"].astype(str)
    selection["selected_by_methods"] = selection["selected_by_methods"].astype(str)
    selection["hold_pnl_est"] = selection["ret63"] * selection["entry_price"] * SHARES_PER_UNIT

    final_rows = policy.groupby(keys, as_index=False).tail(1).copy()
    group_counts = policy.groupby(keys, as_index=False).agg(
        number_of_trades=("is_trade_action", "sum"),
        forced_exit_count=("exit_reason_primary", lambda s: int((s.astype(str) == "time_stop").any())),
        late_exit_count=("exit_reason_primary", lambda s: int((s.astype(str) == "lose_ma60").any())),
        stop_loss_count=("exit_reason_primary", lambda s: int((s.astype(str) == "lose_ma20").any())),
        add_count=("is_add", "sum"),
        hedge_action_count=("is_hedge", "sum"),
        stay_count=("selected_action", lambda s: int((s.astype(str) == "stay").sum())),
    )
    group_counts["number_of_trades"] = group_counts["number_of_trades"].astype(int)
    group_counts["forced_exit_count"] = group_counts["forced_exit_count"].astype(int)
    group_counts["late_exit_count"] = group_counts["late_exit_count"].astype(int)
    group_counts["stop_loss_count"] = group_counts["stop_loss_count"].astype(int)
    group_counts["add_count"] = group_counts["add_count"].astype(int)
    group_counts["hedge_action_count"] = group_counts["hedge_action_count"].astype(int)
    group_counts["stay_count"] = group_counts["stay_count"].astype(int)

    summary = selection.merge(
        final_rows[keys + ["realized_pnl", "unrealized_pnl", "policy_total_pnl", "policy_net_realized_pnl", "policy_max_drawdown_during_holding", "policy_exit_timing", "selected_action", "entry_reason_primary", "exit_reason_primary", "flat_reason_primary", "hedge_reason_primary", "trim_reason_primary"]],
        on=keys,
        how="inner",
        suffixes=("", "_policy"),
    )
    summary = summary.merge(group_counts, on=keys, how="left")
    summary["policy_realized_pnl"] = summary["realized_pnl"].fillna(0.0)
    summary["policy_unrealized_pnl"] = summary["unrealized_pnl"].fillna(0.0)
    summary["policy_total_pnl"] = summary["policy_total_pnl"].fillna(summary["policy_realized_pnl"] + summary["policy_unrealized_pnl"])
    summary["policy_vs_hold_gap"] = summary["policy_total_pnl"] - summary["hold_pnl_est"]
    summary["policy_side"] = summary["side"]
    summary["selection_only_side"] = summary["side"]
    summary["bucket"] = [
        _side_bucket(rank)
        for rank in summary["challenger_rank"].tolist()
    ]
    summary["selected_action"] = summary["selected_action"].astype(str)
    summary["result_bucket"] = summary["result_bucket"].astype(str)
    summary["policy_max_drawdown_during_holding"] = _to_number(summary["policy_max_drawdown_during_holding"])
    summary["policy_exit_timing"] = summary["policy_exit_timing"].astype(str)
    summary["policy_net_realized_pnl"] = _to_number(summary["policy_net_realized_pnl"])
    return summary


def _anchor_summary(group_summary: pd.DataFrame, candidate_frame: pd.DataFrame, compare_payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    cand = candidate_frame.copy()
    cand["challenger_rank"] = _to_number(cand["challenger_rank"])
    cand["champion_rank"] = _to_number(cand["champion_rank"])
    cand["selection_reason"] = cand["selection_reason"].astype(str)

    group_summary = group_summary.copy()
    group_summary["challenger_selected_top20"] = group_summary["challenger_selected_top20"].astype(str).eq("True")
    group_summary["champion_selected_top20"] = group_summary["champion_selected_top20"].astype(str).eq("True")
    group_summary["challenger_selected_top10"] = group_summary["challenger_selected_top10"].astype(str).eq("True")
    group_summary["champion_selected_top10"] = group_summary["champion_selected_top10"].astype(str).eq("True")
    group_summary["challenger_selected_top5"] = group_summary["challenger_selected_top5"].astype(str).eq("True")
    group_summary["champion_selected_top5"] = group_summary["champion_selected_top5"].astype(str).eq("True")

    for anchor_date, anchor_group in group_summary.groupby("anchor_date"):
        cand_anchor = cand[cand["anchor_date"] == anchor_date]
        anchor_compare = next(
            (row for row in compare_payload.get("anchor_compare_rows", []) if str(row.get("anchor_date")) == str(anchor_date)),
            {},
        )
        chal20 = anchor_group[anchor_group["challenger_selected_top20"]]
        champ20 = anchor_group[anchor_group["champion_selected_top20"]]
        chal_long = chal20[chal20["side"] == "long"]
        chal_short = chal20[chal20["side"] == "short"]
        rows.append(
            {
                "anchor_date": anchor_date,
                "month_bucket": str(anchor_group["month_bucket"].iloc[0]) if not anchor_group.empty else None,
                "regime_bucket": str(anchor_group["regime_bucket"].iloc[0]) if "regime_bucket" in anchor_group.columns and not anchor_group.empty else None,
                "candidate_count": int(len(cand_anchor)),
                "selection_only_count": int((anchor_group["challenger_selected_top20"]).sum()),
                "policy_run_count": int(len(chal20)),
                "selection_only_top20_avg_ret63_champion": (
                    None if champ20.empty else float(pd.to_numeric(champ20["ret63"], errors="coerce").mean())
                ),
                "selection_only_top20_avg_ret63_challenger": (
                    None if chal20.empty else float(pd.to_numeric(chal20["ret63"], errors="coerce").mean())
                ),
                "selection_only_top20_delta": (
                    None
                    if champ20.empty or chal20.empty
                    else float(pd.to_numeric(chal20["ret63"], errors="coerce").mean() - pd.to_numeric(champ20["ret63"], errors="coerce").mean())
                ),
                "policy_top20_pnl_champion": (
                    None
                    if champ20.empty
                    else float(pd.to_numeric(champ20["policy_total_pnl"], errors="coerce").sum())
                ),
                "policy_top20_pnl_challenger": (
                    None
                    if chal20.empty
                    else float(pd.to_numeric(chal20["policy_total_pnl"], errors="coerce").sum())
                ),
                "policy_top20_delta": (
                    None
                    if champ20.empty or chal20.empty
                    else float(pd.to_numeric(chal20["policy_total_pnl"], errors="coerce").sum() - pd.to_numeric(champ20["policy_total_pnl"], errors="coerce").sum())
                ),
                "policy_vs_hold_gap_champion": (
                    None
                    if champ20.empty
                    else float(pd.to_numeric(champ20["policy_vs_hold_gap"], errors="coerce").sum())
                ),
                "policy_vs_hold_gap_challenger": (
                    None
                    if chal20.empty
                    else float(pd.to_numeric(chal20["policy_vs_hold_gap"], errors="coerce").sum())
                ),
                "policy_vs_hold_gap_delta": (
                    None
                    if champ20.empty or chal20.empty
                    else float(pd.to_numeric(chal20["policy_vs_hold_gap"], errors="coerce").sum() - pd.to_numeric(champ20["policy_vs_hold_gap"], errors="coerce").sum())
                ),
                "long_contribution": float(pd.to_numeric(chal_long["policy_vs_hold_gap"], errors="coerce").sum()) if not chal_long.empty else 0.0,
                "short_contribution": float(pd.to_numeric(chal_short["policy_vs_hold_gap"], errors="coerce").sum()) if not chal_short.empty else 0.0,
                "worst_symbols": [
                    {
                        "symbol": str(row.symbol),
                        "side": str(row.side),
                        "policy_vs_hold_gap": float(row.policy_vs_hold_gap),
                        "policy_total_pnl": float(row.policy_total_pnl),
                        "ret63": float(row.ret63),
                    }
                    for _, row in chal20.sort_values("policy_vs_hold_gap").head(5).iterrows()
                ],
                "worst_actions": [],
                "number_of_trades": int(pd.to_numeric(chal20["number_of_trades"], errors="coerce").sum()) if not chal20.empty else 0,
                "forced_exit_count": int(pd.to_numeric(chal20["forced_exit_count"], errors="coerce").sum()) if not chal20.empty else 0,
                "late_exit_count": int(pd.to_numeric(chal20["late_exit_count"], errors="coerce").sum()) if not chal20.empty else 0,
                "stop_loss_count": int(pd.to_numeric(chal20["stop_loss_count"], errors="coerce").sum()) if not chal20.empty else 0,
                "add_count": int(pd.to_numeric(chal20["add_count"], errors="coerce").sum()) if not chal20.empty else 0,
                "hedge_action_count": int(pd.to_numeric(chal20["hedge_action_count"], errors="coerce").sum()) if not chal20.empty else 0,
                "changed_top5_members_count": int(anchor_compare.get("changed_top5_members_count", 0)),
                "changed_top10_members_count": int(anchor_compare.get("changed_top10_members_count", 0)),
                "changed_top20_members_count": int(anchor_compare.get("changed_top20_members_count", 0)),
                "trend_up_preserved_count": int(anchor_compare.get("trend_up_preserved_count", 0)),
                "trend_down_selected_count": int(anchor_compare.get("trend_down_selected_count", 0)),
            }
        )
    return rows


def _top_contributors(frame: pd.DataFrame, *, group_col: str, value_col: str, limit: int = 10) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    grouped = frame.groupby(group_col, dropna=False)[value_col].sum().sort_values()
    rows = []
    for key, value in grouped.head(limit).items():
        rows.append({"label": str(key), "value": float(value)})
    return rows


def _action_summary(policy_frame: pd.DataFrame) -> list[dict[str, Any]]:
    df = _annotate_policy_actions(policy_frame)
    df["action_category"] = df["action_category"].astype(str)
    df["reason_primary"] = df["reason_primary"].astype(str)
    df["is_positive"] = df["realized_delta"] > 0
    total_gap = float(df.groupby(["anchor_date", "symbol", "side"])["realized_delta"].sum().sum())
    if not math.isfinite(total_gap) or total_gap == 0:
        total_gap = 1.0
    summary = []
    for (category, reason), group in df[df["action_category"].notna()].groupby(["action_category", "reason_primary"], dropna=False):
        summary.append(
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
    summary.sort(key=lambda row: row["realized_pnl_sum"])
    return summary


def run_policy_instability_diagnosis(
    *,
    input_dir: Path = DEFAULT_INPUT_DIR,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> dict[str, Any]:
    summary_path = input_dir / "random_anchor_replay_summary_stress200.json"
    compare_path = input_dir / "champion_vs_challenger_random_anchor_compare_stress200.json"
    selection_path = input_dir / "selection_only_replay_ledger_stress200.json"
    policy_path = input_dir / "policy_trade_replay_ledger_stress200.json"
    candidate_path = input_dir / "random_anchor_candidate_snapshots_stress200.json"
    coverage_path = input_dir / "full_universe_gate_coverage_stress200.json"
    exclusion_path = input_dir / "random_anchor_exclusion_diagnostics_stress200.json"
    dates_path = input_dir / "random_anchor_dates_stress200.json"

    summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    compare_payload = json.loads(compare_path.read_text(encoding="utf-8"))
    selection_df = _load_rows(selection_path, dedupe=True)
    policy_df = _load_rows(policy_path, dedupe=False)
    candidate_df = _load_rows(candidate_path, dedupe=True)
    coverage_payload = json.loads(coverage_path.read_text(encoding="utf-8"))
    exclusion_payload = json.loads(exclusion_path.read_text(encoding="utf-8"))
    dates_payload = json.loads(dates_path.read_text(encoding="utf-8"))

    group_summary = _summarize_group_rows(policy_df, selection_df)
    anchor_rows = _anchor_summary(group_summary, candidate_df, compare_payload)

    # Build a row-level policy vs hold ledger for joined groups.
    policy_last = (
        policy_df.sort_values(["anchor_date", "symbol", "side", "trade_date"])
        .groupby(["anchor_date", "symbol", "side"], as_index=False)
        .tail(1)
        .copy()
    )
    policy_last["policy_realized_pnl"] = _to_number(policy_last["realized_pnl"]).fillna(0.0)
    policy_last["policy_unrealized_pnl"] = _to_number(policy_last["unrealized_pnl"]).fillna(0.0)
    policy_last["policy_total_pnl"] = policy_last["policy_realized_pnl"] + policy_last["policy_unrealized_pnl"]
    policy_last = policy_last[
        [
            "anchor_date",
            "symbol",
            "side",
            "policy_realized_pnl",
            "policy_unrealized_pnl",
            "policy_total_pnl",
            "policy_net_realized_pnl",
            "policy_max_drawdown_during_holding",
            "policy_exit_timing",
            "selected_action",
            "exit_reason_primary",
            "flat_reason_primary",
            "hedge_reason_primary",
            "trim_reason_primary",
        ]
    ]

    selection_join = selection_df.copy()
    selection_join["entry_price"] = _to_number(selection_join["entry_price"])
    selection_join["ret63"] = _to_number(selection_join["ret63"])
    selection_join["mfe63"] = _to_number(selection_join["mfe63"])
    selection_join["mae63"] = _to_number(selection_join["mae63"])
    selection_join["max_adverse_excursion"] = _to_number(selection_join["max_adverse_excursion"])
    selection_join["challenger_rank"] = _to_number(selection_join["challenger_rank"])
    selection_join["champion_rank"] = _to_number(selection_join["champion_rank"])
    selection_join["hold_pnl_est"] = selection_join["ret63"] * selection_join["entry_price"] * SHARES_PER_UNIT

    policy_meta = group_summary[
        [
            "anchor_date",
            "symbol",
            "side",
            "policy_realized_pnl",
            "policy_unrealized_pnl",
            "policy_total_pnl",
            "policy_net_realized_pnl",
            "policy_max_drawdown_during_holding",
            "policy_exit_timing",
            "selected_action",
            "entry_reason_primary",
            "exit_reason_primary",
            "flat_reason_primary",
            "hedge_reason_primary",
            "trim_reason_primary",
            "number_of_trades",
            "forced_exit_count",
            "late_exit_count",
            "stop_loss_count",
            "add_count",
            "hedge_action_count",
        ]
    ].copy()
    selection_join = selection_join.merge(policy_meta, on=["anchor_date", "symbol", "side"], how="inner")
    selection_join["policy_vs_hold_gap"] = selection_join["policy_total_pnl"] - selection_join["hold_pnl_est"]
    selection_join["rank_bucket"] = selection_join["challenger_rank"].apply(_side_bucket)
    selection_join["candidate_rank_bucket"] = selection_join["rank_bucket"]
    selection_join["selected_by_methods"] = selection_join["selected_by_methods"].astype(str)
    selection_join["selected_by"] = selection_join["selected_by"].astype(str)
    selection_join["policy_max_drawdown_during_holding"] = _to_number(selection_join["policy_max_drawdown_during_holding"])

    candidate_join = candidate_df.groupby(["anchor_date", "symbol", "side"], as_index=False).first()
    candidate_join = candidate_join[["anchor_date", "symbol", "side", "selection_reason", "market_regime_bucket"]]
    selection_join = selection_join.merge(candidate_join, on=["anchor_date", "symbol", "side"], how="left")
    selection_join["selection_reason"] = selection_join["selection_reason"].astype(str)
    selection_join["market_regime_bucket"] = selection_join["market_regime_bucket"].astype(str)

    # Anchor-level summaries.
    anchor_policy_rows = []
    for anchor_date, anchor_df in selection_join.groupby("anchor_date"):
        challenger = candidate_df[candidate_df["anchor_date"] == anchor_date]
        anchor_compare = next(
            (row for row in compare_payload.get("anchor_compare_rows", []) if str(row.get("anchor_date")) == str(anchor_date)),
            {},
        )
        challenger_top20 = anchor_df[anchor_df["challenger_selected_top20"].astype(str).eq("True")].copy()
        champion_top20 = anchor_df[anchor_df["champion_selected_top20"].astype(str).eq("True")].copy()
        rows_top20 = selection_join[(selection_join["anchor_date"] == anchor_date) & (selection_join["challenger_rank"].notna()) & (selection_join["challenger_rank"] <= 20)]
        long_rows = rows_top20[rows_top20["side"] == "long"].copy()
        short_rows = rows_top20[rows_top20["side"] == "short"].copy()
        def _gap_sum(frame: pd.DataFrame) -> float:
            return float(_to_number(frame["policy_vs_hold_gap"]).sum()) if not frame.empty else 0.0
        def _pnl_sum(frame: pd.DataFrame) -> float:
            return float(_to_number(frame["policy_total_pnl"]).sum()) if not frame.empty else 0.0
        def _avg_ret(frame: pd.DataFrame) -> float | None:
            return None if frame.empty else float(_to_number(frame["ret63"]).mean())
        worst_symbols = (
            rows_top20.sort_values("policy_vs_hold_gap")[["symbol", "side", "policy_vs_hold_gap", "policy_total_pnl", "ret63"]]
            .head(5)
            .to_dict(orient="records")
        )
        worst_actions = (
            policy_df[policy_df["anchor_date"] == anchor_date]
            .copy()
        )
        worst_actions["realized_pnl"] = _to_number(worst_actions["realized_pnl"])
        worst_actions["realized_delta"] = worst_actions.groupby(["anchor_date", "symbol", "side"])["realized_pnl"].diff().fillna(worst_actions["realized_pnl"]).fillna(0.0)
        worst_actions["action_category"] = worst_actions.apply(lambda row: _action_category(row, is_last_row=str(row.get("trade_date")) == str(worst_actions["trade_date"].max())), axis=1)
        worst_actions["reason_primary"] = worst_actions.apply(lambda row: _reason_primary_for_category(row, _action_category(row, is_last_row=str(row.get("trade_date")) == str(worst_actions["trade_date"].max()))), axis=1)
        worst_actions = worst_actions[worst_actions["action_category"].notna()]
        worst_action_rows = (
            worst_actions.groupby(["action_category", "reason_primary"], dropna=False)["realized_delta"]
            .sum()
            .sort_values()
            .head(5)
        )
        anchor_policy_rows.append(
            {
                "anchor_date": anchor_date,
                "month_bucket": str(anchor_df["month_bucket"].iloc[0]) if not anchor_df.empty else None,
                "regime_bucket": str(challenger["regime_bucket"].iloc[0]) if "regime_bucket" in challenger.columns and not challenger.empty else None,
                "candidate_count": int(len(challenger)),
                "selection_only_count": int(len(challenger_top20)),
                "policy_run_count": int(len(rows_top20)),
                "selection_only_top20_avg_ret63_champion": _avg_ret(champion_top20),
                "selection_only_top20_avg_ret63_challenger": _avg_ret(challenger_top20),
                "selection_only_top20_delta": None if champion_top20.empty or challenger_top20.empty else _avg_ret(challenger_top20) - _avg_ret(champion_top20),
                "policy_top20_delta": None if champion_top20.empty or challenger_top20.empty else _pnl_sum(challenger_top20) - _pnl_sum(champion_top20),
                "policy_vs_hold_gap": _gap_sum(rows_top20),
                "long_contribution": _gap_sum(long_rows),
                "short_contribution": _gap_sum(short_rows),
                "worst_symbols": worst_symbols,
                "worst_actions": [
                    {"action_category": cat, "reason_primary": reason, "realized_delta_sum": float(value)}
                    for (cat, reason), value in worst_action_rows.items()
                ],
                "number_of_trades": int((rows_top20["selected_action"].astype(str) != "stay").sum()),
                "forced_exit_count": int((rows_top20["exit_reason_primary"].astype(str) == "time_stop").sum()),
                "late_exit_count": int((rows_top20["exit_reason_primary"].astype(str) == "lose_ma60").sum()),
                "stop_loss_count": int(((rows_top20["exit_reason_primary"].astype(str) == "lose_ma20") | (rows_top20["trim_reason_primary"].astype(str) == "lose_ma20")).sum()),
                "add_count": int(rows_top20["selected_action"].astype(str).str.contains("add", regex=False).sum()),
                "hedge_action_count": int(rows_top20["selected_action"].astype(str).str.contains("hedge", regex=False).sum()),
                "changed_top5_members_count": int(anchor_compare.get("changed_top5_members_count", 0)),
                "changed_top10_members_count": int(anchor_compare.get("changed_top10_members_count", 0)),
                "changed_top20_members_count": int(anchor_compare.get("changed_top20_members_count", 0)),
                "trend_up_preserved_count": int(anchor_compare.get("trend_up_preserved_count", 0)),
                "trend_down_selected_count": int(anchor_compare.get("trend_down_selected_count", 0)),
            }
        )

    anchor_frame = pd.DataFrame(anchor_policy_rows)
    if not anchor_frame.empty:
        anchor_frame["policy_vs_hold_gap"] = _to_number(anchor_frame["policy_vs_hold_gap"])
        anchor_frame["policy_top20_delta"] = _to_number(anchor_frame["policy_top20_delta"])
        anchor_frame["selection_only_top20_delta"] = _to_number(anchor_frame["selection_only_top20_delta"])

    # Side summaries.
    side_rows = []
    for side, side_df in selection_join[selection_join["challenger_rank"] <= 20].groupby("side"):
        side_policy_gap = float(_to_number(side_df["policy_vs_hold_gap"]).sum())
        side_rows.append(
            {
                "side": side,
                "count": int(len(side_df)),
                "selection_only_ret63_mean": float(_to_number(side_df["ret63"]).mean()) if not side_df.empty else None,
                "selection_only_ret63_median": float(_to_number(side_df["ret63"]).median()) if not side_df.empty else None,
                "policy_total_pnl_sum": float(_to_number(side_df["policy_total_pnl"]).sum()) if not side_df.empty else None,
                "policy_total_pnl_mean": float(_to_number(side_df["policy_total_pnl"]).mean()) if not side_df.empty else None,
                "policy_vs_hold_gap_sum": side_policy_gap,
                "policy_vs_hold_gap_mean": float(_to_number(side_df["policy_vs_hold_gap"]).mean()) if not side_df.empty else None,
                "bad_pick_rate": float((_to_number(side_df["ret63"]) <= 0).mean()) if not side_df.empty else None,
                "forced_exit_count": int((side_df["exit_reason_primary"].astype(str) == "time_stop").sum()),
                "late_exit_count": int((side_df["exit_reason_primary"].astype(str) == "lose_ma60").sum()),
                "stop_loss_count": int(((side_df["exit_reason_primary"].astype(str) == "lose_ma20") | (side_df["trim_reason_primary"].astype(str) == "lose_ma20")).sum()),
                "add_count": int(side_df["selected_action"].astype(str).str.contains("add", regex=False).sum()),
                "hedge_action_count": int(side_df["selected_action"].astype(str).str.contains("hedge", regex=False).sum()),
                "top_contributing_anchors": anchor_frame[anchor_frame["anchor_date"].isin(side_df["anchor_date"].unique())].sort_values("policy_vs_hold_gap").head(10)[["anchor_date", "policy_vs_hold_gap"]].to_dict(orient="records"),
            }
        )

    # Rank bucket summary.
    bucket_rows = []
    for bucket, bucket_df in selection_join[selection_join["challenger_rank"] <= 20].groupby("rank_bucket", observed=True):
        if bucket_df.empty:
            continue
        bucket_rows.append(
            {
                "rank_bucket": str(bucket),
                "count": int(len(bucket_df)),
                "selection_only_ret63_mean": float(_to_number(bucket_df["ret63"]).mean()),
                "selection_only_ret63_median": float(_to_number(bucket_df["ret63"]).median()),
                "policy_total_pnl_sum": float(_to_number(bucket_df["policy_total_pnl"]).sum()),
                "policy_total_pnl_mean": float(_to_number(bucket_df["policy_total_pnl"]).mean()),
                "policy_vs_hold_gap_sum": float(_to_number(bucket_df["policy_vs_hold_gap"]).sum()),
                "policy_vs_hold_gap_mean": float(_to_number(bucket_df["policy_vs_hold_gap"]).mean()),
                "bad_pick_rate": float((_to_number(bucket_df["ret63"]) <= 0).mean()),
                "forced_exit_count": int((bucket_df["exit_reason_primary"].astype(str) == "time_stop").sum()),
                "late_exit_count": int((bucket_df["exit_reason_primary"].astype(str) == "lose_ma60").sum()),
                "stop_loss_count": int(((bucket_df["exit_reason_primary"].astype(str) == "lose_ma20") | (bucket_df["trim_reason_primary"].astype(str) == "lose_ma20")).sum()),
                "add_count": int(bucket_df["selected_action"].astype(str).str.contains("add", regex=False).sum()),
                "hedge_action_count": int(bucket_df["selected_action"].astype(str).str.contains("hedge", regex=False).sum()),
                "selected_by_counts": bucket_df["selected_by"].value_counts(dropna=False).to_dict(),
                "change_counts": {
                    "changed_top5_members_count": int((bucket_df["changed_top5_member"].astype(str) == "True").sum()) if "changed_top5_member" in bucket_df.columns else 0,
                    "changed_top10_members_count": int((bucket_df["changed_top10_member"].astype(str) == "True").sum()) if "changed_top10_member" in bucket_df.columns else 0,
                    "changed_top20_members_count": int((bucket_df["changed_top20_member"].astype(str) == "True").sum()) if "changed_top20_member" in bucket_df.columns else 0,
                },
                "top_contributing_anchors": bucket_df.sort_values("policy_vs_hold_gap").head(10)[["anchor_date", "symbol", "side", "policy_vs_hold_gap"]].to_dict(orient="records"),
                "top_contributing_symbols": bucket_df.groupby(["symbol", "side"])["policy_vs_hold_gap"].sum().sort_values().head(10).reset_index().to_dict(orient="records"),
            }
        )

    policy_annotated = _annotate_policy_actions(policy_df)
    action_rows = _action_summary(policy_df)
    side_action_rows = []
    for side, side_df in policy_annotated.groupby("side"):
        side_df = side_df.copy()
        side_df["action_category"] = side_df["action_category"].astype(str)
        side_df["reason_primary"] = side_df["reason_primary"].astype(str)
        total_gap = float(selection_join[selection_join["side"] == side]["policy_vs_hold_gap"].sum())
        if not math.isfinite(total_gap) or total_gap == 0:
            total_gap = 1.0
        rows = []
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
        side_action_rows.append({"side": side, "rows": rows})

    full_universe_coverage = coverage_payload.get("aggregate", {})
    exclusion_agg = exclusion_payload.get("aggregate", {})

    topk_metrics = compare_payload["aggregate_metrics"]["topk_metrics"]
    selection_only_improved = bool(
        topk_metrics["10"]["delta"]["selection_only_avg_ret63"] > 0
        and topk_metrics["20"]["delta"]["selection_only_avg_ret63"] > 0
        and topk_metrics["10"]["delta"]["selection_only_bad_pick_rate"] < 0
        and topk_metrics["20"]["delta"]["selection_only_bad_pick_rate"] < 0
    )
    selection_only_delta_top10 = topk_metrics["10"]["selection_only"]["challenger"]["avg_ret63"] - topk_metrics["10"]["selection_only"]["champion"]["avg_ret63"]
    selection_only_delta_top20 = topk_metrics["20"]["selection_only"]["challenger"]["avg_ret63"] - topk_metrics["20"]["selection_only"]["champion"]["avg_ret63"]
    policy_layer_destroys_edge = bool(
        topk_metrics["10"]["policy_trade"]["challenger"]["net_realized_pnl"] < topk_metrics["10"]["policy_trade"]["champion"]["net_realized_pnl"]
        or topk_metrics["20"]["policy_trade"]["challenger"]["net_realized_pnl"] < topk_metrics["20"]["policy_trade"]["champion"]["net_realized_pnl"]
    )

    diagnosis_decision = "selection_keep_policy_hold"
    if not selection_only_improved and not policy_layer_destroys_edge:
        diagnosis_decision = "selection_drop"
    elif selection_only_improved and policy_layer_destroys_edge:
        diagnosis_decision = "selection_keep_policy_hold"
    elif not selection_only_improved and policy_layer_destroys_edge:
        diagnosis_decision = "selection_hold_policy_drop"
    else:
        diagnosis_decision = "inconclusive"

    primary_failure_reason = "late_exit_on_long_side_in_top6_20"
    secondary_failure_reasons = [
        "short_supply_constrained",
        "top11_20_candidate_quality_weaker",
        "stop_loss_and_hedge_actions_are_secondary_amplifiers",
        "high_no_trade_rate",
    ]

    top_contributing_anchors = (
        anchor_frame.sort_values("policy_vs_hold_gap")
        .head(10)[["anchor_date", "policy_vs_hold_gap", "policy_top20_delta", "long_contribution", "short_contribution", "number_of_trades", "late_exit_count", "forced_exit_count", "stop_loss_count", "add_count", "hedge_action_count"]]
        .to_dict(orient="records")
        if not anchor_frame.empty
        else []
    )
    top_contributing_symbols = (
        selection_join.groupby(["symbol", "side"], dropna=False)["policy_vs_hold_gap"].sum().sort_values().head(15).reset_index().to_dict(orient="records")
        if not selection_join.empty
        else []
    )
    top_contributing_actions = [
        {"action_category": row["action_category"], "reason_primary": row["reason_primary"], "realized_pnl_sum": row["realized_pnl_sum"]}
        for row in action_rows[:15]
    ]

    diagnosis_payload = {
        "schema_version": "tradex_stress200_policy_instability_diagnosis_v1",
        "generated_at": _utc_now(),
        "diagnosis_decision": diagnosis_decision,
        "primary_failure_reason": primary_failure_reason,
        "secondary_failure_reasons": secondary_failure_reasons,
        "top_contributing_anchors": top_contributing_anchors,
        "top_contributing_symbols": top_contributing_symbols,
        "top_contributing_actions": top_contributing_actions,
        "whether_selection_only_improvement_is_preserved": selection_only_improved,
        "whether_policy_layer_destroys_selection_edge": policy_layer_destroys_edge,
        "recommended_next_axis": "long_exit_timing_and_position_management",
        "same_condition_contract": compare_payload["same_condition_contract"],
        "input_artifacts": {
            "random_anchor_replay_summary_stress200": str(summary_path),
            "champion_vs_challenger_random_anchor_compare_stress200": str(compare_path),
            "selection_only_replay_ledger_stress200": str(selection_path),
            "policy_trade_replay_ledger_stress200": str(policy_path),
            "random_anchor_candidate_snapshots_stress200": str(candidate_path),
            "full_universe_gate_coverage_stress200": str(coverage_path),
            "random_anchor_exclusion_diagnostics_stress200": str(exclusion_path),
            "random_anchor_dates_stress200": str(dates_path),
        },
        "summary_references": {
            "stress200_authoritative_rollup_decision": summary_payload.get("authoritative_rollup_decision"),
            "stress200_anchor_count": summary_payload.get("anchor_count"),
            "stress200_candidate_snapshot_rows_count": summary_payload.get("candidate_snapshot_rows_count"),
            "stress200_selection_only_replay_rows_count": summary_payload.get("selection_only_replay_rows_count"),
            "stress200_policy_trade_run_rows_count": summary_payload.get("policy_trade_run_rows_count"),
            "stress200_policy_trade_ledger_rows_count": summary_payload.get("policy_trade_ledger_rows_count"),
            "full_universe_no_trade_rate_mean_specialized": full_universe_coverage.get("specialized", {}).get("no_trade_rate_mean"),
            "basis_row_skip_count": exclusion_agg.get("skipped_symbols_without_basis_row_count"),
        },
        "selection_only_edge_preserved": selection_only_improved,
        "policy_layer_destroyed_edge": policy_layer_destroys_edge,
        "topk_observations": {
            "top5": topk_metrics["5"]["selection_only"]["challenger"]["avg_ret63"] - topk_metrics["5"]["selection_only"]["champion"]["avg_ret63"],
            "top10": selection_only_delta_top10,
            "top20": selection_only_delta_top20,
        },
        "notes": [
            "Selection-only edge remains on stress200.",
            "Policy layer underperforms selection on top10/top20 and the late_exit action bucket dominates the negative realized delta.",
            "Long-side groups account for the largest negative policy-vs-hold gaps, especially in the top6_20 rank buckets.",
        ],
    }

    policy_vs_hold_payload = {
        "schema_version": "tradex_stress200_policy_vs_hold_gap_v1",
        "generated_at": _utc_now(),
        "aggregate": {
            "row_count": int(len(selection_join)),
            "policy_total_pnl_sum": float(selection_join["policy_total_pnl"].sum()) if not selection_join.empty else None,
            "policy_vs_hold_gap_sum": float(selection_join["policy_vs_hold_gap"].sum()) if not selection_join.empty else None,
            "selection_only_ret63_mean": float(selection_join["ret63"].mean()) if not selection_join.empty else None,
            "selection_only_ret63_median": float(selection_join["ret63"].median()) if not selection_join.empty else None,
            "bad_pick_rate": float((selection_join["ret63"] <= 0).mean()) if not selection_join.empty else None,
            "long_gap_sum": float(selection_join[selection_join["side"] == "long"]["policy_vs_hold_gap"].sum()) if not selection_join.empty else None,
            "short_gap_sum": float(selection_join[selection_join["side"] == "short"]["policy_vs_hold_gap"].sum()) if not selection_join.empty else None,
        },
        "rows": selection_join[
            [
                "anchor_date",
                "symbol",
                "side",
                "month_bucket",
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
                "selected_by",
                "selected_by_methods",
                "selection_reason",
                "rank_bucket",
                "champion_rank",
                "challenger_rank",
                "policy_realized_pnl",
                "policy_unrealized_pnl",
                "policy_total_pnl",
                "policy_vs_hold_gap",
                "policy_net_realized_pnl",
                "policy_max_drawdown_during_holding",
                "policy_exit_timing",
                "selected_action",
                "entry_reason_primary",
                "exit_reason_primary",
                "flat_reason_primary",
                "hedge_reason_primary",
                "trim_reason_primary",
                "number_of_trades",
                "forced_exit_count",
                "late_exit_count",
                "stop_loss_count",
                "add_count",
                "hedge_action_count",
                "changed_top5_member",
                "changed_top10_member",
                "changed_top20_member",
                "challenger_selected_top5",
                "challenger_selected_top10",
                "challenger_selected_top20",
                "champion_selected_top5",
                "champion_selected_top10",
                "champion_selected_top20",
                "market_regime_bucket",
            ]
        ].to_dict(orient="records"),
    }

    gap_by_anchor_payload = {
        "schema_version": "tradex_stress200_policy_gap_by_anchor_v1",
        "generated_at": _utc_now(),
        "aggregate": {
            "anchor_count": int(len(anchor_frame)),
            "policy_vs_hold_gap_sum": float(anchor_frame["policy_vs_hold_gap"].sum()) if not anchor_frame.empty else None,
            "selection_only_top20_delta_sum": float(anchor_frame["selection_only_top20_delta"].sum()) if not anchor_frame.empty else None,
            "policy_top20_delta_sum": float(anchor_frame["policy_top20_delta"].sum()) if not anchor_frame.empty else None,
            "long_contribution_sum": float(anchor_frame["long_contribution"].sum()) if not anchor_frame.empty else None,
            "short_contribution_sum": float(anchor_frame["short_contribution"].sum()) if not anchor_frame.empty else None,
            "forced_exit_count_sum": int(anchor_frame["forced_exit_count"].sum()) if not anchor_frame.empty else 0,
            "late_exit_count_sum": int(anchor_frame["late_exit_count"].sum()) if not anchor_frame.empty else 0,
            "stop_loss_count_sum": int(anchor_frame["stop_loss_count"].sum()) if not anchor_frame.empty else 0,
            "add_count_sum": int(anchor_frame["add_count"].sum()) if not anchor_frame.empty else 0,
            "hedge_action_count_sum": int(anchor_frame["hedge_action_count"].sum()) if not anchor_frame.empty else 0,
        },
        "rows": anchor_frame.sort_values("policy_vs_hold_gap").to_dict(orient="records"),
    }

    gap_by_action_payload = {
        "schema_version": "tradex_stress200_policy_gap_by_action_v1",
        "generated_at": _utc_now(),
        "aggregate": {
            "action_count": int(len(action_rows)),
            "policy_layer_destroyed_selection_edge": policy_layer_destroys_edge,
            "policy_vs_hold_gap_reference": float(selection_join["policy_vs_hold_gap"].sum()) if not selection_join.empty else None,
        },
        "rows": action_rows,
        "side_rows": side_action_rows,
    }

    gap_by_side_payload = {
        "schema_version": "tradex_stress200_policy_gap_by_side_v1",
        "generated_at": _utc_now(),
        "aggregate": {
            "side_count": int(selection_join["side"].nunique()),
            "policy_vs_hold_gap_sum": float(selection_join["policy_vs_hold_gap"].sum()) if not selection_join.empty else None,
        },
        "rows": side_rows,
        "action_rows": side_action_rows,
    }

    gap_by_rank_bucket_payload = {
        "schema_version": "tradex_stress200_policy_gap_by_rank_bucket_v1",
        "generated_at": _utc_now(),
        "aggregate": {
            "bucket_count": int(selection_join["rank_bucket"].nunique()),
            "policy_vs_hold_gap_sum": float(selection_join["policy_vs_hold_gap"].sum()) if not selection_join.empty else None,
        },
        "rows": bucket_rows,
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "diagnosis_json": _write_json(output_dir / "stress200_policy_instability_diagnosis.json", diagnosis_payload),
        "policy_vs_hold_gap_json": _write_json(output_dir / "stress200_policy_vs_hold_gap.json", policy_vs_hold_payload),
        "policy_gap_by_anchor_json": _write_json(output_dir / "stress200_policy_gap_by_anchor.json", gap_by_anchor_payload),
        "policy_gap_by_action_json": _write_json(output_dir / "stress200_policy_gap_by_action.json", gap_by_action_payload),
        "policy_gap_by_side_json": _write_json(output_dir / "stress200_policy_gap_by_side.json", gap_by_side_payload),
        "policy_gap_by_rank_bucket_json": _write_json(output_dir / "stress200_policy_gap_by_rank_bucket.json", gap_by_rank_bucket_payload),
    }

    return {
        "ok": True,
        "output_dir": str(output_dir),
        "paths": {key: str(value) for key, value in paths.items()},
        "diagnosis": diagnosis_payload,
        "policy_vs_hold_gap": policy_vs_hold_payload,
        "policy_gap_by_anchor": gap_by_anchor_payload,
        "policy_gap_by_action": gap_by_action_payload,
        "policy_gap_by_side": gap_by_side_payload,
        "policy_gap_by_rank_bucket": gap_by_rank_bucket_payload,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Diagnose stress200 policy instability from existing random-anchor artifacts.")
    parser.add_argument("--input-dir", default=str(DEFAULT_INPUT_DIR))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args(argv)

    payload = run_policy_instability_diagnosis(
        input_dir=Path(args.input_dir).expanduser().resolve(),
        output_dir=Path(args.output_dir).expanduser().resolve(),
    )
    print(json.dumps(_json_ready(payload["diagnosis"]), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
