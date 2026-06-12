from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_pre_crash_short_portfolio_replay_v1 import _apply_topk_cooldown, _score


AXIS_ID = "short_observable_visual_label_shadow_rank_replay_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_observable_visual_label_shadow_rank_replay_v1")
DEFAULT_EVENTS_PATH = Path(
    r"G:\Tradex\short_visual_micro_path_replay_v1"
    r"\20260605T010450Z-short_visual_micro_path_replay_v1"
    r"\short_visual_micro_path_replay_events.jsonl"
)
DEFAULT_DOWNSIDE_STATS = Path(
    r"G:\Tradex\short_visual_micro_path_downside_stats_v1"
    r"\20260605T011408Z-short_visual_micro_path_downside_stats_v1"
    r"\short_visual_micro_path_downside_stats.json"
)
TOP_K_VALUES = (3, 5, 10)
COOLDOWN_DAYS = 5
MIN_CHANGED_TOP10_MEMBERS = 50
OVERLAY_SCALES = (1.0, 3.0, 6.0, 10.0, 30.0, 100.0)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(v) for v in value]
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


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(_json_ready(row), ensure_ascii=False) + "\n" for row in rows),
        encoding="utf-8",
    )


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        out = float(value)
    except Exception:
        return default
    return out if math.isfinite(out) else default


def _label_stat_lookup(stats: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(row["visual_micro_label"]): row for row in stats["visual_micro_label_downside_stats"]}


def _observable_label_overlay(label: str, stat: dict[str, Any] | None, baseline: dict[str, Any]) -> float:
    if not stat:
        return -0.10
    n = int(stat.get("n") or 0)
    p6_lift = _safe_float(stat.get("p_mfe_ge_6pct")) - _safe_float(baseline.get("p_mfe_ge_6pct"))
    p8_lift = _safe_float(stat.get("p_mfe_ge_8pct")) - _safe_float(baseline.get("p_mfe_ge_8pct"))
    mean_lift = _safe_float(stat.get("mean_short_ret")) - _safe_float(baseline.get("mean_short_ret"))
    stop_delta = _safe_float(stat.get("stop_hit_rate"), 1.0) - _safe_float(baseline.get("stop_hit_rate"), 1.0)
    overlay = 0.50 * p6_lift + 0.35 * p8_lift + 1.50 * mean_lift - 0.45 * max(stop_delta, 0.0)
    confidence = str(stat.get("downside_confidence") or "")
    if confidence == "medium":
        overlay += 0.025
    elif confidence == "provisional":
        overlay += 0.010
    elif confidence in {"low", "thin_sample"}:
        overlay -= 0.035
    if n < 30:
        overlay -= 0.050
    if label in {"BounceRiskHigh", "PullbackBeforeBreak"}:
        overlay -= 0.050
    return overlay


def _prepare_frame(events: list[dict[str, Any]], stats: dict[str, Any]) -> pd.DataFrame:
    label_stats = _label_stat_lookup(stats)
    baseline = stats["baseline"]
    rows: list[dict[str, Any]] = []
    for event in events:
        features = {
            key: event.get(key)
            for key in (
                "range_20_0",
                "range_40_20",
                "dist_prior_80_high",
                "red_cluster_10",
                "weak_close_cluster_10",
            )
        }
        label = str(event.get("visual_micro_label") or "")
        stat = label_stats.get(label)
        baseline_score = _score(features)
        overlay = _observable_label_overlay(label, stat, baseline)
        rows.append(
            {
                **event,
                "baseline_rank_score": baseline_score,
                "observable_visual_label_overlay": overlay,
                "label_stat_n": None if stat is None else stat.get("n"),
                "label_downside_confidence": None if stat is None else stat.get("downside_confidence"),
                "label_p_mfe_ge_6pct": None if stat is None else stat.get("p_mfe_ge_6pct"),
                "label_p_mfe_ge_8pct": None if stat is None else stat.get("p_mfe_ge_8pct"),
                "label_stop_hit_rate": None if stat is None else stat.get("stop_hit_rate"),
            }
        )
    return pd.DataFrame(rows)


def _summarize(selected: pd.DataFrame, *, ranker_id: str, top_k: int, variant_id: str) -> dict[str, Any]:
    if selected.empty:
        return {"ranker_id": ranker_id, "variant_id": variant_id, "top_k": top_k, "n": 0}
    ret = pd.to_numeric(selected["short_ret"], errors="coerce")
    mfe = pd.to_numeric(selected["mfe_20"], errors="coerce")
    by_month = selected.assign(short_ret_metric=ret).groupby("month")["short_ret_metric"].mean()
    return {
        "ranker_id": ranker_id,
        "variant_id": variant_id,
        "top_k": top_k,
        "cooldown_days": COOLDOWN_DAYS,
        "n": int(len(selected)),
        "symbols": int(selected["code"].nunique()),
        "months": int(selected["month"].nunique()),
        "mean_short_ret": float(ret.mean()),
        "median_short_ret": float(ret.median()),
        "win_rate": float((ret > 0).mean()),
        "target_hit_rate": float(selected["target_hit"].astype(bool).mean()),
        "stop_hit_rate": float(selected["stop_hit"].astype(bool).mean()),
        "denial_exit_rate": float(selected["denial_exit"].astype(bool).mean()),
        "mean_mfe_20": float(mfe.mean()),
        "p_mfe_ge_6pct": float((mfe >= 0.06).mean()),
        "p_mfe_ge_8pct": float((mfe >= 0.08).mean()),
        "p_mfe_ge_10pct": float((mfe >= 0.10).mean()),
        "positive_month_rate": float((by_month > 0).mean()),
        "mean_monthly_avg_ret": float(by_month.mean()),
        "label_counts": selected["visual_micro_label"].value_counts().to_dict(),
        "exit_reason_counts": selected["exit_reason"].value_counts().to_dict(),
    }


def _select(df: pd.DataFrame, score_col: str, top_k: int) -> pd.DataFrame:
    ranked = df.rename(columns={score_col: "rank_score"}).copy()
    return _apply_topk_cooldown(ranked, top_k, COOLDOWN_DAYS)


def _member_set(frame: pd.DataFrame) -> set[tuple[str, int]]:
    return set(zip(frame["code"].astype(str), frame["signal_ymd"].astype(int)))


def _compare(df: pd.DataFrame) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    examples: list[dict[str, Any]] = []
    baseline_selections: dict[int, pd.DataFrame] = {}
    for top_k in TOP_K_VALUES:
        baseline_sel = _select(df, "baseline_rank_score", top_k)
        baseline_selections[top_k] = baseline_sel
    for scale in OVERLAY_SCALES:
        variant_id = f"observable_label_scale_{str(scale).replace('.', 'p')}"
        variant_df = df.copy()
        variant_df["shadow_rank_score"] = variant_df["baseline_rank_score"] + variant_df["observable_visual_label_overlay"] * float(scale)
        for top_k in TOP_K_VALUES:
            baseline_sel = baseline_selections[top_k]
            shadow_sel = _select(variant_df, "shadow_rank_score", top_k)
            baseline_summary = _summarize(baseline_sel, ranker_id="baseline_score", variant_id="baseline", top_k=top_k)
            shadow_summary = _summarize(
                shadow_sel,
                ranker_id="observable_visual_label_shadow",
                variant_id=variant_id,
                top_k=top_k,
            )
            base_members = _member_set(baseline_sel)
            shadow_members = _member_set(shadow_sel)
            changed_members = len(base_members.symmetric_difference(shadow_members)) // 2
            row = {
                "variant_id": variant_id,
                "overlay_scale": scale,
                "top_k": top_k,
                "cooldown_days": COOLDOWN_DAYS,
                "changed_selected_members_count": changed_members,
                "baseline": baseline_summary,
                "shadow": shadow_summary,
                "mean_short_ret_lift": shadow_summary.get("mean_short_ret", 0.0) - baseline_summary.get("mean_short_ret", 0.0),
                "win_rate_lift": shadow_summary.get("win_rate", 0.0) - baseline_summary.get("win_rate", 0.0),
                "p_mfe_ge_6pct_lift": shadow_summary.get("p_mfe_ge_6pct", 0.0) - baseline_summary.get("p_mfe_ge_6pct", 0.0),
                "p_mfe_ge_8pct_lift": shadow_summary.get("p_mfe_ge_8pct", 0.0) - baseline_summary.get("p_mfe_ge_8pct", 0.0),
                "stop_hit_rate_delta": shadow_summary.get("stop_hit_rate", 0.0) - baseline_summary.get("stop_hit_rate", 0.0),
                "positive_month_rate_lift": shadow_summary.get("positive_month_rate", 0.0)
                - baseline_summary.get("positive_month_rate", 0.0),
            }
            min_changed = MIN_CHANGED_TOP10_MEMBERS if top_k == 10 else max(10, MIN_CHANGED_TOP10_MEMBERS // 2)
            row["local_decision"] = (
                "keep"
                if changed_members >= min_changed
                and row["mean_short_ret_lift"] > 0
                and row["p_mfe_ge_6pct_lift"] > 0
                and row["stop_hit_rate_delta"] <= 0
                else "drop_or_hold"
            )
            rows.append(row)
            added = shadow_members - base_members
            if added:
                added_rows = shadow_sel[
                    shadow_sel.apply(lambda r: (str(r["code"]), int(r["signal_ymd"])) in added, axis=1)
                ].copy()
                added_rows["top_k"] = top_k
                added_rows["variant_id"] = variant_id
                examples.extend(added_rows.sort_values("short_ret", ascending=False).head(50).to_dict(orient="records"))
    rows.sort(key=lambda r: (r["local_decision"] == "keep", r["mean_short_ret_lift"], r["p_mfe_ge_6pct_lift"]), reverse=True)
    return rows, examples


def _decision(comparisons: list[dict[str, Any]]) -> dict[str, Any]:
    keep = [row for row in comparisons if row["local_decision"] == "keep"]
    best = keep[0] if keep else (comparisons[0] if comparisons else None)
    if keep:
        return {
            "authoritative_decision": "keep_observable_visual_label_shadow_rank_for_ranking_candidate",
            "candidate_local_decision": best,
            "reason": "Observable visual label shadow rank improved return/downside probability without increasing stop hit.",
            "production_promotion_allowed": False,
        }
    if best and best["changed_selected_members_count"] > 0:
        return {
            "authoritative_decision": "hold_observable_visual_label_shadow_rank_needs_refinement",
            "candidate_local_decision": best,
            "reason": "Observable visual labels branch ranking but did not satisfy all keep gates.",
            "production_promotion_allowed": False,
        }
    return {
        "authoritative_decision": "drop_observable_visual_label_shadow_rank_no_branching",
        "reason": "Observable visual labels did not produce ranking branch.",
        "production_promotion_allowed": False,
    }


def _markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Short Observable Visual Label Shadow Rank Replay v1",
        "",
        f"- authoritative_decision: `{payload['research_decision']['authoritative_decision']}`",
        f"- source_events_path: `{payload['source_events_path']}`",
        f"- downside_stats_path: `{payload['downside_stats_path']}`",
        "",
        "| variant | top_k | changed | ret_lift | win_lift | p6_lift | p8_lift | stop_delta | month_lift | decision |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in payload["comparisons"]:
        lines.append(
            f"| {row['variant_id']} | {row['top_k']} | {row['changed_selected_members_count']} | {row['mean_short_ret_lift']:.4f} | "
            f"{row['win_rate_lift']:.4f} | {row['p_mfe_ge_6pct_lift']:.4f} | {row['p_mfe_ge_8pct_lift']:.4f} | "
            f"{row['stop_hit_rate_delta']:.4f} | {row['positive_month_rate_lift']:.4f} | {row['local_decision']} |"
        )
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- Uses only signal-day observable visual_micro_label statistics.",
            "- Does not use VisualContinuationPermit, early_bucket, or any post-signal continuation feature.",
            "- Shadow replay only. No production ranking, MeeMee, or runtime DB changed.",
        ]
    )
    return "\n".join(lines) + "\n"


def run(events_path: Path, downside_stats_path: Path, output_root: Path) -> Path:
    run_dir = output_root / _run_id()
    events = _load_jsonl(events_path)
    stats = _read_json(downside_stats_path)
    df = _prepare_frame(events, stats)
    comparisons, changed_examples = _compare(df)
    decision = _decision(comparisons)
    payload = {
        "run_id": run_dir.name,
        "created_at": _utc_now(),
        "axis_id": AXIS_ID,
        "source_events_path": str(events_path),
        "downside_stats_path": str(downside_stats_path),
        "fixed_evaluation_conditions": {
            "entry_population": "inherits short_visual_micro_path_replay_v1 event universe",
            "ranking_baseline_score": "tradex_pre_crash_short_portfolio_replay_v1._score",
            "changed_axis": "observable visual_micro_label probability overlay only",
            "forbidden_features": ["VisualContinuationPermit", "early_bucket", "post-signal continuation"],
            "top_k_values": list(TOP_K_VALUES),
            "cooldown_days": COOLDOWN_DAYS,
            "overlay_scales": list(OVERLAY_SCALES),
            "exit": "inherits realistic_downside_target replay with sl8 and bullish denial",
            "runtime_db_write": False,
        },
        "score_formula": {
            "baseline_rank_score": "existing pre-crash short portfolio diagnostic score",
            "shadow_rank_score": "baseline_rank_score + observable_visual_label_overlay",
            "observable_visual_label_overlay": "label p6/p8/mean-return lift minus stop-hit penalty, confidence and sample adjustments",
        },
        "comparisons": comparisons,
        "changed_member_examples": changed_examples[:200],
        "research_decision": decision,
        "runtime_db_write": False,
        "meemee_modified": False,
        "production_ranking_modified": False,
    }
    _write_json(run_dir / "observable_visual_label_shadow_rank_replay.json", payload)
    _write_jsonl(run_dir / "observable_visual_label_shadow_rank_changed_examples.jsonl", changed_examples[:500])
    (run_dir / "observable_visual_label_shadow_rank_replay_summary.md").write_text(_markdown(payload), encoding="utf-8")
    _write_json(
        run_dir / "_ARTIFACT_COMPLETE.json",
        {
            "status": "complete",
            "axis_id": AXIS_ID,
            "generated_at": _utc_now(),
            "required_files": [
                "observable_visual_label_shadow_rank_replay.json",
                "observable_visual_label_shadow_rank_changed_examples.jsonl",
                "observable_visual_label_shadow_rank_replay_summary.md",
                "_ARTIFACT_COMPLETE.json",
            ],
        },
    )
    return run_dir


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events-path", type=Path, default=DEFAULT_EVENTS_PATH)
    parser.add_argument("--downside-stats-path", type=Path, default=DEFAULT_DOWNSIDE_STATS)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(args.events_path, args.downside_stats_path, args.output_root))


if __name__ == "__main__":
    main()
