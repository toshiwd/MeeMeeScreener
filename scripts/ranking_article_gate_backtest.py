from __future__ import annotations

import argparse
import json
import math
from datetime import date, datetime, timezone
from pathlib import Path
import shutil
import sys
from typing import Any

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.core.config import config
from scripts.monthly_box_time_window_study import (
    TIME_LABELS,
    _add_time_window_features,
    _prepare_monthly_box_frame,
)
from scripts.note_trade_repro_backtest import _resolve_default_db_paths
from scripts.ranking_state_fusion_backtest import (
    DEFAULT_BUCKETS,
    DEFAULT_ROUND_TRIP_COST,
    _apply_round_trip_cost,
    _code_concentration,
    _monthly_summary,
    _overlap_metrics,
    _safe_float,
    _summary_from_returns,
)

ARTICLE_SCRIPT_SCHEMA_VERSION = "ranking_article_gate_backtest_v1"
DEFAULT_PANEL_PATHS = (
    Path("tmp/ranking_state_fusion_backtest/daily_selection_panel.parquet"),
    Path("tmp/ranking_entry_quality_backtest/daily_selection_panel.parquet"),
    Path("tmp/ranking_backtest_quality/daily_selection_panel.parquet"),
)


def _parse_date(value: str | None) -> date | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    raise ValueError("date must be YYYY-MM-DD or YYYYMMDD")


def _fmt(value: Any, digits: int = 3) -> str:
    if value is None:
        return "-"
    if isinstance(value, float) and not math.isfinite(value):
        return "-"
    if isinstance(value, (int, np.integer)):
        return str(int(value))
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def _load_panel(panel_path: Path) -> pd.DataFrame:
    if not panel_path.exists():
        raise FileNotFoundError(f"panel parquet not found: {panel_path}")
    frame = pd.read_parquet(panel_path)
    frame = frame.copy()
    frame["as_of_iso"] = frame.get("as_of_iso")
    if "as_of_iso" not in frame.columns or frame["as_of_iso"].isna().all():
        frame["as_of_iso"] = pd.to_datetime(frame["as_of"], format="%Y%m%d", errors="coerce").dt.date.astype(str)
    frame["as_of_iso"] = frame["as_of_iso"].astype(str)
    frame["code"] = frame["code"].astype(str)
    frame["setupType"] = frame["setupType"].fillna("watch").astype(str)
    frame["displayScore"] = pd.to_numeric(frame.get("displayScore"), errors="coerce")
    frame["entryQualified"] = frame["entryQualified"] == True  # noqa: E712
    return frame


def _build_article_features(db_paths: list[Path]) -> pd.DataFrame:
    daily = _prepare_monthly_box_frame(db_paths)
    daily = _add_time_window_features(daily)
    daily = daily.copy()
    daily["as_of_iso"] = pd.to_datetime(daily["dt"]).dt.date.astype(str)
    cols = [
        "code",
        "as_of_iso",
        "box_active",
        "box_zone",
        "box_month_bucket",
        "box_month_index",
        "monthly_context",
        "weekly_context",
        "timing_label",
        "timing_gate",
        "daily_pattern_2",
        "daily_pattern_3",
    ]
    out = daily[cols].copy()
    out["article_breakout_gate"] = (
        out["box_active"]
        & out["box_zone"].isin(["upper", "breakout"])
        & out["box_month_bucket"].isin(["6-8", "9-12", "13-14"])
        & out["timing_label"].isin(["month_end_1_3", "day17_window"])
    )
    out["article_bottom_gate"] = (
        out["box_active"]
        & out["box_zone"].isin(["lower", "mid"])
        & out["box_month_bucket"].isin(["4-5", "6-8", "9-12"])
        & out["timing_label"].isin(["month_start_1_3", "day9_window", "day17_window"])
    )
    out["article_gate"] = out["article_breakout_gate"] | out["article_bottom_gate"]
    out["article_breakout_best"] = (
        out["article_breakout_gate"] & out["box_month_bucket"].eq("9-12") & out["timing_label"].eq("month_end_1_3")
    )
    out["article_watch_best"] = (
        out["article_bottom_gate"] & out["box_month_bucket"].eq("6-8") & out["timing_label"].isin(["month_start_1_3", "day9_window"])
    )
    out["article_best_gate"] = out["article_breakout_best"] | out["article_watch_best"]
    return out.drop_duplicates(["code", "as_of_iso"], keep="last").reset_index(drop=True)


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(path)


def _load_existing_bridge_snapshot() -> dict[str, Any]:
    bridge_latest_dir = Path(config.RESEARCH_BRIDGE_DIR) / "latest"
    snapshot_path = bridge_latest_dir / "research_prior_snapshot.json"
    if not snapshot_path.exists():
        return {}
    try:
        with snapshot_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _normalize_article_prior_code_map(
    selected: pd.DataFrame,
    *,
    variant: str,
    strategy_id: str,
) -> tuple[dict[str, Any], str, str]:
    if selected.empty:
        raise ValueError("selected article prior frame is empty")
    frame = selected.copy().reset_index(drop=True)
    frame["code"] = frame["code"].astype(str)
    frame["rank"] = np.arange(1, len(frame) + 1, dtype=np.int64)
    score_source = pd.to_numeric(frame.get("displayScore"), errors="coerce").fillna(0.0).astype(float)
    score_min = float(score_source.min()) if len(score_source) > 0 else 0.0
    score_max = float(score_source.max()) if len(score_source) > 0 else 0.0
    if score_max - score_min > 0.0:
        score_norm = (score_source - score_min) / (score_max - score_min)
    else:
        score_norm = pd.Series([1.0] * len(frame), index=frame.index, dtype=float)
    frame["score_norm"] = score_norm.fillna(0.0).clip(0.0, 1.0)
    n = max(len(frame), 1)
    bonus_cap = 0.01
    codes = frame["code"].tolist()
    rank_map = {str(row["code"]): int(row["rank"]) for row in frame.to_dict(orient="records")}
    fit_score_map = {str(row["code"]): float(row["score_norm"]) for row in frame.to_dict(orient="records")}
    signal_strength_map: dict[str, float] = {}
    for row in frame.to_dict(orient="records"):
        code_key = str(row["code"])
        rank_value = int(row["rank"])
        signal_strength_map[code_key] = float(max(0.0, min(1.0, 1.0 - ((rank_value - 1) / max(n - 1, 1)))))
    bonus_map = {
        str(row["code"]): float(max(0.0, min(bonus_cap, bonus_cap * (0.40 + 0.60 * float(row["score_norm"])))))
        for row in frame.to_dict(orient="records")
    }
    family_map = {str(row["code"]): "monthly_box_time_window" for row in frame.to_dict(orient="records")}
    tag_map = {str(row["code"]): variant for row in frame.to_dict(orient="records")}
    decision_reason_map = {
        str(row["code"]): [
            "月足ボックス記事の組み合わせ条件",
            f"variant={variant}",
            "小さなpriorとして反映",
        ]
        for row in frame.to_dict(orient="records")
    }
    adoption_reason_map = decision_reason_map.copy()
    risk_watch_map = {
        str(row["code"]): [
            "baselineを置換せず補助priorとしてのみ反映",
            "state gateへ昇格するまで小さく使う",
        ]
        for row in frame.to_dict(orient="records")
    }
    promotion_stage_map = {str(row["code"]): "assist" for row in frame.to_dict(orient="records")}
    provisional_map = {str(row["code"]): False for row in frame.to_dict(orient="records")}
    latest_as_of = str(frame.iloc[0].get("as_of_iso") or "").strip() or None
    up_payload = {
        "asof": latest_as_of,
        "codes": codes,
        "rank_map": rank_map,
        "fit_score_map": fit_score_map,
        "signal_strength_map": signal_strength_map,
        "pattern_tag_map": tag_map,
        "decision_reason_map": decision_reason_map,
        "adoption_reason_map": adoption_reason_map,
        "risk_watch_map": risk_watch_map,
        "promotion_stage_map": promotion_stage_map,
        "provisional_map": provisional_map,
        "hypothesis_family_map": family_map,
        "bonus_map": bonus_map,
        "bonus_cap": bonus_cap,
        "source_pattern": variant,
        "source_disposition": "article_gate_prior",
    }
    return up_payload, latest_as_of or "", strategy_id


def _publish_article_gate_prior(
    *,
    payload: dict[str, Any],
    selected: pd.DataFrame,
    variant: str,
    source_artifacts: dict[str, str],
) -> dict[str, Any]:
    bridge_latest_dir = Path(config.RESEARCH_BRIDGE_DIR) / "latest"
    bridge_latest_dir.mkdir(parents=True, exist_ok=True)
    existing = _load_existing_bridge_snapshot()
    strategy_id = str(payload.get("schema_version") or ARTICLE_SCRIPT_SCHEMA_VERSION)
    up_payload, latest_asof, _ = _normalize_article_prior_code_map(selected, variant=variant, strategy_id=strategy_id)
    if not latest_asof:
        raise ValueError("unable to resolve article prior asof")
    published_at = datetime.now(timezone.utc).isoformat()
    snapshot = {
        **existing,
        "schema_version": ARTICLE_SCRIPT_SCHEMA_VERSION,
        "strategy_id": f"ranking_article_gate_prior_{variant}",
        "source_dataset_id": "ranking_article_gate_backtest",
        "source_artifacts": {
            **(existing.get("source_artifacts") if isinstance(existing.get("source_artifacts"), dict) else {}),
            **source_artifacts,
        },
        "run_id": f"ranking_article_gate_prior_{variant}_{latest_asof}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
        "generated_at": published_at,
        "asof": latest_asof,
        "provisional": False,
        "up": up_payload,
        "summary": {
            "family_leaderboard": [
                {
                    "family": "monthly_box_time_window",
                    "variant": variant,
                    "asof": latest_asof,
                    "sample_count": int(len(selected)),
                    "top_code_count": int(len(up_payload["codes"])),
                    "bonus_cap": up_payload["bonus_cap"],
                    "note": "small long-side prior derived from monthly-box article gate",
                }
            ],
            "next_promotion_candidates": [
                {
                    "family": "monthly_box_time_window",
                    "variant": variant,
                    "codes": up_payload["codes"][:10],
                }
            ],
            "worst_failure_patterns": [],
            "action_queue": [
                "反映済みのarticle gateは小さな補助priorとしてのみ使用",
                "baseline置換ではなく上位候補の微調整に限定",
            ],
            "provisional_deterioration": [],
        },
    }
    if isinstance(existing.get("down"), dict):
        snapshot["down"] = existing["down"]
    else:
        snapshot.setdefault("down", {})
    _write_json_atomic(bridge_latest_dir / "research_prior_snapshot.json", snapshot)
    bridge_manifest_path = bridge_latest_dir / "bridge_manifest.json"
    bridge_manifest = {"generated_at": published_at, "artifacts": {}}
    if bridge_manifest_path.exists():
        try:
            with bridge_manifest_path.open("r", encoding="utf-8") as handle:
                loaded = json.load(handle)
            if isinstance(loaded, dict):
                bridge_manifest = loaded
        except Exception:
            bridge_manifest = {"generated_at": published_at, "artifacts": {}}
    artifacts_slot = bridge_manifest.get("artifacts")
    if not isinstance(artifacts_slot, dict):
        artifacts_slot = {}
    artifacts_slot["research_prior_snapshot.json"] = {
        "source_type": "ranking_article_gate_backtest",
        "source_id": snapshot["run_id"],
        "generated_at": published_at,
        "filename": "research_prior_snapshot.json",
    }
    bridge_manifest["generated_at"] = published_at
    bridge_manifest["artifacts"] = artifacts_slot
    _write_json_atomic(bridge_manifest_path, bridge_manifest)
    return {
        "research_prior_snapshot_path": str(bridge_latest_dir / "research_prior_snapshot.json"),
        "bridge_manifest_path": str(bridge_manifest_path),
        "run_id": snapshot["run_id"],
        "strategy_id": snapshot["strategy_id"],
        "asof": latest_asof,
        "selected_count": int(len(selected)),
    }


def _score_panel(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    frame["rank_score"] = frame.groupby("as_of")["rank"].transform(lambda s: 1.0 - (s - 1.0) / max(float(s.max()), 1.0))
    frame["displayScore_norm"] = frame.groupby("as_of")["displayScore"].transform(
        lambda s: (s - s.min()) / (s.max() - s.min()) if len(s.dropna()) > 1 and float(s.max() - s.min()) > 0.0 else 0.0
    ).fillna(0.0)
    frame["is_breakout"] = frame["setupType"].eq("breakout")
    frame["is_watch"] = frame["setupType"].eq("watch")
    frame["is_reject"] = frame["setupType"].eq("reject")
    return frame


def _merge_article_features(panel: pd.DataFrame, article: pd.DataFrame) -> pd.DataFrame:
    merged = panel.merge(article, how="left", on=["code", "as_of_iso"], suffixes=("", "_article"))
    for column in (
        "article_gate",
        "article_breakout_gate",
        "article_bottom_gate",
        "article_breakout_best",
        "article_watch_best",
        "article_best_gate",
        "timing_gate",
        "box_active",
    ):
        if column not in merged.columns:
            merged[column] = False
        merged[column] = pd.Series(merged[column], index=merged.index).astype("boolean").fillna(False).astype(bool)
    merged["box_zone"] = merged["box_zone"].fillna("na").astype(str)
    merged["box_month_bucket"] = merged["box_month_bucket"].fillna("na").astype(str)
    merged["timing_label"] = merged["timing_label"].fillna("other").astype(str)
    return merged


def _pick_group(group: pd.DataFrame, *, variant: str, bucket_size: int) -> pd.DataFrame:
    working = _score_panel(group)
    if variant == "baseline":
        pass
    elif variant == "article_filter":
        working = working[working["article_gate"]].copy()
    elif variant == "article_entryQualified_filter":
        working = working[working["article_gate"] & working["entryQualified"]].copy()
    elif variant == "article_best_filter":
        working = working[working["article_best_gate"]].copy()
    elif variant == "article_best_entryQualified_filter":
        working = working[working["article_best_gate"] & working["entryQualified"]].copy()
    elif variant == "article_weighted_v1":
        working["article_score"] = (
            0.018 * working["article_gate"].astype(float)
            + 0.015 * working["article_breakout_gate"].astype(float)
            + 0.012 * working["article_bottom_gate"].astype(float)
            + 0.008 * working["entryQualified"].astype(float)
            + 0.010 * working["is_breakout"].astype(float)
            + 0.005 * working["is_watch"].astype(float)
            - 0.008 * working["is_reject"].astype(float)
            + 0.006 * working["displayScore_norm"].astype(float)
        )
        working = working.sort_values(["article_score", "rank", "displayScore", "code"], ascending=[False, True, False, True], kind="stable")
    elif variant == "article_weighted_v2":
        working["article_score"] = (
            0.028 * working["article_best_gate"].astype(float)
            + 0.012 * working["article_breakout_best"].astype(float)
            + 0.012 * working["article_watch_best"].astype(float)
            + 0.010 * working["entryQualified"].astype(float)
            + 0.008 * working["is_breakout"].astype(float)
            + 0.005 * working["is_watch"].astype(float)
            - 0.010 * working["is_reject"].astype(float)
            + 0.006 * working["displayScore_norm"].astype(float)
        )
        working = working.sort_values(["article_score", "rank", "displayScore", "code"], ascending=[False, True, False, True], kind="stable")
    else:
        raise ValueError(f"unknown variant: {variant}")

    if variant not in {"article_weighted_v1", "article_weighted_v2"}:
        working = working.sort_values(["rank", "displayScore", "code"], ascending=[True, False, True], kind="stable")
    working = working.head(int(bucket_size)).copy()
    if working.empty:
        return working
    for horizon in (5, 20, 60):
        working[f"forward_return_{horizon}_net"] = _apply_round_trip_cost(working[f"forward_return_{horizon}"], round_trip_cost=DEFAULT_ROUND_TRIP_COST)
    return working


def _select_for_variant(panel: pd.DataFrame, *, variant: str, bucket_size: int) -> pd.DataFrame:
    selected: list[pd.DataFrame] = []
    for _, group in panel.groupby("as_of", sort=False):
        picked = _pick_group(group, variant=variant, bucket_size=bucket_size)
        if not picked.empty:
            selected.append(picked)
    if not selected:
        return panel.iloc[0:0].copy()
    return pd.concat(selected, ignore_index=True)


def _cohort_summary(panel: pd.DataFrame) -> dict[str, Any]:
    if panel.empty:
        return {
            "sample_count": 0,
            "daily_count": 0,
            "avg_per_day": None,
            "return_summary": {},
            "return_summary_net": {},
            "overlap": {"daily_overlap_rate": None, "daily_turnover_rate": None},
            "concentration": {"unique_codes": 0, "top_code_share": None, "top5_code_share": None},
            "monthly": {"month_count": 0, "positive_month_rate": None, "worst_month": None, "best_month": None},
        }
    summary = {
        "sample_count": int(len(panel)),
        "daily_count": int(panel["as_of"].nunique()) if "as_of" in panel.columns else 0,
        "avg_per_day": float(len(panel) / max(1, int(panel["as_of"].nunique()))) if "as_of" in panel.columns else None,
        "overlap": _overlap_metrics(panel),
        "concentration": _code_concentration(panel),
        "monthly": _monthly_summary(panel, return_col="forward_return_20_net"),
    }
    for horizon in (5, 20, 60):
        raw_col = f"forward_return_{horizon}"
        net_col = f"forward_return_{horizon}_net"
        summary[f"return_{horizon}"] = _summary_from_returns(panel[raw_col] if raw_col in panel.columns else pd.Series(dtype=float))
        summary[f"return_{horizon}_net"] = _summary_from_returns(panel[net_col] if net_col in panel.columns else pd.Series(dtype=float))
    return summary


def _build_report(payload: dict[str, Any]) -> str:
    lines = [
        "# Ranking Article Gate Backtest",
        "",
        f"- generated_at: {payload.get('generated_at')}",
        f"- period: {payload.get('period', {}).get('start_date')} .. {payload.get('period', {}).get('end_date')}",
        f"- round_trip_cost: {payload.get('round_trip_cost')}",
        "",
        "## Verdict",
        f"- usable: {payload.get('verdict')}",
        "",
        "## Variant Summary",
    ]
    for name, variant in (payload.get("variants") or {}).items():
        top10 = (variant or {}).get("top10") if isinstance(variant, dict) else {}
        lines.append(
            f"- {name}: sample={top10.get('sample_count')}, days={top10.get('daily_count')}, "
            f"net20_mean={_fmt((top10.get('return_20_net') or {}).get('mean'))}, "
            f"net20_pf={_fmt((top10.get('return_20_net') or {}).get('profit_factor'))}, "
            f"net20_mdd={_fmt((top10.get('return_20_net') or {}).get('mdd'))}, "
            f"top_code_share={_fmt((top10.get('concentration') or {}).get('top_code_share'))}"
        )
    lines.append("")
    lines.append("## Article Gate Snapshot")
    lines.append("")
    lines.append("| phase | all_mean20 | gate_mean20 | gate_pf20 | best_timing | best_mean20 |")
    lines.append("| --- | ---: | ---: | ---: | --- | ---: |")
    for row in payload.get("article_gate_snapshot", []):
        lines.append(
            "| "
            + f"{row['phase']} | {_fmt(row['all_mean20'])} | {_fmt(row['gate_mean20'])} | {_fmt(row['gate_pf20'])} | "
            + f"{row['best_timing_label']} | {_fmt(row['best_timing_mean20'])} |"
        )
    published_prior = payload.get("published_prior")
    if isinstance(published_prior, dict) and published_prior:
        lines.append("")
        lines.append("## Published Prior")
        lines.append(f"- research_prior_snapshot: `{published_prior.get('research_prior_snapshot_path')}`")
        lines.append(f"- bridge_manifest: `{published_prior.get('bridge_manifest_path')}`")
        lines.append(f"- strategy_id: `{published_prior.get('strategy_id')}`")
        lines.append(f"- asof: `{published_prior.get('asof')}`")
        lines.append(f"- selected_count: `{published_prior.get('selected_count')}`")
    return "\n".join(lines) + "\n"


def _build_article_gate_snapshot(panel: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for setup_type, group in panel.groupby("setupType", dropna=False):
        all20 = _summary_from_returns(group["forward_return_20"])
        gate20 = _summary_from_returns(group.loc[group["article_gate"], "forward_return_20"])
        timing = group.loc[group["article_gate"]].groupby("timing_label", dropna=False)["forward_return_20"].mean().dropna()
        best_label = "na"
        best_mean = None
        if not timing.empty:
            best_label = str(timing.idxmax())
            best_mean = float(timing.max())
        rows.append(
            {
                "phase": str(setup_type),
                "all_mean20": all20["mean"],
                "gate_mean20": gate20["mean"],
                "gate_pf20": gate20["profit_factor"],
                "best_timing_label": best_label,
                "best_timing_mean20": best_mean,
            }
        )
    rows.sort(key=lambda item: item["phase"])
    return rows


def run_ranking_article_gate_backtest(
    *,
    panel_path: Path,
    db_paths: list[Path],
    bucket_sizes: tuple[int, ...] = DEFAULT_BUCKETS,
    publish_prior: bool = False,
    publish_variant: str = "article_best_entryQualified_filter",
    publish_bucket_size: int = 10,
    source_artifacts: dict[str, str] | None = None,
) -> dict[str, Any]:
    panel = _load_panel(panel_path)
    article = _build_article_features(db_paths)
    merged = _merge_article_features(panel, article)

    variants = {
        "baseline": "baseline topK",
        "article_filter": "article gate filter",
        "article_entryQualified_filter": "article gate + entryQualified",
        "article_best_filter": "best article gate filter",
        "article_best_entryQualified_filter": "best article gate + entryQualified",
        "article_weighted_v1": "article weighted v1",
        "article_weighted_v2": "article weighted v2",
    }
    out: dict[str, Any] = {}
    for variant_name, label in variants.items():
        variant_payload: dict[str, Any] = {"label": label, "bucket_summaries": {}}
        for bucket in bucket_sizes:
            selected = _select_for_variant(merged, variant=variant_name, bucket_size=int(bucket))
            summary = _cohort_summary(selected)
            variant_payload["bucket_summaries"][f"top{int(bucket)}"] = summary
        variant_payload["top10"] = (
            variant_payload["bucket_summaries"].get("top10")
            or variant_payload["bucket_summaries"].get(f"top{int(bucket_sizes[-1])}")
        )
        out[variant_name] = variant_payload

    top10 = {k: v["top10"] for k, v in out.items() if isinstance(v, dict)}
    baseline_net20 = _safe_float(((top10.get("baseline") or {}).get("return_20_net") or {}).get("mean"))
    best_variant = "baseline"
    best_net20 = baseline_net20
    for name, summary in top10.items():
        net20 = _safe_float((summary.get("return_20_net") or {}).get("mean"))
        if net20 is not None and (best_net20 is None or net20 > best_net20):
            best_variant = name
            best_net20 = net20

    verdict = "not_usable_yet"
    if best_variant in {"article_weighted_v1", "article_weighted_v2"} and best_net20 is not None and baseline_net20 is not None and best_net20 >= baseline_net20:
        verdict = "watch"
    if best_variant != "baseline" and best_net20 is not None and baseline_net20 is not None and best_net20 > baseline_net20 + 0.001:
        verdict = "usable"

    payload = {
        "schema_version": ARTICLE_SCRIPT_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "period": {
            "start_date": str(pd.to_datetime(merged["as_of_iso"]).min().date()) if not merged.empty else None,
            "end_date": str(pd.to_datetime(merged["as_of_iso"]).max().date()) if not merged.empty else None,
        },
        "round_trip_cost": DEFAULT_ROUND_TRIP_COST,
        "bucket_sizes": list(bucket_sizes),
        "variants": out,
        "article_gate_snapshot": _build_article_gate_snapshot(merged),
        "best_variant": best_variant,
        "baseline_variant": "baseline",
        "verdict": verdict,
    }
    if publish_prior:
        publish_source_artifacts = source_artifacts or {}
        merged_as_of_numeric = pd.to_numeric(merged["as_of"], errors="coerce") if not merged.empty else pd.Series(dtype=float)
        merged_as_of_numeric = merged_as_of_numeric.fillna(np.nan)
        latest_as_of = int(merged_as_of_numeric.dropna().max()) if not merged_as_of_numeric.dropna().empty else None
        if latest_as_of is None:
            raise ValueError("unable to resolve latest as_of for article prior publish")
        latest_group = merged.loc[merged_as_of_numeric == float(latest_as_of)].copy()
        selected = _pick_group(latest_group, variant=publish_variant, bucket_size=int(publish_bucket_size))
        if selected.empty:
            raise ValueError(f"article prior publish produced no rows for variant={publish_variant}")
        payload["published_prior"] = _publish_article_gate_prior(
            payload=payload,
            selected=selected,
            variant=publish_variant,
            source_artifacts=publish_source_artifacts,
        )
    return payload


def _resolve_default_panel_path() -> Path:
    for candidate in DEFAULT_PANEL_PATHS:
        if candidate.exists():
            return candidate
    raise FileNotFoundError("daily_selection_panel.parquet not found in known tmp paths")


def _resolve_default_output_dir(output_dir: Path | None) -> Path:
    if output_dir is not None:
        return output_dir
    return Path("tmp") / "ranking_article_gate_backtest"


def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest article-inspired monthly box time gates against ranking selection.")
    parser.add_argument("--panel-path", type=Path, default=None)
    parser.add_argument("--db-path", action="append", dest="db_paths", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--publish-prior", action="store_true")
    parser.add_argument("--publish-prior-variant", type=str, default="article_best_entryQualified_filter")
    parser.add_argument("--publish-prior-bucket-size", type=int, default=10)
    args = parser.parse_args()

    panel_path = args.panel_path or _resolve_default_panel_path()
    db_paths = args.db_paths or _resolve_default_db_paths()
    output_dir = _resolve_default_output_dir(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    payload = run_ranking_article_gate_backtest(
        panel_path=panel_path,
        db_paths=db_paths,
        publish_prior=bool(args.publish_prior),
        publish_variant=str(args.publish_prior_variant),
        publish_bucket_size=int(args.publish_prior_bucket_size),
        source_artifacts={
            "ranking_article_gate_backtest.json": str(output_dir / "ranking_article_gate_backtest.json"),
            "ranking_article_gate_backtest.md": str(output_dir / "ranking_article_gate_backtest.md"),
        },
    )
    (output_dir / "ranking_article_gate_backtest.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (output_dir / "ranking_article_gate_backtest.md").write_text(_build_report(payload), encoding="utf-8")


if __name__ == "__main__":
    main()
