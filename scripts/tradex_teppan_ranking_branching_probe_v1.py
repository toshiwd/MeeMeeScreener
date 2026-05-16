from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import duckdb
import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.backend.services import tradex_research_contracts as contracts
from scripts import tradex_teppan_chart_pattern_discovery_v1 as discovery
from scripts import tradex_teppan_loss_guard_v1 as loss_guard


AXIS_ID = "teppan_ranking_branching_probe_v1"
SCHEMA_PREFIX = "tradex_teppan_ranking_branching_probe_v1"
DEFAULT_SOURCE_ROWS_PARQUET = Path(
    r"G:\Tradex\candidate_generation_pre_filter_context_shape_v1\20260429T145332Z-7bd554ac\candidate_prefilter_rows.parquet"
)
DEFAULT_PATTERN_ROOT = Path(r"G:\Tradex\teppan_chart_pattern_discovery_v1")
DEFAULT_PATTERN_RUN_ID = "20260514T000000Z-current-runtime-teppan-discovery-v1-teppan_chart_pattern_discovery_v1"
DEFAULT_GUARD_ROOT = Path(r"G:\Tradex\teppan_loss_guard_v1")
DEFAULT_GUARD_RUN_ID = "20260514T000000Z-current-runtime-teppan-loss-guard-v1-teppan_loss_guard_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\teppan_ranking_branching_probe_v1")

BOOST_VALUE = 0.04
PROMOTION_POOL_MIN_RANK = 6
PROMOTION_POOL_MAX_RANK = 20
SEVERE_LOSS_THRESHOLD = -0.10
TOP_K_VALUES = (5, 10, 20)

REQUIRED_ARTIFACTS = (
    "evaluation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "ranking_coverage_audit.json",
    "branching_probe.json",
    "compare.json",
    "selected_event_ledger.jsonl",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
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
    except (TypeError, ValueError):
        pass
    return value


def _json_text(payload: Any) -> str:
    return json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, default=str)


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_json_text(payload) + "\n", encoding="utf-8")
    return path


def _write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(_json_text(row) + "\n" for row in rows), encoding="utf-8")
    return path


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _stable_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(_json_text(payload).encode("utf-8")).hexdigest()


def _safe_path(value: str | Path | None, default: Path) -> Path:
    if value is None or not str(value).strip():
        return default.resolve()
    return Path(str(value)).expanduser().resolve()


def _run_dir(root: str | Path, run_id: str, default_root: Path) -> Path:
    path = _safe_path(root, default_root) / str(run_id).strip()
    if not path.exists():
        raise FileNotFoundError(f"run directory not found: {path}")
    return path


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _safe_mean(series: pd.Series) -> float | None:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return None
    return float(numeric.mean())


def _ymd_from_date_text(value: Any) -> int:
    text = str(value or "").strip()
    if not text:
        return 0
    return int(pd.to_datetime(text).strftime("%Y%m%d"))


def _date_text_from_ymd(value: int) -> str:
    return pd.to_datetime(str(int(value)), format="%Y%m%d").strftime("%Y-%m-%d")


def _load_source_rows(path: str | Path) -> pd.DataFrame:
    source_path = _safe_path(path, DEFAULT_SOURCE_ROWS_PARQUET)
    if not source_path.exists():
        raise FileNotFoundError(f"source rows parquet not found: {source_path}")
    frame = pd.read_parquet(source_path).copy()
    required = {"anchor_date", "side", "symbol", "champion_rank", "champion_score", "forward_ret_20d"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"source rows missing required columns: {missing}")
    frame = frame.reset_index(drop=True).copy()
    frame["source_row_id"] = frame.index.astype(int)
    frame["anchor_date"] = frame["anchor_date"].astype(str)
    frame["anchor_ymd"] = frame["anchor_date"].map(_ymd_from_date_text).astype(int)
    frame["side"] = frame["side"].astype(str)
    frame["symbol"] = frame["symbol"].astype(str)
    frame["champion_rank"] = pd.to_numeric(frame["champion_rank"], errors="coerce").astype("Int64")
    frame["champion_score"] = pd.to_numeric(frame["champion_score"], errors="coerce")
    frame["forward_ret_20d"] = pd.to_numeric(frame["forward_ret_20d"], errors="coerce")
    if "month_bucket" not in frame.columns:
        frame["month_bucket"] = frame["anchor_date"].str[:7]
    else:
        frame["month_bucket"] = frame["month_bucket"].fillna(frame["anchor_date"].str[:7]).astype(str)
    return frame


def _load_runtime_ranking_rows(
    *,
    source_db: str | Path,
    start_ymd: int,
    end_ymd: int,
    direction: str = "up",
    rank_limit: int = 20,
) -> pd.DataFrame:
    source_path = discovery._resolve_source_db(source_db)
    normalized_direction = str(direction or "up").strip().lower()
    if normalized_direction not in {"up", "down"}:
        raise ValueError("direction must be 'up' or 'down'")
    side = "long" if normalized_direction == "up" else "short"
    signal_side = "buy" if normalized_direction == "up" else "sell"
    conn = duckdb.connect(str(source_path), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT
                r.dt,
                r.dir,
                r.rank,
                r.code,
                r.display_score,
                s.forward_return_20
            FROM ranking_appearance_daily AS r
            JOIN signal_decision_daily AS s
              ON s.dt = r.dt
             AND s.code = r.code
             AND s.side = ?
            WHERE r.dir = ?
              AND r.dt BETWEEN ? AND ?
              AND r.rank <= ?
              AND r.display_score IS NOT NULL
              AND s.forward_return_20 IS NOT NULL
            ORDER BY r.dt, r.dir, r.rank, r.code
            """,
            [signal_side, normalized_direction, int(start_ymd), int(end_ymd), int(rank_limit)],
        ).fetchall()
    finally:
        conn.close()
    frame = pd.DataFrame(rows, columns=["anchor_ymd", "direction", "champion_rank", "symbol", "champion_score", "forward_ret_20d"])
    if frame.empty:
        raise ValueError("runtime ranking source returned no rows")
    frame["anchor_ymd"] = pd.to_numeric(frame["anchor_ymd"], errors="coerce").astype(int)
    frame["anchor_date"] = frame["anchor_ymd"].map(_date_text_from_ymd)
    frame["side"] = side
    frame["symbol"] = frame["symbol"].astype(str)
    frame["champion_rank"] = pd.to_numeric(frame["champion_rank"], errors="coerce").astype("Int64")
    frame["champion_score"] = pd.to_numeric(frame["champion_score"], errors="coerce")
    frame["forward_ret_20d"] = pd.to_numeric(frame["forward_ret_20d"], errors="coerce")
    if normalized_direction == "down":
        frame["forward_ret_20d"] = -frame["forward_ret_20d"]
    frame["month_bucket"] = frame["anchor_date"].str[:7]
    frame["source_row_id"] = range(len(frame))
    return frame


def _load_pattern_lookup(pattern_dir: Path) -> dict[tuple[str, str], dict[str, Any]]:
    path = pattern_dir / "teppan_candidates.json"
    if not path.exists():
        raise FileNotFoundError(f"teppan candidates artifact missing: {path}")
    payload = _load_json(path)
    lookup: dict[tuple[str, str], dict[str, Any]] = {}
    for row in payload.get("candidates") or []:
        if not isinstance(row, dict):
            continue
        family = str(row.get("pattern_family") or "")
        key = str(row.get("pattern_key") or "")
        if family and key:
            lookup[(family, key)] = dict(row)
    if not lookup:
        raise ValueError(f"no teppan-like candidate patterns found in: {path}")
    return lookup


def _pattern_key(frame: pd.DataFrame, columns: tuple[str, ...]) -> pd.Series:
    out = pd.Series("", index=frame.index, dtype=object)
    parts = []
    for column in columns:
        values = frame[column].astype(str) if column in frame.columns else pd.Series("", index=frame.index)
        parts.append(column + "=" + values)
    if not parts:
        return out
    out = parts[0]
    for part in parts[1:]:
        out = out + "|" + part
    return out


def _load_daily_monthly_for_source(source_db: Path, source_rows: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    min_ymd = int(source_rows["anchor_ymd"].min())
    max_ymd = int(source_rows["anchor_ymd"].max())
    min_ts = pd.to_datetime(str(min_ymd), format="%Y%m%d") - pd.DateOffset(days=520)
    data_start = int(min_ts.strftime("%Y%m%d"))
    conn = duckdb.connect(str(source_db), read_only=True)
    try:
        daily = discovery._load_daily_rows(conn, start_ymd=data_start, end_ymd=max_ymd)
        monthly = discovery._load_monthly_rows(conn, start_ymd=data_start, end_ymd=max_ymd)
    finally:
        conn.close()
    return daily, monthly


def build_teppan_tags_for_source(
    *,
    source_rows: pd.DataFrame,
    source_db: str | Path,
    pattern_dir: str | Path,
) -> pd.DataFrame:
    source_path = discovery._resolve_source_db(source_db)
    pattern_lookup = _load_pattern_lookup(Path(pattern_dir))
    daily, monthly = _load_daily_monthly_for_source(source_path, source_rows)
    anchors = discovery.build_anchor_features(daily, monthly, anchor_start_ymd=int(source_rows["anchor_ymd"].min()))
    wanted_pairs = set(zip(source_rows["symbol"].astype(str), source_rows["anchor_ymd"].astype(int)))
    anchors = anchors[anchors.apply(lambda row: (str(row["code"]), int(row["ymd"])) in wanted_pairs, axis=1)].copy()
    if anchors.empty:
        return pd.DataFrame(
            columns=[
                "symbol",
                "anchor_ymd",
                "teppan_pattern_match",
                "teppan_guard_pass",
                "teppan_branch_signal",
                "best_pattern_family",
                "best_pattern_key",
                "best_pattern_decision",
                "best_teppan_score",
                "matched_pattern_count",
                "guard_block_reason",
            ]
        )

    guard_risk = loss_guard._composite_downside_risk(anchors).fillna(False).astype(bool)
    anchors["teppan_guard_pass"] = ~guard_risk
    anchors["guard_block_reason"] = guard_risk.map(lambda value: "composite_downside_risk" if value else "")
    match_rows: list[dict[str, Any]] = []
    for family_id, columns in discovery.PATTERN_FAMILIES:
        keys = _pattern_key(anchors, columns)
        for idx, key in keys.items():
            meta = pattern_lookup.get((family_id, str(key)))
            if meta is None:
                continue
            row = anchors.loc[idx]
            match_rows.append(
                {
                    "symbol": str(row["code"]),
                    "anchor_ymd": int(row["ymd"]),
                    "pattern_family": family_id,
                    "pattern_key": str(key),
                    "pattern_decision": str(meta.get("pattern_decision") or ""),
                    "teppan_score": _safe_float(meta.get("teppan_score")) or 0.0,
                    "teppan_guard_pass": bool(row["teppan_guard_pass"]),
                    "guard_block_reason": str(row["guard_block_reason"]),
                }
            )
    if not match_rows:
        return pd.DataFrame(
            [
                {
                    "symbol": str(symbol),
                    "anchor_ymd": int(ymd),
                    "teppan_pattern_match": False,
                    "teppan_guard_pass": False,
                    "teppan_branch_signal": False,
                    "best_pattern_family": "",
                    "best_pattern_key": "",
                    "best_pattern_decision": "",
                    "best_teppan_score": None,
                    "matched_pattern_count": 0,
                    "guard_block_reason": "no_teppan_pattern_match",
                }
                for symbol, ymd in sorted(wanted_pairs)
            ]
        )
    matches = pd.DataFrame(match_rows)
    grouped = []
    for (symbol, ymd), group in matches.groupby(["symbol", "anchor_ymd"], sort=False):
        best = group.sort_values(["teppan_score", "pattern_family", "pattern_key"], ascending=[False, True, True], kind="stable").iloc[0]
        guard_pass = bool(group["teppan_guard_pass"].any())
        grouped.append(
            {
                "symbol": str(symbol),
                "anchor_ymd": int(ymd),
                "teppan_pattern_match": True,
                "teppan_guard_pass": guard_pass,
                "teppan_branch_signal": guard_pass,
                "best_pattern_family": str(best["pattern_family"]),
                "best_pattern_key": str(best["pattern_key"]),
                "best_pattern_decision": str(best["pattern_decision"]),
                "best_teppan_score": float(best["teppan_score"]),
                "matched_pattern_count": int(len(group)),
                "guard_block_reason": "" if guard_pass else str(best["guard_block_reason"] or "composite_downside_risk"),
            }
        )
    found = {(row["symbol"], row["anchor_ymd"]) for row in grouped}
    for symbol, ymd in sorted(wanted_pairs - found):
        grouped.append(
            {
                "symbol": str(symbol),
                "anchor_ymd": int(ymd),
                "teppan_pattern_match": False,
                "teppan_guard_pass": False,
                "teppan_branch_signal": False,
                "best_pattern_family": "",
                "best_pattern_key": "",
                "best_pattern_decision": "",
                "best_teppan_score": None,
                "matched_pattern_count": 0,
                "guard_block_reason": "no_teppan_pattern_match",
            }
        )
    return pd.DataFrame(grouped)


def _rank_with_teppan_boost(source_rows: pd.DataFrame, tags: pd.DataFrame, *, boost_value: float = BOOST_VALUE) -> pd.DataFrame:
    tagged = source_rows.merge(tags, on=["symbol", "anchor_ymd"], how="left")
    defaults = {
        "teppan_pattern_match": False,
        "teppan_guard_pass": False,
        "teppan_branch_signal": False,
        "best_pattern_family": "",
        "best_pattern_key": "",
        "best_pattern_decision": "",
        "best_teppan_score": None,
        "matched_pattern_count": 0,
        "guard_block_reason": "missing_teppan_tag",
    }
    for column, default in defaults.items():
        if column not in tagged.columns:
            tagged[column] = default
        elif default is None:
            tagged[column] = tagged[column].where(tagged[column].notna(), None)
        else:
            tagged[column] = tagged[column].fillna(default)
    rank = pd.to_numeric(tagged["champion_rank"], errors="coerce")
    tagged["teppan_boost_eligible"] = (
        tagged["side"].eq("long")
        & tagged["teppan_branch_signal"].astype(bool)
        & rank.ge(PROMOTION_POOL_MIN_RANK)
        & rank.le(PROMOTION_POOL_MAX_RANK)
    )
    tagged["challenger_score"] = pd.to_numeric(tagged["champion_score"], errors="coerce")
    tagged.loc[tagged["teppan_boost_eligible"], "challenger_score"] = (
        tagged.loc[tagged["teppan_boost_eligible"], "challenger_score"] + float(boost_value)
    )
    ranked_parts: list[pd.DataFrame] = []
    for _, group in tagged.groupby(["anchor_date", "side"], sort=True):
        ordered = group.sort_values(["challenger_score", "champion_rank", "symbol"], ascending=[False, True, True], kind="stable").copy()
        ordered["challenger_rank"] = range(1, len(ordered) + 1)
        ranked_parts.append(ordered)
    ranked = pd.concat(ranked_parts, ignore_index=True) if ranked_parts else tagged.assign(challenger_rank=pd.Series(dtype=int))
    for top_k in TOP_K_VALUES:
        ranked[f"champion_selected_top{top_k}"] = pd.to_numeric(ranked["champion_rank"], errors="coerce").le(top_k)
        ranked[f"challenger_selected_top{top_k}"] = pd.to_numeric(ranked["challenger_rank"], errors="coerce").le(top_k)
        ranked[f"changed_top{top_k}_member"] = ranked[f"champion_selected_top{top_k}"] != ranked[f"challenger_selected_top{top_k}"]
    ranked["rank_changed"] = pd.to_numeric(ranked["champion_rank"], errors="coerce") != pd.to_numeric(ranked["challenger_rank"], errors="coerce")
    return ranked


def _topk_metrics(frame: pd.DataFrame, prefix: str, top_k: int) -> dict[str, Any]:
    selected = frame[frame[f"{prefix}_selected_top{top_k}"].fillna(False).astype(bool)]
    ret = pd.to_numeric(selected["forward_ret_20d"], errors="coerce")
    return {
        "row_count": int(len(selected)),
        "avg_ret20": _safe_mean(ret),
        "median_ret20": _safe_float(ret.median()) if not ret.dropna().empty else None,
        "win_rate20": _safe_mean(ret.gt(0.0).astype(float)) if not selected.empty else None,
        "severe_loss_rate20": _safe_mean(ret.le(SEVERE_LOSS_THRESHOLD).astype(float)) if not selected.empty else None,
    }


def _metric_delta(candidate: Any, baseline: Any) -> float | None:
    c = _safe_float(candidate)
    b = _safe_float(baseline)
    if c is None or b is None:
        return None
    return float(c - b)


def _compare_payload(ranked: pd.DataFrame, same_condition: dict[str, Any]) -> dict[str, Any]:
    by_topk: dict[str, Any] = {}
    for top_k in TOP_K_VALUES:
        champion = _topk_metrics(ranked, "champion", top_k)
        challenger = _topk_metrics(ranked, "challenger", top_k)
        added = ranked[ranked[f"challenger_selected_top{top_k}"] & ~ranked[f"champion_selected_top{top_k}"]]
        removed = ranked[ranked[f"champion_selected_top{top_k}"] & ~ranked[f"challenger_selected_top{top_k}"]]
        by_topk[f"top{top_k}"] = {
            "champion": champion,
            "challenger": challenger,
            "delta": {
                "avg_ret20": _metric_delta(challenger["avg_ret20"], champion["avg_ret20"]),
                "median_ret20": _metric_delta(challenger["median_ret20"], champion["median_ret20"]),
                "win_rate20": _metric_delta(challenger["win_rate20"], champion["win_rate20"]),
                "severe_loss_rate20": _metric_delta(challenger["severe_loss_rate20"], champion["severe_loss_rate20"]),
            },
            "changed_members_count": int(ranked[f"changed_top{top_k}_member"].sum()),
            "added_count": int(len(added)),
            "removed_count": int(len(removed)),
            "added_avg_ret20": _safe_mean(added["forward_ret_20d"]) if not added.empty else None,
            "removed_avg_ret20": _safe_mean(removed["forward_ret_20d"]) if not removed.empty else None,
        }
    candidate_result = {
        "candidate_id": AXIS_ID,
        "candidate_local_decision": "hold",
        "decision": "hold",
        "decision_reasons": [{"code": "decision_computed_in_research_decision_json", "status": "info"}],
        "feature_family": "boundary_feature",
        "artifact_detail_level": contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        "fallback_status": contracts.TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
        "victory_metrics": by_topk,
        "long_horizon_regime_score": 0.0,
        "recent_adaptation_score": 0.0,
    }
    return {
        "schema_version": f"{SCHEMA_PREFIX}_compare_v1",
        "diagnostics_schema_version": f"{SCHEMA_PREFIX}_diagnostics_v1",
        "family_id": AXIS_ID,
        "generated_at": _utc_now(),
        "baseline_run_id": "champion_rank",
        "candidate_results": [candidate_result],
        "same_condition_contract": same_condition,
        "ranking_compare": by_topk,
        "candidate_scoring_created": True,
        "production_ranking_changed": False,
        "meemee_reflectable": False,
        "silent_fallback_used": False,
    }


def _ranking_coverage(source_rows: pd.DataFrame) -> dict[str, Any]:
    per_set = source_rows.groupby(["anchor_date", "side"], sort=False).agg(
        row_count=("symbol", "count"),
        max_rank=("champion_rank", "max"),
        min_rank=("champion_rank", "min"),
        score_count=("champion_score", lambda s: int(pd.to_numeric(s, errors="coerce").notna().sum())),
    )
    decision_set_count = int(len(per_set))
    complete_top5 = int(((per_set["min_rank"] <= 1) & (per_set["max_rank"] >= 5) & (per_set["row_count"] >= 5)).sum())
    complete_top10 = int(((per_set["min_rank"] <= 1) & (per_set["max_rank"] >= 10) & (per_set["row_count"] >= 10)).sum())
    complete_top20 = int(((per_set["min_rank"] <= 1) & (per_set["max_rank"] >= 20) & (per_set["row_count"] >= 20)).sum())
    score_rows = int(pd.to_numeric(source_rows["champion_score"], errors="coerce").notna().sum())
    return {
        "schema_version": f"{SCHEMA_PREFIX}_ranking_coverage_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "decision_set_count": decision_set_count,
        "source_row_count": int(len(source_rows)),
        "ranking_score_covered_count": score_rows,
        "ranking_score_coverage_rate": 0.0 if len(source_rows) == 0 else score_rows / len(source_rows),
        "complete_top5_decision_set_count": complete_top5,
        "complete_top10_decision_set_count": complete_top10,
        "complete_top20_decision_set_count": complete_top20,
        "complete_top5_decision_set_rate": 0.0 if decision_set_count == 0 else complete_top5 / decision_set_count,
        "complete_top10_decision_set_rate": 0.0 if decision_set_count == 0 else complete_top10 / decision_set_count,
        "complete_top20_decision_set_rate": 0.0 if decision_set_count == 0 else complete_top20 / decision_set_count,
        "complete_champion_ranking_available": bool(decision_set_count > 0 and complete_top20 == decision_set_count),
        "sparse_ranking_used_as_champion_proof": False,
    }


def _branching_probe(ranked: pd.DataFrame) -> dict[str, Any]:
    changed_top5 = int(ranked["changed_top5_member"].sum())
    changed_top10 = int(ranked["changed_top10_member"].sum())
    changed_top20 = int(ranked["changed_top20_member"].sum())
    changed_rank = int(ranked["rank_changed"].sum())
    if changed_top5:
        reason = "top5_member_swap"
    elif changed_top10:
        reason = "top10_member_swap"
    elif changed_rank:
        reason = "rank_reorder_inside_pool"
    else:
        reason = "no_divergence"
    grouped = ranked.groupby(["anchor_date", "side"], sort=False)
    changed_sets = int(grouped["rank_changed"].any().sum()) if len(ranked) else 0
    signal_rows = ranked[ranked["teppan_boost_eligible"].fillna(False).astype(bool)]
    return {
        "schema_version": f"{SCHEMA_PREFIX}_branching_probe_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boost_value": BOOST_VALUE,
        "promotion_pool_rank_min": PROMOTION_POOL_MIN_RANK,
        "promotion_pool_rank_max": PROMOTION_POOL_MAX_RANK,
        "teppan_pattern_match_count": int(ranked["teppan_pattern_match"].fillna(False).astype(bool).sum()),
        "teppan_guard_pass_count": int(ranked["teppan_guard_pass"].fillna(False).astype(bool).sum()),
        "teppan_boost_eligible_count": int(len(signal_rows)),
        "decision_set_count": int(grouped.ngroups) if len(ranked) else 0,
        "changed_decision_set_count": changed_sets,
        "changed_top5_members_count": changed_top5,
        "changed_top10_members_count": changed_top10,
        "changed_top20_members_count": changed_top20,
        "changed_rank_count": changed_rank,
        "selection_divergence_reason": reason,
        "future_labels_used_in_selection": False,
        "silent_fallback_used": False,
    }


def _time_block_stability(ranked: pd.DataFrame) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for year, group in ranked.groupby(ranked["anchor_date"].astype(str).str[:4], sort=True):
        champ = _topk_metrics(group, "champion", 10)
        chall = _topk_metrics(group, "challenger", 10)
        rows.append(
            {
                "year": str(year),
                "top10_avg_ret20_delta": _metric_delta(chall["avg_ret20"], champ["avg_ret20"]),
                "top10_severe_loss_rate20_delta": _metric_delta(chall["severe_loss_rate20"], champ["severe_loss_rate20"]),
                "changed_top10_members_count": int(group["changed_top10_member"].sum()),
            }
        )
    deltas = [row["top10_avg_ret20_delta"] for row in rows if row["top10_avg_ret20_delta"] is not None and row["changed_top10_members_count"] > 0]
    positive = sum(1 for value in deltas if value >= 0.0)
    rate = None if not deltas else positive / len(deltas)
    return {
        "schema_version": f"{SCHEMA_PREFIX}_time_block_stability_v1",
        "rows": rows,
        "changed_time_block_count": len(deltas),
        "positive_changed_time_block_rate": rate,
        "stable_enough": bool(rate is not None and rate >= 0.55),
    }


def _research_decision(compare: dict[str, Any], branching: dict[str, Any], coverage: dict[str, Any], stability: dict[str, Any], output_dir: Path) -> dict[str, Any]:
    top5 = compare["ranking_compare"]["top5"]
    top10 = compare["ranking_compare"]["top10"]
    top5_ret_delta = top5["delta"]["avg_ret20"]
    top10_ret_delta = top10["delta"]["avg_ret20"]
    top10_severe_delta = top10["delta"]["severe_loss_rate20"]
    changed = int(branching["changed_top5_members_count"]) + int(branching["changed_top10_members_count"])
    complete = bool(coverage["complete_champion_ranking_available"])
    reasons: list[dict[str, Any]] = [
        {"code": "branching_happened", "status": "pass" if changed > 0 else "fail", "value": changed},
        {"code": "top5_ret20_nonnegative", "status": "pass" if top5_ret_delta is not None and top5_ret_delta >= 0.0 else "fail", "value": top5_ret_delta},
        {"code": "top10_ret20_nonnegative", "status": "pass" if top10_ret_delta is not None and top10_ret_delta >= 0.0 else "fail", "value": top10_ret_delta},
        {"code": "top10_severe_loss_not_worse", "status": "pass" if top10_severe_delta is not None and top10_severe_delta <= 0.0 else "fail", "value": top10_severe_delta},
        {"code": "time_block_stability", "status": "pass" if stability["stable_enough"] else "hold_blocker", "value": stability},
        {"code": "complete_champion_ranking_available", "status": "pass" if complete else "hold_blocker", "value": complete},
    ]
    if changed <= 0:
        decision = "hold"
        authoritative = "teppan_ranking_branching_hold_no_branching"
        typed_reason = "no_material_rank_branching"
    elif top5_ret_delta is not None and top10_ret_delta is not None and top10_severe_delta is not None and top5_ret_delta >= 0.0 and top10_ret_delta >= 0.0 and top10_severe_delta <= 0.0 and stability["stable_enough"] and complete:
        decision = "keep"
        authoritative = "teppan_ranking_branching_keep_candidate"
        typed_reason = "same_condition_branching_helped_and_coverage_complete"
    elif top5_ret_delta is not None and top10_ret_delta is not None and top10_severe_delta is not None and (top5_ret_delta < 0.0 or top10_ret_delta < 0.0 or top10_severe_delta > 0.0):
        decision = "drop"
        authoritative = "teppan_ranking_branching_drop_quality_worse"
        typed_reason = "same_condition_branching_hurt_quality"
    else:
        decision = "hold"
        authoritative = "teppan_ranking_branching_hold_incomplete_gate"
        typed_reason = "branching_exists_but_gate_incomplete"
    return {
        "schema_version": f"{SCHEMA_PREFIX}_research_decision_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "artifact_root": str(output_dir),
        "decision": decision,
        "authoritative_research_decision": authoritative,
        "candidate_local_decision": decision,
        "session_aggregate_decision": decision,
        "typed_reason": typed_reason,
        "decision_reasons": reasons,
        "changed_top5_members_count": branching["changed_top5_members_count"],
        "changed_top10_members_count": branching["changed_top10_members_count"],
        "changed_rank_count": branching["changed_rank_count"],
        "selection_divergence_reason": branching["selection_divergence_reason"],
        "candidate_scoring_created": True,
        "production_ranking_changed": False,
        "meemee_reflectable": False,
        "publish_bundle_created": False,
        "silent_fallback_used": False,
    }


def _artifact_complete(output_dir: Path, paths: dict[str, str], decision: dict[str, Any]) -> dict[str, Any]:
    existing = {name: (output_dir / name).exists() for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"}
    return {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "artifact_root": str(output_dir),
        "required_artifacts": list(REQUIRED_ARTIFACTS),
        "existing_artifacts": existing,
        "complete": all(existing.values()),
        "authoritative_research_decision": decision["authoritative_research_decision"],
        "candidate_scoring_created": True,
        "production_ranking_changed": False,
        "meemee_reflectable": False,
        "publish_bundle_created": False,
        "silent_fallback_used": False,
        "paths": paths,
    }


def run_teppan_ranking_branching_probe_v1(
    *,
    source_rows_parquet: str | Path = DEFAULT_SOURCE_ROWS_PARQUET,
    source_mode: str = "parquet",
    source_db: str | Path | None = None,
    start_ymd: int | None = None,
    end_ymd: int | None = None,
    direction: str = "up",
    rank_limit: int = 20,
    pattern_root: str | Path = DEFAULT_PATTERN_ROOT,
    pattern_run_id: str = DEFAULT_PATTERN_RUN_ID,
    guard_root: str | Path = DEFAULT_GUARD_ROOT,
    guard_run_id: str = DEFAULT_GUARD_RUN_ID,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
    precomputed_tags: pd.DataFrame | None = None,
) -> dict[str, Any]:
    source_db_path = discovery._resolve_source_db(source_db)
    normalized_source_mode = str(source_mode or "parquet").strip().lower()
    if normalized_source_mode == "runtime-ranking":
        if start_ymd is None or end_ymd is None:
            raise ValueError("start_ymd and end_ymd are required for runtime-ranking source mode")
        source_rows = _load_runtime_ranking_rows(
            source_db=source_db_path,
            start_ymd=int(start_ymd),
            end_ymd=int(end_ymd),
            direction=direction,
            rank_limit=int(rank_limit),
        )
    elif normalized_source_mode == "parquet":
        source_rows = _load_source_rows(source_rows_parquet)
    else:
        raise ValueError("source_mode must be 'parquet' or 'runtime-ranking'")
    pattern_dir = _run_dir(pattern_root, pattern_run_id, DEFAULT_PATTERN_ROOT)
    guard_dir = _run_dir(guard_root, guard_run_id, DEFAULT_GUARD_ROOT)
    output_base = _safe_path(output_root, DEFAULT_OUTPUT_ROOT)
    run_name = run_id.strip() if run_id else _default_run_id()
    if not run_name.endswith(AXIS_ID):
        run_name = f"{run_name}-{AXIS_ID}"
    output_dir = output_base / run_name
    output_dir.mkdir(parents=True, exist_ok=True)

    pattern_decision = _load_json(pattern_dir / "research_decision.json")
    guard_decision = _load_json(guard_dir / "research_decision.json")
    tags = precomputed_tags.copy() if precomputed_tags is not None else build_teppan_tags_for_source(
        source_rows=source_rows,
        source_db=source_db_path,
        pattern_dir=pattern_dir,
    )
    ranked = _rank_with_teppan_boost(source_rows, tags)
    coverage = _ranking_coverage(source_rows)
    same_condition = contracts.build_same_condition_contract(
        universe=sorted(source_rows["symbol"].astype(str).unique().tolist()),
        period_segments=[
            {
                "label": "historical_champion_ranked_candidate_rows",
                "start_date": str(source_rows["anchor_ymd"].min()),
                "end_date": str(source_rows["anchor_ymd"].max()),
            }
        ],
        top_k=10,
        regime="all",
        cost_model=contracts.TRADEX_DEFAULT_COST_MODEL,
        artifact_detail_level=contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        fallback_status=contracts.TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
        feature_family="boundary_feature",
    ).to_dict()
    compare = _compare_payload(ranked, same_condition)
    branching = _branching_probe(ranked)
    stability = _time_block_stability(ranked)
    decision = _research_decision(compare, branching, coverage, stability, output_dir)
    compare["candidate_results"][0]["candidate_local_decision"] = decision["candidate_local_decision"]
    compare["candidate_results"][0]["decision"] = decision["candidate_local_decision"]
    compare["candidate_results"][0]["decision_reasons"] = decision["decision_reasons"]
    source_refs = {
        "schema_version": f"{SCHEMA_PREFIX}_source_artifact_refs_v1",
        "generated_at": _utc_now(),
        "source_mode": normalized_source_mode,
        "source_rows_parquet": str(_safe_path(source_rows_parquet, DEFAULT_SOURCE_ROWS_PARQUET)),
        "source_db": str(source_db_path),
        "pattern_dir": str(pattern_dir),
        "guard_dir": str(guard_dir),
        "pattern_authoritative_research_decision": pattern_decision.get("authoritative_research_decision"),
        "guard_authoritative_research_decision": guard_decision.get("authoritative_research_decision"),
        "silent_fallback_used": False,
    }
    evaluation_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_evaluation_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary": "TRADEX-only",
        "research_phase": "ranking branching probe over teppan-like chart patterns",
        "same_condition_contract": same_condition,
        "ranking_adjustment": {
            "mode": "static_teppan_guarded_soft_boost",
            "boost_value": BOOST_VALUE,
            "eligible_side": "long",
            "eligible_champion_rank_min": PROMOTION_POOL_MIN_RANK,
            "eligible_champion_rank_max": PROMOTION_POOL_MAX_RANK,
            "source_mode": normalized_source_mode,
            "direction": direction,
            "rank_limit": int(rank_limit),
        },
        "future_label_policy": {
            "future_labels_used_in_selection": False,
            "forward_ret_20d_used_for_evaluation_only": True,
        },
        "candidate_scoring_created": True,
        "production_ranking_changed": False,
        "meemee_reflectable": False,
        "publish_bundle_created": False,
        "silent_fallback_used": False,
    }
    evaluation_contract["contract_hash"] = _stable_hash(evaluation_contract)
    run_manifest = contracts.build_run_manifest(
        session_id=run_name,
        seed=0,
        random_seed=0,
        input_artifacts=[
            {"name": "source_rows_parquet", "path": source_refs["source_rows_parquet"]},
            {"name": "source_db", "path": source_refs["source_db"]},
            {"name": "pattern_dir", "path": source_refs["pattern_dir"]},
            {"name": "guard_dir", "path": source_refs["guard_dir"]},
        ],
        asof=str(int(source_rows["anchor_ymd"].max())),
        config={"axis_id": AXIS_ID, "boost_value": BOOST_VALUE, "candidate_scoring_created": True},
        universe=sorted(source_rows["symbol"].astype(str).unique().tolist()),
        period={"start_date": str(int(source_rows["anchor_ymd"].min())), "end_date": str(int(source_rows["anchor_ymd"].max())), "label": "historical_champion_ranked_candidate_rows"},
        horizon="20d",
        artifact_detail_level=contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        fallback_status=contracts.TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
        cost_model=contracts.TRADEX_DEFAULT_COST_MODEL,
    )
    contracts.validate_run_manifest(run_manifest)
    contracts.validate_compare_artifact(compare)

    ledger_columns = [
        "anchor_date",
        "side",
        "symbol",
        "champion_rank",
        "challenger_rank",
        "champion_score",
        "challenger_score",
        "forward_ret_20d",
        "teppan_pattern_match",
        "teppan_guard_pass",
        "teppan_boost_eligible",
        "best_pattern_family",
        "best_pattern_key",
        "best_pattern_decision",
        "best_teppan_score",
        "guard_block_reason",
        "rank_changed",
        "changed_top5_member",
        "changed_top10_member",
        "changed_top20_member",
    ]
    ledger_rows = ranked[ledger_columns].to_dict(orient="records")
    paths: dict[str, str] = {}
    for name, payload in {
        "evaluation_contract.json": evaluation_contract,
        "run_manifest.json": run_manifest,
        "source_artifact_refs.json": source_refs,
        "ranking_coverage_audit.json": coverage,
        "branching_probe.json": branching,
        "compare.json": compare,
        "research_decision.json": decision,
    }.items():
        paths[name] = str(_write_json(output_dir / name, payload))
    paths["selected_event_ledger.jsonl"] = str(_write_jsonl(output_dir / "selected_event_ledger.jsonl", ledger_rows))
    complete = _artifact_complete(output_dir, paths, decision)
    paths["_ARTIFACT_COMPLETE.json"] = str(_write_json(output_dir / "_ARTIFACT_COMPLETE.json", complete))
    return {
        "output_dir": str(output_dir),
        "paths": paths,
        "authoritative_research_decision": decision["authoritative_research_decision"],
        "decision": decision["decision"],
        "changed_top5_members_count": decision["changed_top5_members_count"],
        "changed_top10_members_count": decision["changed_top10_members_count"],
        "changed_rank_count": decision["changed_rank_count"],
        "selection_divergence_reason": decision["selection_divergence_reason"],
        "candidate_scoring_created": True,
        "production_ranking_changed": False,
        "meemee_reflectable": False,
        "publish_bundle_created": False,
        "silent_fallback_used": False,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-rows-parquet", default=str(DEFAULT_SOURCE_ROWS_PARQUET))
    parser.add_argument("--source-mode", default="parquet", choices=("parquet", "runtime-ranking"))
    parser.add_argument("--source-db", default="")
    parser.add_argument("--start-ymd", type=int, default=0)
    parser.add_argument("--end-ymd", type=int, default=0)
    parser.add_argument("--direction", default="up", choices=("up", "down"))
    parser.add_argument("--rank-limit", type=int, default=20)
    parser.add_argument("--pattern-root", default=str(DEFAULT_PATTERN_ROOT))
    parser.add_argument("--pattern-run-id", default=DEFAULT_PATTERN_RUN_ID)
    parser.add_argument("--guard-root", default=str(DEFAULT_GUARD_ROOT))
    parser.add_argument("--guard-run-id", default=DEFAULT_GUARD_RUN_ID)
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)
    result = run_teppan_ranking_branching_probe_v1(
        source_rows_parquet=args.source_rows_parquet,
        source_mode=args.source_mode,
        source_db=args.source_db.strip() or None,
        start_ymd=args.start_ymd or None,
        end_ymd=args.end_ymd or None,
        direction=args.direction,
        rank_limit=args.rank_limit,
        pattern_root=args.pattern_root,
        pattern_run_id=args.pattern_run_id,
        guard_root=args.guard_root,
        guard_run_id=args.guard_run_id,
        output_root=args.output_root,
        run_id=args.run_id.strip() or None,
    )
    print(_json_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
