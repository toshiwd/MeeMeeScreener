from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import tradex_iizuka_signal_expectancy_v1 as base

SCRIPT_NAME = "tradex_iizuka_signal_expectancy_phase1g_v1"
SCHEMA_VERSION = "tradex_iizuka_signal_expectancy_phase1g_v1"
DEFAULT_PHASE1C_ROOT = Path(r"G:\Tradex\iizuka_signal_expectancy_v1_phase1c\20260509T050355Z-080636")
DEFAULT_PHASE1F_ROOT = Path(r"G:\Tradex\iizuka_signal_expectancy_v1_phase1f\20260509T054212Z-033207")
DEFAULT_SEARCH_ROOT = Path(r"G:\Tradex")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\iizuka_signal_expectancy_v1_phase1g")
CANDIDATE_ID = "monthly_C_mixed_pullback_end_reclaim7_v1"
REQUIRED_ARTIFACTS = [
    "run_manifest.json",
    "input_resolution.json",
    "phase1g_source_inventory.json",
    "phase1g_point_in_time_safety_audit.json",
    "phase1g_reconstruction_audit.json",
    "phase1g_safe_overlap_gate.json",
    "phase1g_optional_pretest_results.json",
    "phase1g_decision.json",
    "_ARTIFACT_COMPLETE.json",
]
KEYWORDS = ("candidate", "ranking", "rank", "pool", "selection", "champion", "prefilter", "reranker", "min_pool")
FUTURE_LABEL_COLUMNS = {
    "forward_ret_5d",
    "forward_ret_10d",
    "forward_ret_20d",
    "path_value_score_v1",
    "mfe_20d",
    "mae_20d",
    "top15_label",
    "top20pct_label",
    "bottom15_label",
    "hit_plus_5_before_minus_5",
    "hit_minus_5_before_plus_5",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    base._write_json(path, payload)


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _safe_path(value: str | Path | None, default: Path) -> Path:
    return base._safe_path(value, default)


def _norm_code(value: Any) -> str:
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text.zfill(4) if text.isdigit() and len(text) < 4 else text


def _norm_date(value: Any) -> str | None:
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    if text.isdigit() and len(text) == 8:
        parsed = pd.to_datetime(text, format="%Y%m%d", errors="coerce")
    else:
        parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.strftime("%Y-%m-%d")


def _choose(columns: list[str], choices: tuple[str, ...]) -> str | None:
    lower = {col.lower(): col for col in columns}
    for choice in choices:
        if choice.lower() in lower:
            return lower[choice.lower()]
    return None


def _discover(search_root: Path, max_sources: int) -> list[Path]:
    if not search_root.exists():
        return []
    items = []
    for path in search_root.rglob("*.parquet"):
        text = str(path).lower()
        if any(keyword in text for keyword in KEYWORDS):
            items.append(path)
    items.sort(key=lambda path: path.stat().st_mtime if path.exists() else 0, reverse=True)
    return items[:max_sources]


def _schema(path: Path) -> tuple[int, list[str]]:
    parquet = pq.ParquetFile(path)
    return int(parquet.metadata.num_rows), list(parquet.schema.names)


def _source_kind(path: Path, columns: list[str]) -> str:
    text = str(path).lower()
    cols = {c.lower() for c in columns}
    if "champion" in text or any(c.startswith("champion_") for c in cols):
        return "champion"
    if "min_pool" in text:
        return "min_pool"
    if "prefilter" in text:
        return "prefilter"
    if "candidate" in text:
        return "candidate_generation"
    if "reranker" in text:
        return "reranker"
    return "derived"


def _read_keys(path: Path, source: dict[str, Any]) -> pd.DataFrame | None:
    code_col = source.get("code_column")
    date_col = source.get("date_column")
    if not code_col or not date_col:
        return None
    cols = [code_col, date_col]
    for col in (source.get("score_columns") or [])[:5] + (source.get("rank_columns") or [])[:8] + (source.get("topk_columns") or [])[:12]:
        if col not in cols:
            cols.append(col)
    schema_cols = set(pq.ParquetFile(path).schema.names)
    if "side" in schema_cols and "side" not in cols:
        cols.append("side")
    frame = pd.read_parquet(path, columns=cols)
    out = pd.DataFrame({"code_norm": frame[code_col].map(_norm_code), "pool_date": frame[date_col].map(_norm_date)})
    for col in frame.columns:
        if col not in {code_col, date_col}:
            out[col] = frame[col]
    if "side" in out.columns:
        out = out.loc[out["side"].astype(str).str.lower().isin(["long", "buy", "up", ""]) | out["side"].isna()].copy()
    return out.dropna(subset=["code_norm", "pool_date"]).reset_index(drop=True)


def _top_counts(rows: pd.DataFrame) -> dict[str, int | None]:
    out = {}
    for k in (5, 10, 20, 50):
        flag = next((c for c in rows.columns if c.lower() in {f"champion_selected_top{k}", f"selected_top{k}", f"baseline_selected_top{k}"}), None)
        rank = next((c for c in ("champion_rank", "rank", "candidate_rank", "min_pool_priority_rank") if c in rows.columns), None)
        if flag:
            out[f"top{k}"] = int(rows[flag].fillna(False).astype(bool).sum())
        elif rank:
            out[f"top{k}"] = int((pd.to_numeric(rows[rank], errors="coerce") <= k).sum())
        else:
            out[f"top{k}"] = None
    return out


def _nearest_prior(mixed: pd.DataFrame, pool: pd.DataFrame) -> pd.DataFrame:
    left = mixed[["code_norm", "decision_date"]].copy()
    left["decision_ts"] = pd.to_datetime(left["decision_date"], errors="coerce")
    right = pool.copy()
    right["pool_ts"] = pd.to_datetime(right["pool_date"], errors="coerce")
    chunks = []
    for code, group in left.groupby("code_norm"):
        source = right.loc[right["code_norm"] == code].sort_values("pool_ts")
        if source.empty:
            continue
        joined = pd.merge_asof(group.sort_values("decision_ts"), source, by="code_norm", left_on="decision_ts", right_on="pool_ts", direction="backward", allow_exact_matches=True)
        chunks.append(joined.dropna(subset=["pool_date"]))
    return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()


def _inspect_source(path: Path, mixed: pd.DataFrame) -> dict[str, Any]:
    try:
        rows, columns = _schema(path)
        code_col = _choose(columns, ("symbol", "code", "ticker", "sec_code"))
        date_col = _choose(columns, ("snapshot_date", "as_of_date", "decision_date", "anchor_date", "trade_date", "date", "rank_date"))
        score_cols = [c for c in columns if "score" in c.lower()]
        rank_cols = [c for c in columns if "rank" in c.lower()]
        topk_cols = [c for c in columns if "selected_top" in c.lower() or "top5" in c.lower() or "top10" in c.lower() or "top20" in c.lower() or "top50" in c.lower()]
        future_cols = sorted(set(c for c in columns if c.lower() in FUTURE_LABEL_COLUMNS))
        explicit_asof = bool(_choose(columns, ("snapshot_date", "as_of_date", "decision_date", "anchor_date", "trade_date", "date")))
        frame = _read_keys(path, {"code_column": code_col, "date_column": date_col, "score_columns": score_cols, "rank_columns": rank_cols, "topk_columns": topk_cols}) if code_col and date_col else None
        date_min = date_max = None
        exact_decision = exact_execution = prior = 0
        top_counts = {"top5": None, "top10": None, "top20": None, "top50": None}
        if frame is not None and len(frame):
            date_min = str(frame["pool_date"].min())
            date_max = str(frame["pool_date"].max())
            exact_decision_rows = mixed.merge(frame, left_on=["code_norm", "decision_date"], right_on=["code_norm", "pool_date"], how="inner")
            exact_execution_rows = mixed.merge(frame, left_on=["code_norm", "execution_date"], right_on=["code_norm", "pool_date"], how="inner")
            prior_rows = _nearest_prior(mixed, frame)
            exact_decision = int(len(exact_decision_rows))
            exact_execution = int(len(exact_execution_rows))
            prior = int(len(prior_rows))
            top_counts = _top_counts(exact_decision_rows if exact_decision else exact_execution_rows if exact_execution else prior_rows)
        no_future = len(future_cols) == 0
        has_rank_or_score = bool(score_cols or rank_cols or topk_cols)
        safe = bool(code_col and date_col and explicit_asof and no_future and has_rank_or_score)
        sufficient = max(exact_decision, exact_execution, prior) >= 100
        return {
            "path": str(path),
            "status": "confirmed" if code_col and date_col else "provisional",
            "row_count": rows,
            "date_range": {"min": date_min, "max": date_max},
            "code_column": code_col,
            "date_column": date_col,
            "score_columns": score_cols[:20],
            "rank_columns": rank_cols[:20],
            "topk_columns": topk_cols[:20],
            "as_of_or_snapshot_semantics": "explicit_date_column" if explicit_asof else "not_explicit",
            "future_return_or_post_horizon_labels_present": bool(future_cols),
            "future_label_columns": future_cols,
            "source_kind": _source_kind(path, columns),
            "safe_for_no_lookahead_pretest": safe,
            "overlap": {
                "code+decision_date": exact_decision,
                "code+execution_date": exact_execution,
                "code+nearest_prior_ranking_date": prior if safe else None,
            },
            "topk_overlap_for_best_safe_policy": top_counts,
            "safe_overlap_sufficient": bool(safe and sufficient),
        }
    except Exception as exc:
        return {"path": str(path), "status": "blocked", "reason": str(exc)}


def _reconstruction_audit(mixed: pd.DataFrame) -> dict[str, Any]:
    return {
        "schema_version": f"{SCHEMA_VERSION}_reconstruction_audit_v1",
        "generated_at_utc": _utc_now(),
        "candidate_id": CANDIDATE_ID,
        "reconstructed_pool_created": False,
        "status": "blocked",
        "reason": "No point-in-time champion scoring contract was identified; reconstructing candidate membership without the champion scoring contract would redefine ranking conditions.",
        "available_safe_inputs": {
            "mixed_signal_rows": int(len(mixed)),
            "confirmed_daily_monthly_features": "available_for_signal_expectancy_only",
        },
        "forbidden_reason": "Would require changing or inferring champion logic, which is out of scope for Phase 1g.",
    }


def run_phase1g(*, phase1c_root: Path, phase1f_root: Path, search_root: Path, output_root: Path, max_sources: int = 250) -> dict[str, Any]:
    session_root = output_root / _session_id()
    session_root.mkdir(parents=True, exist_ok=True)
    mixed = pd.read_parquet(phase1c_root / "phase1c_mixed_signal_rows.parquet").copy()
    mixed["code_norm"] = mixed["symbol"].map(_norm_code)
    mixed["decision_date"] = mixed["decision_date"].map(_norm_date)
    mixed["execution_date"] = mixed["execution_date"].map(_norm_date)
    sources = [_inspect_source(path, mixed) for path in _discover(search_root, max_sources)]
    safe_sources = [s for s in sources if s.get("safe_overlap_sufficient")]
    selected = max(safe_sources, key=lambda s: max((s.get("overlap") or {}).values())) if safe_sources else None
    reconstruction = _reconstruction_audit(mixed)
    valid_pool = selected is not None
    pretest = {
        "schema_version": f"{SCHEMA_VERSION}_optional_pretest_results_v1",
        "generated_at_utc": _utc_now(),
        "pretest_ran": False,
        "reason": "No safe point-in-time pool with sufficient overlap was found." if not valid_pool else "Pretest deferred to separately versioned run after pool source selection.",
    }
    if valid_pool:
        decision = "blocked_pending_point_in_time_pool"
        reason = "safe_source_identified_but_pretest_not_run_in_this_audit_only_implementation"
    else:
        decision = "blocked_pending_point_in_time_pool"
        reason = "no_safe_point_in_time_candidate_pool_or_reconstruction_available"
    gate = {
        "schema_version": f"{SCHEMA_VERSION}_safe_overlap_gate_v1",
        "generated_at_utc": _utc_now(),
        "candidate_id": CANDIDATE_ID,
        "valid_point_in_time_pool_found": valid_pool,
        "selected_source_path": selected.get("path") if selected else None,
        "selected_key_policy": None,
        "matched_signal_rows": max((selected.get("overlap") or {}).values()) if selected else 0,
        "topk_overlap": selected.get("topk_overlap_for_best_safe_policy") if selected else None,
        "nearest_next_policy_accepted": False,
        "gate_pass": False,
        "reason": reason,
    }
    decision_payload = {
        "schema_version": f"{SCHEMA_VERSION}_decision_v1",
        "generated_at_utc": _utc_now(),
        "candidate_id": CANDIDATE_ID,
        "authoritative_decision": decision,
        "decision_reason": reason,
        "valid_point_in_time_pool_found": valid_pool,
        "reconstructed_pool_created": False,
        "selected_source_path": selected.get("path") if selected else None,
        "selected_key_policy": None,
        "matched_signal_rows": gate["matched_signal_rows"],
        "topk_overlap": gate["topk_overlap"],
        "optional_pretest_ran": False,
        "next_ranking_challenger_candidate_allowed": False,
        "candidate_generation_or_explain_treatment": "candidate_generation_hold" if not valid_pool else None,
        "meemee_reflection": "blocked",
        "production_ranking_changed": False,
        "publish_changed": False,
    }
    artifacts = {
        "run_manifest.json": {"schema_version": f"{SCHEMA_VERSION}_manifest_v1", "generated_at_utc": _utc_now(), "script_name": SCRIPT_NAME, "session_root": str(session_root), "boundary": "TRADEX-only", "phase1c_root": str(phase1c_root), "phase1f_root": str(phase1f_root), "signal_definition_changed": False, "thresholds_changed": False, "ranking_challenger_created": False, "meemee_changed": False, "production_ranking_changed": False, "publish_changed": False},
        "input_resolution.json": {"schema_version": f"{SCHEMA_VERSION}_input_resolution_v1", "generated_at_utc": _utc_now(), "phase1c_root": str(phase1c_root), "phase1f_root": str(phase1f_root), "search_root": str(search_root), "mixed_signal_rows": int(len(mixed)), "inventoried_source_count": len(sources)},
        "phase1g_source_inventory.json": {"schema_version": f"{SCHEMA_VERSION}_source_inventory_v1", "generated_at_utc": _utc_now(), "candidate_id": CANDIDATE_ID, "sources": sources},
        "phase1g_point_in_time_safety_audit.json": {"schema_version": f"{SCHEMA_VERSION}_point_in_time_safety_audit_v1", "generated_at_utc": _utc_now(), "candidate_id": CANDIDATE_ID, "safe_source_count": len(safe_sources), "safe_sources": safe_sources[:20], "nearest_next_policy_allowed": False},
        "phase1g_reconstruction_audit.json": reconstruction,
        "phase1g_safe_overlap_gate.json": gate,
        "phase1g_optional_pretest_results.json": pretest,
        "phase1g_decision.json": decision_payload,
    }
    for name, payload in artifacts.items():
        _write_json(session_root / name, payload)
    complete = {"schema_version": f"{SCHEMA_VERSION}_artifact_complete_v1", "generated_at_utc": _utc_now(), "session_root": str(session_root), "required_artifacts": REQUIRED_ARTIFACTS, "all_present": all((session_root / artifact).exists() for artifact in REQUIRED_ARTIFACTS if artifact != "_ARTIFACT_COMPLETE.json")}
    _write_json(session_root / "_ARTIFACT_COMPLETE.json", complete)
    return {"session_root": str(session_root), "decision": decision, "optional_pretest_ran": False}


def main() -> None:
    parser = argparse.ArgumentParser(description="TRADEX Iizuka Phase 1g point-in-time candidate pool audit")
    parser.add_argument("--phase1c-root", default=str(DEFAULT_PHASE1C_ROOT))
    parser.add_argument("--phase1f-root", default=str(DEFAULT_PHASE1F_ROOT))
    parser.add_argument("--search-root", default=str(DEFAULT_SEARCH_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--max-sources", type=int, default=250)
    args = parser.parse_args()
    result = run_phase1g(
        phase1c_root=_safe_path(args.phase1c_root, DEFAULT_PHASE1C_ROOT),
        phase1f_root=_safe_path(args.phase1f_root, DEFAULT_PHASE1F_ROOT),
        search_root=_safe_path(args.search_root, DEFAULT_SEARCH_ROOT),
        output_root=_safe_path(args.output_root, DEFAULT_OUTPUT_ROOT),
        max_sources=args.max_sources,
    )
    print(json.dumps(base._json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
