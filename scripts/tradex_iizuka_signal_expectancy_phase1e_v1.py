from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import tradex_iizuka_signal_expectancy_v1 as base

SCRIPT_NAME = "tradex_iizuka_signal_expectancy_phase1e_v1"
SCHEMA_VERSION = "tradex_iizuka_signal_expectancy_phase1e_v1"
DEFAULT_PHASE1C_ROOT = Path(r"G:\Tradex\iizuka_signal_expectancy_v1_phase1c\20260509T050355Z-080636")
DEFAULT_PHASE1D_ROOT = Path(r"G:\Tradex\iizuka_signal_expectancy_v1_phase1d\20260509T051551Z-956303")
DEFAULT_SEARCH_ROOT = Path(r"G:\Tradex")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\iizuka_signal_expectancy_v1_phase1e")
CANDIDATE_ID = "monthly_C_mixed_pullback_end_reclaim7_v1"
REQUIRED_ARTIFACTS = [
    "run_manifest.json",
    "input_resolution.json",
    "phase1e_source_inventory.json",
    "phase1e_key_alignment_audit.json",
    "phase1e_candidate_funnel_audit.json",
    "phase1e_same_period_champion_pool_audit.json",
    "phase1e_decision.json",
    "_ARTIFACT_COMPLETE.json",
]
DISCOVERY_KEYWORDS = ("candidate", "ranking", "rank", "prefilter", "pool", "selection", "champion", "reranker")


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


def _normalize_date(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "nat", "none", "null"}:
        return None
    if text.endswith(".0"):
        text = text[:-2]
    if text.isdigit() and len(text) == 8:
        parsed = pd.to_datetime(text, format="%Y%m%d", errors="coerce")
    else:
        parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.strftime("%Y-%m-%d")


def _date_series(series: pd.Series) -> pd.Series:
    return series.map(_normalize_date)


def _discover_parquets(search_root: Path, explicit_sources: list[Path], limit: int) -> list[Path]:
    found: dict[str, Path] = {}
    for path in explicit_sources:
        if path.exists() and path.suffix.lower() == ".parquet":
            found[str(path.resolve())] = path.resolve()
    if search_root.exists():
        candidates = []
        for path in search_root.rglob("*.parquet"):
            name = path.name.lower()
            full = str(path).lower()
            if any(key in name or key in full for key in DISCOVERY_KEYWORDS):
                candidates.append(path)
        candidates.sort(key=lambda p: p.stat().st_mtime if p.exists() else 0, reverse=True)
        for path in candidates[:limit]:
            found[str(path.resolve())] = path.resolve()
    return list(found.values())


def _schema(path: Path) -> tuple[int | None, list[str]]:
    meta = pq.ParquetFile(path)
    return meta.metadata.num_rows if meta.metadata else None, list(meta.schema.names)


def _choose(columns: list[str], names: tuple[str, ...]) -> str | None:
    lower = {col.lower(): col for col in columns}
    for name in names:
        if name.lower() in lower:
            return lower[name.lower()]
    return None


def _kind(path: Path, columns: list[str]) -> str:
    text = str(path).lower()
    cols = {c.lower() for c in columns}
    if "champion" in text or any(c.startswith("champion_") for c in cols):
        return "champion"
    if "prefilter" in text:
        return "prefilter"
    if "candidate_generation" in text or "candidate" in text:
        return "candidate_generation"
    if "reranker" in text:
        return "reranker"
    if "selection" in text:
        return "selection"
    return "derived"


def _inspect_source(path: Path, mixed_min: str, mixed_max: str) -> dict[str, Any]:
    try:
        row_count, columns = _schema(path)
        symbol_col = _choose(columns, ("symbol", "code", "ticker", "sec_code"))
        date_col = _choose(columns, ("anchor_date", "decision_date", "trade_date", "date", "rank_date", "snapshot_date"))
        score_cols = [c for c in columns if "score" in c.lower()]
        rank_cols = [c for c in columns if "rank" in c.lower()]
        topk_cols = [c for c in columns if "selected_top" in c.lower() or "top5" in c.lower() or "top10" in c.lower() or "top20" in c.lower() or "top50" in c.lower()]
        date_min = None
        date_max = None
        matches_period = False
        if date_col:
            sample = pd.read_parquet(path, columns=[date_col])
            dates = _date_series(sample[date_col]).dropna()
            if len(dates):
                date_min = str(dates.min())
                date_max = str(dates.max())
                matches_period = not (date_max < mixed_min or date_min > mixed_max)
        status = "confirmed" if symbol_col and date_col else "provisional"
        return {
            "path": str(path),
            "status": status,
            "row_count": int(row_count) if row_count is not None else None,
            "date_range": {"min": date_min, "max": date_max},
            "code_or_symbol_column": symbol_col,
            "date_column": date_col,
            "score_columns": score_cols[:20],
            "rank_columns": rank_cols[:20],
            "topk_columns": topk_cols[:20],
            "topk_available": bool(topk_cols or rank_cols),
            "source_kind": _kind(path, columns),
            "matches_phase1c_source_period": bool(matches_period),
        }
    except Exception as exc:
        return {"path": str(path), "status": "blocked", "reason": str(exc)}


def _read_alignment_frame(source: dict[str, Any]) -> pd.DataFrame | None:
    path = Path(str(source.get("path") or ""))
    symbol_col = source.get("code_or_symbol_column")
    date_col = source.get("date_column")
    if not path.exists() or not symbol_col or not date_col:
        return None
    columns = [symbol_col, date_col]
    for col in (source.get("score_columns") or [])[:5] + (source.get("rank_columns") or [])[:10] + (source.get("topk_columns") or [])[:20]:
        if col not in columns:
            columns.append(col)
    if "side" in pq.ParquetFile(path).schema.names and "side" not in columns:
        columns.append("side")
    frame = pd.read_parquet(path, columns=columns)
    out = pd.DataFrame(
        {
            "code_norm": frame[symbol_col].map(_norm_code),
            "source_date": _date_series(frame[date_col]),
        }
    )
    for col in frame.columns:
        if col not in {symbol_col, date_col}:
            out[col] = frame[col]
    if "side" in out.columns:
        out = out.loc[out["side"].astype(str).str.lower().isin(["long", "buy", "up", ""]) | out["side"].isna()].copy()
    out = out.dropna(subset=["code_norm", "source_date"]).drop_duplicates().reset_index(drop=True)
    return out


def _topk_counts(joined: pd.DataFrame) -> dict[str, int | None]:
    out: dict[str, int | None] = {}
    for k in (5, 10, 20, 50):
        flag = None
        for col in joined.columns:
            if col.lower() in {f"champion_selected_top{k}", f"selected_top{k}", f"baseline_selected_top{k}"}:
                flag = col
                break
        rank_col = None
        for name in ("champion_rank", "rank", "candidate_rank"):
            if name in joined.columns:
                rank_col = name
                break
        if flag:
            out[f"top{k}"] = int(joined[flag].fillna(False).astype(bool).sum())
        elif rank_col:
            out[f"top{k}"] = int((pd.to_numeric(joined[rank_col], errors="coerce") <= k).sum())
        else:
            out[f"top{k}"] = None
    return out


def _nearest_match_count(mixed: pd.DataFrame, source: pd.DataFrame, direction: str) -> tuple[int, pd.DataFrame]:
    left = mixed[["code_norm", "decision_date", "execution_date"]].copy()
    left["decision_ts"] = pd.to_datetime(left["decision_date"], errors="coerce")
    right = source.copy()
    right["source_ts"] = pd.to_datetime(right["source_date"], errors="coerce")
    chunks = []
    for code, group in left.groupby("code_norm"):
        r = right.loc[right["code_norm"] == code].sort_values("source_ts")
        if r.empty:
            continue
        merged = pd.merge_asof(
            group.sort_values("decision_ts"),
            r,
            left_on="decision_ts",
            right_on="source_ts",
            by="code_norm",
            direction=direction,
            allow_exact_matches=True,
        )
        chunks.append(merged.dropna(subset=["source_date"]))
    joined = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
    return int(len(joined)), joined


def _alignment_for_source(mixed: pd.DataFrame, source: dict[str, Any]) -> dict[str, Any]:
    frame = _read_alignment_frame(source)
    if frame is None or frame.empty:
        return {"path": source.get("path"), "status": "blocked", "reason": "source not key-alignable"}
    signal = mixed[["code_norm", "decision_date", "execution_date"]].drop_duplicates()
    policy_results: dict[str, Any] = {}
    for policy, signal_date in (("code+decision_date", "decision_date"), ("code+execution_date", "execution_date")):
        joined = signal.merge(frame, left_on=["code_norm", signal_date], right_on=["code_norm", "source_date"], how="inner")
        policy_results[policy] = {
            "matched_signal_rows": int(len(joined[["code_norm", signal_date]].drop_duplicates())),
            "joined_rows": int(len(joined)),
            "topk_counts": _topk_counts(joined),
        }
    prior_count, prior_joined = _nearest_match_count(mixed, frame, "backward")
    next_count, next_joined = _nearest_match_count(mixed, frame, "forward")
    policy_results["code+nearest_prior_ranking_date"] = {
        "matched_signal_rows": prior_count,
        "joined_rows": int(len(prior_joined)),
        "topk_counts": _topk_counts(prior_joined),
    }
    policy_results["code+nearest_next_ranking_date"] = {
        "matched_signal_rows": next_count,
        "joined_rows": int(len(next_joined)),
        "topk_counts": _topk_counts(next_joined),
    }
    best_policy = max(policy_results.items(), key=lambda item: item[1]["matched_signal_rows"])
    return {
        "path": source.get("path"),
        "status": "confirmed",
        "source_kind": source.get("source_kind"),
        "row_count": source.get("row_count"),
        "date_range": source.get("date_range"),
        "best_key_policy": best_policy[0],
        "best_matched_signal_rows": best_policy[1]["matched_signal_rows"],
        "best_topk_counts": best_policy[1]["topk_counts"],
        "policy_results": policy_results,
    }


def _funnel_audit(mixed: pd.DataFrame, best: dict[str, Any], source_info: dict[str, Any]) -> dict[str, Any]:
    frame = _read_alignment_frame(source_info)
    if frame is None or frame.empty:
        return {"status": "blocked", "reason": "best source not readable"}
    signal_symbols = set(mixed["code_norm"])
    source_symbols = set(frame["code_norm"])
    symbol_only = len(signal_symbols & source_symbols)
    matched = int(best.get("best_matched_signal_rows") or 0)
    stages = {
        "source_universe": {
            "matched_signal_rows": int(mixed["code_norm"].isin(source_symbols).sum()),
            "match_rate": float(mixed["code_norm"].isin(source_symbols).mean()) if len(mixed) else None,
            "lost_count": int((~mixed["code_norm"].isin(source_symbols)).sum()),
            "likely_loss_reason": "symbol_not_in_source_surface",
        },
        "date_aligned_candidate_pool": {
            "matched_signal_rows": matched,
            "match_rate": float(matched / len(mixed)) if len(mixed) else None,
            "lost_count": int(len(mixed) - matched),
            "likely_loss_reason": "date_alignment_or_prefilter_exclusion",
        },
    }
    topk = best.get("best_topk_counts") or {}
    for key in ("top50", "top20", "top10", "top5"):
        count = topk.get(key)
        stages[key] = {
            "matched_signal_rows": count,
            "match_rate": float(count / len(mixed)) if isinstance(count, int) and len(mixed) else None,
            "lost_count": int(len(mixed) - count) if isinstance(count, int) else None,
            "likely_loss_reason": "not_in_topk_or_topk_unavailable",
        }
    if matched == 0 and symbol_only > 0:
        likely = "date_key_mismatch_or_signal_dates_absent_from_surface"
    elif matched == 0:
        likely = "source_universe_or_surface_mismatch"
    elif (topk.get("top20") or 0) == 0:
        likely = "present_before_topk_but_not_champion_top20"
    else:
        likely = "topk_exposure_present"
    return {
        "schema_version": f"{SCHEMA_VERSION}_candidate_funnel_audit_v1",
        "generated_at_utc": _utc_now(),
        "candidate_id": CANDIDATE_ID,
        "status": "confirmed",
        "best_source_path": best.get("path"),
        "best_key_policy": best.get("best_key_policy"),
        "stage_results": stages,
        "funnel_loss_stage": likely,
    }


def _same_period_pool_audit(best: dict[str, Any], source_info: dict[str, Any], mixed: pd.DataFrame) -> dict[str, Any]:
    valid = (
        source_info.get("matches_phase1c_source_period") is True
        and source_info.get("topk_available") is True
        and int(best.get("best_matched_signal_rows") or 0) > 0
    )
    if valid:
        status = "confirmed"
        reason = "existing_same_period_candidate_or_champion_surface_is_key_alignable"
    elif source_info.get("matches_phase1c_source_period") is not True:
        status = "blocked"
        reason = "best_source_does_not_match_phase1c_signal_period"
    elif int(best.get("best_matched_signal_rows") or 0) <= 0:
        status = "blocked"
        reason = "no_key_aligned_signal_rows_in_best_source"
    else:
        status = "blocked"
        reason = "best_source_lacks_topk_or_rank_columns"
    return {
        "schema_version": f"{SCHEMA_VERSION}_same_period_champion_pool_audit_v1",
        "generated_at_utc": _utc_now(),
        "candidate_id": CANDIDATE_ID,
        "status": status,
        "valid_ranking_exposure_source_found": valid,
        "best_source_path": best.get("path"),
        "best_key_policy": best.get("best_key_policy"),
        "matched_signal_rows": best.get("best_matched_signal_rows"),
        "topk_counts": best.get("best_topk_counts"),
        "construction_attempted": False,
        "construction_status": "not_needed" if valid else "blocked",
        "reason": reason,
        "audit_only": True,
        "production_ranking_changed": False,
        "publish_changed": False,
    }


def run_phase1e(
    *,
    phase1c_root: Path,
    phase1d_root: Path,
    output_root: Path,
    search_root: Path = DEFAULT_SEARCH_ROOT,
    max_sources: int = 200,
) -> dict[str, Any]:
    started = time.perf_counter()
    session_root = output_root / _session_id()
    session_root.mkdir(parents=True, exist_ok=True)
    mixed = pd.read_parquet(phase1c_root / "phase1c_mixed_signal_rows.parquet")
    mixed = mixed.copy()
    mixed["code_norm"] = mixed["symbol"].map(_norm_code)
    mixed["decision_date"] = _date_series(mixed["decision_date"])
    mixed["execution_date"] = _date_series(mixed["execution_date"])
    phase1c_decision = _load_json(phase1c_root / "phase1c_signal_decision.json")
    phase1d_decision = _load_json(phase1d_root / "phase1d_decision.json")
    phase1d_input = _load_json(phase1d_root / "input_resolution.json")
    explicit = [Path(str(phase1d_input.get("ranking_surface_path") or ""))]
    sources = _discover_parquets(search_root, explicit, max_sources)
    mixed_min = str(min(mixed["decision_date"]))
    mixed_max = str(max(mixed["decision_date"]))
    inventory_rows = [_inspect_source(path, mixed_min, mixed_max) for path in sources]
    alignments = [_alignment_for_source(mixed, item) for item in inventory_rows if item.get("status") in {"confirmed", "provisional"}]
    confirmed_alignments = [item for item in alignments if item.get("status") == "confirmed"]
    if confirmed_alignments:
        best = max(
            confirmed_alignments,
            key=lambda item: (
                int(item.get("best_matched_signal_rows") or 0),
                int(((item.get("best_topk_counts") or {}).get("top20") or 0)),
                int(item.get("row_count") or 0),
            ),
        )
        source_info = next(item for item in inventory_rows if item.get("path") == best.get("path"))
    else:
        best = {"status": "blocked", "best_matched_signal_rows": 0, "best_topk_counts": {}}
        source_info = {"status": "blocked"}
    funnel = _funnel_audit(mixed, best, source_info) if best.get("status") == "confirmed" else {"status": "blocked", "reason": "no key-alignable source"}
    same_period = _same_period_pool_audit(best, source_info, mixed) if best.get("status") == "confirmed" else {
        "schema_version": f"{SCHEMA_VERSION}_same_period_champion_pool_audit_v1",
        "generated_at_utc": _utc_now(),
        "candidate_id": CANDIDATE_ID,
        "status": "blocked",
        "valid_ranking_exposure_source_found": False,
        "reason": "no key-alignable candidate or ranking source found",
        "audit_only": True,
        "production_ranking_changed": False,
        "publish_changed": False,
    }

    matched = int(best.get("best_matched_signal_rows") or 0)
    top20 = int(((best.get("best_topk_counts") or {}).get("top20") or 0) or 0)
    valid_source = bool(same_period.get("valid_ranking_exposure_source_found"))
    if valid_source and top20 > 0:
        decision = "proceed_to_ranking_pretest"
        reason = "valid_same_period_topk_exposure_source_found"
    elif matched > 0:
        decision = "hold_as_candidate_generation_signal"
        reason = "signal_reaches_candidate_surface_but_not_topk"
    elif confirmed_alignments:
        decision = "analysis_only"
        reason = "key_alignable_sources_found_but_signal_has_no_overlap"
    else:
        decision = "blocked"
        reason = "correct_ranking_exposure_source_cannot_be_identified"

    source_inventory = {
        "schema_version": f"{SCHEMA_VERSION}_source_inventory_v1",
        "generated_at_utc": _utc_now(),
        "candidate_id": CANDIDATE_ID,
        "search_root": str(search_root),
        "inventoried_source_count": len(inventory_rows),
        "sources": inventory_rows,
    }
    key_alignment = {
        "schema_version": f"{SCHEMA_VERSION}_key_alignment_audit_v1",
        "generated_at_utc": _utc_now(),
        "candidate_id": CANDIDATE_ID,
        "phase1c_mixed_row_count": int(len(mixed)),
        "mixed_decision_date_range": {"min": mixed_min, "max": mixed_max},
        "alignment_results": alignments,
        "best_source_path": best.get("path"),
        "best_key_policy": best.get("best_key_policy"),
        "best_matched_signal_rows": matched,
        "best_topk_counts": best.get("best_topk_counts"),
    }
    decision_payload = {
        "schema_version": f"{SCHEMA_VERSION}_decision_v1",
        "generated_at_utc": _utc_now(),
        "candidate_id": CANDIDATE_ID,
        "authoritative_decision": decision,
        "decision_reason": reason,
        "phase1c_authoritative_decision": phase1c_decision.get("authoritative_rollup_decision"),
        "phase1d_authoritative_decision": phase1d_decision.get("authoritative_decision"),
        "valid_ranking_exposure_source_found": valid_source,
        "best_source_path": best.get("path"),
        "best_key_policy": best.get("best_key_policy"),
        "matched_signal_rows": matched,
        "topk_counts": best.get("best_topk_counts"),
        "ranking_pretest_allowed_next": decision == "proceed_to_ranking_pretest",
        "candidate_generation_or_explain_treatment": "candidate_generation" if decision == "hold_as_candidate_generation_signal" else "explain" if decision == "analysis_only" else None,
        "meemee_reflection": "blocked",
        "ranking_challenger": "not_created",
        "production_ranking_changed": False,
        "publish_changed": False,
    }
    manifest = {
        "schema_version": f"{SCHEMA_VERSION}_manifest_v1",
        "generated_at_utc": _utc_now(),
        "script_name": SCRIPT_NAME,
        "session_root": str(session_root),
        "phase1c_root": str(phase1c_root),
        "phase1d_root": str(phase1d_root),
        "boundary": "TRADEX-only",
        "signal_definition_changed": False,
        "thresholds_changed": False,
        "ranking_challenger_created": False,
        "meemee_changed": False,
        "production_ranking_changed": False,
        "publish_changed": False,
        "runtime_seconds": float(time.perf_counter() - started),
    }
    input_resolution = {
        "schema_version": f"{SCHEMA_VERSION}_input_resolution_v1",
        "generated_at_utc": _utc_now(),
        "phase1c_root": str(phase1c_root),
        "phase1d_root": str(phase1d_root),
        "phase1c_mixed_signal_rows": str(phase1c_root / "phase1c_mixed_signal_rows.parquet"),
        "phase1c_signal_decision": str(phase1c_root / "phase1c_signal_decision.json"),
        "phase1d_decision": str(phase1d_root / "phase1d_decision.json"),
        "search_root": str(search_root),
        "mixed_signal_row_count": int(len(mixed)),
        "inventoried_source_count": len(inventory_rows),
        "key_alignable_source_count": len(confirmed_alignments),
    }

    _write_json(session_root / "run_manifest.json", manifest)
    _write_json(session_root / "input_resolution.json", input_resolution)
    _write_json(session_root / "phase1e_source_inventory.json", source_inventory)
    _write_json(session_root / "phase1e_key_alignment_audit.json", key_alignment)
    _write_json(session_root / "phase1e_candidate_funnel_audit.json", funnel)
    _write_json(session_root / "phase1e_same_period_champion_pool_audit.json", same_period)
    _write_json(session_root / "phase1e_decision.json", decision_payload)
    complete = {
        "schema_version": f"{SCHEMA_VERSION}_artifact_complete_v1",
        "generated_at_utc": _utc_now(),
        "session_root": str(session_root),
        "required_artifacts": REQUIRED_ARTIFACTS,
        "all_present": all((session_root / artifact).exists() for artifact in REQUIRED_ARTIFACTS if artifact != "_ARTIFACT_COMPLETE.json"),
    }
    _write_json(session_root / "_ARTIFACT_COMPLETE.json", complete)
    return {"session_root": str(session_root), "decision": decision, "ranking_pretest_allowed_next": decision == "proceed_to_ranking_pretest"}


def main() -> None:
    parser = argparse.ArgumentParser(description="TRADEX Iizuka Phase 1e ranking exposure source audit")
    parser.add_argument("--phase1c-root", default=str(DEFAULT_PHASE1C_ROOT))
    parser.add_argument("--phase1d-root", default=str(DEFAULT_PHASE1D_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--search-root", default=str(DEFAULT_SEARCH_ROOT))
    parser.add_argument("--max-sources", type=int, default=200)
    args = parser.parse_args()
    result = run_phase1e(
        phase1c_root=_safe_path(args.phase1c_root, DEFAULT_PHASE1C_ROOT),
        phase1d_root=_safe_path(args.phase1d_root, DEFAULT_PHASE1D_ROOT),
        output_root=_safe_path(args.output_root, DEFAULT_OUTPUT_ROOT),
        search_root=_safe_path(args.search_root, DEFAULT_SEARCH_ROOT),
        max_sources=args.max_sources,
    )
    print(json.dumps(base._json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
