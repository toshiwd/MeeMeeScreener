from __future__ import annotations

import json
from typing import Any

import numpy as np
import pandas as pd

from research.config import ResearchConfig
from research.study_build import build_study_dataset
from research.study_scoring import adoption_gate, evaluate_trial, retention_gate, write_fold_artifacts
from research.study_search_space import generate_base_trials, generate_refinement_trials
from research.study_storage import (
    ADOPTED_HYPOTHESES_FILE,
    TOP_HYPOTHESES_FILE,
    dataset_path,
    find_latest_resume_study,
    init_trial_state,
    load_dataset_meta,
    load_trial_state,
    save_trial_state,
    set_combo_state,
    study_paths,
    update_study_manifest,
    write_frame,
    write_json_payload,
)
from research.storage import ResearchPaths, read_csv, read_json


def _coerce_bool_series(series: pd.Series) -> pd.Series:
    normalized = series.astype(str).str.strip().str.lower()
    return normalized.isin({"1", "true", "t", "yes", "y"})


def _normalize_result_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    out = frame.copy()
    for col in ("retained", "adopted", "is_pareto"):
        if col in out.columns:
            out[col] = _coerce_bool_series(out[col])
    for col in (
        "oos_return",
        "profit_factor",
        "positive_window_ratio",
        "worst_drawdown",
        "stability",
        "cluster_consistency",
        "study_score",
    ):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def _pareto_flags(frame: pd.DataFrame) -> pd.Series:
    if frame.empty:
        return pd.Series(dtype=bool)
    flags = pd.Series(True, index=frame.index, dtype=bool)
    for idx, row in frame.iterrows():
        dominated = False
        for jdx, other in frame.iterrows():
            if idx == jdx:
                continue
            better_or_equal = (
                float(other["oos_return"]) >= float(row["oos_return"])
                and float(other["profit_factor"]) >= float(row["profit_factor"])
                and float(other["positive_window_ratio"]) >= float(row["positive_window_ratio"])
                and float(other["stability"]) >= float(row["stability"])
                and float(other["cluster_consistency"]) >= float(row["cluster_consistency"])
                and float(other["worst_drawdown"]) <= float(row["worst_drawdown"])
            )
            strictly_better = (
                float(other["oos_return"]) > float(row["oos_return"])
                or float(other["profit_factor"]) > float(row["profit_factor"])
                or float(other["positive_window_ratio"]) > float(row["positive_window_ratio"])
                or float(other["stability"]) > float(row["stability"])
                or float(other["cluster_consistency"]) > float(row["cluster_consistency"])
                or float(other["worst_drawdown"]) < float(row["worst_drawdown"])
            )
            if better_or_equal and strictly_better:
                dominated = True
                break
        flags.loc[idx] = not dominated
    return flags


def _zscore(series: pd.Series, invert: bool = False) -> pd.Series:
    vals = pd.to_numeric(series, errors="coerce").fillna(0.0)
    std = float(vals.std())
    if std <= 1e-9:
        out = pd.Series(0.0, index=vals.index, dtype=float)
    else:
        out = (vals - float(vals.mean())) / std
    return -out if invert else out


def _rank_trials(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    frame = _normalize_result_frame(frame)
    out_parts: list[pd.DataFrame] = []
    for (timeframe, family), grp in frame.groupby(["timeframe", "family"], dropna=False):
        work = grp.copy().reset_index(drop=True)
        work["is_pareto"] = _pareto_flags(work)
        work["study_score"] = (
            0.25 * _zscore(work["oos_return"])
            + 0.20 * _zscore(work["profit_factor"])
            + 0.20 * _zscore(work["positive_window_ratio"])
            + 0.15 * _zscore(work["stability"])
            + 0.10 * _zscore(work["cluster_consistency"])
            + 0.10 * _zscore(work["worst_drawdown"], invert=True)
        )
        work = work.sort_values(["is_pareto", "study_score"], ascending=[False, False]).reset_index(drop=True)
        work["rank_within_combo"] = np.arange(1, len(work) + 1, dtype=int)
        out_parts.append(work)
    return pd.concat(out_parts, ignore_index=True)


def _distribution_horizon(selected_rows: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if selected_rows.empty:
        return pd.DataFrame(columns=["trial_id", "timeframe", "family", "horizon", "mean", "median", "win_rate", "samples"])
    value_cols = [col for col in selected_rows.columns if col.startswith("window_pnl_h")]
    for (trial_id, timeframe, family), grp in selected_rows.groupby(["trial_id", "timeframe", "family"], dropna=False):
        for col in value_cols:
            horizon = col.replace("window_pnl_h", "")
            vals = pd.to_numeric(grp[col], errors="coerce").dropna()
            if vals.empty:
                continue
            rows.append(
                {
                    "trial_id": trial_id,
                    "timeframe": timeframe,
                    "family": family,
                    "horizon": horizon,
                    "mean": float(vals.mean()),
                    "median": float(vals.median()),
                    "win_rate": float(np.mean(vals > 0.0)),
                    "samples": int(len(vals)),
                }
            )
    return pd.DataFrame(rows)


def _distribution_group(selected_rows: pd.DataFrame, group_col: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if selected_rows.empty or group_col not in selected_rows.columns:
        return pd.DataFrame(columns=["trial_id", "timeframe", "family", "group_col", "group_key", "mean_primary_return", "samples"])
    for (trial_id, timeframe, family), grp in selected_rows.groupby(["trial_id", "timeframe", "family"], dropna=False):
        for key, g in grp.groupby(group_col, dropna=False):
            vals = pd.to_numeric(g["primary_return"], errors="coerce").dropna()
            if vals.empty:
                continue
            rows.append(
                {
                    "trial_id": trial_id,
                    "timeframe": timeframe,
                    "family": family,
                    "group_col": group_col,
                    "group_key": str(key),
                    "mean_primary_return": float(vals.mean()),
                    "samples": int(len(vals)),
                }
            )
    return pd.DataFrame(rows)


def _top_payload(frame: pd.DataFrame) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    for row in frame.to_dict(orient="records"):
        item = dict(row)
        try:
            item["params"] = json.loads(str(item.get("params_json") or "{}"))
        except Exception:
            item["params"] = {}
        items.append(item)
    return {
        "generated_at": pd.Timestamp.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "items": items,
    }


def _resolve_parent_trials(
    combo_frame: pd.DataFrame,
    preferred_ids: list[str],
    *,
    fallback_limit: int,
) -> list[dict[str, Any]]:
    if combo_frame.empty:
        return []
    ranked = combo_frame.sort_values(["retained", "oos_return"], ascending=[False, False]).reset_index(drop=True)
    if preferred_ids:
        lookup = {
            str(row.get("trial_id")): row
            for row in ranked.to_dict(orient="records")
        }
        ordered = [lookup[trial_id] for trial_id in preferred_ids if trial_id in lookup]
        if ordered:
            return ordered
    return ranked.head(fallback_limit).to_dict(orient="records")


def _load_existing_trace(paths: ResearchPaths, study_id: str) -> pd.DataFrame:
    trace_path = study_paths(paths, study_id)["search_trace"]
    if not trace_path.exists():
        return pd.DataFrame()
    return _normalize_result_frame(read_csv(trace_path))


def _load_dataset(paths: ResearchPaths, study_id: str, timeframe: str) -> pd.DataFrame:
    frame = read_csv(dataset_path(paths, study_id, timeframe))
    frame["event_date"] = pd.to_datetime(frame["event_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    frame["month_bucket"] = frame["month_bucket"].astype(str)
    return frame


def _commit_trial(
    results: list[dict[str, Any]],
    selected_parts: list[pd.DataFrame],
    bad_rows: list[dict[str, Any]],
    paths: ResearchPaths,
    study_id: str,
    timeframe: str,
    family: str,
    trial_id: str,
    params: dict[str, Any],
    evaluation: Any,
    config: ResearchConfig,
) -> None:
    summary = dict(evaluation.summary)
    summary["trial_id"] = trial_id
    summary["params_json"] = json.dumps(params, ensure_ascii=False, sort_keys=True)
    summary["timeframe"] = timeframe
    summary["family"] = family

    retained, retained_reasons = retention_gate(summary, config)
    adopted, adopted_reasons = adoption_gate(summary, config)
    summary["retained"] = bool(retained)
    summary["adopted"] = bool(retained and adopted)
    summary["retention_failed_reasons"] = ",".join(retained_reasons)
    summary["adoption_failed_reasons"] = ",".join(adopted_reasons if retained else retained_reasons)
    results.append(summary)

    if retained:
        selected = evaluation.selected_rows.copy()
        if not selected.empty:
            selected["trial_id"] = trial_id
            selected["timeframe"] = timeframe
            selected["family"] = family
            selected_parts.append(selected)
        write_fold_artifacts(paths, study_id, trial_id, evaluation.fold_artifacts)
    else:
        bad_rows.append(
            {
                "trial_id": trial_id,
                "timeframe": timeframe,
                "family": family,
                "samples": int(summary.get("samples", 0)),
                "oos_return": float(summary.get("oos_return", 0.0)),
                "profit_factor": float(summary.get("profit_factor", 0.0)),
                "positive_window_ratio": float(summary.get("positive_window_ratio", 0.0)),
                "worst_drawdown": float(summary.get("worst_drawdown", 0.0)),
                "reasons": ",".join(retained_reasons),
            }
        )


def _build_trial_summary(
    timeframe: str,
    family: str,
    trial_id: str,
    params: dict[str, Any],
    evaluation: Any,
    config: ResearchConfig,
) -> dict[str, Any]:
    summary = dict(evaluation.summary)
    summary["trial_id"] = trial_id
    summary["params_json"] = json.dumps(params, ensure_ascii=False, sort_keys=True)
    summary["timeframe"] = timeframe
    summary["family"] = family

    retained, retained_reasons = retention_gate(summary, config)
    adopted, adopted_reasons = adoption_gate(summary, config)
    summary["retained"] = bool(retained)
    summary["adopted"] = bool(retained and adopted)
    summary["retention_failed_reasons"] = ",".join(retained_reasons)
    summary["adoption_failed_reasons"] = ",".join(adopted_reasons if retained else retained_reasons)
    return summary


def _append_summary_rows(
    results: list[dict[str, Any]],
    bad_rows: list[dict[str, Any]],
    summary: dict[str, Any],
) -> None:
    results.append(summary)
    if bool(summary.get("retained")):
        return
    bad_rows.append(
        {
            "trial_id": str(summary.get("trial_id") or ""),
            "timeframe": str(summary.get("timeframe") or ""),
            "family": str(summary.get("family") or ""),
            "samples": int(summary.get("samples", 0)),
            "oos_return": float(summary.get("oos_return", 0.0)),
            "profit_factor": float(summary.get("profit_factor", 0.0)),
            "positive_window_ratio": float(summary.get("positive_window_ratio", 0.0)),
            "worst_drawdown": float(summary.get("worst_drawdown", 0.0)),
            "reasons": str(summary.get("retention_failed_reasons") or ""),
        }
    )


def _result_trial_ids(results: list[dict[str, Any]], timeframe: str, family: str) -> set[str]:
    out: set[str] = set()
    for row in results:
        if str(row.get("timeframe") or "") != timeframe or str(row.get("family") or "") != family:
            continue
        trial_id = str(row.get("trial_id") or "").strip()
        if trial_id:
            out.add(trial_id)
    return out


def _replay_completed_trials(
    *,
    results: list[dict[str, Any]],
    bad_rows: list[dict[str, Any]],
    dataset: pd.DataFrame,
    timeframe: str,
    family: str,
    combo: dict[str, Any],
    config: ResearchConfig,
) -> int:
    rebuilt = 0
    existing_ids = _result_trial_ids(results, timeframe, family)
    base_ids = [str(item).strip() for item in combo.get("base_completed_ids", []) if str(item).strip()]
    refine_ids = [str(item).strip() for item in combo.get("refine_completed_ids", []) if str(item).strip()]
    if not base_ids and not refine_ids:
        return 0

    base_trials = generate_base_trials(
        config,
        timeframe,
        family,
        completed_hashes=set(),
        target_count=max(len(base_ids), int(config.study.trials_per_family.get(timeframe, 0))),
    )
    base_map = {str(trial["param_hash"]): trial for trial in base_trials}

    for trial_id in base_ids:
        if trial_id in existing_ids:
            continue
        trial = base_map.get(trial_id)
        if trial is None:
            raise ValueError(f"resume reconstruction failed: missing base params for trial_id={trial_id}")
        evaluation = evaluate_trial(dataset.copy(), config, timeframe, family, trial["params"])
        summary = _build_trial_summary(timeframe, family, trial_id, trial["params"], evaluation, config)
        _append_summary_rows(results, bad_rows, summary)
        existing_ids.add(trial_id)
        rebuilt += 1

    preferred_ids = [str(item).strip() for item in combo.get("best_trial_ids", []) if str(item).strip()]
    base_frame = pd.DataFrame(
        [
            row
            for row in results
            if str(row.get("timeframe") or "") == timeframe
            and str(row.get("family") or "") == family
            and str(row.get("trial_id") or "") in set(base_ids)
        ]
    )
    ranked_base_frame = _rank_trials(base_frame)
    parent_trials = _resolve_parent_trials(
        ranked_base_frame,
        preferred_ids,
        fallback_limit=max(1, int(config.study.top_refinement_parents)),
    )
    if not preferred_ids:
        combo["best_trial_ids"] = [str(item.get("trial_id") or "") for item in parent_trials if str(item.get("trial_id") or "")]

    if not refine_ids:
        return rebuilt

    refine_trials = generate_refinement_trials(
        config,
        timeframe,
        family,
        parent_trials,
        completed_hashes=set(base_ids),
        target_count=max(
            len(refine_ids),
            len(combo.get("queued_refinements", [])),
            int(config.study.refinement_trials_per_family.get(timeframe, 0)),
        ),
    )
    refine_map = {str(trial["param_hash"]): trial for trial in refine_trials}
    for trial_id in refine_ids:
        if trial_id in existing_ids:
            continue
        trial = refine_map.get(trial_id)
        if trial is None:
            raise ValueError(f"resume reconstruction failed: missing refine params for trial_id={trial_id}")
        evaluation = evaluate_trial(dataset.copy(), config, timeframe, family, trial["params"])
        summary = _build_trial_summary(timeframe, family, trial_id, trial["params"], evaluation, config)
        _append_summary_rows(results, bad_rows, summary)
        existing_ids.add(trial_id)
        rebuilt += 1
    return rebuilt


def run_study_search(
    paths: ResearchPaths,
    config: ResearchConfig,
    study_id: str | None = None,
    *,
    snapshot_id: str | None = None,
    resume: bool = False,
    timeframes: tuple[str, ...] | None = None,
    families: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    resolved_study_id = study_id or find_latest_resume_study(paths, snapshot_id=snapshot_id)
    if not resolved_study_id:
        raise FileNotFoundError("no resumable study found")

    manifest = read_json(study_paths(paths, resolved_study_id)["manifest"])
    meta = load_dataset_meta(paths, resolved_study_id)
    selected_timeframes = tuple(timeframes or manifest.get("timeframes") or config.study.timeframes)
    selected_families = tuple(families or manifest.get("families") or config.study.families)
    state = init_trial_state(paths, resolved_study_id, timeframes=selected_timeframes, families=selected_families)
    if resume:
        state = load_trial_state(paths, resolved_study_id)

    existing = _load_existing_trace(paths, resolved_study_id)
    results = existing.to_dict(orient="records") if not existing.empty else []
    selected_parts: list[pd.DataFrame] = []
    bad_rows: list[dict[str, Any]] = []
    dataset_cache: dict[str, pd.DataFrame] = {}

    for timeframe in selected_timeframes:
        if timeframe not in (meta.get("datasets") or {}):
            continue
        dataset = _load_dataset(paths, resolved_study_id, timeframe)
        dataset_cache[timeframe] = dataset
        for family in selected_families:
            combo_key = f"{timeframe}::{family}"
            combo = state.setdefault("combos", {}).setdefault(
                combo_key,
                {
                    "base_completed_ids": [],
                    "refine_completed_ids": [],
                    "queued_refinements": [],
                    "seen_param_hashes": [],
                    "best_trial_ids": [],
                    "status": "pending",
                },
            )
            if resume and str(combo.get("status", "")) == "completed":
                continue
            rebuilt = 0
            if resume:
                rebuilt = _replay_completed_trials(
                    results=results,
                    bad_rows=bad_rows,
                    dataset=dataset,
                    timeframe=timeframe,
                    family=family,
                    combo=combo,
                    config=config,
                )
                if rebuilt > 0:
                    save_trial_state(paths, resolved_study_id, state)
                    write_frame(paths, resolved_study_id, "search_trace", _rank_trials(pd.DataFrame(results)))
            seen_hashes = set(str(x) for x in combo.get("seen_param_hashes", []))
            base_target = max(
                0,
                int(config.study.trials_per_family.get(timeframe, 0)) - len(combo.get("base_completed_ids", [])),
            )
            base_trials = generate_base_trials(
                config,
                timeframe,
                family,
                completed_hashes=seen_hashes,
                target_count=base_target,
            )
            if base_trials:
                set_combo_state(state, timeframe, family, {"status": "running_base"})
            for trial in base_trials:
                trial_id = str(trial["param_hash"])
                evaluation = evaluate_trial(dataset.copy(), config, timeframe, family, trial["params"])
                _commit_trial(
                    results,
                    selected_parts,
                    bad_rows,
                    paths,
                    resolved_study_id,
                    timeframe,
                    family,
                    trial_id,
                    trial["params"],
                    evaluation,
                    config,
                )
                combo.setdefault("base_completed_ids", []).append(trial_id)
                combo.setdefault("seen_param_hashes", []).append(trial_id)
                save_trial_state(paths, resolved_study_id, state)

            combo_frame = pd.DataFrame(
                [
                    row
                    for row in results
                    if str(row.get("timeframe")) == timeframe and str(row.get("family")) == family
                ]
            )
            combo_frame = _rank_trials(combo_frame)
            parent_limit = max(1, int(config.study.top_refinement_parents))
            parent_trials = _resolve_parent_trials(
                combo_frame,
                [str(item) for item in combo.get("best_trial_ids", [])],
                fallback_limit=parent_limit,
            )
            combo["best_trial_ids"] = [str(item.get("trial_id")) for item in parent_trials]
            save_trial_state(paths, resolved_study_id, state)
            refine_target = max(
                0,
                int(config.study.refinement_trials_per_family.get(timeframe, 0))
                - len(combo.get("refine_completed_ids", [])),
            )
            refine_trials = generate_refinement_trials(
                config,
                timeframe,
                family,
                parent_trials,
                completed_hashes=set(str(x) for x in combo.get("seen_param_hashes", [])),
                target_count=refine_target,
            )
            if refine_trials:
                set_combo_state(
                    state,
                    timeframe,
                    family,
                    {
                        "status": "running_refine",
                        "queued_refinements": [str(item["param_hash"]) for item in refine_trials],
                    },
                )
            for trial in refine_trials:
                trial_id = str(trial["param_hash"])
                evaluation = evaluate_trial(dataset.copy(), config, timeframe, family, trial["params"])
                _commit_trial(
                    results,
                    selected_parts,
                    bad_rows,
                    paths,
                    resolved_study_id,
                    timeframe,
                    family,
                    trial_id,
                    trial["params"],
                    evaluation,
                    config,
                )
                combo.setdefault("refine_completed_ids", []).append(trial_id)
                combo.setdefault("seen_param_hashes", []).append(trial_id)
                save_trial_state(paths, resolved_study_id, state)

            combo_ranked = _rank_trials(
                pd.DataFrame(
                    [
                        row
                        for row in results
                        if str(row.get("timeframe")) == timeframe and str(row.get("family")) == family
                    ]
                )
            )
            combo["best_trial_ids"] = combo_ranked.head(
                int(config.study.retention_gates.top_hypotheses_per_combo)
            )["trial_id"].astype(str).tolist() if "trial_id" in combo_ranked.columns else []
            combo["queued_refinements"] = []
            combo["status"] = "completed"
            save_trial_state(paths, resolved_study_id, state)

    result_frame = _rank_trials(pd.DataFrame(results))
    if result_frame.empty:
        raise ValueError(f"no completed study trials for study_id={resolved_study_id}")

    top_limit = int(config.study.retention_gates.top_hypotheses_per_combo)
    top_frame = (
        result_frame.groupby(["timeframe", "family"], as_index=False, group_keys=False)
        .head(top_limit)
        .reset_index(drop=True)
    )
    adopted_frame = top_frame[_coerce_bool_series(top_frame["adopted"])].reset_index(drop=True)

    top_selected_parts: list[pd.DataFrame] = []
    for row in top_frame.to_dict(orient="records"):
        timeframe = str(row.get("timeframe"))
        family = str(row.get("family"))
        trial_id = str(row.get("trial_id"))
        try:
            params = json.loads(str(row.get("params_json") or "{}"))
        except Exception:
            params = {}
        dataset = dataset_cache.get(timeframe)
        if dataset is None:
            dataset = _load_dataset(paths, resolved_study_id, timeframe)
            dataset_cache[timeframe] = dataset
        evaluation = evaluate_trial(dataset.copy(), config, timeframe, family, params)
        selected = evaluation.selected_rows.copy()
        if selected.empty:
            continue
        selected["trial_id"] = trial_id
        selected["timeframe"] = timeframe
        selected["family"] = family
        top_selected_parts.append(selected)
        write_fold_artifacts(paths, resolved_study_id, trial_id, evaluation.fold_artifacts)

    selected_retained = pd.concat(top_selected_parts, ignore_index=True) if top_selected_parts else pd.DataFrame()
    dist_horizon = _distribution_horizon(selected_retained)
    dist_cluster = _distribution_group(selected_retained, "cluster_key")
    dist_regime = _distribution_group(selected_retained, "regime_key")
    bad_frame = result_frame[~_coerce_bool_series(result_frame["retained"])].copy()

    write_frame(paths, resolved_study_id, "search_trace", result_frame)
    write_frame(paths, resolved_study_id, "oos_metrics", result_frame)
    write_frame(paths, resolved_study_id, "distribution_by_horizon", dist_horizon)
    write_frame(paths, resolved_study_id, "distribution_by_cluster", dist_cluster)
    write_frame(paths, resolved_study_id, "distribution_by_regime", dist_regime)
    write_frame(paths, resolved_study_id, "bad_hypotheses_summary", bad_frame if not bad_frame.empty else pd.DataFrame(bad_rows))
    write_json_payload(paths, resolved_study_id, "top_hypotheses", _top_payload(top_frame))
    write_json_payload(paths, resolved_study_id, "adopted_hypotheses", _top_payload(adopted_frame))

    update_study_manifest(
        paths,
        resolved_study_id,
        {
            "status": "searched",
            "timeframes": list(selected_timeframes),
            "families": list(selected_families),
            "top_hypotheses_file": TOP_HYPOTHESES_FILE,
            "adopted_hypotheses_file": ADOPTED_HYPOTHESES_FILE,
        },
    )

    return {
        "ok": True,
        "study_id": resolved_study_id,
        "trials": int(len(result_frame)),
        "top_hypotheses": int(len(top_frame)),
        "adopted_hypotheses": int(len(adopted_frame)),
        "timeframes": list(selected_timeframes),
        "families": list(selected_families),
    }


def run_study_loop(
    paths: ResearchPaths,
    config: ResearchConfig,
    snapshot_id: str,
    *,
    timeframes: tuple[str, ...],
    families: tuple[str, ...],
    resume: bool = False,
    study_id: str | None = None,
) -> dict[str, Any]:
    resolved_study_id = study_id
    if resume and not resolved_study_id:
        resolved_study_id = find_latest_resume_study(paths, snapshot_id=snapshot_id)
    if not resolved_study_id:
        resolved_study_id = paths.next_study_id(snapshot_id=snapshot_id)

    sdir = study_paths(paths, resolved_study_id)
    existing_meta = read_json(sdir["dataset_meta"]) if sdir["dataset_meta"].exists() else {"datasets": {}}
    datasets_meta = existing_meta.get("datasets") if isinstance(existing_meta, dict) else {}

    start_date = None
    end_date = None
    snapshot_daily = read_csv(paths.snapshot_dir(snapshot_id) / "daily.csv")
    if not snapshot_daily.empty:
        date_series = pd.to_datetime(snapshot_daily["date"], errors="coerce").dropna().sort_values()
        if not date_series.empty:
            start_date = date_series.min().strftime("%Y-%m-%d")
            end_date = date_series.max().strftime("%Y-%m-%d")
    if not start_date or not end_date:
        raise ValueError(f"snapshot {snapshot_id} has no usable daily rows")

    built: list[dict[str, Any]] = []
    for timeframe in timeframes:
        already = timeframe in (datasets_meta or {}) and dataset_path(paths, resolved_study_id, timeframe).exists()
        if resume and already:
            continue
        built.append(
            build_study_dataset(
                paths=paths,
                config=config,
                snapshot_id=snapshot_id,
                timeframe=timeframe,
                start_date=start_date,
                end_date=end_date,
                study_id=resolved_study_id,
            )
        )

    search_result = run_study_search(
        paths=paths,
        config=config,
        study_id=resolved_study_id,
        snapshot_id=snapshot_id,
        resume=resume,
        timeframes=timeframes,
        families=families,
    )
    update_study_manifest(paths, resolved_study_id, {"status": "completed"})
    return {
        "ok": True,
        "study_id": resolved_study_id,
        "built": built,
        "search": search_result,
    }
