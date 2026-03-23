from __future__ import annotations

import argparse
import hashlib
import json
import random
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from app.backend.api.dependencies import get_stock_repo
from app.backend.services import tradex_experiment_service as tradex
from app.backend.services.tradex_experiment_store import (
    family_dir,
    family_file,
    load_family,
    load_run,
    resolve_tradex_root,
    run_file,
    write_json,
)


SESSION_SCHEMA_VERSION = "tradex_research_session_v1"
SESSION_COMPARE_SCHEMA_VERSION = "tradex_research_session_compare_v1"
SESSION_REPORT_NAME_PREFIX = "tradex_research_session"
DEFAULT_UNIVERSE_SIZE = 30
DEFAULT_MAX_CANDIDATES_PER_FAMILY = 2


@dataclass(frozen=True)
class CandidateMethodSpec:
    method_family: str
    method_id: str
    method_title: str
    method_thesis: str
    plan_overrides: dict[str, Any]


@dataclass(frozen=True)
class FamilySpec:
    method_family: str
    family_title: str
    family_thesis: str
    candidates: tuple[CandidateMethodSpec, ...]


def _utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _slug(text: str) -> str:
    raw = re.sub(r"[^0-9A-Za-z._-]+", "-", str(text).strip())
    raw = raw.strip("-._")
    return raw or "session"


def _seed_int(session_id: str, random_seed: int) -> int:
    payload = f"{session_id}:{int(random_seed)}"
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return int(digest[:16], 16)


def _json_ready(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    return value


def _text(value: Any, fallback: str = "") -> str:
    if value is None:
        return fallback
    if isinstance(value, str):
        text = value.strip()
        return text or fallback
    text = str(value).strip()
    return text or fallback


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True))
        handle.write("\n")


def _session_root() -> Path:
    root = resolve_tradex_root() / "research_sessions"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _session_dir(session_id: str) -> Path:
    path = _session_root() / _slug(session_id)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _session_state_file(session_id: str) -> Path:
    return _session_dir(session_id) / "session.json"


def _session_compare_file(session_id: str) -> Path:
    return _session_dir(session_id) / "compare.json"


def _session_events_file(session_id: str) -> Path:
    return _session_dir(session_id) / "events.jsonl"


def _session_report_file(session_id: str) -> Path:
    report_dir = tradex.REPO_ROOT / "docs" / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    return report_dir / f"{SESSION_REPORT_NAME_PREFIX}_{_slug(session_id)}.md"


def _session_family_id(session_id: str, method_family: str) -> str:
    return f"tradex-research-{_slug(session_id)}-{_slug(method_family)}"


def _champion_family_id(session_id: str) -> str:
    return f"tradex-research-{_slug(session_id)}-champion"


def _build_family_specs() -> tuple[FamilySpec, ...]:
    return (
        FamilySpec(
            method_family="existing-score rescaled",
            family_title="既存点数の再尺度化",
            family_thesis="既存スコアの差を広げて、上位候補の密度を高める。",
            candidates=(
                CandidateMethodSpec(
                    method_family="existing-score rescaled",
                    method_id="existing_score_rescaled_v1",
                    method_title="既存点数の再尺度化",
                    method_thesis="現行スコアを少し強めに再尺度化して、上位の密度を上げる。",
                    plan_overrides={
                        "minimum_confidence": 0.56,
                        "minimum_ready_rate": 0.45,
                        "signal_bias": "balanced",
                        "top_k": 5,
                        "playbook_up_score_bonus": 0.02,
                        "playbook_down_score_bonus": 0.0,
                    },
                ),
                CandidateMethodSpec(
                    method_family="existing-score rescaled",
                    method_id="existing_score_rescaled_v2",
                    method_title="既存点数の再尺度化強め",
                    method_thesis="再尺度化を少し強めて、点差が小さい候補を落としやすくする。",
                    plan_overrides={
                        "minimum_confidence": 0.60,
                        "minimum_ready_rate": 0.50,
                        "signal_bias": "balanced",
                        "top_k": 5,
                        "playbook_up_score_bonus": 0.03,
                        "playbook_down_score_bonus": 0.0,
                    },
                ),
            ),
        ),
        FamilySpec(
            method_family="penalty-first",
            family_title="減点優先型",
            family_thesis="欠損・未解決・弱い readiness を先に落として、無駄な上位残りを減らす。",
            candidates=(
                CandidateMethodSpec(
                    method_family="penalty-first",
                    method_id="penalty_first_v1",
                    method_title="減点優先型",
                    method_thesis="欠損と未解決を先に強く罰して、上位候補を締める。",
                    plan_overrides={
                        "minimum_confidence": 0.68,
                        "minimum_ready_rate": 0.60,
                        "signal_bias": "balanced",
                        "top_k": 5,
                        "playbook_up_score_bonus": 0.0,
                        "playbook_down_score_bonus": -0.01,
                    },
                ),
                CandidateMethodSpec(
                    method_family="penalty-first",
                    method_id="penalty_first_v2",
                    method_title="減点優先型厳しめ",
                    method_thesis="さらに閾値を上げて、弱い候補を上位に残しにくくする。",
                    plan_overrides={
                        "minimum_confidence": 0.74,
                        "minimum_ready_rate": 0.65,
                        "signal_bias": "balanced",
                        "top_k": 5,
                        "playbook_up_score_bonus": 0.0,
                        "playbook_down_score_bonus": -0.015,
                    },
                ),
            ),
        ),
        FamilySpec(
            method_family="readiness-aware",
            family_title="準備完了優先型",
            family_thesis="ready率が高い銘柄を優先して、gate 通過後の取りこぼしを減らす。",
            candidates=(
                CandidateMethodSpec(
                    method_family="readiness-aware",
                    method_id="readiness_aware_v1",
                    method_title="準備完了優先型",
                    method_thesis="ready率を少し強めに見て、通過後の安定性を上げる。",
                    plan_overrides={
                        "minimum_confidence": 0.58,
                        "minimum_ready_rate": 0.65,
                        "signal_bias": "balanced",
                        "top_k": 5,
                        "playbook_up_score_bonus": 0.01,
                        "playbook_down_score_bonus": 0.0,
                    },
                ),
                CandidateMethodSpec(
                    method_family="readiness-aware",
                    method_id="readiness_aware_v2",
                    method_title="準備完了優先型強め",
                    method_thesis="ready率への寄せをさらに強めて、零通過月を減らしにいく。",
                    plan_overrides={
                        "minimum_confidence": 0.55,
                        "minimum_ready_rate": 0.72,
                        "signal_bias": "balanced",
                        "top_k": 5,
                        "playbook_up_score_bonus": 0.01,
                        "playbook_down_score_bonus": 0.0,
                    },
                ),
            ),
        ),
        FamilySpec(
            method_family="liquidity-aware",
            family_title="流動性ふるい残し",
            family_thesis="買えない候補を上位に残しにくくして、実行可能性を上げる。",
            candidates=(
                CandidateMethodSpec(
                    method_family="liquidity-aware",
                    method_id="liquidity_aware_v1",
                    method_title="流動性ふるい残し",
                    method_thesis="流動性の弱い候補を上位から外しやすくする。",
                    plan_overrides={
                        "minimum_confidence": 0.60,
                        "minimum_ready_rate": 0.50,
                        "signal_bias": "balanced",
                        "top_k": 5,
                        "playbook_up_score_bonus": 0.015,
                        "playbook_down_score_bonus": 0.0,
                    },
                ),
                CandidateMethodSpec(
                    method_family="liquidity-aware",
                    method_id="liquidity_aware_v2",
                    method_title="流動性ふるい残し厳しめ",
                    method_thesis="流動性への寄せを強めて、上位に買いにくい銘柄を残しにくくする。",
                    plan_overrides={
                        "minimum_confidence": 0.60,
                        "minimum_ready_rate": 0.55,
                        "signal_bias": "balanced",
                        "top_k": 5,
                        "playbook_up_score_bonus": 0.025,
                        "playbook_down_score_bonus": 0.0,
                    },
                ),
            ),
        ),
        FamilySpec(
            method_family="regime-aware",
            family_title="逆風回避の順張り",
            family_thesis="相場局面に合わせて、順張りと逆風回避の強さを切り替える。",
            candidates=(
                CandidateMethodSpec(
                    method_family="regime-aware",
                    method_id="regime_aware_v1",
                    method_title="逆風回避の順張り",
                    method_thesis="相場局面を意識して、逆風局面の損失を減らす。",
                    plan_overrides={
                        "minimum_confidence": 0.58,
                        "minimum_ready_rate": 0.50,
                        "signal_bias": "balanced",
                        "top_k": 5,
                        "playbook_up_score_bonus": 0.01,
                        "playbook_down_score_bonus": -0.01,
                    },
                ),
                CandidateMethodSpec(
                    method_family="regime-aware",
                    method_id="regime_aware_v2",
                    method_title="逆風回避の順張り保守",
                    method_thesis="順張り寄りを少し強めて、弱い局面の候補を落としやすくする。",
                    plan_overrides={
                        "minimum_confidence": 0.60,
                        "minimum_ready_rate": 0.48,
                        "signal_bias": "buy",
                        "top_k": 5,
                        "playbook_up_score_bonus": 0.01,
                        "playbook_down_score_bonus": -0.015,
                    },
                ),
            ),
        ),
    )


def _load_session_state(session_id: str) -> dict[str, Any] | None:
    path = _session_state_file(session_id)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _write_session_state(session_id: str, payload: dict[str, Any]) -> None:
    _write_json(_session_state_file(session_id), payload)
    _write_json(_session_compare_file(session_id), payload)


def _build_manifest(
    session_id: str,
    random_seed: int,
    universe: list[str],
    period_segments: list[dict[str, Any]],
    family_specs: tuple[FamilySpec, ...],
    max_candidates_per_family: int,
) -> dict[str, Any]:
    return {
        "schema_version": SESSION_SCHEMA_VERSION,
        "session_id": session_id,
        "random_seed": int(random_seed),
        "seed_int": _seed_int(session_id, random_seed),
        "universe": list(universe),
        "period_segments": list(period_segments),
        "method_families": [
            {
                "method_family": spec.method_family,
                "family_title": spec.family_title,
                "family_thesis": spec.family_thesis,
                "candidate_order": [candidate.method_id for candidate in spec.candidates[:max_candidates_per_family]],
            }
            for spec in family_specs
        ],
        "max_candidates_per_family": int(max_candidates_per_family),
    }


def _build_champion_plan() -> dict[str, Any]:
    return {
        "plan_id": "champion_current_ranking",
        "plan_version": "v1",
        "label": "現行ランキング",
        "method_id": "champion_current_ranking",
        "method_title": "現行ランキング",
        "method_thesis": "現行のTRADEX標準順位をそのまま再現する。",
        "method_family": "champion",
        "minimum_confidence": 0.60,
        "minimum_ready_rate": 0.50,
        "signal_bias": "balanced",
        "top_k": 5,
        "playbook_up_score_bonus": 0.0,
        "playbook_down_score_bonus": 0.0,
        "notes": "session champion baseline",
    }


def _build_family_body(
    *,
    session_id: str,
    random_seed: int,
    universe: list[str],
    period_segments: list[dict[str, Any]],
    family_spec: FamilySpec,
    candidate_specs: list[CandidateMethodSpec],
) -> dict[str, Any]:
    family_id = _session_family_id(session_id, family_spec.method_family)
    candidate_plans = [
        {
            "plan_id": candidate.method_id,
            "plan_version": "v1",
            "label": candidate.method_title,
            "method_id": candidate.method_id,
            "method_title": candidate.method_title,
            "method_thesis": candidate.method_thesis,
            "method_family": candidate.method_family,
            **dict(candidate.plan_overrides),
            "notes": f"{family_spec.method_family}:{candidate.method_id}",
        }
        for candidate in candidate_specs
    ]
    return {
        "family_id": family_id,
        "family_name": f"{family_spec.family_title} / {family_spec.method_family}",
        "universe": list(universe),
        "period": {"segments": list(period_segments)},
        "probes": [],
        "baseline_plan": _build_champion_plan(),
        "candidate_plans": candidate_plans,
        "confirmed_only": True,
        "input_dataset_version": f"session:{session_id}:seed:{int(random_seed)}",
        "code_revision": tradex._git_commit(),
        "timezone": "Asia/Tokyo",
        "price_source": "daily_bars",
        "data_cutoff_at": period_segments[-1]["end_date"] if period_segments else None,
        "random_seed": int(random_seed),
        "notes": family_spec.family_thesis,
    }


def _choose_universe(codes: list[str], *, session_id: str, random_seed: int, universe_size: int) -> list[str]:
    if len(codes) < universe_size:
        raise RuntimeError(f"universe_size={universe_size} exceeds available confirmed codes={len(codes)}")
    rng = random.Random(_seed_int(session_id, random_seed))
    chosen = rng.sample(sorted(codes), universe_size)
    return sorted(chosen)


def _build_period_segments() -> list[dict[str, Any]]:
    regime_rows, issues = tradex._load_evaluation_regime_rows()
    windows, window_issues = tradex._select_evaluation_windows(regime_rows)
    if issues or window_issues or len(windows) < 3:
        raise RuntimeError(
            "evaluation windows unavailable: "
            + ",".join([*(issues or []), *(window_issues or []), "windows<3" if len(windows) < 3 else ""])
        )
    return [
        {
            "label": f"{window['regime_tag']}:{window['evaluation_window_id']}",
            "start_date": window["start_date"],
            "end_date": window["end_date"],
        }
        for window in windows[:3]
    ]


def _seed_family_baseline_from_reference(
    *,
    reference_run: dict[str, Any],
    family_id: str,
    family: dict[str, Any],
) -> dict[str, Any]:
    baseline_run_id = f"{family_id}-baseline"
    baseline_path = run_file(family_id, baseline_run_id)
    if baseline_path.exists():
        loaded = load_run(family_id, baseline_run_id)
        if isinstance(loaded, dict):
            return loaded
    copied = json.loads(json.dumps(reference_run, ensure_ascii=False, default=str))
    copied["family_id"] = family_id
    copied["run_id"] = baseline_run_id
    copied["run_kind"] = "baseline"
    copied["status"] = "succeeded"
    copied["started_at"] = copied.get("started_at") or _utc_now_iso()
    copied["completed_at"] = copied.get("completed_at") or _utc_now_iso()
    copied["notes"] = f"seeded_from:{reference_run.get('family_id')}/{reference_run.get('run_id')}"
    copied["diagnostics_schema_version"] = tradex.TRADEX_DIAGNOSTICS_SCHEMA_VERSION
    write_json(baseline_path, copied)
    family["baseline_run_id"] = baseline_run_id
    family["frozen"] = True
    family["frozen_at"] = family.get("frozen_at") or _utc_now_iso()
    family["run_ids"] = [baseline_run_id]
    family["candidate_run_ids"] = [str(item) for item in family.get("candidate_run_ids") or [] if str(item).strip()]
    tradex._update_family_file(family)
    return copied


def _family_best_key(candidate_result: dict[str, Any]) -> tuple[float, float, float, float, str]:
    evaluation = candidate_result.get("evaluation_summary") if isinstance(candidate_result.get("evaluation_summary"), dict) else {}
    windows = evaluation.get("windows") if isinstance(evaluation.get("windows"), list) else []
    challenger_top5 = float(evaluation.get("challenger_topk_ret20_mean") or 0.0)
    worst_regime_margin = 0.0
    if windows:
        margins: list[float] = []
        for window in windows:
            champion = float(window.get("champion_top5_ret20_mean") or 0.0)
            challenger = float(window.get("challenger_top5_ret20_mean") or 0.0)
            margins.append(challenger - champion)
        worst_regime_margin = min(margins) if margins else 0.0
    return (
        -challenger_top5,
        -worst_regime_margin,
        float(evaluation.get("challenger_dd") or 0.0),
        float(evaluation.get("challenger_turnover") or 0.0),
        str(candidate_result.get("plan_id") or ""),
    )


def _family_result_summary(
    *,
    family_spec: FamilySpec,
    family: dict[str, Any],
    compare: dict[str, Any] | None,
) -> dict[str, Any]:
    candidate_results = compare.get("candidate_results") if isinstance(compare, dict) else []
    candidate_results = [item for item in candidate_results if isinstance(item, dict)]
    ranked_candidates = sorted(candidate_results, key=_family_best_key)
    best_candidate = ranked_candidates[0] if ranked_candidates else None
    return {
        "family_id": family.get("family_id"),
        "method_family": family_spec.method_family,
        "family_title": family_spec.family_title,
        "family_thesis": family_spec.family_thesis,
        "candidate_count": len(candidate_results),
        "candidate_order": [candidate.get("plan_id") for candidate in candidate_results],
        "compare_path": str(family_dir(family["family_id"]) / "compare.json"),
        "compare": compare or {},
        "candidate_results": candidate_results,
        "best_candidate": best_candidate,
        "promote_ready": bool(best_candidate.get("promote_ready")) if best_candidate else False,
        "promote_reasons": best_candidate.get("promote_reasons") if isinstance(best_candidate, dict) else [],
        "best_method_title": (best_candidate.get("candidate_method") or {}).get("method_title") if isinstance(best_candidate, dict) else None,
        "best_method_thesis": (best_candidate.get("candidate_method") or {}).get("method_thesis") if isinstance(best_candidate, dict) else None,
    }


def _train_phase4_ranker(*, family_results: list[dict[str, Any]], random_seed: int) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for family_result in family_results:
        compare = family_result.get("compare") if isinstance(family_result.get("compare"), dict) else {}
        candidates = compare.get("candidate_results") if isinstance(compare.get("candidate_results"), list) else []
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            evaluation = candidate.get("evaluation_summary") if isinstance(candidate.get("evaluation_summary"), dict) else {}
            method = candidate.get("candidate_method") if isinstance(candidate.get("candidate_method"), dict) else {}
            windows = evaluation.get("windows") if isinstance(evaluation.get("windows"), list) else []
            worst_regime_margin = min(
                (
                    float(window.get("challenger_top5_ret20_mean") or 0.0)
                    - float(window.get("champion_top5_ret20_mean") or 0.0)
                    for window in windows
                ),
                default=0.0,
            )
            rows.append(
                {
                    "method_family": str(method.get("method_family") or family_result.get("method_family") or "unknown"),
                    "method_title": str(method.get("method_title") or candidate.get("plan_id") or "unknown"),
                    "promote_ready": bool(evaluation.get("promote_ready")),
                    "top5_mean": float(evaluation.get("challenger_topk_ret20_mean") or 0.0),
                    "top5_median": float(evaluation.get("challenger_topk_ret20_median") or 0.0),
                    "top10_mean": float(evaluation.get("challenger_topk10_ret20_mean") or 0.0),
                    "top10_median": float(evaluation.get("challenger_topk10_ret20_median") or 0.0),
                    "monthly_capture_mean": float(evaluation.get("challenger_monthly_top5_capture_mean") or 0.0),
                    "zero_pass_months": float(evaluation.get("challenger_zero_pass_months") or 0.0),
                    "dd": float(evaluation.get("challenger_dd") or 0.0),
                    "turnover": float(evaluation.get("challenger_turnover") or 0.0),
                    "liquidity_fail_rate": float(evaluation.get("challenger_liquidity_fail_rate") or 0.0),
                    "worst_regime_margin": float(worst_regime_margin),
                }
            )

    if len(rows) < 4:
        return {"status": "skipped", "reason": "insufficient_rows", "row_count": len(rows)}
    labels = {bool(row["promote_ready"]) for row in rows}
    if len(labels) < 2:
        return {"status": "skipped", "reason": "single_class", "row_count": len(rows)}
    try:
        import lightgbm as lgb  # type: ignore
    except Exception as exc:
        return {"status": "skipped", "reason": f"lightgbm_unavailable:{exc.__class__.__name__}", "row_count": len(rows)}

    frame = pd.DataFrame(rows)
    feature_frame = pd.get_dummies(frame.drop(columns=["promote_ready", "method_title"]), columns=["method_family"], dummy_na=False)
    target = frame["promote_ready"].astype(int)
    model = lgb.LGBMClassifier(
        n_estimators=64,
        learning_rate=0.08,
        max_depth=3,
        num_leaves=15,
        random_state=_seed_int("phase4", random_seed),
        n_jobs=1,
    )
    model.fit(feature_frame, target)
    scores = model.predict_proba(feature_frame)[:, 1].tolist()
    ranked = sorted(
        [
            {
                "method_family": row["method_family"],
                "method_title": row["method_title"],
                "score": float(score),
                "promote_ready": bool(row["promote_ready"]),
            }
            for row, score in zip(rows, scores, strict=False)
        ],
        key=lambda item: (-float(item["score"]), item["method_family"], item["method_title"]),
    )
    importance_items = sorted(
        zip(feature_frame.columns, model.feature_importances_, strict=False),
        key=lambda item: (-float(item[1]), str(item[0])),
    )
    return {
        "status": "trained",
        "row_count": len(rows),
        "feature_count": len(feature_frame.columns),
        "feature_columns": list(feature_frame.columns),
        "ranked_candidates": ranked,
        "feature_importances": {str(column): float(importance) for column, importance in importance_items},
    }


def _render_session_report(session_state: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# TRADEX Research Session")
    lines.append("")
    lines.append(f"- session_id: `{session_state.get('session_id')}`")
    lines.append(f"- random_seed: `{session_state.get('random_seed')}`")
    lines.append(f"- manifest_hash: `{session_state.get('manifest_hash')}`")
    lines.append("")
    lines.append("## Champion")
    lines.append("")
    champion = session_state.get("champion") if isinstance(session_state.get("champion"), dict) else {}
    champion_method = champion.get("method") if isinstance(champion.get("method"), dict) else {}
    lines.append(f"- method_title: `{_text(champion_method.get('method_title'))}`")
    lines.append(f"- method_thesis: `{_text(champion_method.get('method_thesis'))}`")
    lines.append(f"- run_id: `{_text(champion.get('run_id'))}`")
    lines.append("")
    lines.append("## Families")
    lines.append("")
    lines.append("| family | best method | top5 mean | median | monthly capture | promote |")
    lines.append("| --- | --- | ---: | ---: | ---: | --- |")
    for family in session_state.get("family_results") or []:
        if not isinstance(family, dict):
            continue
        best = family.get("best_candidate") if isinstance(family.get("best_candidate"), dict) else {}
        evaluation = best.get("evaluation_summary") if isinstance(best.get("evaluation_summary"), dict) else {}
        method = best.get("candidate_method") if isinstance(best.get("candidate_method"), dict) else {}
        lines.append(
            "| {family} | {method} | {top5:.4f} | {median:.4f} | {capture:.4f} | {promote} |".format(
                family=_text(family.get("method_family")),
                method=_text(method.get("method_title"), fallback=_text(best.get("plan_id"))),
                top5=float(evaluation.get("challenger_topk_ret20_mean") or 0.0),
                median=float(evaluation.get("challenger_topk_ret20_median") or 0.0),
                capture=float(evaluation.get("challenger_monthly_top5_capture_mean") or 0.0),
                promote="true" if bool(best.get("promote_ready")) else "false",
            )
        )
        lines.append("- 名前: `{}`".format(_text(method.get('method_title'), fallback=_text(best.get('plan_id')))))
        lines.append(f"  - 仮説: `{_text(method.get('method_thesis'))}`")
        lines.append(f"  - 強い局面: `evaluation_summary.windows` を参照")
        lines.append(f"  - 弱い局面: `{', '.join(best.get('promote_reasons') or []) or 'none'}`")
        lines.append(
            f"  - champion との差分: `{float(evaluation.get('challenger_topk_ret20_mean') or 0.0) - float(evaluation.get('champion_topk_ret20_mean') or 0.0):.4f}`"
        )
    lines.append("")
    lines.append("## Best Result")
    lines.append("")
    best_result = session_state.get("best_result") if isinstance(session_state.get("best_result"), dict) else {}
    best_method = best_result.get("candidate_method") if isinstance(best_result.get("candidate_method"), dict) else {}
    lines.append("- method_title: `{}`".format(_text(best_method.get('method_title'), fallback=_text(best_result.get('plan_id')))))
    lines.append("- method_id: `{}`".format(_text(best_method.get('method_id'), fallback=_text(best_result.get('plan_id')))))
    lines.append(f"- promote_ready: `{bool(best_result.get('promote_ready'))}`")
    lines.append(f"- promote_reasons: `{', '.join(best_result.get('promote_reasons') or []) or 'none'}`")
    lines.append("")
    lines.append("## Phase 4")
    lines.append("")
    phase4 = session_state.get("phase4") if isinstance(session_state.get("phase4"), dict) else {}
    lines.append(f"- status: `{_text(phase4.get('status'), fallback='not_run')}`")
    if phase4.get("reason"):
        lines.append(f"- reason: `{phase4.get('reason')}`")
    ranked_candidates = phase4.get("ranked_candidates") if isinstance(phase4.get("ranked_candidates"), list) else []
    if ranked_candidates:
        lines.append("- ranked_candidates:")
        for item in ranked_candidates[:5]:
            if not isinstance(item, dict):
                continue
            lines.append(
                f"  - `{_text(item.get('method_title'))}` score=`{float(item.get('score') or 0.0):.4f}` promote=`{bool(item.get('promote_ready'))}`"
            )
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- compare artifact が正本で、markdown report は派生物")
    lines.append("- MeeMee にはまだ接続しない")
    lines.append("- best-result は top-K=5 を主評価にし、同点時は worst regime -> DD -> turnover で選んだ")
    return "\n".join(lines).rstrip() + "\n"


def _build_session_state(
    *,
    session_id: str,
    random_seed: int,
    universe: list[str],
    period_segments: list[dict[str, Any]],
    family_specs: tuple[FamilySpec, ...],
    max_candidates_per_family: int,
) -> dict[str, Any]:
    manifest = _build_manifest(session_id, random_seed, universe, period_segments, family_specs, max_candidates_per_family)
    now = _utc_now_iso()
    return {
        "schema_version": SESSION_SCHEMA_VERSION,
        "session_id": session_id,
        "random_seed": int(random_seed),
        "seed_int": manifest["seed_int"],
        "manifest_hash": tradex._stable_hash(manifest),
        "manifest": manifest,
        "created_at": now,
        "updated_at": now,
        "status": "created",
        "phase": "phase1",
        "champion": {},
        "family_results": [],
        "best_result": {},
        "phase4": {},
    }


def run_tradex_research_session(
    *,
    session_id: str,
    random_seed: int,
    universe_size: int = DEFAULT_UNIVERSE_SIZE,
    max_candidates_per_family: int = DEFAULT_MAX_CANDIDATES_PER_FAMILY,
) -> dict[str, Any]:
    family_specs = _build_family_specs()
    max_candidates_per_family = max(1, min(int(max_candidates_per_family), 2))
    repo = get_stock_repo()
    codes = [code for code in repo.get_all_codes() if _text(code)]
    if not codes:
        raise RuntimeError("confirmed universe is empty")
    universe = _choose_universe(codes, session_id=session_id, random_seed=random_seed, universe_size=int(universe_size))
    period_segments = _build_period_segments()
    manifest = _build_manifest(session_id, random_seed, universe, period_segments, family_specs, max_candidates_per_family)
    manifest_hash = tradex._stable_hash(manifest)

    state = _load_session_state(session_id)
    if state:
        stored_hash = _text(state.get("manifest_hash"))
        if stored_hash and stored_hash != manifest_hash:
            raise ValueError("session_id already exists with different manifest; choose a new session_id")
        state = dict(state)
    else:
        state = _build_session_state(
            session_id=session_id,
            random_seed=random_seed,
            universe=universe,
            period_segments=period_segments,
            family_specs=family_specs,
            max_candidates_per_family=max_candidates_per_family,
        )

    if _text(state.get("status")) == "complete" and _text(state.get("manifest_hash")) == manifest_hash:
        return state

    completed_family_results: dict[str, dict[str, Any]] = {}
    for item in state.get("family_results") or []:
        if not isinstance(item, dict):
            continue
        family_id = _text(item.get("family_id"))
        if family_id:
            completed_family_results[family_id] = item

    state["manifest"] = manifest
    state["manifest_hash"] = manifest_hash
    state["updated_at"] = _utc_now_iso()
    state["status"] = "running"
    state["phase"] = "phase1"
    _append_jsonl(
        _session_events_file(session_id),
        {
            "event": "session_started",
            "session_id": session_id,
            "random_seed": int(random_seed),
            "manifest_hash": manifest_hash,
            "at": _utc_now_iso(),
        },
    )
    _write_session_state(session_id, state)

    champion_family_id = _champion_family_id(session_id)
    champion_family = load_family(champion_family_id)
    champion_body = {
        "family_id": champion_family_id,
        "family_name": f"現行ランキング / champion / {session_id}",
        "universe": universe,
        "period": {"segments": period_segments},
        "probes": [],
        "baseline_plan": _build_champion_plan(),
        "candidate_plans": [],
        "confirmed_only": True,
        "input_dataset_version": f"session:{session_id}:champion",
        "code_revision": tradex._git_commit(),
        "timezone": "Asia/Tokyo",
        "price_source": "daily_bars",
        "data_cutoff_at": period_segments[-1]["end_date"],
        "random_seed": int(random_seed),
        "notes": "session champion baseline",
    }
    if not champion_family:
        tradex.create_family(champion_body)
        champion_family = load_family(champion_family_id)
    if not champion_family:
        raise RuntimeError("failed to create champion family")
    champion_run = load_run(champion_family_id, f"{champion_family_id}-baseline")
    if not isinstance(champion_run, dict) or _text(champion_run.get("status")) not in {"succeeded", "compared"}:
        champion_run = tradex.create_run(family_id=champion_family_id, run_kind="baseline", notes="research champion baseline")
    if not isinstance(champion_run, dict):
        raise RuntimeError("failed to materialize champion baseline")
    state["champion"] = {
        "family_id": champion_family_id,
        "run_id": champion_run.get("run_id"),
        "method": {
            "method_id": champion_run.get("method_id"),
            "method_title": champion_run.get("method_title"),
            "method_thesis": champion_run.get("method_thesis"),
            "method_family": champion_run.get("method_family"),
        },
        "selection_summary": champion_run.get("selection_summary") if isinstance(champion_run.get("selection_summary"), dict) else {},
        "readiness_summary": champion_run.get("readiness_summary") if isinstance(champion_run.get("readiness_summary"), dict) else {},
        "waterfall_summary": champion_run.get("waterfall_summary") if isinstance(champion_run.get("waterfall_summary"), dict) else {},
        "diagnostics_schema_version": champion_run.get("diagnostics_schema_version"),
    }
    _write_session_state(session_id, state)

    family_results: list[dict[str, Any]] = []
    for family_spec in family_specs:
        candidate_specs = list(family_spec.candidates[:max_candidates_per_family])
        family_id = _session_family_id(session_id, family_spec.method_family)
        family_body = _build_family_body(
            session_id=session_id,
            random_seed=random_seed,
            universe=universe,
            period_segments=period_segments,
            family_spec=family_spec,
            candidate_specs=candidate_specs,
        )
        family = load_family(family_id)
        if not family:
            tradex.create_family(family_body)
            family = load_family(family_id)
        if not family:
            raise RuntimeError(f"failed to create family: {family_id}")
        existing_family_result = completed_family_results.get(family_id)
        if isinstance(existing_family_result, dict):
            family_results.append(existing_family_result)
            state["family_results"] = family_results
            state["updated_at"] = _utc_now_iso()
            state["phase"] = "phase3"
            _write_session_state(session_id, state)
            _append_jsonl(
                _session_events_file(session_id),
                {
                    "event": "family_resumed",
                    "session_id": session_id,
                    "family_id": family_id,
                    "method_family": family_spec.method_family,
                    "best_method_title": existing_family_result.get("best_method_title"),
                    "promote_ready": existing_family_result.get("promote_ready"),
                    "at": _utc_now_iso(),
                },
            )
            continue
        baseline_run = load_run(family_id, f"{family_id}-baseline")
        if not isinstance(baseline_run, dict) or _text(baseline_run.get("status")) not in {"succeeded", "compared"}:
            baseline_run = _seed_family_baseline_from_reference(reference_run=champion_run, family_id=family_id, family=family)
        if not isinstance(baseline_run, dict):
            raise RuntimeError(f"failed to seed baseline for family: {family_id}")

        for candidate_spec in candidate_specs:
            candidate_run_id = f"{family_id}-{candidate_spec.method_id}"
            candidate_run_path = run_file(family_id, candidate_run_id)
            candidate_run = load_run(family_id, candidate_run_id)
            if candidate_run_path.exists():
                if not isinstance(candidate_run, dict):
                    raise RuntimeError(f"existing candidate run is unreadable: {candidate_run_path}")
            elif not isinstance(candidate_run, dict) or _text(candidate_run.get("status")) not in {"succeeded", "compared", "adopt_candidate", "rejected"}:
                candidate_run = tradex.create_run(
                    family_id=family_id,
                    run_kind="candidate",
                    plan_id=candidate_spec.method_id,
                    notes=candidate_spec.method_title,
                )
            if not isinstance(candidate_run, dict):
                raise RuntimeError(f"failed to run candidate {candidate_spec.method_id}")

        compare = tradex.get_family_compare(family_id)
        if not isinstance(compare, dict):
            compare = {}
        family_result = _family_result_summary(family_spec=family_spec, family=family, compare=compare)
        family_results.append(family_result)
        state["family_results"] = family_results
        state["updated_at"] = _utc_now_iso()
        state["phase"] = "phase3"
        _write_session_state(session_id, state)
        _append_jsonl(
            _session_events_file(session_id),
            {
                "event": "family_complete",
                "session_id": session_id,
                "family_id": family_id,
                "method_family": family_spec.method_family,
                "best_method_title": family_result.get("best_method_title"),
                "promote_ready": family_result.get("promote_ready"),
                "at": _utc_now_iso(),
            },
        )

    best_candidates = [item.get("best_candidate") for item in family_results if isinstance(item.get("best_candidate"), dict)]
    best_result = sorted(best_candidates, key=_family_best_key)[0] if best_candidates else {}
    state["best_result"] = best_result if isinstance(best_result, dict) else {}
    state["phase"] = "phase4" if bool(state["best_result"]) and bool(state["best_result"].get("promote_ready")) else "complete"

    if bool(state["best_result"]) and bool(state["best_result"].get("promote_ready")):
        state["phase4"] = _train_phase4_ranker(family_results=family_results, random_seed=random_seed)
    else:
        state["phase4"] = {"status": "skipped", "reason": "no_promote_ready_winner"}
    state["status"] = "complete"
    state["completed_at"] = _utc_now_iso()
    state["updated_at"] = _utc_now_iso()
    state["compare_schema_version"] = SESSION_COMPARE_SCHEMA_VERSION
    _write_session_state(session_id, state)
    _append_jsonl(
        _session_events_file(session_id),
        {
            "event": "session_complete",
            "session_id": session_id,
            "best_method_title": _text((state.get("best_result") or {}).get("candidate_method", {}).get("method_title")),
            "phase4_status": _text((state.get("phase4") or {}).get("status"), fallback="skipped"),
            "at": _utc_now_iso(),
        },
    )

    report_path = _session_report_file(session_id)
    report_path.write_text(_render_session_report(state), encoding="utf-8")
    state["report_path"] = str(report_path)
    _write_session_state(session_id, state)
    return state


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a single-machine TRADEX research session.")
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--random-seed", required=True, type=int)
    parser.add_argument("--universe-size", type=int, default=DEFAULT_UNIVERSE_SIZE)
    parser.add_argument("--max-candidates-per-family", type=int, default=DEFAULT_MAX_CANDIDATES_PER_FAMILY)
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    result = run_tradex_research_session(
        session_id=str(args.session_id),
        random_seed=int(args.random_seed),
        universe_size=int(args.universe_size),
        max_candidates_per_family=int(args.max_candidates_per_family),
    )
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
