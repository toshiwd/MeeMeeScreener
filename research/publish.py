from __future__ import annotations

from typing import Any

import pandas as pd

from research.config import ResearchConfig
from research.storage import ResearchPaths, now_utc_iso, read_csv, read_json, write_csv, write_json


REQUIRED_COLUMNS: tuple[str, ...] = (
    "asof_date",
    "code",
    "score",
    "pred_return",
    "pred_prob_tp",
    "risk_dn",
    "model_version",
    "feature_version",
    "label_version",
    "run_id",
    "created_at",
)

DEFAULT_QUALITY_GATE: dict[str, float | int] = {
    "min_test_overall_return_at20": 0.0030,
    "min_test_long_return_at20": 0.0150,
    "min_test_short_return_at20": -0.0100,
    "max_test_risk_mae_p90": 0.1200,
    "min_test_months": 6,
}


def _as_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


def _as_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return int(default)


def _detect_active_regime(top20_long: pd.DataFrame, top20_short: pd.DataFrame) -> dict[str, Any]:
    parts: list[pd.DataFrame] = []
    for frame in (top20_long, top20_short):
        if frame is None or frame.empty:
            continue
        tmp = frame.copy()
        if "regime_key" not in tmp.columns:
            continue
        if "phase" in tmp.columns:
            inf = tmp[tmp["phase"].astype(str).str.lower() == "inference"].copy()
            if not inf.empty:
                tmp = inf
        if "asof_date" in tmp.columns:
            max_asof = pd.to_datetime(tmp["asof_date"], errors="coerce").max()
            if pd.notna(max_asof):
                tmp = tmp[pd.to_datetime(tmp["asof_date"], errors="coerce") == max_asof]
        parts.append(tmp[["regime_key"]].copy())

    if not parts:
        return {"detected": False, "active_regime_key": None, "active_regime_prefix": None, "samples": 0}

    merged = pd.concat(parts, ignore_index=True)
    merged["regime_key"] = merged["regime_key"].astype(str).str.strip()
    merged = merged[merged["regime_key"] != ""]
    if merged.empty:
        return {"detected": False, "active_regime_key": None, "active_regime_prefix": None, "samples": 0}

    counts = merged["regime_key"].value_counts()
    key = str(counts.index[0])
    prefix = key.split("_")[0] if "_" in key else key
    return {
        "detected": True,
        "active_regime_key": key,
        "active_regime_prefix": prefix,
        "samples": int(len(merged)),
    }


def _gate_thresholds(
    config: ResearchConfig | None,
    active_regime: dict[str, Any] | None = None,
) -> tuple[dict[str, float | int], str | None]:
    if config is None:
        return dict(DEFAULT_QUALITY_GATE), None
    pg = config.publish_gate
    thresholds: dict[str, float | int] = {
        "min_test_overall_return_at20": float(pg.min_test_overall_return_at20),
        "min_test_long_return_at20": float(pg.min_test_long_return_at20),
        "min_test_short_return_at20": float(pg.min_test_short_return_at20),
        "max_test_risk_mae_p90": float(pg.max_test_risk_mae_p90),
        "min_test_months": int(pg.min_test_months),
    }
    overrides = pg.regime_overrides if isinstance(pg.regime_overrides, dict) else {}
    if not overrides:
        return thresholds, None

    active_key = (
        str(active_regime.get("active_regime_key")).strip()
        if isinstance(active_regime, dict) and active_regime.get("active_regime_key") is not None
        else ""
    )
    active_prefix = (
        str(active_regime.get("active_regime_prefix")).strip()
        if isinstance(active_regime, dict) and active_regime.get("active_regime_prefix") is not None
        else ""
    )

    chosen_key: str | None = None
    for k in (active_key, active_prefix, "*"):
        if k and isinstance(overrides.get(k), dict):
            chosen_key = k
            break

    if chosen_key is None:
        return thresholds, None

    patch = overrides.get(chosen_key)
    if isinstance(patch, dict):
        for kk, vv in patch.items():
            if kk == "min_test_months":
                thresholds[kk] = _as_int(vv, _as_int(thresholds.get(kk), 0))
            else:
                thresholds[kk] = _as_float(vv, _as_float(thresholds.get(kk), 0.0))
    return thresholds, chosen_key


def _quality_gate_result(evaluation_payload: dict[str, Any], thresholds: dict[str, float | int]) -> dict[str, Any]:
    metrics_by_phase = (
        evaluation_payload.get("metrics_by_phase")
        if isinstance(evaluation_payload.get("metrics_by_phase"), dict)
        else {}
    )
    test_metrics = metrics_by_phase.get("test") if isinstance(metrics_by_phase.get("test"), dict) else {}
    test_overall = test_metrics.get("overall") if isinstance(test_metrics.get("overall"), dict) else {}
    test_long = test_metrics.get("long") if isinstance(test_metrics.get("long"), dict) else {}
    test_short = test_metrics.get("short") if isinstance(test_metrics.get("short"), dict) else {}

    actual = {
        "test_months": _as_int(test_overall.get("months"), 0),
        "test_overall_return_at20": _as_float(test_overall.get("return_at20"), 0.0),
        "test_long_return_at20": _as_float(test_long.get("return_at20"), 0.0),
        "test_short_return_at20": _as_float(test_short.get("return_at20"), 0.0),
        "test_risk_mae_p90": _as_float(test_overall.get("risk_mae_p90"), 0.0),
    }
    t = thresholds
    checks = {
        "months": actual["test_months"] >= _as_int(t["min_test_months"], 6),
        "overall_return": actual["test_overall_return_at20"] >= _as_float(t["min_test_overall_return_at20"], 0.0),
        "long_return": actual["test_long_return_at20"] >= _as_float(t["min_test_long_return_at20"], 0.0),
        "short_return": actual["test_short_return_at20"] >= _as_float(t["min_test_short_return_at20"], -1.0),
        "risk_p90": actual["test_risk_mae_p90"] <= _as_float(t["max_test_risk_mae_p90"], 1.0),
    }
    failed = [k for k, ok in checks.items() if not bool(ok)]
    return {
        "passed": len(failed) == 0,
        "thresholds": dict(t),
        "actual": actual,
        "checks": checks,
        "failed_checks": failed,
    }


def _prepare_top20_public(
    top20: pd.DataFrame,
    run_id: str,
    model_version: str,
    feature_version: str,
    label_version: str,
    created_at: str,
    phases: tuple[str, ...],
) -> pd.DataFrame:
    frame = top20.copy()
    if frame.empty:
        out = pd.DataFrame(columns=list(REQUIRED_COLUMNS))
        return out
    frame["asof_date"] = pd.to_datetime(frame["asof_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    if "phase" in frame.columns:
        allow = {str(p).strip().lower() for p in phases if str(p).strip()}
        if allow:
            frame = frame[frame["phase"].astype(str).str.lower().isin(allow)].copy()
    frame = (
        frame.sort_values(["asof_date", "score"], ascending=[True, False])
        .groupby("asof_date", as_index=False, group_keys=False)
        .head(20)
        .reset_index(drop=True)
    )
    frame["model_version"] = model_version
    frame["feature_version"] = feature_version
    frame["label_version"] = label_version
    frame["run_id"] = run_id
    frame["created_at"] = created_at
    for col in REQUIRED_COLUMNS:
        if col not in frame.columns:
            frame[col] = None
    return frame[list(REQUIRED_COLUMNS)].copy()


def run_publish(
    paths: ResearchPaths,
    run_id: str,
    allow_non_pareto: bool = False,
    publish_phases: tuple[str, ...] = ("test", "inference"),
    allow_quality_gate_fail: bool = False,
    legacy_publish: bool = False,
    config: ResearchConfig | None = None,
) -> dict[str, Any]:
    run_dir = paths.run_dir(run_id)
    if not run_dir.exists():
        raise FileNotFoundError(f"run not found: {run_id}")
    repo_published_root = (paths.repo_root / "published").resolve()
    if (not legacy_publish) and paths.published_root.resolve() == repo_published_root:
        raise RuntimeError("publish gate failed: repo published/ requires --legacy-publish")

    manifest = read_json(run_dir / "manifest.json")
    created_at = now_utc_iso()
    model_version = str(manifest.get("model_version") or "unknown")
    feature_version = str(manifest.get("feature_version") or "unknown")
    label_version = str(manifest.get("label_version") or "unknown")

    top20_long = read_csv(run_dir / "top20_long.csv")
    top20_short = read_csv(run_dir / "top20_short.csv")
    evaluation_path = run_dir / "evaluation.json"
    if not evaluation_path.exists():
        raise RuntimeError("publish gate failed: evaluation.json is required. run evaluate first.")
    evaluation_payload = read_json(evaluation_path)
    is_pareto = bool(
        evaluation_payload.get("pareto", {}).get("is_pareto")
        if isinstance(evaluation_payload.get("pareto"), dict)
        else False
    )
    if (not allow_non_pareto) and (not is_pareto):
        raise RuntimeError("publish gate failed: run is not Pareto-optimal")
    active_regime = _detect_active_regime(top20_long, top20_short)
    thresholds, override_key = _gate_thresholds(config, active_regime=active_regime)
    quality_gate = _quality_gate_result(evaluation_payload, thresholds=thresholds)
    if (not allow_quality_gate_fail) and (not bool(quality_gate["passed"])):
        failed = ", ".join([str(x) for x in quality_gate.get("failed_checks", [])])
        raise RuntimeError(f"publish gate failed: quality gate failed ({failed})")

    public_long = _prepare_top20_public(
        top20=top20_long,
        run_id=run_id,
        model_version=model_version,
        feature_version=feature_version,
        label_version=label_version,
        created_at=created_at,
        phases=publish_phases,
    )
    public_short = _prepare_top20_public(
        top20=top20_short,
        run_id=run_id,
        model_version=model_version,
        feature_version=feature_version,
        label_version=label_version,
        created_at=created_at,
        phases=publish_phases,
    )

    version_name = paths.next_publish_version_name()
    version_dir = paths.published_root / version_name
    version_dir.mkdir(parents=True, exist_ok=True)
    write_csv(version_dir / "long_top20.csv", public_long)
    write_csv(version_dir / "short_top20.csv", public_short)

    payload = {
        "published_version": version_name,
        "published_at": created_at,
        "legacy_publish": bool(legacy_publish),
        "run_id": run_id,
        "source_manifest": manifest,
        "publish_gate": {
            "require_pareto": bool(not allow_non_pareto),
            "is_pareto": bool(is_pareto),
            "require_quality_gate": bool(not allow_quality_gate_fail),
            "quality_gate_passed": bool(quality_gate["passed"]),
            "quality_gate": quality_gate,
            "active_regime": active_regime,
            "regime_override_key": override_key,
        },
        "publish_phases": list(publish_phases),
        "files": {
            "long_top20": "long_top20.csv",
            "short_top20": "short_top20.csv",
        },
        "evaluation": evaluation_payload,
    }
    write_json(version_dir / "manifest.json", payload)

    paths.replace_latest_atomically(version_dir)
    return {
        "ok": True,
        "run_id": run_id,
        "is_pareto": bool(is_pareto),
        "legacy_publish": bool(legacy_publish),
        "published_version": version_name,
        "latest_dir": str(paths.latest_published_dir),
        "long_rows": int(len(public_long)),
        "short_rows": int(len(public_short)),
    }
