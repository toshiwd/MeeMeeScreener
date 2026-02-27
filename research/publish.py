from __future__ import annotations

from typing import Any

import pandas as pd

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
) -> dict[str, Any]:
    run_dir = paths.run_dir(run_id)
    if not run_dir.exists():
        raise FileNotFoundError(f"run not found: {run_id}")

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
        "run_id": run_id,
        "source_manifest": manifest,
        "publish_gate": {
            "require_pareto": bool(not allow_non_pareto),
            "is_pareto": bool(is_pareto),
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
        "published_version": version_name,
        "latest_dir": str(paths.latest_published_dir),
        "long_rows": int(len(public_long)),
        "short_rows": int(len(public_short)),
    }
