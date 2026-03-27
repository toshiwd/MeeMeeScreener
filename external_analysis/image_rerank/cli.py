from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

from external_analysis.image_rerank.artifacts import sha256_file, verify_roundtrip, write_json
from external_analysis.image_rerank.contracts import ImageRerankJobConfig, build_run_manifest
from external_analysis.image_rerank.dataset import (
    build_base_score_artifact,
    build_historical_samples,
    build_snapshot_rows,
    compute_candidate_universe_hash,
    load_bars_frame,
    normalize_as_of_date,
)
from external_analysis.image_rerank.fusion import build_fusion_sweep, fuse_scores
from external_analysis.image_rerank.labels import label_samples
from external_analysis.image_rerank.metrics import build_compare_readout, compute_binary_metrics, compute_top_k_metrics, rank_rows
from external_analysis.image_rerank.model import score_image_classifier, train_image_classifier
from external_analysis.image_rerank.paths import (
    image_rerank_run_dir,
    image_rerank_run_inputs_dir,
    image_rerank_run_manifests_dir,
    image_rerank_run_models_dir,
    image_rerank_run_outputs_dir,
    image_rerank_run_renders_dir,
)
from external_analysis.image_rerank.renderer import DEFAULT_PALETTE, RendererConfig, render_day80_chart
from external_analysis.image_rerank.split import (
    assign_split_role,
    build_split_audit_manifest,
    build_time_block_split_manifest,
    classify_boundary_reasons,
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _resolve_renderer_backend(requested: str) -> str:
    backend = str(requested or "auto").strip().lower()
    if backend == "pil":
        return "pil"
    if backend == "agg":
        try:
            import matplotlib  # noqa: F401

            return "agg"
        except Exception:
            return "pil"
    try:
        import matplotlib  # noqa: F401

        return "agg"
    except Exception:
        return "pil"


def _label_target(bucket: str) -> int:
    return 1 if str(bucket) == "positive" else 0


def _render_sample_rows(
    *,
    rows: list[dict[str, Any]],
    renders_dir: Path,
    subset: str,
    render_config: RendererConfig,
) -> list[str]:
    image_paths: list[str] = []
    for row in rows:
        image_path = renders_dir / subset / str(row["code"]) / f'{row["as_of_date"]}.png'
        render_day80_chart(bars=list(row["feature_window"]), path=image_path, config=render_config)
        image_paths.append(str(image_path))
        row["render_path"] = str(image_path)
    return image_paths


def run_image_rerank_phase0_3(
    *,
    export_db_path: str | None,
    as_of_snapshot_date: str | int,
    run_id: str | None = None,
    verify_profile: str = "smoke",
    top_k: int = 10,
    block_size_days: int = 30,
    embargo_days: int = 20,
    feature_lookback_days: int = 80,
    label_horizon_days: int = 20,
    positive_quantile: float = 0.85,
    negative_quantile: float = 0.15,
    neutral_weight: float = 0.25,
    base_weight: float = 0.70,
    image_weight: float = 0.30,
    renderer_backend: str = "auto",
) -> dict[str, Any]:
    snapshot_date = normalize_as_of_date(as_of_snapshot_date)
    resolved_run_id = str(run_id or f"image-rerank-{snapshot_date}")
    config = ImageRerankJobConfig(
        run_id=resolved_run_id,
        export_db_path=str(export_db_path or ""),
        as_of_snapshot_date=snapshot_date,
        verify_profile=str(verify_profile),
        top_k=int(top_k),
        block_size_days=int(block_size_days),
        embargo_days=int(embargo_days),
        feature_lookback_days=int(feature_lookback_days),
        label_horizon_days=int(label_horizon_days),
        positive_quantile=float(positive_quantile),
        negative_quantile=float(negative_quantile),
        neutral_weight=float(neutral_weight),
        base_weight=float(base_weight),
        image_weight=float(image_weight),
        renderer_backend=str(renderer_backend),
    )

    run_dir = image_rerank_run_dir(config.run_id)
    inputs_dir = image_rerank_run_inputs_dir(config.run_id)
    manifests_dir = image_rerank_run_manifests_dir(config.run_id)
    outputs_dir = image_rerank_run_outputs_dir(config.run_id)
    renders_dir = image_rerank_run_renders_dir(config.run_id)
    models_dir = image_rerank_run_models_dir(config.run_id)

    bars_frame = load_bars_frame(export_db_path)
    trading_dates = sorted({int(value) for value in bars_frame["trade_date"].tolist()})
    if snapshot_date not in trading_dates:
        raise RuntimeError(f"snapshot date missing from export db: {snapshot_date}")
    date_to_index = {trade_date: index for index, trade_date in enumerate(trading_dates)}

    historical_samples = build_historical_samples(
        bars_frame=bars_frame,
        snapshot_date=snapshot_date,
        feature_lookback_days=config.feature_lookback_days,
        label_horizon_days=config.label_horizon_days,
    )
    if not historical_samples:
        raise RuntimeError("no historical samples available")

    split_manifest = build_time_block_split_manifest(
        run_id=config.run_id,
        trading_dates=trading_dates,
        block_size_days=config.block_size_days,
        embargo_days=config.embargo_days,
        feature_lookback_days=config.feature_lookback_days,
        label_horizon_days=config.label_horizon_days,
    )
    sample_indices = [date_to_index[int(sample["as_of_date"])] for sample in historical_samples]
    split_manifest = build_split_audit_manifest(
        split_manifest=split_manifest,
        sample_indices=sample_indices,
        feature_lookback_days=config.feature_lookback_days,
        label_horizon_days=config.label_horizon_days,
    )

    for sample in historical_samples:
        sample["as_of_index"] = date_to_index[int(sample["as_of_date"])]
        split_info = assign_split_role(as_of_date=sample["as_of_date"], split_manifest=split_manifest)
        sample["split_role"] = split_info["split_role"]
        sample["block_index"] = split_info["block_index"]
        sample["block_start_index"] = split_info["block_start_index"]
        sample["block_end_index"] = split_info["block_end_index"]
        sample["is_purged"] = False
        sample["purge_reason"] = ""
        sample["purge_reason_codes"] = []
        boundary_index = int(split_info["block_index"])
        if boundary_index >= 0 and boundary_index + 1 < len(split_manifest.get("blocks") or []):
            protected_block = split_manifest["blocks"][boundary_index + 1]
            reasons = classify_boundary_reasons(
                as_of_index=int(sample["as_of_index"]),
                feature_lookback_days=config.feature_lookback_days,
                label_horizon_days=config.label_horizon_days,
                protected_start_index=int(protected_block["block_start_index"]),
                protected_end_index=int(protected_block["block_end_index"]),
                purge_start_index=int(split_manifest["boundary_checks"][boundary_index]["purge_start_index"]),
                embargo_end_index=int(split_manifest["boundary_checks"][boundary_index]["embargo_end_index"]),
            )
        else:
            reasons = []
        if reasons and split_info["split_role"] in {"train", "val"}:
            sample["is_purged"] = True
            sample["purge_reason_codes"] = list(reasons)
            sample["purge_reason"] = ",".join(reasons)
            sample["split_role"] = "purged"

    split_path = manifests_dir / "split.json"
    write_json(split_path, split_manifest)
    verify_roundtrip(split_path, split_manifest)

    base_score_artifact = build_base_score_artifact(export_db_path=export_db_path, as_of_snapshot_date=snapshot_date)
    candidate_universe_hash = compute_candidate_universe_hash(base_score_artifact["rows"], as_of_snapshot_date=snapshot_date)
    base_score_artifact["candidate_universe_hash"] = candidate_universe_hash
    base_path = inputs_dir / "base_score.json"
    write_json(base_path, base_score_artifact)
    verify_roundtrip(base_path, base_score_artifact)

    label_threshold_rows = [sample for sample in historical_samples if sample["split_role"] == "train" and not sample["is_purged"]]
    if not label_threshold_rows:
        raise RuntimeError("no training rows available for label threshold calibration")
    _, label_manifest = label_samples(
        samples=label_threshold_rows,
        positive_quantile=config.positive_quantile,
        negative_quantile=config.negative_quantile,
        neutral_weight=config.neutral_weight,
        liquidity_min_average_volume=1.0,
        label_horizon_days=config.label_horizon_days,
    )
    positive_threshold = float(label_manifest["positive_bucket_rule"]["threshold"])
    negative_threshold = float(label_manifest["negative_bucket_rule"]["threshold"])
    sample_weight_policy = dict(label_manifest.get("sample_weight_policy") or {})

    def _apply_label(sample: dict[str, Any]) -> dict[str, Any]:
        if sample["is_purged"]:
            return {**sample, "label_bucket": "excluded", "label_value": None, "sample_weight": 0.0, "label_reason": "purged"}
        if float(sample.get("liquidity_proxy") or 0.0) < 1.0 or int(sample.get("feature_row_count") or 0) <= 0 or int(sample.get("future_row_count") or 0) <= 0:
            return {**sample, "label_bucket": "excluded", "label_value": None, "sample_weight": 0.0, "label_reason": "excluded"}
        future_return = float(sample.get("future_return") or 0.0)
        if future_return >= positive_threshold:
            label_bucket = "positive"
        elif future_return <= negative_threshold:
            label_bucket = "negative"
        else:
            label_bucket = "neutral"
        return {
            **sample,
            "label_bucket": label_bucket,
            "label_value": 1 if label_bucket == "positive" else 0,
            "sample_weight": float(sample_weight_policy.get(label_bucket, config.neutral_weight)),
            "label_reason": "quantile_bucket",
        }

    labeled_samples = [_apply_label(sample) for sample in historical_samples]
    label_manifest["counts"] = {
        "sample_count": len(labeled_samples),
        "positive_count": sum(1 for sample in labeled_samples if sample["label_bucket"] == "positive"),
        "negative_count": sum(1 for sample in labeled_samples if sample["label_bucket"] == "negative"),
        "neutral_count": sum(1 for sample in labeled_samples if sample["label_bucket"] == "neutral"),
        "excluded_count": sum(1 for sample in labeled_samples if sample["label_bucket"] == "excluded"),
        "purged_count": sum(1 for sample in labeled_samples if sample["is_purged"]),
    }
    label_path = manifests_dir / "label.json"
    write_json(label_path, label_manifest)
    verify_roundtrip(label_path, label_manifest)

    snapshot_rows = build_snapshot_rows(
        bars_frame=bars_frame,
        snapshot_date=snapshot_date,
        feature_lookback_days=config.feature_lookback_days,
        label_horizon_days=config.label_horizon_days,
        base_score_artifact=base_score_artifact,
    )

    render_config = RendererConfig(
        backend=_resolve_renderer_backend(config.renderer_backend),
        dpi=144,
        canvas_size=(224, 224),
        padding=10,
        linewidth=1.5,
        feature_lookback_days=config.feature_lookback_days,
        palette=dict(DEFAULT_PALETTE),
    )
    render_manifest = {
        "schema_version": "tradex_image_rerank_render_v1",
        "run_id": config.run_id,
        "created_at": _utc_now_iso(),
        "renderer_version": render_config.renderer_version,
        "backend": render_config.backend,
        "dpi": render_config.dpi,
        "palette": render_config.resolved_palette(),
        "canvas_size": [render_config.canvas_size[0], render_config.canvas_size[1]],
        "padding": render_config.padding,
        "linewidth": render_config.linewidth,
        "counts": {"historical_sample_count": len(labeled_samples), "snapshot_candidate_count": len(snapshot_rows)},
    }
    render_path = manifests_dir / "render.json"
    write_json(render_path, render_manifest)
    verify_roundtrip(render_path, render_manifest)

    train_rows: list[dict[str, Any]] = []
    val_rows: list[dict[str, Any]] = []
    test_rows: list[dict[str, Any]] = []
    for sample in labeled_samples:
        if sample.get("is_purged"):
            continue
        role = str(sample.get("split_role") or "")
        if role == "train":
            train_rows.append(sample)
        elif role == "val":
            val_rows.append(sample)
        elif role == "test":
            test_rows.append(sample)

    train_image_paths = _render_sample_rows(rows=train_rows, renders_dir=renders_dir, subset="train", render_config=render_config)
    val_image_paths = _render_sample_rows(rows=val_rows, renders_dir=renders_dir, subset="val", render_config=render_config)
    test_image_paths = _render_sample_rows(rows=test_rows, renders_dir=renders_dir, subset="test", render_config=render_config)

    train_labels = [_label_target(row["label_bucket"]) for row in train_rows]
    train_weights = [float(row["sample_weight"]) for row in train_rows]
    image_model_artifact, train_scores = train_image_classifier(
        train_image_paths=train_image_paths,
        train_labels=train_labels,
        sample_weights=train_weights,
        feature_size=12,
    )
    model_path = models_dir / "image_model.json"
    write_json(model_path, image_model_artifact)
    verify_roundtrip(model_path, image_model_artifact)

    val_scores = score_image_classifier(image_model_artifact, image_paths=val_image_paths, feature_size=12).tolist() if val_image_paths else []
    test_scores = score_image_classifier(image_model_artifact, image_paths=test_image_paths, feature_size=12).tolist() if test_image_paths else []
    oos_rows = [row for row in val_rows + test_rows if row["label_bucket"] in {"positive", "negative"}]
    oos_scores = [score for row, score in zip(val_rows + test_rows, val_scores + test_scores, strict=False) if row["label_bucket"] in {"positive", "negative"}]
    oos_labels = [_label_target(row["label_bucket"]) for row in oos_rows]
    phase2_metrics = {
        "schema_version": "tradex_image_rerank_phase2_metrics_v1",
        "run_id": config.run_id,
        "created_at": _utc_now_iso(),
        "train_sample_count": len(train_rows),
        "validation_sample_count": len(val_rows),
        "test_sample_count": len(test_rows),
        "oos_metrics": compute_binary_metrics(labels=oos_labels, scores=oos_scores),
        "training_score_mean": float(np.mean(train_scores)) if len(train_scores) else None,
        "validation_score_mean": float(np.mean(val_scores)) if len(val_scores) else None,
        "test_score_mean": float(np.mean(test_scores)) if len(test_scores) else None,
        "label_counts": dict(label_manifest.get("counts") or {}),
    }
    phase2_path = outputs_dir / "phase2_metrics.json"
    write_json(phase2_path, phase2_metrics)
    verify_roundtrip(phase2_path, phase2_metrics)

    snapshot_image_paths = _render_sample_rows(rows=snapshot_rows, renders_dir=renders_dir, subset="snapshot", render_config=render_config)
    image_scores = score_image_classifier(image_model_artifact, image_paths=snapshot_image_paths, feature_size=12).tolist()
    base_scores = [float(row["base_score"]) for row in snapshot_rows]
    fused_scores = fuse_scores(base_scores=base_scores, image_scores=image_scores, base_weight=config.base_weight, image_weight=config.image_weight).tolist()
    base_rows = [
        {
            **row,
            "base_score": float(score),
            "label_bucket": next((item["label_bucket"] for item in labeled_samples if item["code"] == row["code"] and item["as_of_date"] == row["as_of_date"]), "neutral"),
        }
        for row, score in zip(snapshot_rows, base_scores, strict=True)
    ]
    fused_rows = [
        {
            **row,
            "base_score": float(base_score),
            "image_score": float(image_score),
            "fused_score": float(fused_score),
            "base_rank": int(row.get("base_rank") or 0),
            "label_bucket": next((item["label_bucket"] for item in labeled_samples if item["code"] == row["code"] and item["as_of_date"] == row["as_of_date"]), "neutral"),
        }
        for row, base_score, image_score, fused_score in zip(snapshot_rows, base_scores, image_scores, fused_scores, strict=True)
    ]
    for fused_rank, row in enumerate(rank_rows(fused_rows, score_key="fused_score"), start=1):
        row["fused_rank"] = fused_rank
    compare_metrics = compute_top_k_metrics(base_rows=base_rows, fused_rows=fused_rows, k=config.top_k)
    compare_readout = build_compare_readout(base_rows=base_rows, fused_rows=fused_rows, k=config.top_k)
    fusion_sweep = build_fusion_sweep(
        base_scores=base_scores,
        image_scores=image_scores,
        base_weight=config.base_weight,
        image_weight=config.image_weight,
    )
    sweep_metrics: dict[str, Any] = {}
    for mode, scores in fusion_sweep.items():
        sweep_rows = [
            {
                **row,
                "base_score": float(base_score),
                "fused_score": float(fused_score),
                "label_bucket": next((item["label_bucket"] for item in labeled_samples if item["code"] == row["code"] and item["as_of_date"] == row["as_of_date"]), "neutral"),
            }
            for row, base_score, fused_score in zip(snapshot_rows, base_scores, scores.tolist(), strict=True)
        ]
        sweep_metrics[mode] = {
            "metrics": compute_top_k_metrics(base_rows=base_rows, fused_rows=sweep_rows, k=config.top_k),
            "top_codes": [row["code"] for row in rank_rows(sweep_rows, score_key="fused_score")[: config.top_k]],
        }
    compare_payload = {
        "schema_version": "tradex_image_rerank_phase3_compare_v1",
        "run_id": config.run_id,
        "created_at": _utc_now_iso(),
        "verify_profile": str(config.verify_profile),
        "as_of_snapshot_date": snapshot_date,
        "top_k": int(config.top_k),
        "base_weight": float(config.base_weight),
        "image_weight": float(config.image_weight),
        "candidate_universe_hash": candidate_universe_hash,
        "metrics": compare_metrics,
        "readout": compare_readout,
        "fusion_sweep": {
            "primary_mode": "rank_improver",
            "modes": sweep_metrics,
        },
        "base_top_rows": rank_rows(base_rows, score_key="base_score")[: config.top_k],
        "fused_top_rows": rank_rows(fused_rows, score_key="fused_score")[: config.top_k],
        "candidate_rows": fused_rows,
    }
    compare_path = outputs_dir / "phase3_compare.json"
    write_json(compare_path, compare_payload)
    verify_roundtrip(compare_path, compare_payload)

    artifacts = {
        "split": {"uri": str(split_path), "checksum": sha256_file(split_path)},
        "label": {"uri": str(label_path), "checksum": sha256_file(label_path)},
        "render": {"uri": str(render_path), "checksum": sha256_file(render_path)},
        "base_score": {"uri": str(base_path), "checksum": sha256_file(base_path)},
        "phase2_metrics": {"uri": str(phase2_path), "checksum": sha256_file(phase2_path)},
        "phase3_compare": {"uri": str(compare_path), "checksum": sha256_file(compare_path)},
        "model": {"uri": str(model_path), "checksum": sha256_file(model_path)},
    }
    counts = {
        "bars_row_count": int(len(bars_frame)),
        "historical_sample_count": len(historical_samples),
        "snapshot_candidate_count": len(snapshot_rows),
        "train_sample_count": len(train_rows),
        "validation_sample_count": len(val_rows),
        "test_sample_count": len(test_rows),
    }
    run_manifest = build_run_manifest(
        config=config,
        candidate_universe_hash=candidate_universe_hash,
        base_score_artifact_uri=str(base_path),
        base_score_artifact_checksum=artifacts["base_score"]["checksum"],
        split_artifact_uri=str(split_path),
        label_artifact_uri=str(label_path),
        render_artifact_uri=str(render_path),
        phase2_metrics_artifact_uri=str(phase2_path),
        phase3_compare_artifact_uri=str(compare_path),
        status="complete",
        counts=counts,
        artifacts=artifacts,
    )
    run_path = run_dir / "run.json"
    write_json(run_path, run_manifest)
    verify_roundtrip(run_path, run_manifest)
    return {"ok": True, "run": run_manifest, "artifacts": artifacts, "phase2_metrics": phase2_metrics, "phase3_compare": compare_payload}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m external_analysis image-rerank-run")
    sub = parser.add_subparsers(dest="cmd", required=True)
    run_parser = sub.add_parser("image-rerank-run", help="Run the TRADEX image rerank Phase0-Phase3 pipeline.")
    run_parser.add_argument("--export-db-path", default=None)
    run_parser.add_argument("--as-of-date", required=True)
    run_parser.add_argument("--run-id", default=None)
    run_parser.add_argument("--verify-profile", default="smoke")
    run_parser.add_argument("--top-k", type=int, default=10)
    run_parser.add_argument("--block-size-days", type=int, default=30)
    run_parser.add_argument("--embargo-days", type=int, default=20)
    run_parser.add_argument("--feature-lookback-days", type=int, default=80)
    run_parser.add_argument("--label-horizon-days", type=int, default=20)
    run_parser.add_argument("--positive-quantile", type=float, default=0.85)
    run_parser.add_argument("--negative-quantile", type=float, default=0.15)
    run_parser.add_argument("--neutral-weight", type=float, default=0.25)
    run_parser.add_argument("--base-weight", type=float, default=0.70)
    run_parser.add_argument("--image-weight", type=float, default=0.30)
    run_parser.add_argument("--renderer-backend", default="auto")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.cmd == "image-rerank-run":
        payload = run_image_rerank_phase0_3(
            export_db_path=args.export_db_path,
            as_of_snapshot_date=args.as_of_date,
            run_id=args.run_id,
            verify_profile=args.verify_profile,
            top_k=args.top_k,
            block_size_days=args.block_size_days,
            embargo_days=args.embargo_days,
            feature_lookback_days=args.feature_lookback_days,
            label_horizon_days=args.label_horizon_days,
            positive_quantile=args.positive_quantile,
            negative_quantile=args.negative_quantile,
            neutral_weight=args.neutral_weight,
            base_weight=args.base_weight,
            image_weight=args.image_weight,
            renderer_backend=args.renderer_backend,
        )
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str))
        return 0
    raise RuntimeError(f"unknown command: {args.cmd}")
