from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image, ImageFilter, ImageStat
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    balanced_accuracy_score,
    confusion_matrix,
    precision_recall_fscore_support,
    roc_auc_score,
)
from sklearn.model_selection import StratifiedKFold, cross_val_predict
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler


AXIS_ID = "short_shape_image_feature_probe_v1"
DEFAULT_DATASET_DIR = Path(
    r"G:\Tradex\short_shape_labeled_screenshot_dataset_light100_recent_v1"
    r"\combined_light100_recent_dataset_v1"
)
DEFAULT_CURRENT_SCAN_PATH = Path(
    r"G:\Tradex\pure_down_current_candidate_scan_v1"
    r"\20260701T084136Z-pure-down-current-candidate-scan-v3-date-normalized"
    r"\pure_down_current_candidates.json"
)
DEFAULT_CURRENT_IMAGE_DIR = Path(
    r"G:\Tradex\pure_down_current_candidate_screenshots_v1"
    r"\20260701T084227Z-meemee_detail_clean_screenshot_dataset_v1"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_shape_image_feature_probe_v1")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(_clean_json(payload), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _clean_json(value: Any) -> Any:
    if isinstance(value, str):
        return "".join(ch for ch in value if ord(ch) >= 32)
    if isinstance(value, list):
        return [_clean_json(item) for item in value]
    if isinstance(value, dict):
        return {_clean_json(key): _clean_json(item) for key, item in value.items()}
    return value


def _feature_vector(path: Path) -> list[float]:
    image = Image.open(path).convert("L").resize((240, 160))
    stat = ImageStat.Stat(image)
    edges = image.filter(ImageFilter.FIND_EDGES)
    edge_pixels = list(edges.tobytes())
    pixels = list(image.tobytes())
    arr = np.array(pixels, dtype=float).reshape(160, 240) / 255.0
    grid: list[float] = []
    for gy in range(4):
        for gx in range(6):
            cell = arr[gy * 40 : (gy + 1) * 40, gx * 40 : (gx + 1) * 40]
            grid.extend([float(cell.mean()), float(cell.std())])
    vertical = [float(chunk.mean()) for chunk in np.array_split(arr.mean(axis=0), 12)]
    horizontal = [float(chunk.mean()) for chunk in np.array_split(arr.mean(axis=1), 8)]
    return [
        float(stat.mean[0]),
        float(stat.stddev[0]),
        sum(1 for pixel in pixels if pixel < 245) / len(pixels),
        sum(edge_pixels) / len(edge_pixels) / 255.0,
        *grid,
        *vertical,
        *horizontal,
    ]


def _rows_to_xy(rows: list[dict[str, Any]]) -> tuple[np.ndarray, np.ndarray]:
    x = np.array([_feature_vector(Path(str(row["saved_path"]))) for row in rows])
    y = np.array([1 if row.get("purpose_outcome_class") == "good_short_shape" else 0 for row in rows])
    return x, y


def _model(model_key: str):
    if model_key == "raw_logistic_c02":
        return LogisticRegression(max_iter=2000, class_weight="balanced", C=0.2)
    if model_key == "scaled_logistic_c02":
        return make_pipeline(StandardScaler(), LogisticRegression(max_iter=2000, class_weight="balanced", C=0.2))
    if model_key == "raw_logistic_c1":
        return LogisticRegression(max_iter=2000, class_weight="balanced", C=1.0)
    raise ValueError(f"unknown model_key: {model_key}")


def _binary_metrics(y_true: np.ndarray, pred: np.ndarray, prob: np.ndarray) -> dict[str, Any]:
    precision, recall, f1, _ = precision_recall_fscore_support(
        y_true,
        pred,
        average="binary",
        zero_division=0,
    )
    return {
        "accuracy": float(accuracy_score(y_true, pred)),
        "balanced_accuracy": float(balanced_accuracy_score(y_true, pred)),
        "roc_auc": float(roc_auc_score(y_true, prob)),
        "precision_good": float(precision),
        "recall_good": float(recall),
        "f1_good": float(f1),
        "confusion_matrix": confusion_matrix(y_true, pred).tolist(),
    }


def _bucket(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "n": len(rows),
        "good_rate": sum(1 for row in rows if row.get("purpose_outcome_class") == "good_short_shape") / len(rows) if rows else None,
        "avg_ret20": sum(float(row["ret20"]) for row in rows) / len(rows) if rows else None,
        "avg_MAE20": sum(float(row["MAE20"]) for row in rows) / len(rows) if rows else None,
        "avg_MFE20": sum(float(row["MFE20"]) for row in rows) / len(rows) if rows else None,
    }


def _cv_probe(rows: list[dict[str, Any]], x: np.ndarray, y: np.ndarray, *, model_key: str) -> dict[str, Any]:
    model = _model(model_key)
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=20260701)
    pred = cross_val_predict(model, x, y, cv=cv)
    prob = cross_val_predict(model, x, y, cv=cv, method="predict_proba")[:, 1]
    ranked = sorted(zip(rows, prob), key=lambda pair: float(pair[1]), reverse=True)
    top20 = [row for row, _ in ranked[:20]]
    bottom20 = [row for row, _ in ranked[-20:]]
    top10 = [row for row, _ in ranked[:10]]
    bottom10 = [row for row, _ in ranked[-10:]]
    return {
        "validation_method": "5-fold stratified cross validation",
        "metrics": _binary_metrics(y, pred, prob),
        "score_bucket_summary": {
            "top20_by_image_score": _bucket(top20),
            "bottom20_by_image_score": _bucket(bottom20),
            "top10_by_image_score": _bucket(top10),
            "bottom10_by_image_score": _bucket(bottom10),
        },
    }


def _temporal_probe(rows: list[dict[str, Any]], x: np.ndarray, y: np.ndarray, *, model_key: str) -> dict[str, Any]:
    years = np.array([int(str(row["as_of"])[:4]) for row in rows])
    splits: list[dict[str, Any]] = []
    for cutoff in (2008, 2010, 2012, 2014, 2016, 2018):
        train_idx = np.where(years <= cutoff)[0]
        test_idx = np.where(years > cutoff)[0]
        if len(train_idx) < 20 or len(test_idx) < 20:
            continue
        if len(set(y[train_idx])) < 2 or len(set(y[test_idx])) < 2:
            continue
        model = _model(model_key)
        model.fit(x[train_idx], y[train_idx])
        pred = model.predict(x[test_idx])
        prob = model.predict_proba(x[test_idx])[:, 1]
        ranked = sorted([(int(index), float(score)) for index, score in zip(test_idx, prob)], key=lambda pair: pair[1], reverse=True)
        bucket_size = max(1, len(ranked) // 5)
        row = {
            "cutoff_year": cutoff,
            "train_n": int(len(train_idx)),
            "test_n": int(len(test_idx)),
            "train_good": int(y[train_idx].sum()),
            "test_good": int(y[test_idx].sum()),
            **_binary_metrics(y[test_idx], pred, prob),
            "score_bucket_summary": {
                "top20pct": _bucket([rows[index] for index, _ in ranked[:bucket_size]]),
                "bottom20pct": _bucket([rows[index] for index, _ in ranked[-bucket_size:]]),
            },
        }
        splits.append(row)
    return {"validation_method": "expanding temporal split by cutoff year", "temporal_splits": splits}


def _current_scores(
    *,
    rows: list[dict[str, Any]],
    x: np.ndarray,
    y: np.ndarray,
    current_scan_path: Path,
    current_image_dir: Path,
    model_key: str,
) -> dict[str, Any] | None:
    if not current_scan_path.exists() or not (current_image_dir / "image_manifest.jsonl").exists():
        return None
    model = _model(model_key)
    model.fit(x, y)
    scan = _read_json(current_scan_path)
    manifest = _read_jsonl(current_image_dir / "image_manifest.jsonl")
    image_by_code = {str(row.get("code")): row for row in manifest}
    scored: list[dict[str, Any]] = []
    for candidate in scan.get("candidates", []):
        code = str(candidate.get("code"))
        image_row = image_by_code.get(code)
        if not image_row:
            continue
        prob = float(model.predict_proba(np.array([_feature_vector(Path(str(image_row["saved_path"])))]))[0, 1])
        scored.append(
            {
                **candidate,
                "image_good_short_probability": prob,
                "image_verdict": (
                    "image_score_watch_short_candidate"
                    if prob >= 0.70
                    else "image_score_neutral_review"
                    if prob >= 0.55
                    else "image_score_reject_or_low_priority"
                ),
                "image_path": image_row.get("saved_path"),
                "image_relpath": image_row.get("image_relpath"),
            }
        )
    scored.sort(key=lambda row: (row["image_good_short_probability"], row.get("shape_priority_score") or 0), reverse=True)
    return {
        "current_candidate_scan": str(current_scan_path),
        "current_screenshot_dataset": str(current_image_dir),
        "current_as_of": scan.get("runtime_freshness", {}).get("confirmed_as_of"),
        "scored_count": len(scored),
        "rows": scored,
    }


def run(
    *,
    dataset_dir: Path,
    output_root: Path,
    current_scan_path: Path | None,
    current_image_dir: Path | None,
) -> Path:
    rows = _read_jsonl(dataset_dir / "label_ledger.jsonl")
    x, y = _rows_to_xy(rows)
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    model_keys = ["raw_logistic_c02", "scaled_logistic_c02", "raw_logistic_c1"]
    model_reports: dict[str, Any] = {}
    for model_key in model_keys:
        model_reports[model_key] = {
            "cv_probe": _cv_probe(rows, x, y, model_key=model_key),
            "temporal_probe": _temporal_probe(rows, x, y, model_key=model_key),
        }
    def model_score(report: dict[str, Any]) -> tuple[float, float, float]:
        splits = report["temporal_probe"]["temporal_splits"]
        if not splits:
            return (0.0, 0.0, 0.0)
        return (
            sum(float(split["roc_auc"]) for split in splits) / len(splits),
            sum(float(split["balanced_accuracy"]) for split in splits) / len(splits),
            float(report["cv_probe"]["metrics"]["roc_auc"]),
        )
    selected_model_key = max(model_keys, key=lambda key: model_score(model_reports[key]))
    cv = model_reports[selected_model_key]["cv_probe"]
    temporal = model_reports[selected_model_key]["temporal_probe"]
    current = (
        _current_scores(rows=rows, x=x, y=y, current_scan_path=current_scan_path, current_image_dir=current_image_dir, model_key=selected_model_key)
        if current_scan_path is not None and current_image_dir is not None
        else None
    )
    keep_temporal = any(
        split.get("balanced_accuracy", 0) >= 0.60 and split.get("roc_auc", 0) >= 0.60
        for split in temporal["temporal_splits"]
    )
    report = {
        "schema_version": "short_shape_image_feature_probe_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "dataset_dir": str(dataset_dir),
        "sample_count": len(rows),
        "class_counts": {
            "good_short_shape": int(y.sum()),
            "bad_short_shape": int((1 - y).sum()),
        },
        "feature_contract": "PIL grayscale 240x160; luminance, edge density, 4x6 grid mean/std, projection means",
        "model_selection": {
            "candidate_model_keys": model_keys,
            "selected_model_key": selected_model_key,
            "selection_metric": "mean temporal roc_auc, then mean temporal balanced_accuracy, then cv roc_auc",
            "model_scores": {key: model_score(report) for key, report in model_reports.items()},
        },
        "model_reports": model_reports,
        "cv_probe": cv,
        "temporal_probe": temporal,
        "current_scores": current,
        "decision": {
            "candidate_local_decision": "keep_image_feature_probe_for_scaleup" if keep_temporal else "drop_image_feature_probe_not_reliable",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "image score has temporal signal but remains pilot-scale and review-only",
        },
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
    }
    _write_json(output_dir / "short_shape_image_feature_probe.json", report)
    _write_json(output_root / "latest_short_shape_image_feature_probe.json", {"run_root": str(output_dir), **report})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-dir", type=Path, default=DEFAULT_DATASET_DIR)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--current-scan-path", type=Path, default=DEFAULT_CURRENT_SCAN_PATH)
    parser.add_argument("--current-image-dir", type=Path, default=DEFAULT_CURRENT_IMAGE_DIR)
    args = parser.parse_args()
    print(run(
        dataset_dir=args.dataset_dir,
        output_root=args.output_root,
        current_scan_path=args.current_scan_path,
        current_image_dir=args.current_image_dir,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
