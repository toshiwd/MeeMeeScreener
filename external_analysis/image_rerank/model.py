from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image


def image_to_feature_vector(path: str | Path, *, size: int = 12) -> np.ndarray:
    image = Image.open(path).convert("RGB").resize((int(size), int(size)), Image.Resampling.BILINEAR)
    return (np.asarray(image, dtype=np.float32) / 255.0).reshape(-1)


def build_feature_matrix(image_paths: list[str | Path], *, size: int = 12) -> np.ndarray:
    vectors = [image_to_feature_vector(path, size=size) for path in image_paths]
    return np.asarray(vectors, dtype=np.float32) if vectors else np.zeros((0, size * size * 3), dtype=np.float32)


def train_image_classifier(
    *,
    train_image_paths: list[str | Path],
    train_labels: list[int],
    sample_weights: list[float] | None = None,
    feature_size: int = 12,
) -> tuple[dict[str, Any], np.ndarray]:
    X = build_feature_matrix(train_image_paths, size=feature_size)
    y = np.asarray(train_labels, dtype=np.int32)
    if X.size == 0:
        raise RuntimeError("no training features available")
    if len(set(y.tolist())) < 2:
        probability = float(y.mean()) if len(y) else 0.0
        artifact = {
            "schema_version": "tradex_image_rerank_model_v1",
            "model_type": "constant",
            "feature_size": int(feature_size),
            "positive_probability": probability,
        }
        scores = np.full(shape=(len(train_labels),), fill_value=probability, dtype=np.float32)
        return artifact, scores
    mean = X.mean(axis=0)
    scale = X.std(axis=0)
    scale = np.where(scale == 0.0, 1.0, scale)
    X_scaled = (X - mean) / scale
    X_aug = np.concatenate([X_scaled, np.ones((X_scaled.shape[0], 1), dtype=np.float32)], axis=1)
    weights = np.asarray(sample_weights, dtype=np.float32) if sample_weights is not None else np.ones((len(y),), dtype=np.float32)
    weights = np.where(weights < 0.0, 0.0, weights)
    w_sqrt = np.sqrt(weights)[:, None]
    X_weighted = X_aug * w_sqrt
    y_weighted = y.astype(np.float32) * w_sqrt[:, 0]
    coef, *_ = np.linalg.lstsq(X_weighted, y_weighted, rcond=None)
    model_weights = coef[:-1].astype(np.float32)
    intercept = float(coef[-1])
    artifact = {
        "schema_version": "tradex_image_rerank_model_v1",
        "model_type": "logistic_regression",
        "feature_size": int(feature_size),
        "scaler": {"mean": mean.tolist(), "scale": scale.tolist()},
        "classifier": {
            "classes": [0, 1],
            "coef": [model_weights.tolist()],
            "intercept": [intercept],
        },
    }
    logits = np.dot(X_scaled, model_weights) + intercept
    scores = (1.0 / (1.0 + np.exp(-logits))).astype(np.float32)
    return artifact, scores


def _predict_from_artifact(artifact: dict[str, Any], features: np.ndarray) -> np.ndarray:
    if str(artifact.get("model_type") or "") == "constant":
        return np.full(shape=(len(features),), fill_value=float(artifact.get("positive_probability") or 0.0), dtype=np.float32)
    mean = np.asarray(artifact["scaler"]["mean"], dtype=np.float32)
    scale = np.asarray(artifact["scaler"]["scale"], dtype=np.float32)
    coef = np.asarray(artifact["classifier"]["coef"], dtype=np.float32)
    intercept = np.asarray(artifact["classifier"]["intercept"], dtype=np.float32)
    standardized = (features - mean) / np.where(scale == 0.0, 1.0, scale)
    logits = np.dot(standardized, coef.T) + intercept
    logits = np.asarray(logits).reshape(-1)
    return (1.0 / (1.0 + np.exp(-logits))).astype(np.float32)


def score_image_classifier(artifact: dict[str, Any], *, image_paths: list[str | Path], feature_size: int | None = None) -> np.ndarray:
    size = int(feature_size or artifact.get("feature_size") or 12)
    features = build_feature_matrix(image_paths, size=size)
    return _predict_from_artifact(artifact, features)
