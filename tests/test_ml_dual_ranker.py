from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import sys
from typing import Any

import numpy as np
import pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.backend.services.ml_config import MLConfig
from app.backend.services import ml_service


@dataclass
class _FakeDataset:
    data: np.ndarray
    label: np.ndarray | None = None
    group: list[int] | None = None
    feature_name: list[str] | None = None


class _FakeBooster:
    def __init__(self, objective: str, dataset: _FakeDataset) -> None:
        self.objective = objective
        self.dataset = dataset

    def predict(self, data: np.ndarray) -> np.ndarray:
        n = int(len(data))
        if self.objective == "lambdarank":
            return np.linspace(0.1, 1.0, n, dtype=float)
        if self.objective == "binary":
            return np.full(n, 0.63, dtype=float)
        return np.full(n, 0.012, dtype=float)

    def save_model(self, path: str) -> None:
        Path(path).write_text("fake-model", encoding="utf-8")


class _FakeLightGBM:
    def __init__(self) -> None:
        self.train_calls: list[dict[str, Any]] = []

    @staticmethod
    def Dataset(
        data: np.ndarray,
        *,
        label: np.ndarray | None = None,
        group: list[int] | None = None,
        feature_name: list[str] | None = None,
        free_raw_data: bool = False,
    ) -> _FakeDataset:
        _ = free_raw_data
        return _FakeDataset(
            data=np.asarray(data, dtype=float),
            label=np.asarray(label, dtype=float) if label is not None else None,
            group=[int(v) for v in group] if group is not None else None,
            feature_name=list(feature_name) if feature_name is not None else None,
        )

    def train(self, params: dict[str, Any], dataset: _FakeDataset, num_boost_round: int) -> _FakeBooster:
        self.train_calls.append(
            {
                "objective": str(params.get("objective") or ""),
                "num_boost_round": int(num_boost_round),
                "label": None if dataset.label is None else np.asarray(dataset.label, dtype=float),
                "group": None if dataset.group is None else list(dataset.group),
            }
        )
        return _FakeBooster(str(params.get("objective") or ""), dataset)


def _build_training_frame(n_dates: int = 35, codes_per_date: int = 60) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    all_dates = [20240101 + i for i in range(n_dates)]
    all_codes = [f"{1300 + i:04d}" for i in range(codes_per_date)]
    for date_idx, dt in enumerate(all_dates):
        for code_idx, code in enumerate(all_codes):
            close = 100.0 + code_idx * 0.5 + date_idx * 0.2
            ret20 = ((code_idx - (codes_per_date / 2.0)) / 200.0) + ((date_idx % 5) - 2) / 1000.0
            ret10 = ret20 * 0.7
            ret5 = ret20 * 0.4
            up20 = int(ret20 > 0.0)
            up10 = int(ret10 > 0.0)
            up5 = int(ret5 > 0.0)
            turn_up = int((code_idx + date_idx) % 5 == 0)
            turn_down = int((code_idx + date_idx) % 4 == 0)
            rows.append(
                {
                    "dt": dt,
                    "code": code,
                    "close": close,
                    "ma7": close * 0.995,
                    "ma20": close * 0.99,
                    "ma60": close * 0.98,
                    "atr14": close * 0.01,
                    "diff20_pct": (close - close * 0.99) / (close * 0.99),
                    "cnt_20_above": 12 + (code_idx % 4),
                    "cnt_7_above": 4 + (code_idx % 2),
                    "close_prev1": close - 0.5,
                    "close_prev5": close - 2.5,
                    "close_prev10": close - 5.0,
                    "ma7_prev1": close * 0.994,
                    "ma20_prev1": close * 0.989,
                    "ma60_prev1": close * 0.979,
                    "diff20_prev1": 0.01,
                    "cnt_20_prev1": 12,
                    "cnt_7_prev1": 4,
                    "ret5": ret5,
                    "ret10": ret10,
                    "ret20": ret20,
                    "up5_label": up5,
                    "up10_label": up10,
                    "up20_label": up20,
                    "train_mask_cls_5": 1,
                    "train_mask_cls_10": 1,
                    "train_mask_cls": 1,
                    "turn_up_label": turn_up,
                    "turn_down_label_5": turn_down,
                    "turn_down_label": turn_down,
                    "turn_down_label_20": turn_down,
                    "train_mask_turn_5": 1,
                    "train_mask_turn": 1,
                    "train_mask_turn_20": 1,
                }
            )
    return pd.DataFrame(rows)


def test_fit_models_builds_dual_rank_groups_and_relevance(monkeypatch) -> None:
    fake_lgb = _FakeLightGBM()
    monkeypatch.setattr(ml_service, "_import_lightgbm", lambda: fake_lgb)

    train_df = _build_training_frame(n_dates=35, codes_per_date=60)
    models = ml_service._fit_models(train_df, MLConfig())

    assert models.rank_up is not None
    assert models.rank_down is not None
    assert models.n_train_rank == 35 * 60
    assert models.n_train_rank_groups == 35

    rank_calls = [call for call in fake_lgb.train_calls if call["objective"] == "lambdarank"]
    assert len(rank_calls) == 2
    for call in rank_calls:
        labels = np.asarray(call["label"], dtype=float)
        groups = [int(v) for v in call["group"]]
        assert len(groups) == 35
        assert sum(groups) == labels.size
        assert float(np.min(labels)) >= 0.0
        assert float(np.max(labels)) <= 4.0
        assert len(np.unique(labels)) >= 3


def test_predict_frame_outputs_rank_columns_and_p_down(monkeypatch) -> None:
    fake_lgb = _FakeLightGBM()
    monkeypatch.setattr(ml_service, "_import_lightgbm", lambda: fake_lgb)

    train_df = _build_training_frame(n_dates=35, codes_per_date=60)
    models = ml_service._fit_models(train_df, MLConfig())
    sample_df = train_df[train_df["dt"].isin([20240110, 20240111])].copy()

    pred = ml_service._predict_frame(sample_df, models, MLConfig())
    assert "p_down" in pred.columns
    assert "rank_up_20" in pred.columns
    assert "rank_down_20" in pred.columns
    assert "p_turn_down_10" in pred.columns
    base_down = 1.0 - pred["p_up"].to_numpy(dtype=float)
    turn_down = pred["p_turn_down_10"].to_numpy(dtype=float)
    expected_down = 0.40 * base_down + 0.60 * turn_down
    assert np.allclose(
        pred["p_down"].to_numpy(dtype=float),
        expected_down,
    )
    assert pred["rank_up_20"].notna().all()
    assert pred["rank_down_20"].notna().all()
