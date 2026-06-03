from __future__ import annotations

import numpy as np

from scripts import tradex_meemee_actionable_shape_image_binary_phase13 as mod


def test_target_maps_only_actionable_shapes() -> None:
    assert mod._target("breakout_hold") == 1
    assert mod._target("breakout_pullback_fail") == 0
    assert mod._target("sideways_range") is None


def test_metrics_reports_binary_quality() -> None:
    metrics = mod._metrics(np.asarray([0, 0, 1, 1]), np.asarray([0.1, 0.2, 0.8, 0.9]))
    assert metrics["roc_auc"] == 1.0
    assert metrics["mcc"] == 1.0
