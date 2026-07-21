from __future__ import annotations

import numpy as np

from scripts.tradex_sideways_human_expansion_v1 import fixed_effect_difference, stratified_permutation


def test_fixed_effect_difference_removes_stratum_level() -> None:
    values = np.array([1.0, 3.0, 101.0, 103.0])
    labels = np.array([False, True, False, True])
    strata = np.array(["a", "a", "b", "b"])
    assert fixed_effect_difference(values, labels, strata) == 2.0


def test_stratified_permutation_detects_large_within_stratum_uplift() -> None:
    strata = np.repeat(np.array(["a", "b", "c", "d"]), 20)
    labels = np.tile(np.r_[np.zeros(10, dtype=bool), np.ones(10, dtype=bool)], 4)
    values = labels.astype(float) * 5.0 + np.tile(np.linspace(0, 0.1, 20), 4)
    difference, p_value = stratified_permutation(values, labels, strata, permutations=1000)
    assert difference > 4.9
    assert p_value < 0.01
