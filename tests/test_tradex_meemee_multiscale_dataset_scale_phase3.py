from __future__ import annotations

from scripts.tradex_meemee_multiscale_dataset_scale_phase3 import add_relative_labels, assign_split, class_balance


def test_phase3_split_embargoes_cross_boundary_label_windows_and_balances_classes() -> None:
    rows = []
    dates = [20240131, 20240229, 20240331, 20240430, 20240531, 20240630, 20240731, 20240831, 20240930, 20241031, 20241130, 20241231]
    for date_index, as_of in enumerate(dates):
        for code_index in range(10):
            rows.append({
                "image_sample_key": f"{as_of}-{code_index}",
                "code": str(1000 + code_index),
                "as_of": as_of,
                "label_start_as_of": as_of + 1,
                "label_end_as_of": dates[min(date_index + 1, len(dates) - 1)],
                "ret20": code_index / 100.0,
            })
    add_relative_labels(rows)
    assignments, leakage = assign_split(rows)
    balance = class_balance(rows, assignments)

    assert leakage["split_leakage_audit_passed"] is True
    assert leakage["split_counts"]["embargo"] > 0
    assert leakage["future_label_window_overlap_train_validation"] is False
    assert leakage["future_label_window_overlap_validation_test"] is False
    assert balance["train_validation_test_all_label_classes_present"] is True
