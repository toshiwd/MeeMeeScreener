from scripts.tradex_short_leaf20_event_ledger_v1 import build_rows, split_for_ymd


def test_fixed_splits_do_not_extend_into_2026():
    assert split_for_ymd(20190104) == "train"
    assert split_for_ymd(20230104) == "validation"
    assert split_for_ymd(20250104) == "test"
    assert split_for_ymd(20260104) is None


def test_breadth_gate_and_frozen_exit_are_applied():
    base = {
        "code": "1234", "ymd": 20240105, "e_ymd": 20240108, "l": 100.0,
        "e_h": 101.0, "e_l": 99.0, "e_c": 100.0,
    }
    for i in range(1, 11):
        base.update({f"f{i}_h": 101.0, f"f{i}_l": 99.0, f"f{i}_c": 100.0})
    kept = {**base, "breadth_below_ma20": 0.40}
    dropped = {**base, "code": "5678", "breadth_below_ma20": 0.3999}
    result = build_rows([kept, dropped])
    assert result["code"].tolist() == ["1234"]
    assert result.iloc[0]["split"] == "test"
    assert result.iloc[0]["ret"] == 0.0
    assert result.iloc[0]["exit_reason"] == "time"
