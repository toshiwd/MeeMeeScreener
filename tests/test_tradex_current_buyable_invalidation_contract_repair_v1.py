from __future__ import annotations

import pandas as pd

from scripts import tradex_current_buyable_invalidation_contract_repair_v1 as mod


def _bars() -> pd.DataFrame:
    rows = []
    for idx in range(65):
        close = 100.0 + idx
        rows.append({"code": "8086", "bar_date": 20260400 + idx, "open": close, "high": close + 2, "low": close - 2, "close": close})
    return pd.DataFrame(rows)


def test_build_asof_features_computes_ma_atr_and_swing_low() -> None:
    bars = _bars()
    as_of = int(bars.iloc[-1]["bar_date"])
    features = mod.build_asof_features(bars, as_of)
    row = features.iloc[0]
    assert round(float(row["ma7"]), 6) == round(sum(range(158, 165)) / 7, 6)
    assert pd.notna(row["ma20"])
    assert pd.notna(row["ma60"])
    assert pd.notna(row["atr14"])
    assert pd.notna(row["recent_swing_low"])


def test_primary_level_selects_highest_long_stop() -> None:
    row = pd.Series({"entry_reference_close": 100.0, "ma20": 95.0, "recent_swing_low": 90.0, "atr14": 3.0})
    name, value = mod.primary_level(row)
    assert name == "invalidation_atr_stop_level"
    assert value == 97.0


def test_build_repaired_rows_adds_primary_level() -> None:
    bars = _bars()
    as_of = int(bars.iloc[-1]["bar_date"])
    features = mod.build_asof_features(bars, as_of)
    candidates = pd.DataFrame([{"as_of_date": as_of, "code": "8086"}])
    rows = mod.build_repaired_rows(candidates, features)
    assert rows["primary_invalidation_level"].notna().all()
    assert rows["contract_version"].eq(mod.CONTRACT_VERSION).all()


def test_no_lookahead_audit_passes_complete_repair() -> None:
    bars = _bars()
    as_of = int(bars.iloc[-1]["bar_date"])
    rows = mod.build_repaired_rows(pd.DataFrame([{"as_of_date": as_of, "code": "8086"}]), mod.build_asof_features(bars, as_of))
    audit = mod.no_lookahead_audit(
        rows,
        {"no_candidate_replacement": True, "validated_buy_count_at_projection": 0},
        {"research_decision": "forward_validation_pending_more_confirmed_bars"},
    )
    assert audit["no_lookahead_pass"] is True
    assert audit["future_outcomes_used"] is False
