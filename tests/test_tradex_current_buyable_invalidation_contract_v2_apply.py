from __future__ import annotations

import pandas as pd

from scripts import tradex_current_buyable_invalidation_contract_v2_apply as mod


def test_build_v2_rows_applies_atr2_stop() -> None:
    rows = pd.DataFrame([{"code": "8086", "entry_reference_close": 100.0, "atr14": 4.0}])
    out = mod.build_v2_rows(rows)
    assert out.iloc[0]["primary_invalidation_level"] == 92.0
    assert out.iloc[0]["invalidation_reason"] == "stop_atr2"
    assert out.iloc[0]["contract_version"] == mod.CONTRACT_VERSION


def test_no_lookahead_requires_repair_and_variant_keep() -> None:
    rows = mod.build_v2_rows(pd.DataFrame([{"code": "8086", "entry_reference_close": 100.0, "atr14": 4.0}]))
    audit = mod.no_lookahead_audit(
        rows,
        {"research_decision": "invalidation_contract_repaired_full_levels_ready"},
        {"research_decision": "invalidation_contract_variant_ready_for_forward_tracking"},
    )
    assert audit["no_lookahead_pass"] is True
    bad = mod.no_lookahead_audit(rows, {"research_decision": "drop"}, {"research_decision": "invalidation_contract_variant_ready_for_forward_tracking"})
    assert bad["no_lookahead_pass"] is False
