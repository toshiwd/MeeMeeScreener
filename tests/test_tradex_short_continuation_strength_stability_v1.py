from scripts import tradex_short_continuation_strength_stability_v1 as mod


def _row(code: str, month: int, ret: float, bucket: str) -> dict:
    return {
        "code": code,
        "month": month,
        "short_ret": ret,
        "setup_state": "SetupReady",
        "to_visual_continuation_permit": True,
        "early_bucket": bucket,
        "stop_hit": ret <= -0.08,
        "target_hit": ret >= 0.06,
    }


def test_build_payload_keeps_stable_early_impulse6_axis(tmp_path) -> None:
    events = []
    for idx in range(18):
        events.append(_row(str(idx % 8), 202401 + idx, 0.04 + (idx % 3) * 0.01, "EarlyImpulse6NoDenial"))
    for idx in range(14):
        events.append(_row(f"x{idx}", 202301 + idx, -0.03, "EarlyImpulse4NoDenial"))

    payload = mod.build_payload({"events": events}, tmp_path / "source.json")

    assert payload["compare"]["challenger"]["n"] == 18
    assert payload["compare"]["changed_member_count"] == 14
    assert payload["research_decision"]["authoritative_rollup_decision"] == "KEEP_REVIEW_ONLY"
    assert payload["runtime_db_write"] is False
    assert payload["meemee_modified"] is False
