from __future__ import annotations

import json

import pandas as pd

from scripts.tradex_sample_replay_2531 import build_entry_reason_report, simulate_sample_replay


def _basis_row(dt: int, *, as_of: str, risk: str, score_hint: float, add_hint: bool = False, exit_hint: bool = False) -> dict[str, object]:
    payload = {
        "code": "2531",
        "name": "Takara Holdings",
        "asOf": as_of,
        "marketRegime": risk,
        "marketRiskOn": risk == "risk_on",
        "marketRiskOff": risk == "risk_off",
        "weeklyBreakoutUpProb": 0.55 if add_hint else 0.45,
        "weeklyBreakoutDownProb": 0.15 if add_hint else 0.20,
        "weeklyRangeProb": 0.25,
        "monthlyBreakoutUpProb": 0.55 if add_hint else 0.40,
        "monthlyBreakoutDownProb": 0.10 if add_hint else 0.20,
        "monthlyRangeProb": 0.20,
        "monthlyBoxState": "box_mid",
        "monthlyBoxPos": 0.45,
        "reclaim60": 1.0 if add_hint else 0.0,
        "v60Core": 1.0 if add_hint else 0.0,
        "v60Strong": 0.0,
        "morningStar": 1.0 if add_hint else 0.0,
        "bullMarubozu": 1.0 if add_hint else 0.0,
        "bearMarubozu": 1.0 if exit_hint else 0.0,
        "shootingStarLike": 1.0 if exit_hint else 0.0,
        "bullEngulfing": 0.0,
        "changePct": 0.02 if add_hint else -0.01,
        "close": 100.0 + score_hint,
        "prevClose": 99.0,
    }
    return {
        "dt": dt,
        "code": "2531",
        "basis_version": "basis:v1",
        "name": "Takara Holdings",
        "source_rank_buy": 1,
        "source_rank_sell": 2,
        "basis_payload_json": json.dumps(payload, ensure_ascii=False),
        "source_as_of": None,
        "basis_source": None,
        "reason_snapshot_json": json.dumps({"tradeRiskWatch": []}, ensure_ascii=False),
        "score_snapshot_json": json.dumps({"tradePriorityScore": 0.5}, ensure_ascii=False),
        "rank_snapshot_json": json.dumps({"sourceRank": 1}, ensure_ascii=False),
        "entry_qualified": False,
        "setup_type": "reject",
    }


def _bar_row(dt: int, *, open_price: float, close_price: float) -> dict[str, object]:
    return {
        "dt": dt,
        "code": "2531",
        "o": open_price,
        "h": max(open_price, close_price),
        "l": min(open_price, close_price),
        "c": close_price,
        "v": 1000,
    }


def test_simulate_sample_replay_builds_reconstructible_long_only_ledger() -> None:
    basis_frame = pd.DataFrame(
        [
            _basis_row(20260105, as_of="2026-01-05", risk="risk_on", score_hint=5.0),
            _basis_row(20260106, as_of="2026-01-06", risk="risk_on", score_hint=8.0, add_hint=True),
            _basis_row(20260107, as_of="2026-01-07", risk="risk_on", score_hint=6.0),
            _basis_row(20260108, as_of="2026-01-08", risk="risk_off", score_hint=-1.0, exit_hint=True),
        ]
    )
    bars_frame = pd.DataFrame(
        [
            _bar_row(20260106, open_price=100.0, close_price=102.0),
            _bar_row(20260107, open_price=103.0, close_price=106.0),
            _bar_row(20260108, open_price=101.0, close_price=99.0),
            _bar_row(20260109, open_price=98.0, close_price=97.0),
        ]
    )

    ledger_frame, config, roundtrip_payload = simulate_sample_replay(
        basis_frame=basis_frame,
        bars_frame=bars_frame,
        start_date="2026-01-05",
        end_date="2026-01-08",
    )

    assert {
        "entry_reason_primary",
        "entry_reason_codes",
        "entry_reason_detail",
        "add_reason_primary",
        "add_reason_codes",
        "add_reason_detail",
        "exit_reason_primary",
        "exit_reason_codes",
        "exit_reason_detail",
        "flat_reason_primary",
        "flat_reason_codes",
        "flat_reason_detail",
    }.issubset(set(ledger_frame.columns))
    assert ledger_frame.iloc[0]["previous_position"] == "0-0"
    assert ledger_frame.iloc[0]["selected_action"] == "long_entry"
    assert ledger_frame.iloc[0]["next_position"] == "0-2"
    assert ledger_frame.iloc[0]["entry_reason_primary"] is not None
    assert ledger_frame.iloc[1]["selected_action"] == "long_add"
    assert ledger_frame.iloc[1]["next_position"] == "0-5"
    assert ledger_frame.iloc[1]["add_reason_primary"] is not None
    assert ledger_frame.iloc[3]["selected_action"] == "long_exit"
    assert ledger_frame.iloc[3]["exit_reason_primary"] is not None
    assert roundtrip_payload["aggregate"]["roundtrip_count"] == 1
    assert roundtrip_payload["aggregate"]["exit_count"] == 1
    assert roundtrip_payload["roundtrips"][0]["entry_reason_summary"]["primary"] is not None
    assert roundtrip_payload["roundtrips"][0]["exit_reason_summary"]["primary"] is not None
    assert config["policy_mode"] == "research-fallback"
    assert ledger_frame.iloc[-1]["next_position"] == "0-0"

    report = build_entry_reason_report(config=config, ledger_frame=ledger_frame, roundtrip_payload=roundtrip_payload)
    assert report["roundtrip_rows"][0]["entry_reason_summary"]["primary"] is not None
    assert report["reason_rollup"]["entry_reason_primary"]
