import pandas as pd

from scripts.tradex_short_state_transition_replay_v1 import add_outcomes, choose_transition, decide, denial_metrics, metrics


def event(family="low20_break_relative_weakness", **updates):
    row = {"family": family, "c": 100.0, "h": 101.0, "ma7": 105.0}
    for i in range(1, 26):
        row.update({f"o{i}": 100.0, f"h{i}": 101.0, f"l{i}": 98.0, f"c{i}": 99.0, f"ma7{i}": 104.0, f"date{i}": i})
    row.update(updates)
    return pd.Series(row)


def test_low20_wait_enters_at_ma7_retest():
    row = event(h2=106.0, o2=103.0)
    result = choose_transition(row, "family_wait")
    assert result["state"] == "entry" and result["entry_offset"] == 2 and result["entry_price"] == 105.0


def test_wait_denial_precedes_entry():
    row = event(o1=99.0, h1=108.0, l1=98.0, c1=107.0, ma71=104.0)
    assert choose_transition(row, "family_wait")["state"] == "denied"


def test_high_zone_second_down_entry():
    row = event("high_zone_climax", o1=102.0, c1=101.0, o2=101.0, c2=98.0)
    result = choose_transition(row, "family_wait")
    assert result["state"] == "entry" and result["entry_offset"] == 2


def test_outcomes_use_entry_relative_horizon():
    row = event(o1=100.0, c6=90.0, h1=102.0, h2=103.0, h3=102.0, h4=101.0, h5=100.0, h6=99.0)
    result = add_outcomes(row, choose_transition(row, "next_open"))
    assert round(result["ret5"], 6) == 0.1 and round(result["mae5"], 6) == -0.03


def test_metrics_reports_supply_tail_and_pf():
    frame = pd.DataFrame([
        {"state": "entry", "wait_days": 1, "sideways_20d": False, "ret5": .1, "ret10": .1, "ret20": .1, "mae5": -.02, "mae10": -.02, "mae20": -.02},
        {"state": "entry", "wait_days": 2, "sideways_20d": True, "ret5": -.05, "ret10": -.05, "ret20": -.05, "mae5": -.08, "mae10": -.08, "mae20": -.08},
        {"state": "missed_drop", "wait_days": 5, "sideways_20d": False, "ret5": None, "ret10": None, "ret20": None, "mae5": None, "mae10": None, "mae20": None},
    ])
    result = metrics(frame)
    assert result["entry_rate"] == 2 / 3 and result["missed_drop_rate"] == 1 / 3
    assert result["h10"]["profit_factor"] == 2.0 and result["h10"]["loss_le_minus5_rate"] == .5


def test_denial_metrics_measure_counterfactual_rise():
    frame = pd.DataFrame([
        {"policy": "family_wait", "state": "denied", "signal_ret5": -.05, "signal_ret10": -.10, "signal_ret20": .02},
        {"policy": "family_wait", "state": "entry", "signal_ret5": .10, "signal_ret10": .10, "signal_ret20": .10},
    ])
    result = denial_metrics(frame)
    assert result["h10"]["up_after_denial_rate"] == 1.0
    assert result["h20"]["up_after_denial_rate"] == 0.0


def test_operational_policy_rejects_positive_edge_with_extreme_tail():
    rows = []
    for i in range(40):
        loss = i < 8
        rows.append({
            "family": "high_zone_climax", "code": str(i), "signal_ymd": 20250101 + i,
            "policy": "next_open", "state": "entry", "wait_days": 1, "sideways_20d": False,
            "ret5": -.2 if loss else .1, "ret10": -.2 if loss else .1, "ret20": -.2 if loss else .1,
            "mae5": -.8 if loss else -.02, "mae10": -.8 if loss else -.02, "mae20": -.8 if loss else -.02,
            "signal_ret5": 0, "signal_ret10": 0, "signal_ret20": 0,
        })
        rows.append({**rows[-1], "policy": "family_wait"})
    result = decide(pd.DataFrame(rows))
    assert result["next_open_ready"] is False
    assert result["operational_policy_decision"] == "drop"
