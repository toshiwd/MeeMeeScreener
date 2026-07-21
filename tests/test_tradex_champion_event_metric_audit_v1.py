import csv, json
from pathlib import Path
from scripts.tradex_champion_event_metric_audit_v1 import run, summarize

def test_summary_metrics():
    rows=[{"date":"1577836800","next_open_return":str(x)} for x in (.1,-.05,.02,-.01)]
    x=summarize(rows); assert x["n"]==4 and x["signal_days"]==1 and x["profit_factor"]==2 and x["p01"]<-.04

def test_audit_separates_mismatch_and_missing(tmp_path:Path):
    buy=tmp_path/"buy.json"; sell=tmp_path/"sell.json"; events=tmp_path/"events.csv"
    buy.write_text(json.dumps({"operational_contract":{"maximum_positions":4,"slot_budget_yen":2400000},"decision":{"authoritative_rollup_decision":"keep_buy"}}),encoding="utf-8")
    sell.write_text(json.dumps({"short_rule":{"entry":"next day low"},"decision":{"authoritative_rollup_decision":"keep_sell"}}),encoding="utf-8")
    with events.open("w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=["date","next_open_return","split"]);w.writeheader();w.writerows([{"date":"1577836800","next_open_return":"0.1","split":"train"},{"date":"1609459200","next_open_return":"-0.05","split":"validation"}])
    p=run(buy,events,sell,tmp_path/"out"); d=json.loads(p.read_text(encoding="utf-8"))
    assert d["lanes"][0]["comparison_lane"]=="entry_horizon_mismatch"
    assert d["lanes"][1]["metrics_by_split"]["test"]["n"] is None
    assert "n" in d["lanes"][1]["metrics_by_split"]["test"]["missing_metric"]
    assert d["cross_lane_comparison"]["status"]=="not_comparable" and not d["runner_reexecuted"]
