import json
from pathlib import Path
import pandas as pd
import scripts.tradex_leaf_cap4_slot24_event_ledger_v1 as m

def test_generate_persists_exact_replay(monkeypatch,tmp_path:Path):
    source=tmp_path/"source.csv";pd.DataFrame([{"code":"1","next_entry_date":1,"exit_date":2,"entry_price":100,"next_open_return":.1,"year":2020,"split":"train","tie_gap_ma60":1}]).to_csv(source,index=False)
    compare=tmp_path/"compare.json"; compare.write_text(json.dumps({"fixed_evaluation_conditions":{"source":str(source),"slot_budget_yen":2400000,"maximum_positions":4,"same_day_candidate_cap":3},"selected_operational_contract":{"accepted_trade_count":1,"eligible_top3_rows":1,"unaffordable_top3_count":0,"red_year_count":0,"pnl_2024_2025_yen":0,"test_money_profit_factor":2,"max_realized_drawdown_yen":0,"max_concurrent_invested_yen":100},"decision":{"authoritative_rollup_decision":"research_only"}}),encoding="utf-8")
    def fake(x,slip):
        z=x.assign(fill_price=100,shares=100,invested_yen=100,exit_price=110,pnl_yen=10)
        return z,{"accepted_trade_count":1,"eligible_top3_rows":1,"unaffordable_top3_count":0,"red_year_count":0,"pnl_2024_2025_yen":0,"test_money_profit_factor":2,"max_realized_drawdown_yen":0,"max_concurrent_invested_yen":100}
    monkeypatch.setattr(m,"replay",fake);p=m.generate(compare,tmp_path/"out");d=json.loads(p.read_text(encoding="utf-8"))
    assert d["event_ledger"]["rows"]==1 and all(d["authoritative_metric_match"].values())
    assert d["metrics_by_split"]["train"]["n"]==1 and not d["rules_or_thresholds_changed"]

def test_contract_mismatch_stops(tmp_path:Path):
    source=tmp_path/"s.csv";source.write_text("x\n",encoding="utf-8");compare=tmp_path/"c.json";compare.write_text(json.dumps({"fixed_evaluation_conditions":{"source":str(source),"slot_budget_yen":1,"maximum_positions":4,"same_day_candidate_cap":3}}),encoding="utf-8")
    try:m.generate(compare,tmp_path/"o")
    except ValueError as e:assert str(e)=="AUTHORITATIVE_CONTRACT_MISMATCH"
    else:assert False
