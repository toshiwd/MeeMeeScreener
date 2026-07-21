from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_high_zone_exposure_rollup_v1 import run


def _artifact(path:Path,decision:str,sections:dict)->Path:
    path.write_text(json.dumps({"decision":{"authoritative_rollup_decision":decision},"fixed_evaluation_conditions":{},**sections}),encoding="utf-8");return path


def _m(mean:float)->dict:
    return {"mean_exposure":1.0,"h10":{"mean":mean,"win_rate":0.6,"profit_factor":2.0,"loss_le_minus10_rate":0.1,"worst_mae":-0.5}}


def test_rollup_selects_initial_expansion(tmp_path:Path):
    a=_artifact(tmp_path/"a.json","keep_high_zone_initial_exposure",{"baseline":{"metrics":_m(1)},"challengers":{"high_price25":{"metrics":_m(2)}}})
    b=_artifact(tmp_path/"b.json","keep_high_zone_episode_exposure",{"challengers":{"combined_episode25":{"metrics":_m(3)}}})
    c=_artifact(tmp_path/"c.json","keep_high_zone_cross_band_episode",{"challengers":{"cross_band_episode25":{"metrics":_m(4)}}})
    d=_artifact(tmp_path/"d.json","keep_high_zone_initial_expansion_episode",{"challengers":{"initial_expansion25":{"metrics":_m(5)},"initial_expansion_or_micro25":{"metrics":_m(4.5)}}})
    ledger=tmp_path/"l.parquet";pd.DataFrame([{"policy":"initial_expansion25","code":"1","signal_ymd":20240101,"ret10":.1,"mae10":-.1,"exposure":1.0,"cross_band_episode":False,"initial_expansion_episode":False,"micro_expansion":False}]).to_parquet(ledger)
    out=run(initial_exposure=a,episode=b,cross_band=c,initial_expansion=d,initial_expansion_ledger=ledger,output_root=tmp_path/"out")
    result=json.loads((out/"compare.json").read_text())
    assert result["decision"]["selected_policy"]=="initial_expansion25"
    assert len(result["family_leaderboard"])==6
