import json
from pathlib import Path

import pytest

from scripts import tradex_severe_loss_current_board_v1 as mod


def _model():
    return {"estimator": "sklearn.tree.DecisionTreeClassifier", "features": mod.FEATURES, "train_medians": {name: 0.0 for name in mod.FEATURES}, "nodes": [{"node": 0, "feature": "side_code", "threshold": 0.0, "left": 1, "right": 2}, {"node": 1, "feature": None, "severe_loss_probability": 0.6}, {"node": 2, "feature": None, "severe_loss_probability": 0.1}]}


def test_json_tree_prediction_is_deterministic():
    model = _model()
    assert mod.predict_tree(model, {"side_code": 1.0}) == .1
    assert mod.predict_tree(model, {"side_code": -1.0}) == .6


def test_board_is_unified_top3_and_japanese_reasons(tmp_path: Path):
    rows=[]
    for direction in ("up","down"):
        for rank in range(1,6):
            row={name:1.0 for name in mod.FEATURES};row.update({"dir":direction,"rank":rank,"code":f"{direction}{rank}","name":"銘柄","close":100.,"anchor_price_close":100.,"ma7":100.,"ma20":100.,"ma60":100.,"ma7_prev1":100.,"ma20_prev1":100.,"ma60_prev1":100.});rows.append(row)
    board=mod.build_board(rows,_model(),"2026-07-10",tmp_path/"model.json","m",tmp_path/"rollup.json","r",tmp_path/"db")
    assert len(board["actionable"])==3 and len(board["watch"])==7
    assert all(row["reason"]=="形状上の大損リスクが相対的に低い" for row in board["actionable"])
    assert board["boundary"]=={"display_only":True,"production":False,"automatic_trading":False,"official_and_provisional_not_mixed":True}


def test_rollup_or_model_hash_mismatch_fails_closed(tmp_path: Path):
    model_path=tmp_path/"frozen_model.json";model_path.write_text(json.dumps(_model()),encoding="utf-8")
    rollup_path=tmp_path/"rollup.json";rollup_path.write_text(json.dumps({"artifact_role":"authoritative","decision":{"authoritative_rollup_decision":"hold"}}),encoding="utf-8")
    with pytest.raises(ValueError,match="ROLLUP_NOT_AUTHORITATIVE"):
        mod.verify_contract(model_path,rollup_path)

    rollup_path.write_text(json.dumps({"artifact_role":"authoritative","decision":{"authoritative_rollup_decision":"current_regime_display_only_keep"},"source_artifacts":[{"path":str(model_path),"sha256":"wrong"}],"model_freeze_evidence":{"features":mod.FEATURES}}),encoding="utf-8")
    with pytest.raises(ValueError,match="FROZEN_MODEL_HASH_MISMATCH"):
        mod.verify_contract(model_path,rollup_path)


def test_date_mismatch_fails_before_board_write(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(mod,"verify_contract",lambda *args:(_model(),{},"m","r"))
    monkeypatch.setattr(mod,"query_candidates",lambda db,date: (_ for _ in ()).throw(ValueError("RANKING_CONFIRMED_DATE_MISMATCH")))
    db=tmp_path/"db";db.write_text("")
    status={"validated":True,"stale":False,"freshness_blocked":False,"selected_runtime_db_path":str(db),"latest_confirmed_daily_bars_date_iso":"2026-07-10"}
    with pytest.raises(ValueError,match="RANKING_CONFIRMED_DATE_MISMATCH"):
        mod.generate(db,tmp_path/"model",tmp_path/"rollup",tmp_path/"out",status)
    assert not (tmp_path/"out").exists()
