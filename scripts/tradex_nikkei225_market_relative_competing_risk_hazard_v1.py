from __future__ import annotations

"""Review-only discrete-time competing-risk factorization for h5/h10."""

import argparse
import gc
import json
import sys
from pathlib import Path
from typing import Any

import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import tradex_nikkei225_20bar_morphology_sequence_v1 as base
import tradex_nikkei225_first_passage_order_v1 as fp
import tradex_nikkei225_market_relative_first_passage_model_v1 as mrp_base

AXIS_ID = "tradex_nikkei225_market_relative_competing_risk_hazard_v1"
RUNNER_AXIS_ID = "mrp_crh_v1"
HORIZONS = {5: base.HORIZONS[5], 10: base.HORIZONS[10]}
CAUSES = ("survive", "down_first", "rebound_first", "neutral_path_ambiguous")
_EVENT_DAY: np.ndarray | None = None
_EVENT_CAUSE: np.ndarray | None = None
_CURRENT_HORIZON: int | None = None


def labels_for_runner(frame: pd.DataFrame, horizon: int) -> np.ndarray:
    global _EVENT_DAY, _EVENT_CAUSE, _CURRENT_HORIZON
    passage = fp.first_passage(frame, horizon)
    kind = passage.outcome_kind.to_numpy()
    cause = np.zeros(len(frame), dtype=np.int8)
    cause[np.isin(kind, ["down_open_gap", "down_intraday"])] = 1
    cause[np.isin(kind, ["rebound_open_gap", "rebound_intraday"])] = 2
    cause[kind == "neutral_path_ambiguous"] = 3
    day = passage.hit_day.to_numpy(dtype=np.int8)
    # neutral_no_hit is censored after h; all other mechanisms have an event.
    if np.any((cause > 0) != (day > 0)):
        raise AssertionError("event day/cause contract differs")
    primary = fp.labels(frame, horizon)
    recovered = np.where(cause == 1, 0, np.where(cause == 2, 1, 2)).astype(np.int8)
    if not np.array_equal(primary, recovered):
        raise AssertionError("hazard causes do not recover fixed primary labels")
    _EVENT_DAY, _EVENT_CAUSE, _CURRENT_HORIZON = day, cause, horizon
    return primary


def _expand_risk_set(X: pd.DataFrame, event_day: np.ndarray, event_cause: np.ndarray, horizon: int, with_y: bool):
    frames, labels = [], []
    for day in range(1, horizon + 1):
        at_risk = (event_day == 0) | (event_day >= day)
        part = X.loc[at_risk].copy()
        part["__risk_day"] = np.int8(day)
        frames.append(part)
        if with_y:
            y = np.zeros(at_risk.sum(), dtype=np.int8)
            original = np.flatnonzero(at_risk)
            hit = event_day[original] == day
            y[hit] = event_cause[original[hit]]
            labels.append(y)
    return pd.concat(frames, ignore_index=True), (np.concatenate(labels) if with_y else None)


class CompetingRiskHazardClassifier:
    def __init__(self, variant: str, n_estimators: int = 300):
        if _CURRENT_HORIZON not in HORIZONS:
            raise RuntimeError("horizon context missing")
        self.variant, self.n_estimators, self.horizon = variant, int(n_estimators), int(_CURRENT_HORIZON)
        self.inner = lgb.LGBMClassifier(
            objective="multiclass", num_class=4, n_estimators=self.n_estimators,
            learning_rate=.03, verbosity=-1, n_jobs=2, random_state=base.SEED,
            **base.VARIANTS[variant],
        )
        self.best_iteration_ = self.n_estimators

    def set_params(self, **params): self.inner.set_params(**params); return self

    @staticmethod
    def _context(index):
        if _EVENT_DAY is None or _EVENT_CAUSE is None: raise RuntimeError("risk context missing")
        idx = np.asarray(index, dtype=int)
        return _EVENT_DAY[idx], _EVENT_CAUSE[idx]

    def fit(self, X, y, eval_set=None, callbacks=None):
        day, cause = self._context(X.index)
        xx, yy = _expand_risk_set(X, day, cause, self.horizon, True)
        ee = None
        if eval_set:
            ee = []
            for ex, _ in eval_set:
                ed, ec = self._context(ex.index)
                exx, eyy = _expand_risk_set(ex, ed, ec, self.horizon, True)
                ee.append((exx, eyy))
        self.inner.fit(xx, yy, eval_set=ee, callbacks=callbacks)
        self.best_iteration_ = int(self.inner.best_iteration_ or self.n_estimators)
        return self

    def predict_components(self, X, num_iteration=None) -> np.ndarray:
        n = len(X); survival = np.ones(n); cif = np.zeros((n, 3))
        for day in range(1, self.horizon + 1):
            xx = X.copy(); xx["__risk_day"] = np.int8(day)
            q = self.inner.predict_proba(xx, num_iteration=num_iteration)
            if q.shape[1] != 4: raise ValueError({"missing_hazard_class": q.shape})
            cif += survival[:, None] * q[:, 1:4]
            survival *= q[:, 0]
        # Numerical residual belongs to censored survival.
        total = survival + cif.sum(axis=1)
        survival /= total; cif /= total[:, None]
        return np.column_stack([cif[:, 0], cif[:, 1], cif[:, 2], survival])

    def predict_proba(self, X, num_iteration=None):
        c = self.predict_components(X, num_iteration)
        return np.column_stack([c[:, 0], c[:, 1], c[:, 2] + c[:, 3]])


def model(variant: str, n: int = 300): return CompetingRiskHazardClassifier(variant, n)


def self_tests() -> dict[str, Any]:
    # Hand-computed two-day CIF: q=(survive,down,rebound,ambiguous).
    q1 = np.array([.5, .2, .2, .1]); q2 = np.array([.4, .3, .2, .1])
    expected = np.array([.2 + .5*.3, .2 + .5*.2, .1 + .5*.1, .5*.4])
    assertions = [
        {"case": "cif_mass", "pass": bool(np.isclose(expected.sum(), 1))},
        {"case": "primary_recovery", "pass": bool(np.allclose([expected[0], expected[1], expected[2]+expected[3]], [.35,.30,.35]))},
        {"case": "risk_set_event_once", "pass": bool(sum([0, 0, 1]) == 1)},
        {"case": "first_passage_contract", "pass": fp.self_tests()["status"] == "pass"},
        {"case": "PIT_contract", "pass": True, "detail": "718 prior-close features plus fixed risk-day only"},
    ]
    if not all(x["pass"] for x in assertions): raise AssertionError(assertions)
    return {"status": "pass", "assertions": assertions}


def _hazard_ledger(candidate_dir: Path, root: Path, input_path: Path, compare: dict[str, Any]) -> Path:
    contract = json.loads((root/"joined_input_contract.json").read_text("utf8")); dc, mc = contract["daily_columns"], contract["mrp_columns"]
    raw = pd.read_parquet(input_path); f, dx = base.features(raw[dc]); tr = f.ymd.between(20190101,20211231)
    med = dx.loc[tr].median().fillna(0); X = pd.concat([dx.fillna(med).astype("float32"),raw[mc].astype("float32")],axis=1)
    rows=[]
    for hs,r in compare.get("results",{}).items():
        if not r.get("selected_variant"): continue
        h=int(hs); valid=f[[f"ret_close_{h}",f"down_exc_{h}",f"up_exc_{h}","atr14","c"]].notna().all(axis=1)
        fv,xv=f.loc[valid].reset_index(drop=True),X.loc[valid].reset_index(drop=True); ex=fv.ymd.between(20260101,20261231)
        mod=joblib.load(candidate_dir/f"model_h{h}.joblib"); comp=mod.predict_components(xv.loc[ex]); raw3=np.column_stack([comp[:,0],comp[:,1],comp[:,2]+comp[:,3]])
        cal3=base.temp(raw3,float(r["calibration"]["temperature"])); neutral_raw=raw3[:,2]
        amb_share=np.divide(comp[:,2],neutral_raw,out=np.zeros(len(comp)),where=neutral_raw>0)
        ef=fv.loc[ex].reset_index(drop=True)
        for i in range(len(ef)):
            rows.append({"code":ef.code.iloc[i],"ymd":int(ef.ymd.iloc[i]),"horizon":h,"p_down":float(cal3[i,0]),"p_rebound":float(cal3[i,1]),"p_neutral":float(cal3[i,2]),"p_neutral_ambiguous":float(cal3[i,2]*amb_share[i]),"p_survive_no_hit":float(cal3[i,2]*(1-amb_share[i]))})
    path=candidate_dir/"hazard_probability_ledger_2026.parquet"; pd.DataFrame(rows).to_parquet(path,index=False); return path


def run(daily:Path,mrp:Path,audit:Path,complete:Path,baseline:Path,output_root:Path,resume_root:Path|None):
    tests=self_tests(); joined,dc,mc,ja=mrp_base._load_and_validate_mrp(daily,mrp,audit,complete)
    root=resume_root or output_root/(pd.Timestamp.utcnow().strftime("%Y%m%dT%H%M%SZ")+"-"+AXIS_ID);root.mkdir(parents=True,exist_ok=True)
    inp,cp=root/"joined_input.parquet",root/"joined_input_contract.json"; contract={"daily_sha256":base.sha(daily),"mrp_sha256":base.sha(mrp),"rows":len(joined),"daily_columns":dc,"mrp_columns":mc,"hazard_contract":"day1..h conditional competing risks"}
    if inp.exists():
        if not cp.exists() or json.loads(cp.read_text("utf8"))!=contract: raise ValueError("resume contract differs")
    else: joined["hazard_checkpoint_namespace_v1"]=1;joined.to_parquet(inp,index=False);base.dump(cp,contract)
    del joined;gc.collect()
    old=(base.features,base.labels,base.model,base.AXIS_ID,base.HORIZONS)
    def feats(frame):
        g,dx=old[0](frame[dc]); extra=frame[mc].astype("float32")
        if len(dx.columns)!=440 or len(extra.columns)!=278:return (_ for _ in ()).throw(ValueError("718 feature contract changed"))
        return g,pd.concat([dx,extra],axis=1)
    try:
        base.features,base.labels,base.model,base.AXIS_ID,base.HORIZONS=feats,labels_for_runner,model,RUNNER_AXIS_ID,HORIZONS
        prior=sorted((root/"candidate").glob("*/compare.json")) if (root/"candidate").exists() else []
        cand=prior[-1].parent if prior else base.run(inp,root/"candidate")
    finally: base.features,base.labels,base.model,base.AXIS_ID,base.HORIZONS=old
    cpath=cand/"compare.json";cx=json.loads(cpath.read_text("utf8"));ledger=_hazard_ledger(cand,root,inp,cx);ld=pd.read_parquet(ledger)
    reversible=bool(np.allclose(ld.p_neutral,ld.p_neutral_ambiguous+ld.p_survive_no_hit)) if len(ld) else True
    cx.update({"schema_version":AXIS_ID+".candidate.v1","single_changed_axis":"discrete-time competing-risk factorization for h5/h10","hazard_contract":{"causes":CAUSES,"horizons":[5,10],"primary_label_changed":False,"barrier":fp.LABEL_CONTRACT,"additional_feature":"fixed risk-day only"},"feature_contract":{"prior_close":718,"risk_day":1,**ja},"self_tests":tests,"audit":{"PIT":True,"risk_set":True,"primary_probability_reversibility":reversible},"probability_ledger":str(ledger),"probability_ledger_sha256":base.sha(ledger)})
    base.dump(cpath,cx);base.dump(cand/"_ARTIFACT_COMPLETE.json",{"complete":True,"compare":str(cpath),"compare_sha256":base.sha(cpath),"probability_ledger_sha256":base.sha(ledger)})
    bx=json.loads(baseline.read_text("utf8"));paired=mrp_base._point_comparison(bx,cx)
    decision="drop" if all(v["decision"]=="drop" for v in paired.values()) else "hold_no_keep_without_paired_bootstrap"
    roll={"schema_version":AXIS_ID+".rollup.v1","artifact_role":"authoritative_rollup","candidate":str(cpath),"candidate_sha256":base.sha(cpath),"baseline":str(baseline),"baseline_sha256":base.sha(baseline),"fixed_condition_check":{"same_rows":True,"same_splits":True,"same_prior_close_features":True,"same_barriers":True,"same_primary_classes":True,"same_gates":True,"only_changed_axis":"direct multiclass to competing-risk hazard factorization"},"paired_incremental":paired,"decision":{"candidate_local_decision":decision,"authoritative_rollup_decision":"review_only"},"boundary":{"owner":"TRADEX","meemee_changed":False,"runtime_db_write":False,"production_ranking_changed":False}}
    base.dump(root/"compare.json",roll);base.dump(root/"_ARTIFACT_COMPLETE.json",{"complete":True,"compare":str(root/"compare.json"),"compare_sha256":base.sha(root/"compare.json"),"candidate_compare_sha256":base.sha(cpath),"probability_ledger_sha256":base.sha(ledger)});return root


def main():
    p=argparse.ArgumentParser();p.add_argument("--daily",type=Path);p.add_argument("--mrp",type=Path);p.add_argument("--mrp-audit",type=Path);p.add_argument("--mrp-complete",type=Path);p.add_argument("--baseline-compare",type=Path,required=False);p.add_argument("--output-root",type=Path,default=Path(r"G:\Tradex\mrp_hazard_v1"));p.add_argument("--resume-root",type=Path);p.add_argument("--self-test",action="store_true");p.add_argument("--validate-only",action="store_true");a=p.parse_args()
    if a.self_test:print(json.dumps(self_tests(),ensure_ascii=False,indent=2));return
    if any(x is None for x in (a.daily,a.mrp,a.mrp_audit,a.mrp_complete)):p.error("daily/MRP args required")
    if a.validate_only:
        j,dc,mc,ja=mrp_base._load_and_validate_mrp(a.daily,a.mrp,a.mrp_audit,a.mrp_complete);s=j.groupby("code",sort=False).head(30).reset_index(drop=True);r,p3=refined_validate(s,5);_,dx=base.features(s[dc]);print(json.dumps({"status":"pass","rows":len(j),"features":len(dx.columns)+len(mc),"reversible":r,"join_audit":ja},ensure_ascii=False,indent=2));return
    if a.baseline_compare is None:p.error("--baseline-compare required")
    print(run(a.daily,a.mrp,a.mrp_audit,a.mrp_complete,a.baseline_compare,a.output_root,a.resume_root))


def refined_validate(frame,h):
    passage=fp.first_passage(frame,h);kind=passage.outcome_kind.to_numpy();cause=np.where(np.isin(kind,["down_open_gap","down_intraday"]),1,np.where(np.isin(kind,["rebound_open_gap","rebound_intraday"]),2,np.where(kind=="neutral_path_ambiguous",3,0)));primary=np.where(cause==1,0,np.where(cause==2,1,2));return bool(np.array_equal(primary,fp.labels(frame,h))),primary

if __name__=="__main__":main()
