"""Evaluate the user's predeclared seven-closes-below-MA7 exhaustion veto."""
import argparse
import hashlib
import json
from pathlib import Path

import duckdb
import pandas as pd


THRESHOLD = 7


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def stats(frame: pd.DataFrame) -> dict:
    x = frame[frame.status.eq("complete")]
    v = x.return_fixed3_pct.dropna(); gain, loss = v[v > 0].sum(), -v[v < 0].sum()
    return {"n": int(len(frame)), "D": int(x.outcome_fixed3.eq("D").sum()),
        "R": int(x.outcome_fixed3.eq("R").sum()), "N": int(x.outcome_fixed3.eq("N").sum()),
        "D_rate": None if x.empty else float(x.outcome_fixed3.eq("D").mean()),
        "R_rate": None if x.empty else float(x.outcome_fixed3.eq("R").mean()),
        "mean_fixed3_pct": None if v.empty else float(v.mean()),
        "mean_h5_close_pct": None if x.empty else float(x.return_h5_close_pct.mean()),
        "profit_factor": None if loss == 0 else float(gain / loss),
        "max_loss_pct": None if v.empty else float(v.min())}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--diagnostic", type=Path, required=True)
    p.add_argument("--db", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args(); args.output.mkdir(parents=True, exist_ok=False)
    source = args.diagnostic / "downside_room_diagnostic_ledger.parquet"
    data = pd.read_parquet(source).copy(); codes = data.code.astype(str).str.zfill(4).unique().tolist()
    con = duckdb.connect(str(args.db), read_only=True)
    prices = con.execute("select code,strftime(to_timestamp(date),'%Y%m%d')::integer ymd,c "
        "from daily_bars where code in (select unnest(?)) order by code,date", [codes]).fetchdf()
    prices.code = prices.code.astype(str).str.zfill(4)
    histories = {code: g.reset_index(drop=True) for code, g in prices.groupby("code")}
    streaks = []
    for row in data.itertuples():
        g = histories[row.code]; hit = g.index[g.ymd.eq(int(row.ymd))]
        if len(hit) != 1: raise RuntimeError(f"missing signal {row.code} {row.ymd}")
        through = g.iloc[:int(hit[0]) + 1].copy(); through["ma7_pit"] = through.c.rolling(7).mean()
        streak = 0
        for below in through.c.lt(through.ma7_pit).iloc[::-1]:
            if not below: break
            streak += 1
        streaks.append(streak)
    data["below_ma7_close_streak"] = streaks
    data["exhaustion_veto"] = data.below_ma7_close_streak.ge(THRESHOLD)
    answered = data[data.human_direction.ne("")]
    human = answered[answered.human_direction.eq("SELL")]
    human_gate = human[~human.exhaustion_veto]
    model = answered[answered.model_direction.eq("SELL")]
    model_gate_diagnostic = model[~model.exhaustion_veto]
    hb, hg, mb, mg = stats(human), stats(human_gate), stats(model), stats(model_gate_diagnostic)
    d_retention = None if hb["D"] == 0 else hg["D"] / hb["D"]
    ledger_path = args.output / "below_ma7_streak_diagnostic_ledger.parquet"; data.to_parquet(ledger_path, index=False)
    keep = hg["R_rate"] < hb["R_rate"] and hg["max_loss_pct"] > hb["max_loss_pct"] and d_retention >= .70
    result = {
        "schema_version": "tradex_blind_below_ma7_streak_axis_v1.compare.v1",
        "artifact_role": "authoritative_predeclared_user_complement_discovery",
        "review_only": True,
        "fixed_conditions": {"axis": "consecutive signal closes below PIT MA7", "threshold": THRESHOLD,
            "threshold_origin": "user predeclared seven-below-seven exhaustion concept",
            "scope": "veto or reduce user new SELL only; never veto model SELL or existing-short management",
            "ma7": "rolling 7 closes through signal close", "execution": "next_session_open", "horizon_sessions": 5,
            "weekly_inputs": [], "costs": "ignored", "clean_oos": False},
        "user_sell_alone": hb, "user_sell_with_exhaustion_gate": hg,
        "model_sell_diagnostic_only": mb, "model_sell_if_misapplied_diagnostic": mg,
        "observed_branching": {"user_candidates": len(human), "gated_user_candidates": len(human_gate),
            "removed": int(human.exhaustion_veto.sum()),
            "removed_outcomes": {str(k): int(v) for k, v in human[human.exhaustion_veto].outcome_fixed3.value_counts().items()},
            "D_retention": d_retention, "R_removed": hb["R"] - hg["R"],
            "selection_divergence_reason": "user SELL is treated as exhausted after seven consecutive closes below MA7"},
        "judgment": {"candidate_local_decision": "keep_discovery_challenger" if keep else "drop",
            "authoritative_rollup_decision": "hold_pending_fresh_unused_human_review" if keep else "drop",
            "reason": "model-side diagnostic is explicitly non-operational because the same veto removes a successful model ADD"},
        "not_changed": ["model sell candidates", "existing short management", "MeeMee", "ranking", "runtime DB", "production trading logic"]}
    cp = args.output / "compare.json"; cp.write_text(json.dumps(result, ensure_ascii=False, indent=2)+"\n",encoding="utf-8")
    audit = {"source_sha256": sha(source), "db_path": str(args.db.resolve()), "db_read_only": True,
        "rows": len(data), "weekly_columns_used": [], "future_selection_columns_used": [], "ledger_sha256": sha(ledger_path)}
    (args.output/"audit.json").write_text(json.dumps(audit,indent=2)+"\n",encoding="utf-8")
    (args.output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json","sha256":sha(cp)},indent=2)+"\n",encoding="utf-8")
    print(json.dumps({"output":str(args.output),"user":hb,"gate":hg,"model_misapply":mg,"branching":result["observed_branching"],"judgment":result["judgment"]},ensure_ascii=False,indent=2))


if __name__ == "__main__": main()
