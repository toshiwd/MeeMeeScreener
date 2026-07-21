"""One-axis OOS test of prior rolling-low state at core promotion."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


def classify(x: pd.DataFrame) -> pd.DataFrame:
    y = x.copy()
    y["prior_low_distance_atr"] = (y.c - y.prior_low) / y.atr14
    y["prior_low_state"] = np.select(
        [
            (y.c < y.prior_low) & (y.prev_c >= y.prior_low_prev),
            (y.c < y.prior_low) & (y.prev_c < y.prior_low_prev),
            (y.l < y.prior_low) & (y.c >= y.prior_low),
        ],
        ["BREAK_CLOSE", "BELOW_AFTER_BREAK", "INTRADAY_BREAK_RECLAIM"],
        default="LIVE_ABOVE",
    )
    y["state_gate_pass"] = (
        y.prior_low_state.isin(["BREAK_CLOSE", "BELOW_AFTER_BREAK"])
        | (y.prior_low_state.eq("LIVE_ABOVE") & y.prior_low_distance_atr.ge(0.75))
    )
    return y


def rates(x: pd.DataFrame) -> dict:
    return {
        "n": len(x),
        "down_first_h5": None if x.empty else float(x.core_label_5.eq(0).mean()),
        "rebound_first_h5": None if x.empty else float(x.core_label_5.eq(1).mean()),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--episodes", type=Path, required=True)
    ap.add_argument("--features", type=Path, required=True)
    ap.add_argument("--human-annotations", type=Path, required=True)
    ap.add_argument("--output-root", type=Path, required=True)
    args = ap.parse_args()
    out = args.output_root / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-tradex_core_prior_low_state_oos_v1"
    out.mkdir(parents=True, exist_ok=False)
    ft = pd.read_parquet(args.features, columns=["code", "ymd", "o", "h", "l", "c", "atr14", "support20"]).sort_values(["code", "ymd"])
    grp = ft.groupby("code", sort=False)
    ft["prev_c"] = grp.c.shift(1)
    # support20 is already point-in-time: min(low) over t-20..t-1.
    ft["prior_low"] = ft.support20
    ft["prior_low_prev"] = grp.support20.shift(1)
    ep = pd.read_parquet(args.episodes)
    core = ep[ep.core_ymd.notna()].copy()
    core["core_ymd"] = core.core_ymd.astype(int)
    core = core.merge(ft, left_on=["code", "core_ymd"], right_on=["code", "ymd"], how="left", validate="one_to_one")
    core = classify(core)
    years = {}
    for year in (2023, 2024, 2025):
        z = core[core.year.eq(year)]
        q = z[z.state_gate_pass]
        years[str(year)] = {
            "champion": rates(z), "challenger": rates(q),
            "coverage": float(len(q) / len(z)),
            "state_results": {state: rates(part) for state, part in z.groupby("prior_low_state")},
        }
    annotations = json.loads(args.human_annotations.read_text(encoding="utf-8"))["annotations"]
    human = pd.DataFrame([{"case_id": r["case_id"], "code": r["code"], "ymd": r["ymd"], "human_decision": r["human_decision"]} for r in annotations])
    ft_h = ft.copy(); ft_h["code"] = ft_h.code.astype(str).str.zfill(4)
    human = classify(human.merge(ft_h, on=["code", "ymd"], how="left", validate="one_to_one"))
    human_rows = human[["case_id", "code", "ymd", "human_decision", "prior_low_state", "prior_low_distance_atr"]].where(pd.notna(human), None).to_dict("records")
    pass_all = all(years[str(y)]["challenger"]["down_first_h5"] > years[str(y)]["challenger"]["rebound_first_h5"] for y in (2023, 2024, 2025))
    payload = {
        "schema_version": "tradex_core_prior_low_state_oos_v1.compare.v1",
        "artifact_role": "authoritative",
        "axis": "core promotion only: prior support20 state",
        "state_contract": {"prior_low": "current-row support20 (prior 20 bars, excluding the current bar)", "prior_low_prev": "previous-row support20", "states": ["LIVE_ABOVE", "BREAK_CLOSE", "BELOW_AFTER_BREAK", "INTRADAY_BREAK_RECLAIM"]},
        "year_results": years, "human_rows": human_rows,
        "judgment": {"decision": "keep" if pass_all else "drop", "reason": "generic rolling support20 state must separate h5 decline from rebound in every year and reproduce annotated swing-low breaks"},
        "not_changed": ["monthly environment", "MA state", "candle state", "probe", "add2", "MeeMee", "ranking", "runtime DB"],
    }
    (out / "compare.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    core[["code", "probe_ymd", "core_ymd", "year", "prior_low", "prior_low_distance_atr", "prior_low_state", "state_gate_pass", "core_label_5"]].to_parquet(out / "core_prior_low_state_ledger.parquet", index=False)
    audit = {"core_rows": len(core), "missing_state_input": int(core.prior_low.isna().sum()), "duplicate_core": int(core.duplicated(["code", "probe_ymd"]).sum()), "future_used": False, "review_only": True}
    (out / "audit.json").write_text(json.dumps(audit, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (out / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json"}, indent=2) + "\n", encoding="utf-8")
    print(out)
    print(json.dumps({"years": years, "judgment": payload["judgment"], "audit": audit}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
