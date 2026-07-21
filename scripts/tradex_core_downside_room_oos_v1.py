"""One-axis OOS test of downside room before promoting a probe to core size."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

THRESHOLD_ATR = 0.75
SUPPORT_COLUMNS = ["ma7", "ma20", "ma60", "ma100", "ma200", "support20"]


def add_room(frame: pd.DataFrame) -> pd.DataFrame:
    x = frame.copy()
    supports = x[SUPPORT_COLUMNS].where(x[SUPPORT_COLUMNS].le(x.c, axis=0))
    x["nearest_support_price"] = supports.max(axis=1)
    x["nearest_support_kind"] = supports.idxmax(axis=1)
    x["downside_room_atr"] = (x.c - x.nearest_support_price) / x.atr14
    x["room_gate_pass"] = x.downside_room_atr.ge(THRESHOLD_ATR)
    return x


def metrics(x: pd.DataFrame, probes: int) -> dict:
    n = len(x)
    down = x.core_label_5.eq(0)
    rebound = x.core_label_5.eq(1)
    return {
        "core_entries": n,
        "down_first_h5": None if not n else float(down.mean()),
        "rebound_first_h5": None if not n else float(rebound.mean()),
        "neutral_h5": None if not n else float((~(down | rebound)).mean()),
        "end_to_end_probe_core_down_h5": None if not probes else float(down.sum() / probes),
        "down_2pct_h5": None if not n else float(x.core_down_exc_5.le(-0.02).mean()),
        "mfe_short_h5_median": None if not n else float((-x.core_down_exc_5).median()),
        "mae_short_h5_median": None if not n else float(x.core_up_exc_5.median()),
        "room_atr_median": None if not n else float(x.downside_room_atr.median()),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--episodes", type=Path, required=True)
    ap.add_argument("--features", type=Path, required=True)
    ap.add_argument("--human-annotations", type=Path, required=True)
    ap.add_argument("--output-root", type=Path, required=True)
    args = ap.parse_args()
    out = args.output_root / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-tradex_core_downside_room_oos_v1"
    out.mkdir(parents=True, exist_ok=False)
    ep = pd.read_parquet(args.episodes)
    core = ep[ep.core_ymd.notna()].copy()
    core["core_ymd"] = core.core_ymd.astype(int)
    feature_cols = ["code", "ymd", "c", "atr14", *SUPPORT_COLUMNS]
    ft = pd.read_parquet(args.features, columns=feature_cols)
    core = core.merge(ft, left_on=["code", "core_ymd"], right_on=["code", "ymd"], how="left", validate="one_to_one")
    core = add_room(core)
    years = {}
    for year in (2023, 2024, 2025):
        probes = int(ep.year.eq(year).sum())
        base = core[core.year.eq(year)]
        challenger = base[base.room_gate_pass]
        bm = metrics(base, probes)
        cm = metrics(challenger, probes)
        years[str(year)] = {
            "champion": bm, "challenger_room_ge_0_75atr": cm,
            "coverage": None if not len(base) else float(len(challenger) / len(base)),
            "delta_down_first_h5": cm["down_first_h5"] - bm["down_first_h5"],
            "delta_rebound_first_h5": cm["rebound_first_h5"] - bm["rebound_first_h5"],
        }
    human_source = json.loads(args.human_annotations.read_text(encoding="utf-8"))["annotations"]
    human = pd.DataFrame([{
        "case_id": r["case_id"], "code": str(r["code"]).zfill(4), "ymd": int(r["ymd"]),
        "human_room_risk": bool(r.get("concepts", {}).get("downside_room_to_support_risk", False)),
        "blind_status": r.get("blind_status", "BLIND_UNSPECIFIED"),
    } for r in human_source])
    ft_h = pd.read_parquet(args.features, columns=feature_cols)
    ft_h["code"] = ft_h.code.astype(str).str.zfill(4)
    human = human.merge(ft_h, on=["code", "ymd"], how="left", validate="one_to_one")
    human = add_room(human)
    human["machine_room_risk"] = ~human.room_gate_pass
    eligible = human[~human.blind_status.eq("OUTCOME_AWARE_EXCLUDE_FROM_ACCURACY")]
    human_diag = {
        "rows": len(human), "eligible_rows": len(eligible),
        "human_risk_positive": int(eligible.human_room_risk.sum()),
        "agreement": int(eligible.human_room_risk.eq(eligible.machine_room_risk).sum()),
        "agreement_rate": float(eligible.human_room_risk.eq(eligible.machine_room_risk).mean()),
        "risk_recall": None if not eligible.human_room_risk.any() else float(eligible.loc[eligible.human_room_risk, "machine_room_risk"].mean()),
        "rows_detail": human[["case_id", "code", "ymd", "human_room_risk", "nearest_support_kind", "downside_room_atr", "machine_room_risk", "blind_status"]].where(pd.notna(human), None).to_dict("records"),
    }
    pass_all = all(years[str(y)]["challenger_room_ge_0_75atr"]["down_first_h5"] > years[str(y)]["challenger_room_ge_0_75atr"]["rebound_first_h5"] for y in (2023, 2024, 2025))
    payload = {
        "schema_version": "tradex_core_downside_room_oos_v1.compare.v1",
        "artifact_role": "authoritative",
        "axis": "core promotion only: nearest support room >= 0.75 ATR",
        "support_set": SUPPORT_COLUMNS, "threshold_atr": THRESHOLD_ATR,
        "year_results": years, "human_annotation_diagnostic": human_diag,
        "judgment": {"decision": "keep" if pass_all else "drop", "down_exceeds_rebound_all_years": pass_all,
                     "reason": "core gate must make h5 down-first exceed rebound-first in every OOS year"},
        "not_changed": ["monthly environment layers", "probe", "add2", "candle gate", "market gate", "MeeMee", "ranking", "runtime DB"],
    }
    (out / "compare.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    core[["code", "probe_ymd", "core_ymd", "year", "nearest_support_kind", "nearest_support_price", "downside_room_atr", "room_gate_pass", "core_label_5", "core_down_exc_5", "core_up_exc_5"]].to_parquet(out / "core_room_ledger.parquet", index=False)
    audit = {"core_rows": len(core), "room_missing": int(core.downside_room_atr.isna().sum()), "future_used_for_gate": False, "duplicate_core": int(core.duplicated(["code", "probe_ymd"]).sum()), "review_only": True}
    (out / "audit.json").write_text(json.dumps(audit, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (out / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json"}, indent=2) + "\n", encoding="utf-8")
    print(out)
    print(json.dumps({"years": years, "human": {k:v for k,v in human_diag.items() if k != "rows_detail"}, "judgment": payload["judgment"], "audit": audit}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
