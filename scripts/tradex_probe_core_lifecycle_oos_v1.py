"""Review-only OOS audit of probe -> core entry -> subsequent short decline."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


EVENT_LEDGER = Path(r"G:\Tradex\monthly_env_probe_add_oos_v1\20260715T021134Z-tradex_monthly_env_probe_add_oos_v1\probe_add_event_ledger.parquet")
FEATURE_LEDGER = Path(r"G:\Tradex\tradex_nikkei225_daily_assessment_feature_ledger_v1\20260714T040047Z-tradex_nikkei225_daily_assessment_feature_ledger_v1\daily_assessment_features.parquet")
OUT_ROOT = Path(r"G:\Tradex\probe_core_lifecycle_oos_v1")
YEARS = (2023, 2024, 2025)


def safe_rate(s: pd.Series) -> float | None:
    return None if len(s) == 0 else float(s.mean())


def episodes(events: pd.DataFrame, features: pd.DataFrame) -> pd.DataFrame:
    keep = ["code", "ymd", "c", "atr14", "down_exc_3", "down_exc_5", "down_exc_10",
            "up_exc_3", "up_exc_5", "up_exc_10", "ret_close_3", "ret_close_5", "ret_close_10"]
    x = events.merge(features[keep], on=["code", "ymd"], how="left", validate="one_to_one")
    rows = []
    for code, g0 in x.sort_values(["code", "ymd"]).groupby("code", sort=False):
        g = g0.reset_index(drop=True)
        active = None
        for _, r in g.iterrows():
            if bool(r.probe_event):
                if active is not None:
                    rows.append(active)
                active = {
                    "code": str(code), "probe_ymd": int(r.ymd), "year": int(str(int(r.ymd))[:4]),
                    "family": str(r.position_family), "environment": str(r.environment),
                    "probe_c": float(r.c), "probe_atr": float(r.atr14),
                    "core_ymd": None, "add2_ymd": None,
                }
            if active is None:
                continue
            if bool(r.add1_event) and active["core_ymd"] is None:
                active["core_ymd"] = int(r.ymd)
                active["core_days"] = int((g.ymd.eq(r.ymd).idxmax()) - (g.ymd.eq(active["probe_ymd"]).idxmax()))
                active["pre_core_move_pct"] = float(r.c / active["probe_c"] - 1.0)
                active["pre_core_move_atr"] = float((r.c - active["probe_c"]) / active["probe_atr"])
                for h in (3, 5, 10):
                    active[f"core_label_{h}"] = int(r[f"label_{h}"])
                    active[f"core_down_exc_{h}"] = float(r[f"down_exc_{h}"])
                    active[f"core_up_exc_{h}"] = float(r[f"up_exc_{h}"])
                    active[f"core_ret_{h}"] = float(r[f"ret_close_{h}"])
            if bool(r.add2_event) and active["add2_ymd"] is None:
                active["add2_ymd"] = int(r.ymd)
            if active is not None and int(r.position_stage) == 0 and int(r.ymd) != active["probe_ymd"]:
                rows.append(active)
                active = None
        if active is not None:
            rows.append(active)
    return pd.DataFrame(rows).drop_duplicates(["code", "probe_ymd"], keep="last")


def cell(g: pd.DataFrame) -> dict:
    core = g[g.core_ymd.notna()].copy()
    out = {
        "probe_episodes": int(len(g)), "codes": int(g.code.nunique()),
        "core_entries": int(len(core)), "probe_to_core_rate": safe_rate(g.core_ymd.notna()),
        "probe_to_core_days_median": None if core.empty else float(core.core_days.median()),
        "pre_core_move_pct_median": None if core.empty else float(core.pre_core_move_pct.median()),
        "pre_core_move_atr_median": None if core.empty else float(core.pre_core_move_atr.median()),
    }
    for h in (3, 5, 10):
        if core.empty:
            out[f"h{h}"] = {}
            continue
        down = core[f"core_label_{h}"].eq(0)
        rebound = core[f"core_label_{h}"].eq(1)
        out[f"h{h}"] = {
            "down_first_given_core": safe_rate(down),
            "rebound_first_given_core": safe_rate(rebound),
            "neutral_given_core": safe_rate(~(down | rebound)),
            "end_to_end_probe_core_down": float(down.sum() / len(g)),
            "down_2pct_reached": safe_rate(core[f"core_down_exc_{h}"].le(-0.02)),
            "down_3pct_reached": safe_rate(core[f"core_down_exc_{h}"].le(-0.03)),
            "mfe_short_median": float((-core[f"core_down_exc_{h}"]).median()),
            "mae_short_median": float(core[f"core_up_exc_{h}"].median()),
            "close_return_median": float(core[f"core_ret_{h}"].median()),
        }
    return out


def main() -> None:
    ev = pd.read_parquet(EVENT_LEDGER)
    ft = pd.read_parquet(FEATURE_LEDGER)
    ep = episodes(ev, ft)
    ep = ep[ep.year.isin(YEARS)].copy()
    results = {str(y): cell(ep[ep.year.eq(y)]) for y in YEARS}
    family = {}
    for fam in sorted(ep.family.dropna().unique()):
        family[fam] = {str(y): cell(ep[ep.year.eq(y) & ep.family.eq(fam)]) for y in YEARS}
    stable = all(results[str(y)]["probe_episodes"] >= 100 for y in YEARS)
    h5_positive = all(results[str(y)]["h5"].get("down_first_given_core", 0) > results[str(y)]["h5"].get("rebound_first_given_core", 1) for y in YEARS)
    decision = "keep" if stable and h5_positive else "drop"
    payload = {
        "schema_version": "tradex_probe_core_lifecycle_oos_v1.compare.v1",
        "artifact_role": "authoritative",
        "research_contract": {"probe": "probe_event", "core_entry": "add1_event at close", "add2": "post-core additional short", "outcome_start": "t+1", "primary": "h5 first-passage after core and end-to-end probe->core->down"},
        "year_results": results, "family_results": family,
        "judgment": {"decision": decision, "breadth_pass": stable, "h5_down_exceeds_rebound_all_years": h5_positive,
                     "reason": "core entry must be followed by down-first more often than rebound-first in every OOS year"},
        "not_changed": ["monthly environment", "probe trigger", "add trigger", "MeeMee", "ranking", "runtime DB"],
    }
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = OUT_ROOT / f"{stamp}-tradex_probe_core_lifecycle_oos_v1"
    out.mkdir(parents=True, exist_ok=False)
    ep.to_parquet(out / "episode_ledger.parquet", index=False)
    (out / "compare.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    audit = {"event_rows": len(ev), "episode_rows": len(ep), "duplicate_episode": int(ep.duplicated(["code", "probe_ymd"]).sum()), "feature_missing_core": int(ep[ep.core_ymd.notna()].core_down_exc_5.isna().sum()), "future_used_for_selection": False}
    (out / "audit.json").write_text(json.dumps(audit, ensure_ascii=False, indent=2), encoding="utf-8")
    (out / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json"}, indent=2), encoding="utf-8")
    print(out)
    print(json.dumps({"judgment": payload["judgment"], "year_results": results, "audit": audit}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
