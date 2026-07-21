from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import tradex_nikkei225_20bar_morphology_sequence_v1 as base
import tradex_nikkei225_first_passage_order_v1 as fp


AXIS_ID = "tradex_nikkei225_non_symmetric_rebound_veto_v1"
DEFAULT_DAILY = Path(r"G:\Tradex\tradex_nikkei225_daily_assessment_feature_ledger_v1\20260714T040047Z-tradex_nikkei225_daily_assessment_feature_ledger_v1\daily_assessment_features.parquet")
DEFAULT_STATE = Path(r"G:\Tradex\pit_sell_state_specialist_v1\20260714T170218Z-tradex_nikkei225_pit_sell_state_specialist_v1-state")
DEFAULT_OUT = Path(r"G:\Tradex\non_symmetric_rebound_veto_v1")
FROZEN_N = {1: 160, 3: 140, 5: 120, 10: 100}


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for b in iter(lambda: f.read(8 << 20), b""): h.update(b)
    return h.hexdigest()


def dump(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def prepare(daily_path: Path, state_root: Path) -> pd.DataFrame:
    complete = json.loads((state_root / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    audit = json.loads((state_root / "audit.json").read_text(encoding="utf-8"))
    if complete.get("complete") is not True or not all(audit.get("checks", {}).values()): raise ValueError("state artifact incomplete")
    d = pd.read_parquet(daily_path).sort_values(["code", "ymd"]).reset_index(drop=True)
    s = pd.read_parquet(state_root / "state_ledger.parquet")
    lanes = pd.read_parquet(state_root / "lane_evidence_ledger.parquet")
    for f in (d, s, lanes):
        f["code"] = f.code.astype(str).str.zfill(4); f["ymd"] = pd.to_numeric(f.ymd, errors="raise").astype(int)
    x = d.merge(s, on=["code", "ymd"], validate="one_to_one").merge(lanes, on=["code", "ymd"], validate="one_to_one")
    atr = x.atr14.replace(0, np.nan)
    x["veto_ma7_rising"] = x.ma7_slope5_atr
    x["veto_ma60_proximity"] = x.dist_ma60_atr.abs()
    x["veto_ma100_proximity"] = ((x.c - x.ma100) / atr).abs()
    x["veto_support_proximity"] = ((x.c - x.support20) / atr).where(x.c >= x.support20)
    x["veto_oversold_extension"] = x.oversold_risk.astype(float)
    x["veto_lower_wick"] = x.lower_wick_ratio
    bull = (x.c > x.o).astype(float)
    x["veto_strong_bull_recovery"] = bull * x.body_ratio * x.close_pos
    x["veto_volume_bull_recovery"] = x.veto_strong_bull_recovery * x.volume_ratio20
    return x


def grid_from_preperiod(x: pd.DataFrame) -> dict[str, Any]:
    e = x[x.s4_sell_trigger_event & x.ymd.between(20190101, 20211231)]
    specs = {
        "ma7_rising": ("veto_ma7_rising", "high", [.50, .60, .70, .80, .90]),
        "ma60_proximity": ("veto_ma60_proximity", "low", [.10, .20, .30, .40]),
        "ma100_proximity": ("veto_ma100_proximity", "low", [.10, .20, .30, .40]),
        "support_proximity": ("veto_support_proximity", "low", [.10, .20, .30, .40]),
        "lower_wick": ("veto_lower_wick", "high", [.50, .60, .70, .80, .90]),
        "strong_bull_recovery": ("veto_strong_bull_recovery", "high", [.50, .60, .70, .80, .90]),
        "volume_bull_recovery": ("veto_volume_bull_recovery", "high", [.50, .60, .70, .80, .90]),
    }
    candidates = []
    for family, (column, direction, qs) in specs.items():
        v = pd.to_numeric(e[column], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        for quantile in qs:
            candidates.append({"id": f"{family}_{direction}_q{int(quantile*100)}", "family": family, "column": column, "direction": direction, "quantile": quantile, "threshold": float(v.quantile(quantile))})
    candidates.append({"id": "oversold_extension_true", "family": "oversold_extension", "column": "veto_oversold_extension", "direction": "true", "quantile": None, "threshold": 1.0})
    return {"policy": "outcome_free_S4_event_feature_distribution_2019_2021", "rows": int(len(e)), "codes": int(e.code.nunique()), "candidates": candidates, "forbidden": ["profit_take", "support_to_resistance", "insufficient_rebound", "buy_add", "combined_score"]}


def veto_mask(frame: pd.DataFrame, rule: dict[str, Any]) -> np.ndarray:
    v = pd.to_numeric(frame[rule["column"]], errors="coerce").to_numpy(float)
    if rule["direction"] == "high": return np.isfinite(v) & (v >= rule["threshold"])
    if rule["direction"] == "low": return np.isfinite(v) & (v <= rule["threshold"])
    return np.isfinite(v) & (v >= .5)


def shares(y: np.ndarray, keep: np.ndarray) -> dict[str, Any]:
    return {"n": int(keep.sum()), "down": None if not keep.any() else float((y[keep] == 0).mean()), "rebound": None if not keep.any() else float((y[keep] == 1).mean()), "neutral": None if not keep.any() else float((y[keep] == 2).mean())}


def choose_2022(events: pd.DataFrame, y: np.ndarray, grid: dict[str, Any]) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    base_keep = np.ones(len(events), dtype=bool); baseline = shares(y, base_keep); rows = []
    for rule in grid["candidates"]:
        keep = ~veto_mask(events, rule); m = shares(y, keep)
        if m["n"]:
            m.update({"retained_share": float(keep.mean()), "down_vs_baseline": m["down"] - baseline["down"], "rebound_vs_baseline": m["rebound"] - baseline["rebound"], "codes": int(events.loc[keep, "code"].nunique()), "months": int(events.loc[keep, "ymd"].astype(str).str[:6].nunique())})
        eligible = bool(m["n"] >= 60 and m.get("retained_share", 0) >= .30 and m.get("codes", 0) >= 50 and m.get("months", 0) >= 9 and m["down_vs_baseline"] >= -.01 and m["rebound_vs_baseline"] <= -.02)
        rows.append({"rule": rule, "metrics": m, "eligible": eligible})
    ok = [r for r in rows if r["eligible"]]
    chosen = None if not ok else min(ok, key=lambda r: (r["metrics"]["rebound_vs_baseline"], -r["metrics"]["down_vs_baseline"], -r["metrics"]["retained_share"]))
    return chosen, rows


def bootstrap_compare(events: pd.DataFrame, y: np.ndarray, keep: np.ndarray, h: int) -> dict[str, Any]:
    vals = {"n": np.ones(len(y)), "k": keep.astype(float), "d": (y == 0).astype(float), "r": (y == 1).astype(float), "kd": keep * (y == 0), "kr": keep * (y == 1)}
    def stat(d, kind): return d["kd"] / d["k"] - d["d"] / d["n"] if kind == "down" else d["kr"] / d["k"] - d["r"] / d["n"]
    out = {}
    month = events.ymd.astype(str).str[:6].to_numpy()
    for name, groups, offset in (("code", events.code.to_numpy(), 0), ("month", month, 1000)):
        out[name] = {k: base.cluster_boot(groups, vals, lambda d, k=k: stat(d, k), base.SEED + h + offset + (0 if k == "down" else 10)) for k in ("down", "rebound")}
    return out


def evaluate_frozen(events: pd.DataFrame, control: pd.DataFrame, y: np.ndarray, control_y: np.ndarray, rule: dict[str, Any], h: int) -> tuple[dict[str, Any], np.ndarray]:
    keep = ~veto_mask(events, rule); b = shares(y, np.ones(len(y), bool)); m = shares(y, keep); c = shares(control_y, np.ones(len(control_y), bool))
    month = events.loc[keep, "ymd"].astype(str).str[:6]
    breadth = {"n": m["n"], "codes": int(events.loc[keep, "code"].nunique()), "months": int(month.nunique()), "max_code": float(events.loc[keep].groupby("code").size().max() / m["n"]), "max_month": float(month.value_counts().max() / m["n"])}
    delta = {"down_vs_s4_baseline": m["down"] - b["down"], "rebound_vs_s4_baseline": m["rebound"] - b["rebound"], "down_uplift_vs_nontrigger": m["down"] - c["down"], "rebound_delta_vs_nontrigger": m["rebound"] - c["rebound"], "baseline_down_uplift_vs_nontrigger": b["down"] - c["down"]}
    years = {}
    year_ok = True
    for year in (2023, 2024, 2025):
        z = events.ymd.between(year*10000+101, year*10000+1231).to_numpy(); kz = keep & z; bz = shares(y, z)
        mz = shares(y, kz); years[str(year)] = {"baseline": bz, "retained": mz, "down_vs_baseline": None if not mz["n"] else mz["down"]-bz["down"], "rebound_vs_baseline": None if not mz["n"] else mz["rebound"]-bz["rebound"]}
        year_ok &= bool(mz["n"] and mz["down"] >= bz["down"]-.02 and mz["rebound"] < bz["rebound"])
    boots = bootstrap_compare(events, y, keep, h)
    breadth_ok = breadth["n"] >= FROZEN_N[h] and breadth["codes"] >= 75 and breadth["months"] >= 24 and breadth["max_code"] <= .05 and breadth["max_month"] <= .15
    direction_ok = delta["down_vs_s4_baseline"] >= -.01 and delta["rebound_vs_s4_baseline"] <= -.02 and delta["down_uplift_vs_nontrigger"] > 0 and delta["down_uplift_vs_nontrigger"] >= delta["baseline_down_uplift_vs_nontrigger"]-.01
    boot_ok = all(v["down"]["ci"][0] > -.01 and v["rebound"]["ci"][1] < 0 for v in boots.values())
    primary_p = max(max(v["rebound"]["p_ge0"], float(v["down"]["ci"][0] <= -.01)) for v in boots.values())
    return {"rule": rule, "baseline": b, "retained": m, "control": c, "delta": delta, "breadth": breadth, "yearly": years, "bootstrap": boots, "primary_p": primary_p, "gate": {"breadth": breadth_ok, "direction": direction_ok, "yearly": year_ok, "bootstrap": boot_ok}, "decision": "provisional_keep" if breadth_ok and direction_ok and year_ok and boot_ok else "drop"}, keep


def run(daily_path: Path, state_root: Path, out_root: Path) -> Path:
    x = prepare(daily_path, state_root); grid = grid_from_preperiod(x); results = {}; filtered = []
    for h in base.HORIZONS:
        yall = fp.labels(x, h); valid = x[[f"ret_close_{h}", f"down_exc_{h}", f"up_exc_{h}", "atr14", "c"]].notna().all(axis=1).to_numpy()
        e22 = valid & x.s4_sell_trigger_event.to_numpy(bool) & x.ymd.between(20220101, 20221231).to_numpy(); chosen, search = choose_2022(x.loc[e22].reset_index(drop=True), yall[e22], grid)
        if chosen is None:
            results[str(h)] = {"decision": "drop_no_2022_veto", "selection_search": search}; continue
        frozen = valid & x.ymd.between(20230101, 20251231).to_numpy(); ev = frozen & x.s4_sell_trigger_event.to_numpy(bool); control = frozen & (~x.s4_sell_trigger_raw.to_numpy(bool)) & (x.s1_top_risk.to_numpy(bool) | x.s2_top_formation.to_numpy(bool) | x.s3_weakening.to_numpy(bool))
        result, keep = evaluate_frozen(x.loc[ev].reset_index(drop=True), x.loc[control].reset_index(drop=True), yall[ev], yall[control], chosen["rule"], h); result["selection_2022"] = chosen; result["selection_search"] = search; results[str(h)] = result
        ledger = x.loc[ev, ["code", "ymd", "state", "s4_sell_trigger_event", "ma7_slope5_atr", "dist_ma60_atr", "ma100", "support20", "oversold_risk", "lower_wick_ratio", "body_ratio", "close_pos", "volume_ratio20", *[c for c in x if c.startswith("veto_")]]].reset_index(drop=True)
        ledger["horizon"] = h; ledger["label_id"] = yall[ev]; ledger["selected_rule"] = chosen["rule"]["id"]; ledger["vetoed"] = ~keep; ledger["retained"] = keep; filtered.append(ledger)
    ps = {int(h): v["primary_p"] for h, v in results.items() if "primary_p" in v}; holm = base.holm(ps) if ps else {}
    for h, adj in holm.items():
        results[str(h)]["holm"] = adj
        if not adj["pass"] or results[str(h)]["decision"] != "provisional_keep": results[str(h)]["decision"] = "drop"
        else: results[str(h)]["decision"] = "keep"
    decision = "keep_review_only" if any(v.get("decision") == "keep" for v in results.values()) else "drop"
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"); out = out_root / f"{stamp}-{AXIS_ID}"; out.mkdir(parents=True, exist_ok=False)
    ledger_columns = ["code", "ymd", "state", "s4_sell_trigger_event", "horizon", "label_id", "selected_rule", "vetoed", "retained"]
    ledger = pd.concat(filtered, ignore_index=True) if filtered else pd.DataFrame({c: pd.Series(dtype="object") for c in ledger_columns})
    ledger_path = out / "filtered_s4_event_ledger_2023_2025.parquet"; ledger.to_parquet(ledger_path, index=False)
    grid_path = out / "preperiod_veto_grid.json"; dump(grid_path, grid)
    payload = {"schema_version": AXIS_ID+".compare.v1", "artifact_role": "authoritative", "research_phase": "effectiveness_judgment", "single_changed_axis": "non_symmetric_rebound_veto_on_fixed_authoritative_S4_events", "source": {"daily": {"path": str(daily_path), "sha256": sha(daily_path)}, "state_root": str(state_root), "state_complete_sha256": sha(state_root/"_ARTIFACT_COMPLETE.json")}, "fixed_contract": {"state_and_events": "immutable", "label": fp.LABEL_CONTRACT, "grid": "2019_2021_outcome_free", "selection": "2022_only", "frozen": "2023_2025", "no_combined_score": True, "buy_add_conversion": False, "profit_take_used": False, "unmeasured_features_used": False}, "results": results, "decision": {"candidate_local_decision": decision, "authoritative_rollup_decision": "review_only"}, "artifacts": {"filtered_event_ledger": {"path": str(ledger_path), "sha256": sha(ledger_path)}, "preperiod_grid": {"path": str(grid_path), "sha256": sha(grid_path)}}, "boundary": {"owner": "TRADEX", "meemee_changed": False, "runtime_db_write": False, "production_ranking_changed": False}}
    compare = out/"compare.json"; dump(compare, payload); dump(out/"_ARTIFACT_COMPLETE.json", {"complete": True, "compare": str(compare), "compare_sha256": sha(compare), "ledger_sha256": sha(ledger_path), "grid_sha256": sha(grid_path)}); return out


def main() -> None:
    p=argparse.ArgumentParser(); p.add_argument("--daily",type=Path,default=DEFAULT_DAILY); p.add_argument("--state-root",type=Path,default=DEFAULT_STATE); p.add_argument("--output-root",type=Path,default=DEFAULT_OUT); a=p.parse_args(); print(run(a.daily,a.state_root,a.output_root))
if __name__ == "__main__": main()
