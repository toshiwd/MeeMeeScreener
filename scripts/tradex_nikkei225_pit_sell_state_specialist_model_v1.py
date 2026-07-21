from __future__ import annotations

import argparse
import gc
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import tradex_nikkei225_20bar_morphology_sequence_v1 as base
import tradex_nikkei225_first_passage_order_v1 as fp
import tradex_nikkei225_retryseq_first_passage_model_v1 as retry_model


AXIS_ID = "tradex_nikkei225_pit_sell_state_specialist_model_v1"
DEFAULT_STATE_ROOT = Path(r"G:\Tradex\pit_sell_state_specialist_v1\20260714T170218Z-tradex_nikkei225_pit_sell_state_specialist_v1-state")
DEFAULT_OUT = Path(r"G:\Tradex\pit_sell_state_specialist_model_v1")


def dump(path: Path, value) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def label_maps(full: pd.DataFrame) -> dict[int, pd.Series]:
    keys = pd.MultiIndex.from_frame(full[["code", "ymd"]].astype({"code": str, "ymd": int}))
    return {h: pd.Series(fp.labels(full, h), index=keys) for h in base.HORIZONS}


def s4_increment(frame: pd.DataFrame, horizon: int) -> dict:
    duplicate_columns = frame.columns[frame.columns.duplicated()].tolist()
    if duplicate_columns or list(frame.columns).count("code") != 1 or list(frame.columns).count("ymd") != 1:
        raise ValueError({"duplicate_or_noncanonical_key_columns": duplicate_columns})
    if frame.duplicated(["code", "ymd"]).any():
        raise ValueError("duplicate code/ymd rows within frozen horizon ledger")
    ymd = pd.to_numeric(frame["ymd"], errors="raise").astype("int64")
    month_series = ymd.astype(str).str.slice(0, 6)
    if not month_series.str.fullmatch(r"\d{6}").all():
        raise ValueError("month extraction from canonical ymd failed")
    event = frame.s4_sell_trigger_event.astype(bool).to_numpy()
    non = (~frame.s4_sell_trigger_raw.astype(bool)).to_numpy()
    y = frame.label_id.to_numpy(dtype=int)
    if event.sum() == 0 or non.sum() == 0:
        return {"decision": "hold_insufficient_breadth", "event_n": int(event.sum()), "control_n": int(non.sum())}
    down_delta = float((y[event] == 0).mean() - (y[non] == 0).mean())
    rebound_delta = float((y[event] == 1).mean() - (y[non] == 1).mean())
    ret_delta = float(frame.loc[event, f"ret_close_{horizon}"].mean() - frame.loc[non, f"ret_close_{horizon}"].mean())
    vals = {
        "n": np.ones(len(frame)), "e": event.astype(float), "c": non.astype(float),
        "ed": event * (y == 0), "cd": non * (y == 0),
        "er": event * (y == 1), "cr": non * (y == 1),
    }
    def stat(d, key):
        return d["ed"] / d["e"] - d["cd"] / d["c"] if key == "down" else d["er"] / d["e"] - d["cr"] / d["c"]
    boots = {}
    months = month_series.to_numpy()
    for name, groups, offset in (("code", frame.code.to_numpy(), 0), ("month", months, 1000)):
        boots[name] = {
            k: base.cluster_boot(groups, vals, lambda d, k=k: stat(d, k), base.SEED + horizon + offset + (0 if k == "down" else 10))
            for k in ("down", "rebound")
        }
    breadth = {
        "event_n": int(event.sum()), "event_codes": int(frame.loc[event, "code"].nunique()),
        "event_months": int(pd.Series(months[event]).nunique()),
        "max_code_share": float(frame.loc[event].groupby("code").size().max() / event.sum()),
        "max_month_share": float(pd.Series(months[event]).value_counts().max() / event.sum()),
    }
    breadth_ok = breadth["event_n"] >= {1: 160, 3: 140, 5: 120, 10: 100}[horizon] and breadth["event_codes"] >= 75 and breadth["event_months"] >= 24 and breadth["max_code_share"] <= .05 and breadth["max_month_share"] <= .15
    direction_ok = down_delta >= .05 and rebound_delta <= -.03
    bootstrap_ok = all(b["down"]["ci"][0] > 0 and b["rebound"]["ci"][1] < 0 for b in boots.values())
    return {
        "event_vs_nontrigger": {"down_share_delta": down_delta, "rebound_share_delta": rebound_delta, "mean_return_delta": ret_delta},
        "breadth": breadth, "bootstrap": boots,
        "gate": {"breadth": breadth_ok, "direction": direction_ok, "bootstrap": bootstrap_ok},
        "decision": "provisional_keep" if breadth_ok and direction_ok and bootstrap_ok else "drop",
    }


def run(daily: Path, retry: Path, retry_audit: Path, retry_complete: Path, state_root: Path, out_root: Path, resume_root: Path | None) -> Path:
    complete = json.loads((state_root / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    audit = json.loads((state_root / "audit.json").read_text(encoding="utf-8"))
    if complete.get("complete") is not True or not all(audit.get("checks", {}).values()):
        raise ValueError("state artifact incomplete")
    joined, daily_cols, retry_cols, join_audit = retry_model._load_and_validate_retry(daily, retry, retry_audit, retry_complete)
    joined["code"] = joined.code.astype(str).str.zfill(4)
    joined["ymd"] = pd.to_numeric(joined.ymd).astype(int)
    states = pd.read_parquet(state_root / "state_ledger.parquet")
    states["code"] = states.code.astype(str).str.zfill(4)
    states["ymd"] = pd.to_numeric(states.ymd).astype(int)
    full = joined.merge(states, on=["code", "ymd"], how="left", validate="one_to_one")
    maps = label_maps(full[daily_cols])
    eligible = full.s1_top_risk.fillna(False) | full.s2_top_formation.fillna(False) | full.s3_weakening.fillna(False) | full.s4_sell_trigger_raw.fillna(False)
    cohort = full.loc[eligible, [*daily_cols, *retry_cols]].reset_index(drop=True)
    cohort_states = full.loc[eligible, ["code", "ymd", "state", "s1_top_risk", "s2_top_formation", "s3_weakening", "s4_sell_trigger_raw", "s4_sell_trigger_event", "trigger_group_count", "trigger_gap_down", "trigger_ma20_break", "trigger_support_break"]].reset_index(drop=True)
    root = resume_root or out_root / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    root.mkdir(parents=True, exist_ok=True)
    inp = root / "cohort_input.parquet"
    state_copy = root / "cohort_state_ledger.parquet"
    if not inp.exists():
        cohort.to_parquet(inp, index=False)
        cohort_states.to_parquet(state_copy, index=False)
    old_features, old_labels, old_axis = base.features, base.labels, base.AXIS_ID
    def features(frame):
        g, dx = old_features(frame[daily_cols])
        return g, pd.concat([dx, frame[retry_cols].astype("float32")], axis=1)
    def labels(frame, h):
        keys = pd.MultiIndex.from_frame(frame[["code", "ymd"]].astype({"code": str, "ymd": int}))
        out = maps[h].reindex(keys)
        if out.isna().any():
            raise ValueError("full-universe first-passage label lookup failed")
        return out.to_numpy(dtype=np.int8)
    try:
        base.features, base.labels, base.AXIS_ID = features, labels, "pit_sell_state_specialist_runner_v1"
        prior = sorted((root / "candidate").glob("*/compare.json")) if (root / "candidate").exists() else []
        candidate_dir = prior[-1].parent if prior else base.run(inp, root / "candidate")
    finally:
        base.features, base.labels, base.AXIS_ID = old_features, old_labels, old_axis
    candidate = json.loads((candidate_dir / "compare.json").read_text(encoding="utf-8"))
    manifest = json.loads((candidate_dir / "feature_manifest.json").read_text(encoding="utf-8"))
    _, X = features(cohort)
    names = manifest["feature_names"]
    X = X.fillna(pd.Series(manifest["median_imputation"])).astype("float32")
    probability_rows, s4_results, s4_events = [], {}, []
    for h in base.HORIZONS:
        item = candidate["results"].get(str(h), {})
        valid = cohort[[f"ret_close_{h}", f"down_exc_{h}", f"up_exc_{h}", "atr14", "c"]].notna().all(axis=1)
        fv = cohort.loc[valid].reset_index(drop=True)
        sv = cohort_states.loc[valid].reset_index(drop=True)
        xv = X.loc[valid, names].reset_index(drop=True)
        y = labels(fv, h)
        frozen = fv.ymd.between(20230101, 20251231).to_numpy()
        tmp = fv.loc[frozen, ["code", "ymd", f"ret_close_{h}"]].reset_index(drop=True)
        state_payload = sv.loc[frozen].drop(columns=["code", "ymd"]).reset_index(drop=True)
        tmp = pd.concat([tmp, state_payload], axis=1)
        if tmp.columns.duplicated().any() or list(tmp.columns).count("code") != 1 or list(tmp.columns).count("ymd") != 1:
            raise ValueError({"post_join_duplicate_columns": tmp.columns[tmp.columns.duplicated()].tolist()})
        expected_keys = pd.MultiIndex.from_frame(fv.loc[frozen, ["code", "ymd"]].reset_index(drop=True))
        actual_keys = pd.MultiIndex.from_frame(tmp[["code", "ymd"]])
        if not actual_keys.equals(expected_keys):
            raise ValueError("post_join code/ymd key order changed")
        tmp["label_id"] = y[frozen]
        tmp["horizon"] = h
        s4_results[str(h)] = s4_increment(tmp, h)
        if "selected_variant" in item:
            mod = joblib.load(candidate_dir / f"model_h{h}.joblib")
            p = mod.predict_proba(xv[names])
            T = float(item["calibration"]["temperature"])
            pc = base.temp(p, T) if item["calibration"]["method"] == "temperature" else p
            for k, name in enumerate(("p_down", "p_rebound", "p_neutral")):
                tmp[name] = pc[frozen, k]
            probability_rows.append(tmp.copy())
        else:
            tmp["p_down"] = np.nan; tmp["p_rebound"] = np.nan; tmp["p_neutral"] = np.nan
            s4_results[str(h)]["model_probability_status"] = "unavailable_drop_no_oof_variant"
        s4_events.append(tmp.loc[tmp.s4_sell_trigger_event].copy())
    probability = pd.concat(probability_rows, ignore_index=True) if probability_rows else pd.DataFrame()
    events = pd.concat(s4_events, ignore_index=True) if s4_events else pd.DataFrame()
    prob_path, event_path = root / "probability_ledger_2023_2025.parquet", root / "s4_event_ledger_2023_2025.parquet"
    probability.to_parquet(prob_path, index=False); events.to_parquet(event_path, index=False)
    decision = "hold_requires_holm_and_general_gate" if any(v.get("decision") == "provisional_keep" for v in s4_results.values()) else "drop"
    payload = {
        "schema_version": AXIS_ID + ".compare.v1", "artifact_role": "authoritative", "research_phase": "effectiveness_judgment",
        "single_changed_axis": "condition fixed retryseq460 model on PIT ordered S1_S2_S3_sell_state_cohort",
        "fixed_contract": {"label": fp.LABEL_CONTRACT, "horizons": list(base.HORIZONS), "variants": base.VARIANTS, "splits": candidate["fixed_contract"]["splits"]},
        "cohort": {"rows": int(len(cohort)), "codes": int(cohort.code.nunique()), "state_artifact": str(state_root), "state_artifact_complete_sha256": retry_model.sha(state_root / "_ARTIFACT_COMPLETE.json")},
        "candidate": str(candidate_dir / "compare.json"), "candidate_results": candidate["results"], "s4_incremental": s4_results,
        "artifacts": {"probability_ledger_2023_2025": {"path": str(prob_path), "sha256": retry_model.sha(prob_path)}, "s4_event_ledger_2023_2025": {"path": str(event_path), "sha256": retry_model.sha(event_path)}},
        "decision": {"candidate_local_decision": decision, "authoritative_rollup_decision": "review_only"},
        "boundary": {"owner": "TRADEX", "meemee_changed": False, "runtime_db_write": False, "production_ranking_changed": False},
        "join_audit": join_audit,
    }
    dump(root / "compare.json", payload)
    dump(root / "_ARTIFACT_COMPLETE.json", {"complete": True, "compare": str(root / "compare.json"), "compare_sha256": retry_model.sha(root / "compare.json")})
    return root


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--daily", type=Path, required=True); ap.add_argument("--retry", type=Path, required=True)
    ap.add_argument("--retry-audit", type=Path, required=True); ap.add_argument("--retry-complete", type=Path, required=True)
    ap.add_argument("--state-root", type=Path, default=DEFAULT_STATE_ROOT); ap.add_argument("--output-root", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--resume-root", type=Path)
    a = ap.parse_args(); print(run(a.daily, a.retry, a.retry_audit, a.retry_complete, a.state_root, a.output_root, a.resume_root))


if __name__ == "__main__": main()
