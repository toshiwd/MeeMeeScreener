#!/usr/bin/env python
"""Build a blinded, annotation-only active-learning review pack.

Outcome labels are permitted solely to balance the human-review queue. They are
written only to answer_key.json and never to review_queue.csv/json.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import tradex_nikkei225_first_passage_order_v1 as fp

DAILY = Path(r"G:\Tradex\tradex_nikkei225_daily_assessment_feature_ledger_v1\20260714T040047Z-tradex_nikkei225_daily_assessment_feature_ledger_v1\daily_assessment_features.parquet")
STATE_ROOT = Path(r"G:\Tradex\failed_rebound_before_rebreak_v1\20260714T174509Z-tradex_nikkei225_failed_rebound_before_rebreak_v1")
LANE = Path(r"G:\Tradex\pit_sell_state_specialist_v1\20260714T170218Z-tradex_nikkei225_pit_sell_state_specialist_v1-state\lane_evidence_ledger.parquet")
OPEN = Path(r"G:\Tradex\open_trigger_topology_audit_v1\20260714T183538Z-tradex_nikkei225_open_trigger_topology_audit_v1\frozen_open_event_ledger.parquet")
S4_COMPARE = Path(r"G:\Tradex\s4_topology_with_failed_rebound_v1\20260714T174532Z-tradex_nikkei225_s4_topology_with_failed_rebound_v1\compare.json")
OPEN_COMPARE = Path(r"G:\Tradex\open_trigger_topology_audit_v1\20260714T183538Z-tradex_nikkei225_open_trigger_topology_audit_v1\compare.json")


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def dump(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def prior_paths(x: pd.DataFrame) -> pd.DataFrame:
    g = x.groupby("code", sort=False)
    er = g.bull_erasure_retry_candidate.transform(lambda q: q.shift().rolling(20, min_periods=1).max()).fillna(0).astype(bool)
    retry_source = x.s2_candidate_today.astype(bool) & ~x.bull_erasure_retry_candidate.astype(bool)
    rt = retry_source.groupby(x.code, sort=False).transform(lambda q: q.shift().rolling(20, min_periods=1).max()).fillna(0).astype(bool)
    x["prior_path"] = np.select([er & rt, er, rt], ["MIXED", "ERASURE", "RETRY"], "UNRESOLVED")
    paths = []
    for _, gdf in x.groupby("code", sort=False):
        vals = gdf.state_v2.astype(str).tolist()
        for i in range(len(vals)):
            seq = []
            for state in vals[max(0, i - 20) : i]:
                if state not in ("NONE", "nan") and (not seq or seq[-1] != state):
                    seq.append(state)
            paths.append(">".join(seq) if seq else "NONE")
    x["prior_state_path_20"] = paths
    return x


def load() -> tuple[pd.DataFrame, dict[str, str]]:
    sources = {
        "daily": str(DAILY), "state_v2": str(STATE_ROOT / "state_ledger_v2.parquet"),
        "lane": str(LANE), "open_event": str(OPEN), "s4_compare": str(S4_COMPARE),
        "open_compare": str(OPEN_COMPARE),
    }
    d = pd.read_parquet(DAILY)
    s = pd.read_parquet(STATE_ROOT / "state_ledger_v2.parquet")
    lane = pd.read_parquet(LANE)
    for q in (d, s, lane):
        q["code"] = q.code.astype(str).str.zfill(4)
        q["ymd"] = pd.to_numeric(q.ymd).astype(int)
    x = d.merge(s, on=["code", "ymd"], validate="one_to_one").merge(lane, on=["code", "ymd"], validate="one_to_one")
    x = prior_paths(x.sort_values(["code", "ymd"]).reset_index(drop=True))
    x["open_gap_pct"] = x.o / x.groupby("code", sort=False).c.shift() - 1.0
    x["label_id"] = fp.labels(x, 3)
    add_cols = [c for c in lane if c.startswith("sell_add_")]
    deduct_cols = [c for c in lane if c.startswith("sell_deduct_")]
    risk_cols = [c for c in lane if c.startswith("rebound_risk_")]
    x["sell_add_count"] = x[add_cols].fillna(0).astype(bool).sum(axis=1)
    x["sell_deduct_count"] = x[deduct_cols].fillna(0).astype(bool).sum(axis=1)
    x["rebound_risk_count"] = x[risk_cols].fillna(0).astype(bool).sum(axis=1)
    x["information_score"] = x.sell_add_count + x.sell_deduct_count + x.rebound_risk_count + 2 * ((x.sell_add_count > 0) & ((x.sell_deduct_count + x.rebound_risk_count) > 0))
    return x, sources


def candidates(x: pd.DataFrame) -> pd.DataFrame:
    period = x.ymd.between(20230101, 20251231) & x.code.ne("6326")
    frames = []
    ab = period & x.sell_action_event_v2.astype(bool) & x.trigger_gap_down.astype(bool) & x.trigger_ma20_break.astype(bool) & ~x.trigger_failed_rebound.astype(bool)
    for path in ("ERASURE", "MIXED", "RETRY"):
        q = x.loc[ab & x.prior_path.eq(path)].copy()
        q["review_family"] = "AB_" + path
        q["open_branch"] = None
        frames.append(q)
    dm = period & x.sell_action_event_v2.astype(bool) & x.trigger_failed_rebound.astype(bool)
    q = x.loc[dm].copy()
    q["review_family"] = "D_FAILED_REBOUND"
    q["open_branch"] = None
    frames.append(q)

    o = pd.read_parquet(OPEN)
    o["code"] = o.code.astype(str).str.zfill(4)
    o["ymd"] = pd.to_numeric(o.ymd).astype(int)
    o = o[(o.horizon.eq(3)) & o.ymd.between(20230101, 20251231) & o.code.ne("6326") & o.prior_state.eq("S3") & o.gap_bin.astype(str).str.startswith("GD") & o.below_ma20.astype(bool)]
    o = o.sort_values("branch").drop_duplicates(["code", "ymd"])
    q = x.merge(o[["code", "ymd", "branch"]], on=["code", "ymd"], how="inner", validate="one_to_one")
    q["review_family"] = "OPEN_S3_GD_BELOW_MA20"
    q["open_branch"] = q.pop("branch")
    frames.append(q)
    return pd.concat(frames, ignore_index=True).drop_duplicates(["code", "ymd", "review_family"])


def select(c: pd.DataFrame) -> pd.DataFrame:
    families = ["AB_ERASURE", "AB_MIXED", "AB_RETRY", "D_FAILED_REBOUND", "OPEN_S3_GD_BELOW_MA20"]
    extras = {0: ["AB_ERASURE", "OPEN_S3_GD_BELOW_MA20"], 1: ["AB_MIXED", "D_FAILED_REBOUND"], 2: ["AB_RETRY", "OPEN_S3_GD_BELOW_MA20"]}
    schedule = [(label, family) for label in (0, 1, 2) for family in families + extras[label]]
    chosen = []
    used_codes: set[str] = set()
    used_dates: set[int] = set()
    month_counts: Counter[str] = Counter()
    year_counts: Counter[str] = Counter()
    for label, family in schedule:
        pool = c[(c.label_id.eq(label)) & c.review_family.eq(family) & ~c.code.isin(used_codes) & ~c.ymd.isin(used_dates)].copy()
        pool["month"] = pool.ymd.astype(str).str[:6]
        pool["year"] = pool.ymd.astype(str).str[:4]
        pool = pool[pool.month.map(month_counts).fillna(0) < 2]
        if pool.empty:
            raise RuntimeError(f"cannot fill {label=} {family=}")
        pool["selection_score"] = pool.information_score * 100 - pool.year.map(year_counts).fillna(0) * 10 - pool.month.map(month_counts).fillna(0) * 5
        row = pool.sort_values(["selection_score", "code", "ymd"], ascending=[False, True, True]).iloc[0]
        chosen.append(row)
        used_codes.add(row.code)
        used_dates.add(int(row.ymd))
        month_counts[row.month] += 1
        year_counts[row.year] += 1
    out = pd.DataFrame(chosen).reset_index(drop=True)
    out["case_id"] = [f"SELL-AL-{i:02d}" for i in range(1, len(out) + 1)]
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output", type=Path, required=True)
    ap.add_argument("--image-manifest", type=Path)
    args = ap.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    x, sources = load()
    chosen = select(candidates(x))
    lane_cols = [
        c for c in chosen
        if (c.startswith("sell_add_") or c.startswith("sell_deduct_") or c.startswith("rebound_risk_"))
        and c not in {"sell_add_count", "sell_deduct_count", "rebound_risk_count"}
    ]
    evidence_cols = [
        "state_v2", "prior_path", "prior_state_path_20", "s1_top_risk", "s2_top_formation", "s3_weakening",
        "trigger_gap_down", "trigger_ma20_break", "trigger_support_break", "trigger_failed_rebound",
        "trigger_group_count_v2", "open_gap_pct", "dist_ma7_atr", "dist_ma20_atr", "dist_ma60_atr",
        "ma7_slope5_atr", "ma20_slope5_atr", "bear_count5", "upper_supply_count5", "lower_rejection_count5",
        "erasure_retrace_fraction", "erasure_retry_recovery_fraction", "erasure_retry_shortfall_atr",
        "rebound_days", "rebound_peak_atr", "rebound_retracement", "current_bear_body_atr",
        "sell_add_count", "sell_deduct_count", "rebound_risk_count", "information_score",
    ]
    queue = chosen[["case_id", "code", "ymd", "review_family", "open_branch", *evidence_cols, *lane_cols]].copy()
    queue.insert(2, "ticker", queue.code)
    image_status = "pending_meemee_capture"
    if args.image_manifest:
        manifest_rows = [json.loads(line) for line in args.image_manifest.read_text(encoding="utf-8").splitlines() if line.strip()]
        image_map = {(str(r["code"]).zfill(4), int(str(r["as_of"]).replace("-", ""))): r["saved_path"] for r in manifest_rows}
        queue["meemee_chart_path"] = [image_map.get((str(code).zfill(4), int(ymd))) for code, ymd in zip(queue.code, queue.ymd)]
        if queue.meemee_chart_path.notna().all():
            image_status = "complete_meemee_capture"
        else:
            image_status = "partial_meemee_capture"
    queue["review_question"] = "この時点で 新規売り / 追加売り / 売り維持 / 見送り / 買い戻し警戒 のどれか。最重要根拠と無効化条件を記録"
    queue["human_decision"] = ""
    queue["human_reason"] = ""
    queue["human_invalidation"] = ""
    queue.to_csv(args.output / "review_queue.csv", index=False)
    queue_records = queue.replace({np.nan: None}).to_dict("records")
    dump(args.output / "review_queue.json", {
        "schema_version": "tradex_sell_active_learning_review_pack_v1",
        "annotation_only": True, "not_evaluation": True,
        "outcomes_used_only_for_class_balance_and_hidden": True,
        "future_information_present": False,
        "image_status": image_status,
        "items": queue_records,
    })
    names = {0: "DOWN_FIRST", 1: "REBOUND_FIRST", 2: "NEUTRAL"}
    answer = []
    for _, r in chosen.iterrows():
        answer.append({
            "case_id": r.case_id, "code": r.code, "ymd": int(r.ymd), "horizon": 3,
            "label_id": int(r.label_id), "label": names[int(r.label_id)],
            "ret_close_3": r.ret_close_3, "down_exc_3": r.down_exc_3, "up_exc_3": r.up_exc_3,
        })
    dump(args.output / "answer_key.json", {
        "schema_version": "tradex_sell_active_learning_answer_key_v1",
        "keep_separate_from_reviewer": True, "not_evaluation": True, "items": answer,
    })
    samples = ",".join(f"{r.code}:{str(int(r.ymd))[:4]}-{str(int(r.ymd))[4:6]}-{str(int(r.ymd))[6:]}" for _, r in chosen.iterrows())
    (args.output / "meemee_capture_samples.txt").write_text(samples + "\n", encoding="utf-8")
    audit = {
        "schema_version": "tradex_sell_active_learning_review_pack_v1.audit",
        "artifact_role": "authoritative_annotation_pack",
        "selection_policy": "outcomes used only to balance 7 DOWN_FIRST / 7 REBOUND_FIRST / 7 NEUTRAL; within cells prefer conflicting lane evidence and code/month/year diversity",
        "horizon": 3, "rows": len(queue), "codes": int(queue.code.nunique()),
        "months": int(queue.ymd.astype(str).str[:6].nunique()),
        "years": queue.ymd.astype(str).str[:4].value_counts().sort_index().to_dict(),
        "families": queue.review_family.value_counts().to_dict(),
        "labels_hidden_counts": Counter(names[int(x)] for x in chosen.label_id).copy(),
        "seed_6326_excluded": not queue.code.eq("6326").any(),
        "review_queue_has_future_columns": any(c in queue for c in ["label_id", "ret_close_3", "down_exc_3", "up_exc_3"]),
        "image_status": image_status,
        "image_manifest": None if not args.image_manifest else {"path": str(args.image_manifest), "sha256": sha(args.image_manifest)},
        "source": {name: {"path": path, "sha256": sha(Path(path))} for name, path in sources.items()},
        "boundary": {"owner": "TRADEX", "review_only": True, "meemee_changed": False, "runtime_db_write": False, "production_ranking_changed": False},
    }
    dump(args.output / "audit.json", audit)
    files = ["review_queue.csv", "review_queue.json", "answer_key.json", "meemee_capture_samples.txt", "audit.json"]
    complete = {"complete": True, "sha256": {name: sha(args.output / name) for name in files}}
    if args.image_manifest:
        complete["sha256"]["meemee_image_manifest"] = sha(args.image_manifest)
        complete["image_sha256"] = {
            str(Path(path)): sha(Path(path)) for path in queue.meemee_chart_path if path and Path(path).exists()
        }
    dump(args.output / "complete.json", complete)


if __name__ == "__main__":
    main()
