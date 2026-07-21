"""One-axis OOS test: mature local box overrides an old post-box state."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

import tradex_monthly_env_probe_add_oos_v1 as base

AXIS_ID = "tradex_monthly_postbox_mature_override_oos_v1"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=Path, required=True)
    ap.add_argument("--retry-features", type=Path, required=True)
    ap.add_argument("--champion-compare", type=Path, required=True)
    ap.add_argument("--output-root", type=Path, required=True)
    args = ap.parse_args()
    out = args.output_root / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    out.mkdir(parents=True, exist_ok=False)
    raw = pd.read_parquet(args.input).sort_values(["code", "ymd"]).reset_index(drop=True)
    retry = pd.read_parquet(args.retry_features)
    retry["ymd"] = pd.to_numeric(retry.ymd, errors="raise").astype(int)
    retry["code"] = retry.code.astype(str)
    retry_cols = ["code", "ymd", "retry_sequence_available", "retry_second_recovery_fraction", "retry_second_shortfall_atr", "retry_local_high_slope_atr_per_bar", "existing_above_ma100_run"]
    raw = raw.merge(retry[retry_cols], on=["code", "ymd"], how="left", validate="one_to_one")
    monthly = base.monthly_environment(raw)
    override = (
        monthly.environment.eq("POST_BOX_BREAKOUT_CONSOLIDATION")
        & monthly.local_box_mature
        & monthly.local_box_top_touch_count.ge(3)
        & monthly.breakout_age.ge(3)
    )
    monthly["environment_champion"] = monthly.environment
    monthly.loc[override, "environment"] = "BOX"
    joined = base.add_daily_features(raw, monthly)
    state = base.lifecycle(joined)
    labeled, details = base.evaluate(state)
    champion = json.loads(args.champion_compare.read_text(encoding="utf-8"))
    changed = monthly[override]
    anchor = {}
    for code, decision_ymd, human in (("6301", 20230531, "POST_BOX_BREAKOUT_CONSOLIDATION"), ("6532", 20230626, "BOX")):
        month = pd.Period(str(decision_ymd)[:6], freq="M")
        hit = monthly[(monthly.code.astype(str).str.zfill(4) == code) & (monthly.effective_month == month)]
        anchor[code] = None if hit.empty else {
            "human": human,
            "champion": str(hit.iloc[-1].environment_champion),
            "challenger": str(hit.iloc[-1].environment),
            "match": str(hit.iloc[-1].environment) == human,
        }
    payload = {
        "schema_version": AXIS_ID + ".compare.v1",
        "artifact_role": "authoritative",
        "axis": "POST_BOX only: override to BOX when local_box_mature and top_touch_count>=3 and breakout_age>=3",
        "fixed_conditions": {"universe": "same Nikkei225 ledger", "years": list(base.YEARS), "probe_add_rules": "unchanged", "costs": "ignored"},
        "human_anchor_comparison": anchor,
        "monthly_branching": {"changed_month_rows": int(override.sum()), "changed_codes": int(changed.code.nunique()), "changed_by_year": changed.month.dt.year.value_counts().sort_index().to_dict()},
        "event_evaluation": details,
        "champion_event_evaluation": champion["event_evaluation"],
        "judgment": {
            "candidate_local_decision": "hold",
            "reason": "matches two anchors but thresholds were derived from them; require additional human labels and OOS lifecycle stability",
        },
        "not_changed": ["daily features", "probe trigger", "core/add trigger", "MeeMee ranking", "runtime DB"],
    }
    (out / "compare.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    monthly.to_parquet(out / "monthly_environment_challenger.parquet", index=False)
    audit = {"future_used_for_environment": False, "duplicate_code_month": int(monthly.duplicated(["code", "month"]).sum()), "review_only": True}
    (out / "audit.json").write_text(json.dumps(audit, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (out / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json"}, indent=2) + "\n", encoding="utf-8")
    print(out)
    print(json.dumps({"anchors": anchor, "branching": payload["monthly_branching"], "judgment": payload["judgment"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
