"""Discover a single PIT long-MA cluster/proximity veto on the frozen blind set."""
import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd


SPAN_THRESHOLDS = (0.5, 0.75, 1.0, 1.5, 2.0, 3.0)
NEAR_THRESHOLDS = (0.25, 0.5, 0.75, 1.0, 1.5, 2.0)
LONG_MAS = ("ma60", "ma100", "ma200")


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def stats(frame: pd.DataFrame) -> dict:
    x = frame[frame.status.eq("complete")]
    values = x.return_fixed3_pct.dropna()
    gain, loss = values[values > 0].sum(), -values[values < 0].sum()
    return {
        "n": int(len(frame)), "D": int(x.outcome_fixed3.eq("D").sum()),
        "R": int(x.outcome_fixed3.eq("R").sum()), "N": int(x.outcome_fixed3.eq("N").sum()),
        "D_rate": None if x.empty else float(x.outcome_fixed3.eq("D").mean()),
        "R_rate": None if x.empty else float(x.outcome_fixed3.eq("R").mean()),
        "mean_fixed3_pct": None if values.empty else float(values.mean()),
        "mean_h5_close_pct": None if x.empty else float(x.return_h5_close_pct.mean()),
        "profit_factor": None if loss == 0 else float(gain / loss),
        "max_loss_pct": None if values.empty else float(values.min()),
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--diagnostic", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    source = args.diagnostic / "downside_room_diagnostic_ledger.parquet"
    data = pd.read_parquet(source).copy()
    ma = data[list(LONG_MAS)].astype(float)
    data["long_ma_span_atr"] = (ma.max(axis=1) - ma.min(axis=1)) / data.atr14
    below_distance = pd.DataFrame({name: (data.c - data[name]) / data.atr14 for name in LONG_MAS})
    below_distance = below_distance.where(below_distance.ge(0))
    data["nearest_lower_long_ma_atr"] = below_distance.min(axis=1)
    data["nearest_lower_long_ma_type"] = below_distance.idxmin(axis=1).where(below_distance.notna().any(axis=1))
    data["lower_long_ma_count"] = below_distance.notna().sum(axis=1)

    answered = data[data.human_direction.ne("")].copy()
    human_base = answered[answered.human_direction.eq("SELL")]
    model_base = answered[answered.model_direction.eq("SELL")]
    scans = []
    for span in SPAN_THRESHOLDS:
        for near in NEAR_THRESHOLDS:
            risk = data.long_ma_span_atr.le(span) & data.nearest_lower_long_ma_atr.le(near)
            human_keep = human_base[~risk.loc[human_base.index]]
            model_keep = model_base[~risk.loc[model_base.index]]
            hs, ms = stats(human_keep), stats(model_keep)
            hb, mb = stats(human_base), stats(model_base)
            scans.append({
                "span_threshold_atr": span, "near_threshold_atr": near,
                "human_removed": int(len(human_base) - len(human_keep)),
                "human_D_retention": None if hb["D"] == 0 else hs["D"] / hb["D"],
                "human_R_removed": hb["R"] - hs["R"], "human": hs,
                "model_removed": int(len(model_base) - len(model_keep)),
                "model_D_retention": None if mb["D"] == 0 else ms["D"] / mb["D"],
                "model_R_removed": mb["R"] - ms["R"], "model": ms,
            })
    eligible = [row for row in scans if row["human_removed"] > 0 and row["human_D_retention"] >= 0.70
                and row["human"]["R_rate"] <= stats(human_base)["R_rate"]
                and row["human"]["max_loss_pct"] >= stats(human_base)["max_loss_pct"]]
    eligible.sort(key=lambda row: (
        row["human_R_removed"], row["human"]["mean_fixed3_pct"], row["model_D_retention"],
        -row["span_threshold_atr"], -row["near_threshold_atr"]), reverse=True)
    selected = eligible[0] if eligible else None
    ledger_path = args.output / "long_ma_cluster_diagnostic_ledger.parquet"
    data.to_parquet(ledger_path, index=False)
    result = {
        "schema_version": "tradex_blind_long_ma_cluster_axis_v1.compare.v1",
        "artifact_role": "authoritative_single_axis_discovery",
        "review_only": True,
        "fixed_conditions": {
            "axis": "MA60/100/200 cluster span plus nearest lower long-MA proximity",
            "span_thresholds_atr": list(SPAN_THRESHOLDS), "near_thresholds_atr": list(NEAR_THRESHOLDS),
            "atr_definition": "simple mean true range 14 through signal close, inherited from downside-room discovery",
            "risk_definition": "span <= threshold AND a lower long MA is within proximity threshold",
            "execution": "next_session_open", "horizon_sessions": 5,
            "weekly_inputs": [], "costs": "ignored", "clean_oos": False,
        },
        "baselines": {"human_sell": stats(human_base), "model_sell_answered": stats(model_base)},
        "threshold_scan": scans, "selected_discovery_candidate": selected,
        "judgment": {
            "candidate_local_decision": "keep_discovery_challenger" if selected else "drop_axis",
            "authoritative_rollup_decision": "hold_pending_fresh_unused_validation" if selected else "drop",
            "reason": "selection requires human D retention >=70%, non-worse R rate and maximum loss; no threshold is adopted without fresh unused validation",
        },
        "not_changed": ["candlestick features", "model actions", "MeeMee", "ranking", "runtime DB", "production trading logic"],
    }
    compare_path = args.output / "compare.json"
    compare_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "source_compare_sha256": sha(args.diagnostic / "compare.json"), "source_ledger_sha256": sha(source),
        "rows": int(len(data)), "direction_answered": int(len(answered)), "weekly_columns_used": [],
        "future_selection_columns_used": [], "ledger_sha256": sha(ledger_path),
    }
    (args.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(compare_path)}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(args.output), "baselines": result["baselines"],
        "selected": selected, "judgment": result["judgment"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
