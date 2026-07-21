from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import roc_auc_score
from sklearn.pipeline import make_pipeline

from tradex_long_fresh_family_events_v1 import FAMILIES, add_scores
from tradex_long_ordinary_pit_compound_tree_v1 import load_rows, metrics


FEATURES = [
    "ret1", "ret5", "ret20", "ret60", "gap_ma20", "gap_ma60",
    "ma20_slope5", "ma60_slope5", "close_pos", "lower_wick_ratio",
    "upper_wick_ratio", "body_ratio", "volume_ratio20", "realized_vol20",
    "market_breadth_ma20", "market_advancers_ratio",
]
COST_PCT = 0.3
DEVELOPMENT_RETENTION_QUANTILE = 0.70


def model():
    return make_pipeline(
        SimpleImputer(strategy="median"),
        HistGradientBoostingClassifier(
            max_iter=120, max_leaf_nodes=7, learning_rate=0.04,
            l2_regularization=2.0, random_state=20260720,
        ),
    )


def report(frame: pd.DataFrame, risk: pd.Series, threshold: float) -> dict:
    source = frame.copy()
    source["risk"] = risk
    kept = source[source.risk <= threshold]
    return {
        "baseline": metrics(source),
        "guarded": metrics(kept),
        "retained_n": int(len(kept)),
        "retention_rate": float(len(kept) / len(source)) if len(source) else None,
        "mean_return_change_pct": float(kept.realized_ret.mean() - source.realized_ret.mean()) if len(kept) else None,
        "win_rate_change": float(kept.realized_ret.gt(0).mean() - source.realized_ret.gt(0).mean()) if len(kept) else None,
        "severe_loss5_rate_change": float(kept.realized_ret.le(-5).mean() - source.realized_ret.le(-5).mean()) if len(kept) else None,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--current-code", default="6724")
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=False)

    data = load_rows(str(args.db), broad_trigger=False, min_date="2016-01-01")
    data["signal_dt"] = pd.to_datetime(data.date, unit="s")
    data = add_scores(data)
    pullback_family = FAMILIES[1]
    events = (data.sort_values(["date", pullback_family, "code"], ascending=[True, False, True])
              .groupby("date", sort=False).head(3).copy())
    current = events[(events.code.astype(str) == str(args.current_code)) &
                     (events.date == events.date.max())]
    matured = events[events.p1_o.notna() & events.p20_c.notna()].copy()
    matured["realized_ret"] = 100.0 * (matured.p20_c / matured.p1_o - 1.0) - COST_PCT
    matured["severe_loss"] = matured.realized_ret.le(-5).astype(int)
    matured["year"] = matured.signal_dt.dt.year

    development = matured[matured.year.between(2016, 2023)].copy()
    oof = pd.Series(np.nan, index=development.index, dtype=float)
    for validation_year in range(2020, 2024):
        train = development[development.year < validation_year]
        valid = development[development.year == validation_year]
        fitted = model().fit(train[FEATURES], train.severe_loss)
        oof.loc[valid.index] = fitted.predict_proba(valid[FEATURES])[:, 1]
    oof_frame = development[oof.notna()].copy()
    oof_risk = oof.loc[oof_frame.index]
    threshold = float(oof_risk.quantile(DEVELOPMENT_RETENTION_QUANTILE))
    development_oof = report(oof_frame, oof_risk, threshold)
    development_oof["risk_auc"] = float(roc_auc_score(oof_frame.severe_loss, oof_risk))

    fixed = model().fit(development[FEATURES], development.severe_loss)
    validation = matured[matured.year.between(2024, 2025)].copy()
    audit = matured[matured.year.eq(2026)].copy()
    validation_result = report(validation, pd.Series(fixed.predict_proba(validation[FEATURES])[:, 1], index=validation.index), threshold)
    audit_result = report(audit, pd.Series(fixed.predict_proba(audit[FEATURES])[:, 1], index=audit.index), threshold)
    yearly = {}
    for year in [2024, 2025, 2026]:
        frame = matured[matured.year.eq(year)].copy()
        yearly[str(year)] = report(frame, pd.Series(fixed.predict_proba(frame[FEATURES])[:, 1], index=frame.index), threshold)

    live_fit = model().fit(matured[matured.year.le(2025)][FEATURES], matured[matured.year.le(2025)].severe_loss)
    current_risk = None if current.empty else float(live_fit.predict_proba(current[FEATURES])[:, 1][0])
    current_pass = current_risk is not None and current_risk <= threshold
    checks = {
        "threshold_selected_without_2024plus": True,
        "validation_retains_at_least_60pct": validation_result["retention_rate"] >= 0.60,
        "validation_severe_loss_rate_improves": validation_result["severe_loss5_rate_change"] < 0,
        "validation_mean_return_not_worse": validation_result["mean_return_change_pct"] >= 0,
        "audit_retains_at_least_50pct": audit_result["retention_rate"] >= 0.50,
        "audit_severe_loss_rate_improves_by_at_least_5points": audit_result["severe_loss5_rate_change"] <= -0.05,
        "audit_mean_return_improves": audit_result["mean_return_change_pct"] > 0,
        "audit_win_rate_improves": audit_result["win_rate_change"] > 0,
        "current_code_has_no_mature_outcome": bool(current.empty or current.p20_c.isna().all()),
    }
    decision = "keep_for_portfolio_comparison" if all(checks.values()) else "drop"
    payload = {
        "schema_version": "tradex_long_fresh_pullback_tail_guard_v1.compare.v1",
        "artifact_role": "authoritative",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "fixed_evaluation_conditions": {
            "universe": "ordinary domestic stocks inherited from load_rows contract",
            "family": pullback_family,
            "selection": "daily top-3 family score unchanged",
            "features": FEATURES,
            "target": "session-20 return <= -5%",
            "entry": "next session open",
            "outcome": "session-20 close",
            "round_trip_cost_pct": COST_PCT,
            "development": "2016-2023",
            "development_oof": "expanding year validation for 2020-2023",
            "threshold": "70th percentile of development OOF risk; fixed before 2024+",
            "validation": "2024-2025",
            "audit": "all matured 2026 events through current DB",
            "production_changed": False,
        },
        "authoritative_result": {
            "risk_threshold": threshold,
            "development_oof": development_oof,
            "validation_2024_2025": validation_result,
            "audit_2026": audit_result,
            "yearly": yearly,
            "current": {
                "code": str(args.current_code), "risk": current_risk,
                "passes_tail_guard": current_pass,
                "risk_margin_to_threshold": None if current_risk is None else threshold - current_risk,
                "training_end_year": 2025,
            },
            "checks": checks,
        },
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": int((validation_result["baseline"]["n"] - validation_result["retained_n"]) + (audit_result["baseline"]["n"] - audit_result["retained_n"])),
            "selection_divergence_reason": "a multi-feature severe-loss risk layer removes only high-risk pullback events; other families and family ranks are unchanged",
        },
        "judgment": {
            "candidate_local_decision": decision,
            "authoritative_rollup_decision": decision,
            "reason_type": "fixed_threshold_oos_tail_loss_reduction",
        },
        "remaining_risks": [
            "Development OOF discrimination is weak and the improvement is concentrated in 2025-2026",
            "The 2024 calendar year did not improve mean return or win rate",
            "Portfolio-level slot competition and compounded NAV impact are not yet tested",
            "Current 6724 risk margin is small and must not be treated as high confidence",
        ],
    }
    (output / "compare.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    (output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json"}), encoding="utf-8")
    print(json.dumps({"decision": decision, "validation": validation_result, "audit": audit_result, "current": payload["authoritative_result"]["current"], "checks": checks}, ensure_ascii=False))


if __name__ == "__main__":
    sys.path[:0] = [str(Path.cwd()), str(Path.cwd() / "scripts")]
    main()
