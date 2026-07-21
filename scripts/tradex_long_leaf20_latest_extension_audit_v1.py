from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


RULE = {
    "ret3_gt": 0.0831200555,
    "ret3_le": 0.1167842001,
    "dist_low20_gt": 0.1682705656,
}


def dedupe(frame: pd.DataFrame) -> pd.DataFrame:
    kept: list[int] = []
    for _, group in frame.sort_values(["code", "bar_index"]).groupby("code", sort=False):
        last = -10**9
        for index, bar_index in zip(group.index, group["bar_index"]):
            if int(bar_index) - last > 5:
                kept.append(index)
                last = int(bar_index)
    return frame.loc[kept].copy()


def metrics(frame: pd.DataFrame) -> dict:
    resolved = frame[frame["realized_ret"].notna()].copy()
    positive = resolved[resolved["realized_ret"] > 0]
    total_positive = float(positive["realized_ret"].sum())
    top3_positive = float(positive.nlargest(3, "realized_ret")["realized_ret"].sum())
    return {
        "n": int(len(resolved)),
        "codes": int(resolved["code"].nunique()),
        "mean_return_pct": None if resolved.empty else float(resolved["realized_ret"].mean()),
        "median_return_pct": None if resolved.empty else float(resolved["realized_ret"].median()),
        "win_rate": None if resolved.empty else float(resolved["realized_ret"].gt(0).mean()),
        "severe_loss5_rate": None if resolved.empty else float(resolved["realized_ret"].le(-5).mean()),
        "top3_positive_profit_share": None if total_positive <= 0 else top3_positive / total_positive,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=False)

    sys.path[:0] = [str(Path.cwd()), str(Path.cwd() / "app")]
    from backend.services.codex_bridge_service import get_runtime_stock_db_status

    runtime = get_runtime_stock_db_status()
    with duckdb.connect(runtime["selected_runtime_db_path"], read_only=True) as conn:
        rows = conn.execute(
            """
            WITH bars AS (
              SELECT code, date, o, h, l, c, v,
                row_number() OVER (PARTITION BY code ORDER BY date) AS bar_index,
                c / lag(c, 3) OVER (PARTITION BY code ORDER BY date) - 1 AS ret3,
                c / min(l) OVER (
                  PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) - 1 AS dist_low20,
                lead(o, 1) OVER (PARTITION BY code ORDER BY date) AS p1_o,
                lead(h, 1) OVER (PARTITION BY code ORDER BY date) AS p1_h,
                lead(l, 1) OVER (PARTITION BY code ORDER BY date) AS p1_l,
                lead(c, 1) OVER (PARTITION BY code ORDER BY date) AS p1_c,
                lead(o, 2) OVER (PARTITION BY code ORDER BY date) AS p2_o,
                lead(c, 3) OVER (PARTITION BY code ORDER BY date) AS p3_c
              FROM daily_bars WHERE source = 'pan'
            )
            SELECT b.*, coalesce(i.name, m.name, '') AS stock_name, i.market_code
            FROM bars b
            LEFT JOIN industry_master i USING (code)
            LEFT JOIN stock_meta m USING (code)
            WHERE b.ret3 > ? AND b.ret3 <= ? AND b.dist_low20 > ?
              AND coalesce(i.market_code, '') <> 'ETF・ETN'
            ORDER BY b.code, b.bar_index
            """,
            [RULE["ret3_gt"], RULE["ret3_le"], RULE["dist_low20_gt"]],
        ).fetchdf()

    rows = dedupe(rows)
    rows["signal_date"] = pd.to_datetime(rows["date"], unit="s")
    entry = rows["p1_o"]
    immediate = rows["p1_h"].div(entry).sub(1).mul(100).ge(3) & rows["p1_l"].div(entry).sub(1).mul(100).gt(-3)
    stall = (rows["p1_c"].div(entry).sub(1).mul(100).le(-2) | rows["p1_l"].div(entry).sub(1).mul(100).le(-3)) & ~immediate
    rows["realized_ret"] = 25 * (rows["p3_c"] / entry - 1)
    rows.loc[immediate, "realized_ret"] = 100 * (
        0.25 * (rows.loc[immediate, "p3_c"] / entry.loc[immediate] - 1)
        + 0.75 * (rows.loc[immediate, "p3_c"] / rows.loc[immediate, "p2_o"] - 1)
    )
    rows.loc[stall, "realized_ret"] = 25 * (rows.loc[stall, "p2_o"] / entry.loc[stall] - 1)
    rows["management_branch"] = np.where(immediate, "add75", np.where(stall, "stall_exit", "probe25"))

    periods = {
        "2019_2023": rows["signal_date"].dt.year.between(2019, 2023),
        "2024_2025": rows["signal_date"].dt.year.between(2024, 2025),
        "2026_through_june12": rows["signal_date"].between("2026-01-01", "2026-06-12"),
        "latest_extension_june15_july14": rows["signal_date"].between("2026-06-15", "2026-07-14"),
        "july_resolved": rows["signal_date"].between("2026-07-01", "2026-07-14"),
    }
    period_metrics = {name: metrics(rows[mask]) for name, mask in periods.items()}
    monthly = {
        str(month): metrics(group)
        for month, group in rows[rows["signal_date"].dt.year.eq(2026)].groupby(rows["signal_date"].dt.to_period("M"))
    }
    latest = period_metrics["latest_extension_june15_july14"]
    checks = {
        "latest_mean_positive": bool((latest["mean_return_pct"] or -999) > 0),
        "latest_win_rate_at_least_50pct": bool((latest["win_rate"] or 0) >= 0.50),
        "latest_severe_loss5_at_most_3pct": bool((latest["severe_loss5_rate"] or 1) <= 0.03),
        "latest_top3_profit_share_at_most_35pct": bool((latest["top3_positive_profit_share"] or 1) <= 0.35),
    }
    payload = {
        "schema_version": "tradex_long_leaf20_latest_extension_audit_v1.compare.v1",
        "artifact_role": "authoritative",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "runtime": runtime,
        "fixed_evaluation_conditions": {
            "rule": RULE,
            "universe": "PAN ordinary stocks; industry_master.market_code ETF/ETN excluded",
            "execution": "next open 25%; add75 at following open after immediate rise; stall exit; otherwise signal+3 close",
            "dedupe": "same code within 5 trading rows keep earliest",
            "costs": "ignored by standing research contract",
        },
        "authoritative_result": {"periods": period_metrics, "monthly_2026": monthly, "checks": checks},
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": int(len(rows)),
            "selection_divergence_reason": "latest-date extension and ETF/ETN exclusion only",
        },
        "judgment": {
            "candidate_local_decision": "drop" if not all(checks.values()) else "keep",
            "authoritative_rollup_decision": "invalidate_previous_practical_claim" if not all(checks.values()) else "hold_for_portfolio_gate",
            "reason_type": "latest_extension_gate",
        },
        "non_scope": ["MeeMee reflection", "ranking mutation", "runtime DB write", "new feature search"],
        "remaining_risks": ["capital allocation not yet simulated", "corporate-action adjustment not separately audited"],
    }
    rows.to_parquet(output / "signal_ledger.parquet", index=False)
    (output / "compare.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    (output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json"}), encoding="utf-8")
    print(json.dumps({"judgment": payload["judgment"], "latest": latest, "checks": checks}, ensure_ascii=False))


if __name__ == "__main__":
    main()
