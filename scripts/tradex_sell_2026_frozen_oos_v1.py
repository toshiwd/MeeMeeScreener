from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

AXIS_ID = "tradex_sell_2026_frozen_oos_v1"
OUT = Path(r"G:\Tradex\sell_2026_frozen_oos_v1")
EXPECTED_PRIOR_COUNTS = {"train": 88, "validation": 65, "test": 89}
MIN_N = 30


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def profit_factor(values: pd.Series) -> float | None:
    gains = float(values[values > 0].sum())
    losses = float(-values[values < 0].sum())
    return gains / losses if losses else None


def metrics(rows: pd.DataFrame) -> dict:
    ret = rows["ret"].astype(float)
    daily = rows.groupby("signal_ymd", sort=True)["ret"].mean() if len(rows) else pd.Series(dtype=float)
    wins, losses = ret[ret > 0], ret[ret < 0]
    payoff = float(wins.mean() / -losses.mean()) if len(wins) and len(losses) else None
    first = date(2026, 1, 1)
    last = datetime.strptime(str(int(rows["signal_ymd"].max())), "%Y%m%d").date() if len(rows) else first
    elapsed_weeks = max((last - first).days / 7.0, 1.0)
    return {
        "n": int(len(rows)),
        "signal_days": int(rows["signal_ymd"].nunique()),
        "signal_days_per_week": float(rows["signal_ymd"].nunique() / elapsed_weeks),
        "expectancy": float(ret.mean()) if len(ret) else None,
        "profit_factor": profit_factor(ret),
        "daily_profit_factor": profit_factor(daily),
        "win_rate": float((ret > 0).mean()) if len(ret) else None,
        "payoff_ratio": payoff,
        "p05_ret": float(ret.quantile(0.05)) if len(ret) else None,
        "loss_mean": float(losses.mean()) if len(losses) else None,
    }


def evaluate(ledger: pd.DataFrame) -> dict:
    prior = ledger[ledger["split"].notna()].groupby("split").size().to_dict()
    reproduction = {key: int(prior.get(key, 0)) == expected for key, expected in EXPECTED_PRIOR_COUNTS.items()}
    shadow = ledger[ledger["signal_ymd"].astype(str).str.startswith("2026")].copy()
    result = metrics(shadow)
    n_pass = result["n"] >= MIN_N
    return {
        "prior_split_reproduction": {"expected": EXPECTED_PRIOR_COUNTS, "actual": {k: int(prior.get(k, 0)) for k in EXPECTED_PRIOR_COUNTS}, "all_match": all(reproduction.values())},
        "shadow_2026": result,
        "decision": "keep_shadow_oos" if n_pass and all(reproduction.values()) else "hold_insufficient_n" if not n_pass else "hold_prior_reproduction_mismatch",
        "n_gate": {"minimum_n": MIN_N, "pass": n_pass},
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--measurement-date", default=date.today().isoformat())
    args = parser.parse_args()
    ledger = pd.read_parquet(args.ledger)
    evaluation = evaluate(ledger)
    now = datetime.now(timezone.utc)
    run = OUT / f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    run.mkdir(parents=True)
    payload = {
        "schema_version": f"{AXIS_ID}.v1", "artifact_role": "authoritative",
        "generated_at": now.isoformat(), "measurement_date": args.measurement_date,
        "axis_id": AXIS_ID, "boundary_owner": "TRADEX",
        "frozen_rule": "support-break capitulation + breadth below MA20 >=40%; next-day signal-low; TP10/SL5/max10; stop-first",
        "ledger": {"path": str(args.ledger), "sha256": sha256(args.ledger)},
        **evaluation,
        "shadow_oos_status": "2026 first fixed measurement; no threshold or rule reselection",
        "runtime_db_write": False, "production_ranking_changed": False, "meemee_unchanged": True,
    }
    path = run / "compare.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
