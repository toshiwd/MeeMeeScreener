from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd

from scripts import portfolio_agent_replay_v1 as mod


def _make_daily(symbol: str, periods: int, offset: float) -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=620)
    rows = []
    for idx, day in enumerate(dates[:periods]):
        drift = idx * (0.10 + offset * 0.005)
        pulse = 1.5 if idx % 31 == 0 else 0.0
        close = 60.0 + offset + drift + pulse
        rows.append(
            {
                "code": symbol,
                "date": int(day.strftime("%Y%m%d")),
                "o": close - 0.6,
                "h": close + 0.8,
                "l": close - 1.0,
                "c": close,
                "v": 100_000 + idx * 100 + int(offset * 1000),
                "source": "pan",
            }
        )
    return pd.DataFrame(rows)


def _write_db(path: Path) -> None:
    daily = pd.concat([_make_daily(f"7{idx:03d}", 560, float(idx)) for idx in range(1, 9)], ignore_index=True)
    benchmark = _make_daily("1306", 560, 20.0)
    daily = pd.concat([daily, benchmark], ignore_index=True).sort_values(["code", "date"]).copy()
    daily["ma20"] = daily.groupby("code")["c"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    daily["ma60"] = daily.groupby("code")["c"].transform(lambda s: s.rolling(60, min_periods=60).mean())
    daily["month_key"] = pd.to_datetime(daily["date"].astype(str), format="%Y%m%d").dt.to_period("M")
    monthly_rows = []
    for (code, month), group in daily.groupby(["code", "month_key"], sort=True):
        monthly_rows.append(
            {
                "code": code,
                "month": int(month.to_timestamp().strftime("%Y%m%d")),
                "o": float(group.iloc[0]["o"]),
                "h": float(group["h"].max()),
                "l": float(group["l"].min()),
                "c": float(group.iloc[-1]["c"]),
                "v": int(group["v"].sum()),
            }
        )
    monthly = pd.DataFrame(monthly_rows).sort_values(["code", "month"]).copy()
    monthly["ma20"] = monthly.groupby("code")["c"].transform(lambda s: s.rolling(3, min_periods=1).mean())
    monthly["ma60"] = monthly.groupby("code")["c"].transform(lambda s: s.rolling(6, min_periods=1).mean())
    conn = duckdb.connect(str(path))
    try:
        daily_db = daily.drop(columns=["month_key"])
        conn.register("daily_db", daily_db)
        conn.register("monthly_db", monthly)
        conn.execute("CREATE TABLE daily_bars AS SELECT code, date, o, h, l, c, v, source FROM daily_db")
        conn.execute("CREATE TABLE daily_ma AS SELECT code, date, ma20, ma60 FROM daily_db")
        conn.execute("CREATE TABLE monthly_bars AS SELECT code, month, o, h, l, c, v FROM monthly_db")
        conn.execute("CREATE TABLE monthly_ma AS SELECT code, month, ma20, ma60 FROM monthly_db")
        conn.execute("CHECKPOINT")
    finally:
        conn.close()


def _run(tmp_path: Path) -> Path:
    db_path = tmp_path / "stocks.duckdb"
    _write_db(db_path)
    result = mod.run_portfolio_agent_replay_v1(
        source_db=db_path,
        output_root=tmp_path / "out",
        run_id="smoke",
        start_ymd=20250401,
        end_ymd=20260401,
        entry_score_threshold=1,
    )
    return Path(result["output_dir"])


def test_synthetic_replay_writes_all_required_artifacts(tmp_path: Path) -> None:
    output_dir = _run(tmp_path)

    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (output_dir / artifact).exists(), artifact

    complete = json.loads((output_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    assert complete["required_artifacts_all_present"] is True
    assert complete["selection_feature_manifest_audit_result"] == "pass"
    assert complete["no_lookahead_audit"] == "pass"
    assert complete["accounting_reconciliation"]["status"] == "pass"
    assert complete["next_open_execution"] == "pass"
    assert complete["critical_logs_non_empty"] is True
    assert complete["complete"] is True


def test_selection_manifest_blocks_diagnostic_columns(tmp_path: Path) -> None:
    output_dir = _run(tmp_path)

    manifest = json.loads((output_dir / "selection_feature_manifest.json").read_text(encoding="utf-8"))
    audit = json.loads((output_dir / "no_lookahead_audit.json").read_text(encoding="utf-8"))

    assert manifest["audit_result"] == "pass"
    assert set(manifest["selection_allowed_columns"]).isdisjoint(set(manifest["outcome_label_columns"]))
    assert {"post_ret_5", "post_ret_20", "mae_20", "mfe_20"}.issubset(set(manifest["diagnostic_only_columns"]))
    assert audit["selection_feature_manifest_path"].endswith("selection_feature_manifest.json")
    assert audit["future_label_use"]["used_in_selection"] is False


def test_next_open_execution_and_accounting_are_proven(tmp_path: Path) -> None:
    output_dir = _run(tmp_path)

    orders = pd.read_csv(output_dir / "orders_ledger.csv")
    filled = orders[orders["order_status"] == "filled"]
    assert not filled.empty
    assert (filled["execution_ymd"].astype(int) > filled["decision_ymd"].astype(int)).all()

    complete = json.loads((output_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    assert complete["accounting_reconciliation"]["status"] == "pass"
    assert complete["accounting_reconciliation"]["max_abs_cash_plus_positions_minus_equity"] <= 0.05


def test_rejected_candidates_and_post_run_labels_are_diagnostic_only(tmp_path: Path) -> None:
    output_dir = _run(tmp_path)

    rejected = pd.read_csv(output_dir / "rejected_candidates.csv")
    labels = pd.read_csv(output_dir / "post_run_outcome_labels.csv")
    actions = [json.loads(line) for line in (output_dir / "daily_action_ledger.jsonl").read_text(encoding="utf-8").splitlines()]

    assert not rejected.empty
    assert not labels.empty
    assert labels["diagnostic_only"].eq(True).all()
    assert labels["was_selected"].eq(True).any()
    assert labels["was_selected"].eq(False).any()
    assert any(row["action"] == "reject" for row in actions)
    assert any(row["action"] == "buy" for row in actions)


def test_failure_summary_supports_profitable_non_failure_label(tmp_path: Path) -> None:
    output_dir = _run(tmp_path)

    summary = json.loads((output_dir / "failure_diagnosis_summary.json").read_text(encoding="utf-8"))
    assert summary["primary_failure_mode"] in summary["valid_failure_modes"]
    if summary["metrics"]["final_equity"] > summary["metrics"]["initial_cash"]:
        assert summary["primary_failure_mode"] in {"no_primary_failure_profit_positive", "profitable_but_with_risks"}
    assert set(summary["action_support"]["supported_but_not_triggered"]) == {"add", "trim"}
