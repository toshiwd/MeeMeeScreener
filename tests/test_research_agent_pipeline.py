from __future__ import annotations

import os
from pathlib import Path
import sys

import pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from research.agent import (  # noqa: E402
    AgentConfig,
    AgentWalkforwardConfig,
    Hypothesis,
    _first_pending_by_priority,
    _build_agent_dataset,
    evaluate_hypothesis,
    run_agent_cycle,
    run_agent_init,
    run_agent_loop,
)
from research.ingest import run_ingest  # noqa: E402
from research.storage import ResearchPaths, read_csv  # noqa: E402


def _write_synthetic_inputs(tmp_path: Path) -> dict[str, Path]:
    dates = pd.bdate_range("2021-01-04", "2023-12-29")
    codes = ["1001", "2002", "3003", "4004"]
    rows: list[dict[str, object]] = []
    for idx, dt in enumerate(dates):
        for code_idx, code in enumerate(codes):
            base = 80.0 + code_idx * 25.0 + idx * (0.09 + code_idx * 0.01)
            wave = ((idx + 7 * code_idx) % 21) - 10
            close = base + wave * (1.0 + code_idx * 0.15)
            open_ = close - (0.6 - 0.05 * code_idx)
            high = close + 1.2 + code_idx * 0.2
            low = close - 1.0 - code_idx * 0.15
            volume = 100_000 + idx * 140 + code_idx * 30_000
            rows.append(
                {
                    "date": dt.strftime("%Y-%m-%d"),
                    "code": code,
                    "open": round(open_, 4),
                    "high": round(high, 4),
                    "low": round(low, 4),
                    "close": round(close, 4),
                    "volume": int(volume),
                }
            )
    daily_csv = tmp_path / "daily.csv"
    pd.DataFrame(rows).to_csv(daily_csv, index=False)

    universe_dir = tmp_path / "universe"
    universe_dir.mkdir(parents=True, exist_ok=True)
    months = sorted(pd.Series(dates).dt.to_period("M").astype(str).unique().tolist())
    for month in months:
        pd.DataFrame({"code": codes}).to_csv(universe_dir / f"{month}.csv", index=False)

    sector_csv = tmp_path / "sector.csv"
    pd.DataFrame(
        [
            {"code": "1001", "sector33_code": "10", "sector33_name": "Tech"},
            {"code": "2002", "sector33_code": "20", "sector33_name": "Retail"},
            {"code": "3003", "sector33_code": "30", "sector33_name": "Finance"},
            {"code": "4004", "sector33_code": "40", "sector33_name": "Energy"},
        ]
    ).to_csv(sector_csv, index=False)
    return {"daily_csv": daily_csv, "universe_dir": universe_dir, "sector_csv": sector_csv}


def _build_paths(tmp_path: Path) -> ResearchPaths:
    return ResearchPaths.build(
        repo_root=tmp_path,
        workspace_root=tmp_path / "workspace",
        published_root=tmp_path / "published",
    )


def _agent_test_config() -> AgentConfig:
    return AgentConfig(
        walkforward=AgentWalkforwardConfig(min_train_years=0, valid_months=2, test_months=1, step_months=1),
        buy_sell_min_samples=6,
        failure_min_repro_folds=1,
    )


def _eval_frame(base_value: float, signal_positive: bool = True) -> pd.DataFrame:
    months = [f"2023-{month:02d}" for month in range(1, 9)]
    rows: list[dict[str, object]] = []
    for month_idx, month in enumerate(months):
        for row_idx in range(12):
            signal = 1.0 if row_idx < 6 else 0.0
            rows.append(
                {
                    "event_date": f"{month}-15",
                    "month_bucket": month,
                    "code": f"{1000 + row_idx:04d}",
                    "regime_key": "mt2_vr1" if row_idx % 2 == 0 else "mt1_vr0",
                    "signal": signal,
                    "long_ret_h5": base_value + (0.01 if signal_positive and signal else -0.01),
                    "long_ret_h10": base_value + (0.02 if signal_positive and signal else -0.01),
                    "long_ret_h20": base_value + (0.04 if signal_positive and signal else -0.01),
                    "long_ret_h40": base_value + (0.03 if signal_positive and signal else -0.01),
                    "long_ret_h60": base_value + (0.02 if signal_positive and signal else -0.01),
                    "short_ret_h5": 0.01 if signal else -0.01,
                    "short_ret_h10": 0.01 if signal else -0.01,
                    "short_ret_h20": 0.01 if signal else -0.01,
                    "short_ret_h40": 0.01 if signal else -0.01,
                    "short_ret_h60": 0.01 if signal else -0.01,
                    "long_close_mae_h5": 0.02,
                    "long_close_mae_h10": 0.03,
                    "long_close_mae_h20": 0.03,
                    "long_close_mae_h40": 0.04,
                    "long_close_mae_h60": 0.05,
                    "short_close_mae_h5": 0.02,
                    "short_close_mae_h10": 0.03,
                    "short_close_mae_h20": 0.03,
                    "short_close_mae_h40": 0.04,
                    "short_close_mae_h60": 0.05,
                    "hold_days_h5": 5.0,
                    "hold_days_h10": 10.0,
                    "hold_days_h20": 20.0,
                    "hold_days_h40": 40.0,
                    "hold_days_h60": 60.0,
                    "long_close_mfe_h20": 0.03,
                    "short_close_mfe_h20": 0.03,
                    "long_tp_05_ret": 0.06 if signal else 0.01,
                    "long_tp_05_mae": 0.02,
                    "long_tp_05_hold": 12.0,
                    "long_tp_08_ret": 0.07 if signal else 0.01,
                    "long_tp_08_mae": 0.02,
                    "long_tp_08_hold": 14.0,
                    "long_tp_10_ret": 0.07 if signal else 0.01,
                    "long_tp_10_mae": 0.02,
                    "long_tp_10_hold": 18.0,
                    "long_tp_15_ret": 0.06 if signal else 0.01,
                    "long_tp_15_mae": 0.02,
                    "long_tp_15_hold": 20.0,
                    "short_tp_05_ret": 0.06 if signal else 0.01,
                    "short_tp_05_mae": 0.02,
                    "short_tp_05_hold": 12.0,
                    "short_tp_08_ret": 0.07 if signal else 0.01,
                    "short_tp_08_mae": 0.02,
                    "short_tp_08_hold": 14.0,
                    "short_tp_10_ret": 0.07 if signal else 0.01,
                    "short_tp_10_mae": 0.02,
                    "short_tp_10_hold": 18.0,
                    "short_tp_15_ret": 0.06 if signal else 0.01,
                    "short_tp_15_mae": 0.02,
                    "short_tp_15_hold": 20.0,
                    "long_stop_03_ret": -0.01 if signal else -0.03,
                    "long_stop_03_mae": 0.01,
                    "long_stop_03_hold": 7.0,
                    "long_stop_05_ret": -0.02 if signal else -0.04,
                    "long_stop_05_mae": 0.015,
                    "long_stop_05_hold": 10.0,
                    "long_stop_08_ret": -0.03 if signal else -0.05,
                    "long_stop_08_mae": 0.02,
                    "long_stop_08_hold": 13.0,
                    "short_stop_03_ret": -0.01 if signal else -0.03,
                    "short_stop_03_mae": 0.01,
                    "short_stop_03_hold": 7.0,
                    "short_stop_05_ret": -0.02 if signal else -0.04,
                    "short_stop_05_mae": 0.015,
                    "short_stop_05_hold": 10.0,
                    "short_stop_08_ret": -0.03 if signal else -0.05,
                    "short_stop_08_mae": 0.02,
                    "short_stop_08_hold": 13.0,
                }
            )
    return pd.DataFrame(rows)


def test_agent_dataset_builder_generates_close_based_outcomes_and_features(tmp_path: Path) -> None:
    inputs = _write_synthetic_inputs(tmp_path)
    paths = _build_paths(tmp_path)
    run_ingest(
        paths=paths,
        daily_csv=str(inputs["daily_csv"]),
        universe_dir=str(inputs["universe_dir"]),
        sector_csv=str(inputs["sector_csv"]),
        snapshot_id="snap_agent01",
    )
    result = _build_agent_dataset(paths, "snap_agent01", _agent_test_config(), max_codes=2, force=True)
    dataset = read_csv(Path(str(result["path"])))
    for column in (
        "ma100",
        "ma200",
        "box_position_20",
        "box_position_60",
        "regime_key",
        "phase_initial",
        "phase_overheated",
        "long_ret_h60",
        "short_ret_h60",
        "long_close_mae_h60",
        "short_close_mfe_h60",
    ):
        assert column in dataset.columns
    sample = dataset.dropna(subset=["long_ret_h60", "long_close_mae_h60"]).iloc[0]
    assert float(sample["long_close_mae_h60"]) >= 0.0
    assert float(sample["short_close_mfe_h60"]) >= 0.0


def test_evaluate_hypothesis_supports_all_gate_types() -> None:
    config = _agent_test_config()

    buy_frame = _eval_frame(0.01, signal_positive=True)
    buy = evaluate_hypothesis(
        buy_frame,
        Hypothesis("buy_test", "buy", "buy", "long", "buy", ("signal",), ({"column": "signal", "op": ">=", "value": 1.0},)),
        config,
    )
    assert buy["decision"] == "adopted"

    skip_frame = _eval_frame(0.0, signal_positive=False)
    skip_frame.loc[skip_frame["signal"] >= 1.0, "long_ret_h20"] = 0.0
    skip_frame.loc[skip_frame["signal"] >= 1.0, "short_ret_h20"] = 0.0
    skip_frame.loc[skip_frame["signal"] >= 1.0, "long_close_mfe_h20"] = 0.01
    skip_frame.loc[skip_frame["signal"] < 1.0, "long_ret_h20"] = 0.02
    skip_frame.loc[skip_frame["signal"] < 1.0, "short_ret_h20"] = 0.02
    skip = evaluate_hypothesis(
        skip_frame,
        Hypothesis("skip_test", "skip", "skip", "both", "skip", ("signal",), ({"column": "signal", "op": ">=", "value": 1.0},)),
        config,
    )
    assert skip["decision"] == "adopted"

    takeprofit = evaluate_hypothesis(
        buy_frame,
        Hypothesis("tp_test", "takeprofit", "takeprofit", "long", "tp", ("signal",), ({"column": "signal", "op": ">=", "value": 1.0},)),
        config,
    )
    assert takeprofit["decision"] == "adopted"

    stop_frame = _eval_frame(-0.04, signal_positive=False)
    stop_frame.loc[stop_frame["signal"] >= 1.0, "long_ret_h60"] = -0.05
    stop_frame.loc[stop_frame["signal"] >= 1.0, "long_stop_03_ret"] = -0.01
    stop = evaluate_hypothesis(
        stop_frame,
        Hypothesis("stop_test", "stop", "stop", "long", "stop", ("signal",), ({"column": "signal", "op": ">=", "value": 1.0},)),
        config,
    )
    assert stop["decision"] == "adopted"

    failure_frame = _eval_frame(0.0, signal_positive=False)
    failure_frame.loc[failure_frame["signal"] >= 1.0, "long_ret_h20"] = -0.04
    failure_frame.loc[failure_frame["signal"] < 1.0, "long_ret_h20"] = 0.03
    failure = evaluate_hypothesis(
        failure_frame,
        Hypothesis("failure_test", "failure", "failure", "long", "failure", ("signal",), ({"column": "signal", "op": ">=", "value": 1.0},)),
        config,
    )
    assert failure["decision"] == "adopted"


def test_agent_cycle_writes_research_outputs(tmp_path: Path) -> None:
    inputs = _write_synthetic_inputs(tmp_path)
    paths = _build_paths(tmp_path)
    run_ingest(
        paths=paths,
        daily_csv=str(inputs["daily_csv"]),
        universe_dir=str(inputs["universe_dir"]),
        sector_csv=str(inputs["sector_csv"]),
        snapshot_id="snap_agent02",
    )
    init_result = run_agent_init(paths, "snap_agent02")
    assert Path(init_result["root"]).exists()

    cycle_result = run_agent_cycle(paths, "snap_agent02", theme="skip", max_hypotheses=1, max_codes=3)
    root = Path(str(cycle_result["root"]))
    assert cycle_result["cycle_id"] == "0001"
    assert (root / "00_specs" / "research_goal.md").exists()
    assert any((root / "03_experiments").glob("exp_0001_skip_*.md"))
    assert (root / "04_results" / "progress.md").exists()
    assert (root / "05_rulebooks").exists()
    assert (root / "06_candidates" / "rule_cards.json").exists()


def test_agent_loop_resume_keeps_cycle_ids_unique(tmp_path: Path) -> None:
    inputs = _write_synthetic_inputs(tmp_path)
    paths = _build_paths(tmp_path)
    run_ingest(
        paths=paths,
        daily_csv=str(inputs["daily_csv"]),
        universe_dir=str(inputs["universe_dir"]),
        sector_csv=str(inputs["sector_csv"]),
        snapshot_id="snap_agent03",
    )
    result = run_agent_loop(paths, "snap_agent03", max_cycles=3, max_codes=3, resume=True)
    cycle_ids = [row["cycle_id"] for row in result["results"] if row.get("cycle_id")]
    hypothesis_ids = [row["hypotheses"][0]["hypothesis_id"] for row in result["results"] if row.get("hypotheses")]
    assert cycle_ids == ["0001", "0002", "0003"]
    assert hypothesis_ids == [
        "skip_box_mid_low_vol",
        "skip_noise_low_atr",
        "skip_overheated_upper_wick",
    ]
    root = Path(str(result["root"]))
    assert (root / "04_results" / "history" / "cycle_0001").exists()
    assert (root / "04_results" / "history" / "cycle_0003").exists()


def test_first_pending_by_priority_skips_hold_only_theme() -> None:
    state = {
        "config": {"priority": ["skip", "buy", "sell"]},
        "hypotheses": [
            {"hypothesis_id": "skip_hold", "theme": "skip", "status": "hold"},
            {"hypothesis_id": "buy_pending", "theme": "buy", "status": "pending"},
            {"hypothesis_id": "sell_pending", "theme": "sell", "status": "pending"},
        ],
    }

    queued = _first_pending_by_priority(state)

    assert [row["hypothesis_id"] for row in queued] == ["buy_pending"]
