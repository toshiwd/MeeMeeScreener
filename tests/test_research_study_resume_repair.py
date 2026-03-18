from __future__ import annotations

import json
import os
from pathlib import Path
import sys

import pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from research.config import config_to_dict, from_dict, load_config
from research.ingest import run_ingest
from research.storage import ResearchPaths, read_csv, read_json
from research.study_build import build_study_dataset
from research.study_search import run_study_search
from research.study_storage import study_paths


def _write_synthetic_inputs(tmp_path: Path) -> dict[str, Path]:
    dates = pd.bdate_range("2023-01-02", "2024-06-28")
    codes = ["1001", "2002", "3003"]
    rows: list[dict[str, object]] = []
    for idx, dt in enumerate(dates):
        for code_idx, code in enumerate(codes):
            base = 100.0 + code_idx * 40.0 + idx * (0.18 + code_idx * 0.02)
            wave = ((idx + 5 * code_idx) % 17) - 8
            close = base + wave * (1.2 + code_idx * 0.2)
            open_ = close - (0.9 - 0.2 * code_idx)
            high = close + 1.5 + code_idx
            low = close - 1.3 - code_idx * 0.3
            volume = 100_000 + idx * 250 + code_idx * 25_000
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
        ]
    ).to_csv(sector_csv, index=False)
    return {"daily_csv": daily_csv, "universe_dir": universe_dir, "sector_csv": sector_csv}


def _study_test_config():
    cfg = load_config(None)
    payload = config_to_dict(cfg)
    payload["split"] = {"train_years": 1, "valid_months": 1, "test_months": 1}
    payload["study"]["trials_per_family"] = {"daily": 2, "weekly": 1, "monthly": 1}
    payload["study"]["refinement_trials_per_family"] = {"daily": 1, "weekly": 0, "monthly": 0}
    payload["study"]["retention_gates"] = {
        "min_profit_factor": 0.0,
        "min_positive_window_ratio": 0.0,
        "max_worst_drawdown": 1.0,
        "min_samples": 1,
        "top_hypotheses_per_combo": 2,
    }
    payload["study"]["adoption_gates"] = {
        "min_oos_return": -1.0,
        "min_pf": 0.0,
        "min_positive_window_ratio": 0.0,
        "max_worst_drawdown": 1.0,
        "min_stability": -1.0,
        "min_cluster_consistency": -1.0,
        "min_fold_months": 1,
    }
    payload["study"]["top_refinement_parents"] = 1
    payload["study"]["random_seed"] = 7
    return from_dict(payload)


def test_study_search_resume_recovers_from_missing_search_trace(tmp_path: Path) -> None:
    inputs = _write_synthetic_inputs(tmp_path)
    paths = ResearchPaths.build(
        repo_root=tmp_path,
        research_home=tmp_path / "research_home",
        workspace_root=tmp_path / "workspace",
        published_root=tmp_path / "published",
    )
    run_ingest(
        paths=paths,
        daily_csv=str(inputs["daily_csv"]),
        universe_dir=str(inputs["universe_dir"]),
        sector_csv=str(inputs["sector_csv"]),
        snapshot_id="snap_resume_repair",
    )
    cfg = _study_test_config()
    build_result = build_study_dataset(
        paths=paths,
        config=cfg,
        snapshot_id="snap_resume_repair",
        timeframe="daily",
        start_date="2023-01-02",
        end_date="2024-06-28",
    )
    study_id = str(build_result["study_id"])

    first = run_study_search(
        paths=paths,
        config=cfg,
        study_id=study_id,
        resume=False,
        timeframes=("daily",),
        families=("bottom",),
    )
    assert first["trials"] >= 1

    spaths = study_paths(paths, study_id)
    spaths["search_trace"].unlink()
    state = read_json(spaths["trial_state"])
    combo = state["combos"]["daily::bottom"]
    combo["status"] = "running_refine"
    combo["best_trial_ids"] = []
    spaths["trial_state"].write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

    resumed = run_study_search(
        paths=paths,
        config=cfg,
        study_id=study_id,
        resume=True,
        timeframes=("daily",),
        families=("bottom",),
    )

    repaired_trace = read_csv(spaths["search_trace"])
    assert resumed["trials"] == first["trials"]
    assert "trial_id" in repaired_trace.columns
    assert repaired_trace["trial_id"].astype(str).nunique() == first["trials"]
