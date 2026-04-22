from __future__ import annotations

import argparse

import pytest

from research import __main__ as research_cli
from research.publish import run_publish
from research.storage import ResearchPaths, write_csv, write_json


def _subparser_choices(parser: argparse.ArgumentParser) -> set[str]:
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            return set(action.choices.keys())
    return set()


def _create_run_artifacts(paths: ResearchPaths, run_id: str) -> None:
    import pandas as pd

    run_dir = paths.run_dir(run_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    write_json(
        run_dir / "manifest.json",
        {
            "run_id": run_id,
            "created_at": "2026-03-12T00:00:00Z",
            "model_version": "m1",
            "feature_version": "f1",
            "label_version": "l1",
        },
    )
    write_json(
        run_dir / "evaluation.json",
        {
            "pareto": {"is_pareto": True},
            "metrics_by_phase": {
                "test": {
                    "overall": {"months": 12, "return_at20": 0.02, "risk_mae_p90": 0.05},
                    "long": {"return_at20": 0.03},
                    "short": {"return_at20": -0.02},
                }
            },
        },
    )
    write_csv(
        run_dir / "top20_long.csv",
        pd.DataFrame(
            [
                {
                    "asof_date": "2026-03-10",
                    "code": "1001",
                    "score": 0.9,
                    "pred_return": 0.02,
                    "pred_prob_tp": 0.6,
                    "risk_dn": 0.1,
                    "phase": "inference",
                }
            ]
        ),
    )
    write_csv(
        run_dir / "top20_short.csv",
        pd.DataFrame(
            [
                {
                    "asof_date": "2026-03-10",
                    "code": "3003",
                    "score": 0.91,
                    "pred_return": -0.02,
                    "pred_prob_tp": 0.62,
                    "risk_dn": 0.1,
                    "phase": "inference",
                }
            ]
        ),
    )


def test_research_cli_no_longer_exposes_decision_signal_prior() -> None:
    parser = research_cli._build_parser()
    assert "decision_signal_prior" not in _subparser_choices(parser)

    with pytest.raises(SystemExit):
        research_cli.main(["decision_signal_prior"])


def test_run_publish_blocks_repo_root_published_dir_outside_test_override(monkeypatch, tmp_path) -> None:
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.delenv("MEEMEE_ALLOW_REPO_PUBLISHED_ROOT", raising=False)

    paths = ResearchPaths.build(
        repo_root=tmp_path,
        research_home=tmp_path / "research_home",
        workspace_root=tmp_path / "workspace",
        published_root=tmp_path / "published",
    )
    _create_run_artifacts(paths, "run_cli_contract")

    with pytest.raises(RuntimeError, match="test-only"):
        run_publish(paths=paths, run_id="run_cli_contract", legacy_publish=True)
