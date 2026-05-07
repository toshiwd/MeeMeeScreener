from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pandas as pd

from scripts import tradex_feature_surface_event_backfill_v1 as mod


def _latest_session(root: Path) -> Path:
    sessions = [p for p in root.iterdir() if p.is_dir()]
    assert sessions, f"no session directories found under {root}"
    return max(sessions, key=lambda p: p.stat().st_mtime)


def test_parse_sample_archive_files() -> None:
    earnings_folder = sorted(mod.JPX_EARNINGS_ARCHIVE.iterdir())[0]
    rights_folder = sorted(mod.JPX_RIGHTS_ARCHIVE.iterdir())[0]
    earnings_file = sorted(earnings_folder.glob("*.xlsx"))[0]
    rights_file = sorted(rights_folder.glob("*.xls"))[0]

    earnings = mod._parse_earnings_file(earnings_file, pd.Timestamp(earnings_folder.name).date())
    rights = mod._parse_rights_file(rights_file, pd.Timestamp(rights_folder.name).date())

    assert len(earnings) > 0
    assert len(rights) > 0
    assert {"code", "planned_date", "snapshot_date", "source_file"}.issubset(earnings.columns)
    assert {"code", "ex_date", "last_rights_date", "snapshot_date", "source_file"}.issubset(rights.columns)


def test_smoke_run_writes_explicit_missing_event_surface(tmp_path: Path) -> None:
    out_root = tmp_path / "event_backfill"
    cmd = [
        sys.executable,
        "scripts\\tradex_feature_surface_event_backfill_v1.py",
        "--output-root",
        str(out_root),
        "--limit-anchor-dates",
        "2",
        "--jobs",
        "2",
    ]
    subprocess.run(cmd, cwd=Path.cwd(), check=True)
    session = _latest_session(out_root)

    decision = json.loads((session / "feature_surface_event_backfill_v1_decision.json").read_text(encoding="utf-8"))
    assert decision["decision"] == "insufficient_event_coverage"
    assert decision["usable_snapshot_count"] == 0

    candidate = pd.read_parquet(session / "candidate_prefilter_rows_event_enriched_v1.parquet")
    assert len(candidate) > 0
    assert candidate["selected_snapshot_date"].isna().all()
    assert candidate["earnings_nearby_flag_feature_status"].eq("missing_no_prior_snapshot").all()
    assert candidate["ex_rights_nearby_flag_feature_status"].eq("missing_no_prior_snapshot").all()
    assert candidate["earnings_nearby_flag"].isna().all()
    assert candidate["ex_rights_nearby_flag"].isna().all()
