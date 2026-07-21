import json
from pathlib import Path

from scripts import tradex_short_entry_timing_pipeline_v1 as pipeline


def _write_json(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def test_pipeline_passes_fresh_stage_artifacts_between_steps(monkeypatch, tmp_path):
    current_root = tmp_path / "current"
    output_root = tmp_path / "pipeline"
    db_path = tmp_path / "stocks.duckdb"
    calls = {}

    def fake_provisional_scan(*, db_path, output_root):
        run_dir = output_root / "scan-run"
        _write_json(
            run_dir / "provisional_entry_timing_candidates.json",
            {
                "decision": {"candidate_local_decision": "scan_ok"},
                "provisional_as_of": 20260702,
                "setup_event_count": 3,
                "current_candidate_count": 2,
            },
        )
        return run_dir

    def fake_watch_board(*, provisional_scan_path, output_root):
        calls["watch_input"] = provisional_scan_path
        run_dir = output_root / "watch-run"
        _write_json(
            run_dir / "provisional_watch_board.json",
            {
                "decision": {"candidate_local_decision": "watch_ok"},
                "provisional_as_of": 20260702,
                "row_count": 2,
            },
        )
        return run_dir

    def fake_trigger_board(*, db_path, watch_board_path, output_root):
        calls["trigger_input"] = watch_board_path
        run_dir = output_root / "trigger-run"
        _write_json(
            run_dir / "provisional_trigger_board.json",
            {
                "decision": {"candidate_local_decision": "trigger_ok"},
                "provisional_as_of": 20260702,
                "rows": [{}, {}],
            },
        )
        return run_dir

    def fake_recheck(*, db_path, trigger_board_path, output_root):
        calls["recheck_input"] = trigger_board_path
        run_dir = output_root / "recheck-run"
        _write_json(
            run_dir / "trigger_recheck.json",
            {
                "decision": {"candidate_local_decision": "recheck_waiting_next_bar"},
                "status_counts": {"waiting_next_bar": 2},
            },
        )
        return run_dir

    monkeypatch.setattr(pipeline, "run_provisional_scan", fake_provisional_scan)
    monkeypatch.setattr(pipeline, "run_watch_board", fake_watch_board)
    monkeypatch.setattr(pipeline, "run_trigger_board", fake_trigger_board)
    monkeypatch.setattr(pipeline, "run_trigger_recheck", fake_recheck)

    run_dir = pipeline.run(
        db_path=db_path,
        output_root=output_root,
        current_root=current_root,
        include_confirmed_scan=False,
    )

    assert calls["watch_input"] == current_root / "scan-run" / "provisional_entry_timing_candidates.json"
    assert calls["trigger_input"] == current_root / "watch-run" / "provisional_watch_board.json"
    assert calls["recheck_input"] == current_root / "trigger-run" / "provisional_trigger_board.json"
    report = json.loads((run_dir / "short_entry_timing_pipeline.json").read_text(encoding="utf-8"))
    assert report["decision"]["candidate_local_decision"] == "recheck_waiting_next_bar"
    assert report["stages"]["provisional_scan"]["current_candidate_count"] == 2
    assert report["stages"]["trigger_recheck"]["status_counts"] == {"waiting_next_bar": 2}
