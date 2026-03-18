import os
import sys

import pandas as pd

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from app.backend.jobs import phase_batch


def test_phase_batch_short_circuits_when_legacy_analysis_disabled(monkeypatch):
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")

    def _raise():
        raise AssertionError("get_conn should not be called")

    monkeypatch.setattr(phase_batch, "get_conn", _raise)

    phase_batch.run_batch(20260301, 20260331, dry_run=False)


def test_phase_batch_run_batch_normalizes_yyyymmdd_arguments(monkeypatch):
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "0")

    captured: dict[str, object] = {}

    class _FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def execute(self, sql, params=None):
            if "SELECT COUNT(*) FROM label_20d" in sql:
                return type("_R", (), {"fetchone": lambda self: (1,)})()
            if "SELECT dt, code, cont_label, ex_label FROM label_20d" in sql:
                return type("_R", (), {"df": lambda self: pd.DataFrame(columns=["dt", "code", "cont_label", "ex_label"])})()
            raise AssertionError(sql)

    monkeypatch.setattr(phase_batch, "get_conn", lambda: _FakeConn())

    def _fake_load_feature_snapshot(conn, start_dt, end_dt):
        return pd.DataFrame(
            [
                {
                    "dt": 1772323200,
                    "code": "1301",
                    "close": 100.0,
                    "ma7": 99.0,
                    "ma20": 98.0,
                    "ma60": 97.0,
                    "diff20_pct": 0.01,
                    "cnt_20_above": 5,
                    "cnt_7_above": 3,
                }
            ]
        )

    monkeypatch.setattr(phase_batch, "_load_feature_snapshot", _fake_load_feature_snapshot)
    monkeypatch.setattr(phase_batch, "_build_phase_records", lambda feature_df, label_df, start_dt, end_dt: captured.update({"start_dt": start_dt, "end_dt": end_dt}) or [])

    phase_batch.run_batch(20260301, 20260331, dry_run=True)

    assert captured["start_dt"] == 1772323200
    assert captured["end_dt"] == 1774915200
