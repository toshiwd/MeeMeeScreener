import os
import sys
from unittest.mock import patch

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from app.backend.jobs.scoring_job import ScoringJob


def _bars(length: int) -> list[tuple]:
    rows = []
    for i in range(length):
        base = 100 + i
        rows.append((20250101 + i, base, base + 2, base - 2, base + 1, 1000 + i))
    return rows


class _RepoStub:
    def __init__(self):
        self.calls: list[tuple] = []
        self.saved_scores = None

    def get_all_codes(self):
        self.calls.append(("get_all_codes",))
        return ["1301", "1302"]

    def get_daily_bars(self, code, limit=200):
        raise AssertionError(f"single fetch should not be used: {code} {limit}")

    def get_daily_bars_batch(self, codes, limit=200):
        self.calls.append(("get_daily_bars_batch", tuple(codes), limit))
        return {
            "1301": _bars(80),
            "1302": _bars(59),
        }

    def save_scores(self, scores, *, replace=False):
        self.calls.append(("save_scores", replace, len(scores)))
        self.saved_scores = (scores, replace)


def test_scoring_job_uses_batch_fetch_and_saves_results():
    repo = _RepoStub()

    with (
        patch("app.backend.jobs.scoring_job.calc_short_a_score", return_value=(10, ["a"], ["badge-a"])),
        patch("app.backend.jobs.scoring_job.calc_short_b_score", return_value=(0, [], [])),
    ):
        results = ScoringJob(repo).run()

    assert repo.calls[0] == ("get_all_codes",)
    assert repo.calls[1] == ("get_daily_bars_batch", ("1301", "1302"), 200)
    assert repo.calls[2] == ("save_scores", True, len(results))
    assert len(results) == 1
    assert results[0]["code"] == "1301"
