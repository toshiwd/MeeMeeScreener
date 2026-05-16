from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import tradex_teppan_ranking_branching_probe_v1 as mod


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _source_rows() -> pd.DataFrame:
    rows = []
    for idx in range(1, 13):
        rows.append(
            {
                "anchor_date": "2025-01-31",
                "side": "long",
                "symbol": f"91{idx:02d}",
                "champion_rank": idx,
                "champion_score": 1.0 - idx * 0.02,
                "forward_ret_20d": 0.08 if idx == 6 else (-0.04 if idx == 5 else 0.01),
                "month_bucket": "2025-01",
            }
        )
    return pd.DataFrame(rows)


def _tags() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol": "9106",
                "anchor_ymd": 20250131,
                "teppan_pattern_match": True,
                "teppan_guard_pass": True,
                "teppan_branch_signal": True,
                "best_pattern_family": "higher_frame_confirmed_daily",
                "best_pattern_key": "synthetic",
                "best_pattern_decision": "high_return_candidate",
                "best_teppan_score": 5.0,
                "matched_pattern_count": 1,
                "guard_block_reason": "",
            }
        ]
    )


def _artifact_roots(tmp_path: Path) -> tuple[Path, Path]:
    pattern = tmp_path / "pattern" / "pattern-run"
    guard = tmp_path / "guard" / "guard-run"
    _write_json(
        pattern / "research_decision.json",
        {"authoritative_research_decision": "promising_patterns_found", "silent_fallback_used": False},
    )
    _write_json(
        pattern / "teppan_candidates.json",
        {
            "candidate_count": 1,
            "candidates": [
                {
                    "pattern_family": "higher_frame_confirmed_daily",
                    "pattern_key": "synthetic",
                    "pattern_decision": "high_return_candidate",
                    "teppan_score": 5.0,
                }
            ],
        },
    )
    _write_json(
        guard / "research_decision.json",
        {"authoritative_research_decision": "keep", "silent_fallback_used": False},
    )
    return pattern.parent, guard.parent


def test_rank_with_teppan_boost_uses_no_forward_return_for_selection() -> None:
    source = _source_rows().copy()
    source["source_row_id"] = range(len(source))
    source["anchor_ymd"] = source["anchor_date"].map(mod._ymd_from_date_text).astype(int)
    source["champion_rank"] = pd.to_numeric(source["champion_rank"], errors="coerce").astype("Int64")
    tags = _tags()
    ranked_a = mod._rank_with_teppan_boost(source, tags)
    source_b = source.copy()
    source_b["forward_ret_20d"] = list(reversed(source_b["forward_ret_20d"].tolist()))
    ranked_b = mod._rank_with_teppan_boost(source_b, tags)

    cols = ["symbol", "challenger_rank", "teppan_boost_eligible"]
    assert ranked_a[cols].sort_values("symbol").reset_index(drop=True).equals(
        ranked_b[cols].sort_values("symbol").reset_index(drop=True)
    )
    assert int(ranked_a["changed_top5_member"].sum()) == 2


def test_run_writes_authoritative_artifacts_and_preserves_boundaries(tmp_path: Path) -> None:
    source_path = tmp_path / "source.parquet"
    _source_rows().to_parquet(source_path, index=False)
    pattern_root, guard_root = _artifact_roots(tmp_path)

    result = mod.run_teppan_ranking_branching_probe_v1(
        source_rows_parquet=source_path,
        source_db=source_path,
        pattern_root=pattern_root,
        pattern_run_id="pattern-run",
        guard_root=guard_root,
        guard_run_id="guard-run",
        output_root=tmp_path / "out",
        run_id="smoke",
        precomputed_tags=_tags(),
    )

    output_dir = Path(result["output_dir"])
    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (output_dir / artifact).exists(), artifact

    complete = json.loads((output_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    decision = json.loads((output_dir / "research_decision.json").read_text(encoding="utf-8"))
    branch = json.loads((output_dir / "branching_probe.json").read_text(encoding="utf-8"))
    contract = json.loads((output_dir / "evaluation_contract.json").read_text(encoding="utf-8"))

    assert complete["complete"] is True
    assert branch["changed_top5_members_count"] == 2
    assert branch["selection_divergence_reason"] == "top5_member_swap"
    assert decision["candidate_scoring_created"] is True
    assert decision["production_ranking_changed"] is False
    assert decision["meemee_reflectable"] is False
    assert decision["silent_fallback_used"] is False
    assert contract["future_label_policy"]["future_labels_used_in_selection"] is False
