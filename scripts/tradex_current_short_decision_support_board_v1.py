from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.current_short_regime_permission_board_v1 import (
    DIST_PRIOR_80_HIGH_THRESHOLD,
    LAST_VOL_RATIO_THRESHOLD,
    RANGE_40_20_THRESHOLD,
    classify as classify_regime_permission,
    find_latest_source_board,
    load_regime_rows,
)
from scripts.tradex_short_downside_target_overlay_v1 import (
    _add_context_features,
    _last_swing_low,
    _load_code_bars,
    _risk_reward,
    _safe_float,
    _target_candidates,
)
from scripts.tradex_short_early_continuation_filter_replay_v1 import _early_continuation
from scripts.tradex_short_realistic_downside_target_replay_v1 import _choose_realistic_target
from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path


AXIS_ID = "current_short_decision_support_board_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\current_short_decision_support_board_v1")
PRIOR_EARLY_CONTINUATION_ARTIFACT = Path(
    r"G:\Tradex\short_early_continuation_filter_replay_v1"
    r"\20260605T004003Z-short_early_continuation_filter_replay_v1"
    r"\early_continuation_filter_replay.json"
)
STOP_LOSS = 0.08
MIN_REQUIRED_EARLY_SESSIONS = 3


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    try:
        import numpy as np

        if isinstance(value, np.generic):
            return _json_ready(value.item())
    except Exception:
        pass
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _momentum_score(row: pd.Series) -> tuple[float, list[str]]:
    score = 0.0
    reasons: list[str] = []
    body = _safe_float(row.get("body_ratio")) or 0.0
    close_pos = _safe_float(row.get("close_pos")) or 0.5
    vol_ratio = _safe_float(row.get("vol_ratio_20")) or 1.0
    ret3 = _safe_float(row.get("ret_3")) or 0.0
    ma20_slope = _safe_float(row.get("ma20_slope5")) or 0.0
    ma60_slope = _safe_float(row.get("ma60_slope5")) or 0.0
    if float(row["c"]) < float(row["o"]):
        score += 1.0
        reasons.append("red_signal_candle")
    if body >= 0.55:
        score += 1.0
        reasons.append("large_body")
    if close_pos <= 0.35:
        score += 1.0
        reasons.append("weak_close")
    if vol_ratio >= 1.5:
        score += 0.75
        reasons.append("high_volume_pressure")
    if ret3 <= -0.05:
        score += 0.75
        reasons.append("three_day_down_momentum")
    if ma20_slope < 0:
        score += 0.5
        reasons.append("ma20_turning_down")
    if ma60_slope < 0:
        score += 0.5
        reasons.append("ma60_turning_down")
    return score, reasons


def _target_overlay(row: pd.Series, idx: int, enriched: pd.DataFrame) -> dict[str, Any]:
    signal_close = float(row["c"])
    levels = _target_candidates(row, _last_swing_low(enriched, idx))
    momentum_score, momentum_reasons = _momentum_score(row)
    target, reason = _choose_realistic_target(levels, momentum_score)
    rr = _risk_reward(signal_close, target, signal_close * (1.0 + STOP_LOSS))
    expected_downside = None if target is None else signal_close / float(target["price"]) - 1.0
    if target is None:
        actionability = "AvoidNoTarget"
    elif rr is not None and rr >= 1.2 and expected_downside is not None and expected_downside >= 0.08:
        actionability = "DownsideReviewCandidate"
    elif rr is not None and rr >= 0.75 and expected_downside is not None and expected_downside >= 0.04:
        actionability = "ScalpOnlyReview"
    else:
        actionability = "AvoidPoorReward"
    return {
        "base_target_actionability": actionability,
        "expected_target_price": None if target is None else float(target["price"]),
        "expected_downside_pct": expected_downside,
        "risk_reward_to_sl8": rr,
        "target_reason": reason,
        "target_level_id": None if target is None else target.get("level_id"),
        "target_level_type": None if target is None else target.get("level_type"),
        "target_ladder": levels,
        "momentum_score": momentum_score,
        "momentum_reasons": momentum_reasons,
    }


def _continuation_overlay(enriched: pd.DataFrame, idx: int, base_target_actionability: str) -> dict[str, Any]:
    available_after = int(max(0, len(enriched) - idx - 1))
    if available_after < MIN_REQUIRED_EARLY_SESSIONS:
        return {
            "continuation_status": "ContinuationPending",
            "continuation_reason": f"requires_{MIN_REQUIRED_EARLY_SESSIONS}_sessions_after_signal;available_{available_after}",
            "available_sessions_after_signal": available_after,
        }
    early = _early_continuation(enriched, idx)
    status = base_target_actionability
    if early["early_bucket"] in {"EarlyImpulse6NoDenial", "EarlyImpulse4NoDenial"}:
        status = "ContinuationPermit"
    elif early["early_bucket"] in {"EarlyDenied", "EarlyNoDenialNoProgress"}:
        status = "ContinuationBlock"
    elif early["early_bucket"] == "EarlyDriftDownNoDenial" and base_target_actionability in {"DownsideReviewCandidate", "ScalpOnlyReview"}:
        status = "ContinuationWatch"
    return {
        **early,
        "continuation_status": status,
        "continuation_reason": str(early["early_bucket"]),
        "available_sessions_after_signal": available_after,
    }


def _final_status(regime_status: str, continuation_status: str, base_target_actionability: str) -> str:
    if regime_status == "Avoid":
        return "Avoid"
    if regime_status == "BlockShort":
        return "BlockShort"
    if regime_status == "RegimeMissing":
        if continuation_status == "ContinuationPermit":
            return "RegimeMissingContinuationPermit"
        if continuation_status == "ContinuationBlock":
            return "RegimeMissingContinuationBlock"
        return "RegimeMissing"
    if continuation_status == "ContinuationPermit":
        return "PermitShort"
    if continuation_status == "ContinuationBlock":
        return "BlockShort"
    if base_target_actionability in {"AvoidPoorReward", "AvoidNoTarget"}:
        return "Avoid"
    return base_target_actionability


def _build_markdown(payload: dict[str, Any]) -> str:
    counts = payload["counts"]
    lines = [
        "# Current Short Decision Support Board v1",
        "",
        f"- source_board_path: `{payload['source_board_path']}`",
        f"- db_path: `{payload['db_path']}`",
        f"- authoritative_decision: `{payload['authoritative_decision']}`",
        "",
        "## Counts",
        "",
    ]
    for key, value in counts.items():
        lines.append(f"- {key}: {value}")
    lines.extend(
        [
            "",
            "## Top Candidates",
            "",
            "| rank | code | signal | regime | target | continuation | final | downside | rr | reason |",
            "|---:|---|---:|---|---|---|---|---:|---:|---|",
        ]
    )
    for item in payload["candidates"][:20]:
        downside = item.get("expected_downside_pct")
        rr = item.get("risk_reward_to_sl8")
        lines.append(
            f"| {item['original_rank']} | {item['code']} | {item['signal_ymd']} | "
            f"{item['regime_permission_status']} | {item['base_target_actionability']} | "
            f"{item['continuation_status']} | {item['final_review_status']} | "
            f"{'' if downside is None else round(float(downside) * 100, 2)} | "
            f"{'' if rr is None else round(float(rr), 2)} | {item['final_reason']} |"
        )
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- Review-only decision support. This is not a trade recommendation.",
            "- Ranking, score, EntryReady geometry, SL8, runtime DB, MeeMee, and production behavior are unchanged.",
            "- RegimeMissing remains unresolved because market_regime_daily freshness is still stale for current board signal dates.",
        ]
    )
    return "\n".join(lines) + "\n"


def run(db_path: Path, output_root: Path, source_board_path: Path, regime_db_path: Path | None) -> Path:
    run_dir = output_root / _run_id()
    board = _read_json(source_board_path)
    raw_candidates = list(board.get("candidates", []))
    signal_ymds = {int(item["signal_ymd"]) for item in raw_candidates if item.get("signal_ymd") is not None}
    regime_rows, regime_meta = load_regime_rows(signal_ymds, regime_db_path)
    codes = {str(item["code"]) for item in raw_candidates if item.get("code") is not None}
    min_ymd = min(signal_ymds) - 20000 if signal_ymds else 20150101
    bars_by_code = _load_code_bars(db_path, codes, min_ymd, 20991231)

    candidates: list[dict[str, Any]] = []
    for rank, item in enumerate(raw_candidates, start=1):
        code = str(item.get("code"))
        signal_ymd = int(item["signal_ymd"])
        regime_status, regime_reason = classify_regime_permission(item, regime_rows.get(signal_ymd))
        base = {
            "code": code,
            "name": item.get("name"),
            "signal_ymd": signal_ymd,
            "original_rank": rank,
            "original_score": item.get("original_score", item.get("rank_score")),
            "source_review_state": item.get("review_state"),
            "range_40_20": item.get("range_40_20"),
            "last_vol_ratio": item.get("last_vol_ratio"),
            "dist_prior_80_high": item.get("dist_prior_80_high"),
            "regime_permission_status": regime_status,
            "regime_permission_reason": regime_reason,
            "advancers_ratio": regime_rows.get(signal_ymd, {}).get("advancers_ratio"),
        }
        frame = bars_by_code.get(code)
        if frame is None or frame.empty:
            out = {
                **base,
                "coverage_status": "missing_daily_bars",
                "base_target_actionability": "NeedsData",
                "continuation_status": "ContinuationPending",
                "final_review_status": "NeedsData",
                "final_reason": "missing_daily_bars",
            }
            candidates.append(out)
            continue
        enriched = _add_context_features(frame)
        matches = enriched.index[enriched["ymd"].astype(int) == signal_ymd].tolist()
        if not matches:
            out = {
                **base,
                "coverage_status": "signal_ymd_not_found",
                "base_target_actionability": "NeedsData",
                "continuation_status": "ContinuationPending",
                "final_review_status": "NeedsData",
                "final_reason": "signal_ymd_not_found",
            }
            candidates.append(out)
            continue
        idx = matches[-1]
        row = enriched.iloc[idx]
        target = _target_overlay(row, idx, enriched)
        continuation = _continuation_overlay(enriched, idx, str(target["base_target_actionability"]))
        final = _final_status(regime_status, str(continuation["continuation_status"]), str(target["base_target_actionability"]))
        reason = f"regime={regime_status};target={target['base_target_actionability']};continuation={continuation['continuation_status']}"
        candidates.append(
            {
                **base,
                "coverage_status": "ready",
                "signal_close": float(row["c"]),
                **target,
                **continuation,
                "final_review_status": final,
                "final_reason": reason,
                "profit_target_rule": "realistic_reference_level_4_15pct",
                "stop_loss_rule": "sl8",
                "invalidation_note": "Uses kept early continuation filter for review-only decision support; no production exit rule changed.",
            }
        )

    final_counts = Counter(item["final_review_status"] for item in candidates)
    regime_counts = Counter(item["regime_permission_status"] for item in candidates)
    continuation_counts = Counter(item["continuation_status"] for item in candidates)
    target_counts = Counter(item["base_target_actionability"] for item in candidates)
    payload = {
        "run_id": run_dir.name,
        "created_at": _utc_now(),
        "axis_id": AXIS_ID,
        "source_board_path": str(source_board_path),
        "db_path": str(db_path),
        "prior_early_continuation_artifact_path": str(PRIOR_EARLY_CONTINUATION_ARTIFACT),
        "fixed_conditions": {
            "entry_ready_range_40_20_min": RANGE_40_20_THRESHOLD,
            "entry_ready_last_vol_ratio_max": LAST_VOL_RATIO_THRESHOLD,
            "dist_prior_80_high_min": DIST_PRIOR_80_HIGH_THRESHOLD,
            "regime_advancers_ratio_min": 0.650360,
            "target_model": "realistic_reference_level_4_15pct",
            "stop_loss": "sl8",
            "early_continuation": "kept ContinuationPermit/Block replay definition",
        },
        "market_regime_used": regime_meta,
        "counts": {
            "total_candidates": len(candidates),
            "final_status_counts": dict(final_counts),
            "regime_permission_counts": dict(regime_counts),
            "target_actionability_counts": dict(target_counts),
            "continuation_counts": dict(continuation_counts),
        },
        "topK_breakdown": {
            f"top{k}": dict(Counter(item["final_review_status"] for item in candidates[:k]))
            for k in (5, 10, 20)
        },
        "candidates": candidates,
        "authoritative_decision": "ready_current_decision_support_board_review_only",
        "runtime_db_write": False,
        "meemee_modified": False,
        "production_ranking_modified": False,
    }
    _write_json(run_dir / "current_short_decision_support_board.json", payload)
    (run_dir / "current_short_decision_support_summary.md").write_text(_build_markdown(payload), encoding="utf-8")
    _write_json(
        run_dir / "_ARTIFACT_COMPLETE.json",
        {
            "status": "complete",
            "axis_id": AXIS_ID,
            "generated_at": _utc_now(),
            "required_files": [
                "current_short_decision_support_board.json",
                "current_short_decision_support_summary.md",
                "_ARTIFACT_COMPLETE.json",
            ],
        },
    )
    return run_dir


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=resolve_runtime_stock_db_path())
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--source-board-path", type=Path, default=find_latest_source_board())
    parser.add_argument("--regime-db-path", type=Path, default=None)
    args = parser.parse_args()
    print(run(args.db_path, args.output_root, args.source_board_path, args.regime_db_path))


if __name__ == "__main__":
    main()
