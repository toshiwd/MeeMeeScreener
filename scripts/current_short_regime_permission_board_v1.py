from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from shared.runtime_stock_db_contract import resolve_runtime_stock_db_path


ARTIFACT_ROOT = Path(r"G:\Tradex\current_short_regime_permission_board_v1")
STATE_BOARD_ROOT = Path(r"G:\Tradex\pre_crash_short_state_review_board_v1")
REVIEW_BOARD_ROOT = Path(r"G:\Tradex\pre_crash_short_review_board_v1")
PRIOR_GATE_ARTIFACT = Path(
    r"G:\Tradex\pre_crash_short_regime_permission_gate_v1"
    r"\20260604T114533Z-pre_crash_short_regime_permission_gate_v1"
    r"\research_decision.json"
)

ADVANCERS_RATIO_THRESHOLD = 0.650360
RANGE_40_20_THRESHOLD = 0.465006
LAST_VOL_RATIO_THRESHOLD = 0.901902
DIST_PRIOR_80_HIGH_THRESHOLD = -0.484599
EXIT_MODE = "pt20_sl8"
INVALIDATION_NOTE = (
    "No new exit rule applied. Early exit / invalidation candle is not tested in this run."
)

STATUSES = ("PermitShort", "BlockShort", "Avoid", "RegimeMissing")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _latest_existing_json(root: Path, filename: str) -> Path | None:
    if not root.exists():
        return None
    paths = [path for path in root.glob(f"*/*{filename}") if path.is_file()]
    if not paths:
        return None
    return max(paths, key=lambda path: path.stat().st_mtime)


def find_latest_source_board() -> Path:
    state_board = _latest_existing_json(STATE_BOARD_ROOT, "state_review_board.json")
    if state_board is not None:
        return state_board
    review_board = _latest_existing_json(REVIEW_BOARD_ROOT, "short_review_board.json")
    if review_board is not None:
        return review_board
    raise FileNotFoundError("No current sell review board artifact found.")


def _db_candidates() -> list[Path]:
    candidates: list[Path] = []
    try:
        candidates.append(resolve_runtime_stock_db_path())
    except Exception:
        pass
    local = Path.home() / "AppData" / "Local"
    candidates.extend(
        [
            local / "MeeMeeScreener-dev" / "data" / "stocks.duckdb",
            local / "MeeMeeScreener" / "data" / "stocks.duckdb",
            Path("data") / "stocks.duckdb",
            Path("app") / "backend" / "stocks.duckdb",
        ]
    )
    seen: set[str] = set()
    existing: list[Path] = []
    for path in candidates:
        resolved = str(path.resolve()) if path.exists() else str(path)
        if resolved in seen:
            continue
        seen.add(resolved)
        if path.exists():
            existing.append(path)
    return existing


def _table_columns(conn: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    return {str(row[0]) for row in conn.execute(f"DESCRIBE {table_name}").fetchall()}


def load_regime_rows(signal_ymds: set[int], db_path: Path | None) -> tuple[dict[int, dict[str, Any]], dict[str, Any]]:
    paths = [db_path] if db_path is not None else _db_candidates()
    attempts: list[dict[str, str]] = []
    required = {"dt", "advancers_ratio"}
    optional = ["breadth_above_ma20", "regime_score"]
    for path in paths:
        if path is None:
            continue
        try:
            with duckdb.connect(str(path), read_only=True) as conn:
                tables = {row[0] for row in conn.execute("SELECT table_name FROM duckdb_tables()").fetchall()}
                if "market_regime_daily" not in tables:
                    attempts.append({"path": str(path), "status": "market_regime_daily_missing"})
                    continue
                cols = _table_columns(conn, "market_regime_daily")
                missing = sorted(required - cols)
                if missing:
                    attempts.append({"path": str(path), "status": f"missing_columns:{','.join(missing)}"})
                    continue
                regime_stats = conn.execute("SELECT count(*), max(dt) FROM market_regime_daily").fetchone()
                regime_count = int(regime_stats[0] or 0) if regime_stats else 0
                max_dt = regime_stats[1] if regime_stats else None
                if regime_count <= 0:
                    attempts.append({"path": str(path), "status": "market_regime_daily_empty"})
                    continue
                select_cols = ["dt", "advancers_ratio"] + [col for col in optional if col in cols]
                rows = conn.execute(
                    f"""
                    SELECT {", ".join(select_cols)}
                    FROM market_regime_daily
                    WHERE dt IN ({", ".join("?" for _ in signal_ymds)})
                    ORDER BY dt
                    """,
                    list(sorted(signal_ymds)),
                ).fetchall()
                regime: dict[int, dict[str, Any]] = {}
                for row in rows:
                    item = dict(zip(select_cols, row, strict=True))
                    dt = int(item.pop("dt"))
                    regime[dt] = {"signal_ymd": dt, **item}
                meta = {
                    "path": str(path),
                    "status": "loaded",
                    "attempts": attempts,
                    "market_regime_daily_rows": regime_count,
                    "max_market_regime_dt": int(max_dt) if max_dt is not None else None,
                    "matched_signal_ymd_count": len(regime),
                }
                return regime, meta
        except Exception as exc:
            attempts.append({"path": str(path), "status": f"error:{type(exc).__name__}:{exc}"})
    return {}, {"path": str(db_path) if db_path else None, "status": "unavailable", "attempts": attempts}


def _numeric(row: dict[str, Any], key: str) -> float | None:
    value = row.get(key)
    if value is None:
        return None
    return float(value)


def classify(row: dict[str, Any], regime_row: dict[str, Any] | None) -> tuple[str, str]:
    reasons: list[str] = []
    range_40_20 = _numeric(row, "range_40_20")
    last_vol_ratio = _numeric(row, "last_vol_ratio")
    dist_prior_80_high = _numeric(row, "dist_prior_80_high")

    geometry_pass = True
    if range_40_20 is None or range_40_20 < RANGE_40_20_THRESHOLD:
        geometry_pass = False
        reasons.append("entry_geometry_failed:range_40_20")
    if last_vol_ratio is None or last_vol_ratio > LAST_VOL_RATIO_THRESHOLD:
        geometry_pass = False
        reasons.append("entry_geometry_failed:last_vol_ratio")
    if dist_prior_80_high is None or dist_prior_80_high < DIST_PRIOR_80_HIGH_THRESHOLD:
        reasons.append("oversold_guard_failed:dist_prior_80_high")
        return "Avoid", ";".join(reasons)
    if not geometry_pass:
        return "Avoid", ";".join(reasons)
    if regime_row is None:
        return "RegimeMissing", "entry_ready_geometry_passed;oversold_guard_passed;market_regime_missing"
    advancers_ratio = regime_row.get("advancers_ratio")
    if advancers_ratio is None:
        return "RegimeMissing", "entry_ready_geometry_passed;oversold_guard_passed;advancers_ratio_missing"
    if float(advancers_ratio) >= ADVANCERS_RATIO_THRESHOLD:
        return "PermitShort", "entry_ready_geometry_passed;oversold_guard_passed;advancers_ratio_permits"
    return "BlockShort", "entry_ready_geometry_passed;oversold_guard_passed;advancers_ratio_blocks"


def _topk_breakdown(candidates: list[dict[str, Any]], k: int) -> dict[str, int]:
    counter = Counter(candidate["permission_status"] for candidate in candidates[:k])
    return {status: int(counter.get(status, 0)) for status in STATUSES}


def _format_candidate_line(candidate: dict[str, Any]) -> str:
    score = candidate.get("original_score")
    score_text = "n/a" if score is None else f"{float(score):.6f}"
    adv = candidate.get("advancers_ratio")
    adv_text = "missing" if adv is None else f"{float(adv):.6f}"
    return (
        f"- #{candidate['original_rank']} {candidate['code']} "
        f"score={score_text} signal_ymd={candidate['signal_ymd']} "
        f"advancers_ratio={adv_text} reason={candidate['reason']}"
    )


def build_markdown(payload: dict[str, Any]) -> str:
    counts = payload["counts"]
    lines = [
        "# Current Short Regime Permission Board v1",
        "",
        f"- source_board_used: `{payload['source_board_path']}`",
        f"- prior_gate_artifact: `{payload['prior_gate_artifact_path']}`",
        f"- market_regime_db: `{payload['market_regime_used']['source_db_path']}`",
        f"- market_regime_status: `{payload['market_regime_used']['source_status']}`",
        "",
        "## Total Counts",
        "",
        f"- total_candidates: {counts['total_candidates']}",
        f"- PermitShort: {counts['permit_short_count']}",
        f"- BlockShort: {counts['block_short_count']}",
        f"- Avoid: {counts['avoid_count']}",
        f"- RegimeMissing: {counts['regime_missing_count']}",
        "",
        "## Top-K Classification",
        "",
        "| bucket | PermitShort | BlockShort | Avoid | RegimeMissing |",
        "|---|---:|---:|---:|---:|",
    ]
    for bucket in ("top5", "top10", "top20"):
        item = payload["topK_breakdown"][bucket]
        lines.append(
            f"| {bucket} | {item['PermitShort']} | {item['BlockShort']} | "
            f"{item['Avoid']} | {item['RegimeMissing']} |"
        )
    lines.extend(["", "## PermitShort Candidates", ""])
    for status in STATUSES:
        if status != "PermitShort":
            continue
        rows = [candidate for candidate in payload["candidates"] if candidate["permission_status"] == status]
        lines.extend([_format_candidate_line(candidate) for candidate in rows] or ["- none"])
    for status in ("BlockShort", "Avoid", "RegimeMissing"):
        lines.extend(["", f"## {status} Candidates", ""])
        rows = [candidate for candidate in payload["candidates"] if candidate["permission_status"] == status]
        lines.extend([_format_candidate_line(candidate) for candidate in rows] or ["- none"])
    lines.extend(
        [
            "",
            "## Review Actionability",
            "",
            f"- actionable_review_candidates_present: {payload['actionable_review_candidates_present']}",
            f"- decision_rule_judgment: {payload['decision_rule_judgment']}",
            "- no_trading_recommendation: This is a TRADEX review-only dry-run and is not a trade recommendation.",
            "",
            "## Next Research Step",
            "",
            "- Regime robustness check only if this current-board dry-run is complete.",
        ]
    )
    return "\n".join(lines) + "\n"


def decision_rule_judgment(counts: dict[str, int]) -> str:
    total = counts["total_candidates"]
    if total == 0:
        return "no_current_candidates"
    if counts["regime_missing_count"] > total / 2:
        return "regime_missing_dominates_stop_and_report_missing_regime_coverage"
    if counts["avoid_count"] > total * 0.8:
        return "almost_all_avoid_no_valid_entryready_short_candidates"
    top_active = counts["permit_short_count"] + counts["block_short_count"]
    if top_active and counts["block_short_count"] >= counts["permit_short_count"] * 2:
        return "keep_gate_as_useful_review_board_blocker"
    if 0 < counts["permit_short_count"] <= max(3, total // 3):
        return "keep_as_review_board_permission_overlay"
    if counts["permit_short_count"] > total * 0.8:
        return "little_current_filtering_value_but_research_keep"
    return "hold_review_overlay_for_more_current_coverage"


def run(source_board_path: Path, prior_gate_artifact_path: Path, db_path: Path | None) -> Path:
    created = _utc_now()
    run_id = f"{created.strftime('%Y%m%dT%H%M%SZ')}-current-short-regime-permission-board-v1"
    output_dir = ARTIFACT_ROOT / run_id
    output_dir.mkdir(parents=True, exist_ok=False)

    source = _read_json(source_board_path)
    prior_gate = _read_json(prior_gate_artifact_path)
    candidates_raw = source.get("candidates")
    if not isinstance(candidates_raw, list):
        raise ValueError("source board JSON does not contain a candidates list")

    signal_ymds = {int(candidate["signal_ymd"]) for candidate in candidates_raw if candidate.get("signal_ymd") is not None}
    regime_rows, regime_meta = load_regime_rows(signal_ymds, db_path)

    candidates: list[dict[str, Any]] = []
    for idx, candidate in enumerate(candidates_raw, start=1):
        signal_ymd = int(candidate["signal_ymd"])
        regime_row = regime_rows.get(signal_ymd)
        status, reason = classify(candidate, regime_row)
        out = {
            "code": str(candidate.get("code")),
            "name": candidate.get("name"),
            "signal_ymd": signal_ymd,
            "original_rank": idx,
            "original_score": candidate.get("original_score", candidate.get("rank_score", candidate.get("score"))),
            "range_40_20": candidate.get("range_40_20"),
            "last_vol_ratio": candidate.get("last_vol_ratio"),
            "dist_prior_80_high": candidate.get("dist_prior_80_high"),
            "advancers_ratio": regime_row.get("advancers_ratio") if regime_row else None,
            "permission_status": status,
            "reason": reason,
            "profit_target_rule": "pt20",
            "stop_loss_rule": "sl8",
            "invalidation_note": INVALIDATION_NOTE,
        }
        candidates.append(out)

    counter = Counter(candidate["permission_status"] for candidate in candidates)
    counts = {
        "total_candidates": len(candidates),
        "permit_short_count": int(counter.get("PermitShort", 0)),
        "block_short_count": int(counter.get("BlockShort", 0)),
        "avoid_count": int(counter.get("Avoid", 0)),
        "regime_missing_count": int(counter.get("RegimeMissing", 0)),
    }
    payload = {
        "run_id": run_id,
        "created_at": created.isoformat(),
        "source_board_path": str(source_board_path),
        "prior_gate_artifact_path": str(prior_gate_artifact_path),
        "gate": {
            "advancers_ratio_threshold": ADVANCERS_RATIO_THRESHOLD,
            "entry_ready_range_40_20_threshold": RANGE_40_20_THRESHOLD,
            "last_vol_ratio_threshold": LAST_VOL_RATIO_THRESHOLD,
            "dist_prior_80_high_threshold": DIST_PRIOR_80_HIGH_THRESHOLD,
            "exit_mode": EXIT_MODE,
            "prior_authoritative_decision": prior_gate.get("authoritative_decision"),
        },
        "counts": counts,
        "topK_breakdown": {
            "top5": _topk_breakdown(candidates, 5),
            "top10": _topk_breakdown(candidates, 10),
            "top20": _topk_breakdown(candidates, 20),
        },
        "market_regime_used": {
            "source_db_path": regime_meta.get("path"),
            "source_status": regime_meta.get("status"),
            "market_regime_daily_rows": regime_meta.get("market_regime_daily_rows"),
            "max_market_regime_dt": regime_meta.get("max_market_regime_dt"),
            "matched_signal_ymd_count": regime_meta.get("matched_signal_ymd_count", 0),
            "rows": [regime_rows[key] for key in sorted(regime_rows)],
            "attempts": regime_meta.get("attempts", []),
        },
        "candidates": candidates,
        "decision_rule_judgment": decision_rule_judgment(counts),
        "actionable_review_candidates_present": counts["permit_short_count"] > 0,
        "non_scope": {
            "ranking_changed": False,
            "scoring_changed": False,
            "entry_geometry_changed": False,
            "exit_changed": False,
            "runtime_db_write": False,
            "meemee_modified": False,
            "production_ranking_modified": False,
            "published": False,
        },
        "source_rank_note": "original_rank is restored from the source board JSON order; original_score is preserved from rank_score when original_score is absent.",
    }
    if payload["market_regime_used"]["matched_signal_ymd_count"] == 0 and counts["regime_missing_count"] > 0:
        payload["decision_rule_judgment"] = "regime_missing_coverage_blocks_permission_classification"

    json_path = output_dir / "current_short_regime_permission_board.json"
    md_path = output_dir / "current_short_regime_permission_summary.md"
    marker_path = output_dir / "_ARTIFACT_COMPLETE.json"
    _write_json(json_path, payload)
    md_path.write_text(build_markdown(payload), encoding="utf-8")
    _write_json(
        marker_path,
        {
            "run_id": run_id,
            "complete": True,
            "json_path": str(json_path),
            "markdown_path": str(md_path),
            "created_at": created.isoformat(),
        },
    )
    return output_dir


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-board-path", type=Path, default=None)
    parser.add_argument("--prior-gate-artifact-path", type=Path, default=PRIOR_GATE_ARTIFACT)
    parser.add_argument("--db-path", type=Path, default=None)
    args = parser.parse_args()

    source_board_path = args.source_board_path or find_latest_source_board()
    output_dir = run(source_board_path, args.prior_gate_artifact_path, args.db_path)
    print(output_dir)


if __name__ == "__main__":
    main()
