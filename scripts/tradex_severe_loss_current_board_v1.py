from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

if __package__ in (None, ""):
    sys.path[:0] = [str(Path(__file__).resolve().parents[1]), str(Path(__file__).resolve().parents[1] / "app")]

from scripts.tradex_point_in_time_chart_shape_priority_top3_v1 import FEATURES


AXIS_ID = "tradex_severe_loss_current_board_v1"
MODEL_ROOT = Path(r"G:\Tradex\point_in_time_severe_loss_classifier_top3_v1")
ROLLUP_ROOT = Path(r"G:\Tradex\severe_loss_current_regime_rollup_v1")
DEFAULT_OUT = Path(r"G:\Tradex\integrated_entry_board_v1")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def latest(root: Path, name: str) -> Path:
    paths = sorted(root.glob(f"*/{name}"), key=lambda path: path.stat().st_mtime)
    if not paths:
        raise ValueError(f"REQUIRED_ARTIFACT_NOT_FOUND:{name}:{root}")
    return paths[-1]


def runtime_status() -> dict[str, Any]:
    from backend.services.codex_bridge_service import get_runtime_stock_db_status
    return get_runtime_stock_db_status()


def verify_contract(model_path: Path, rollup_path: Path) -> tuple[dict, dict, str, str]:
    model = json.loads(model_path.read_text(encoding="utf-8")); rollup = json.loads(rollup_path.read_text(encoding="utf-8"))
    if rollup.get("artifact_role") != "authoritative" or rollup.get("decision", {}).get("authoritative_rollup_decision") != "current_regime_display_only_keep":
        raise ValueError("ROLLUP_NOT_AUTHORITATIVE_DISPLAY_ONLY_KEEP")
    model_hash, rollup_hash = sha256(model_path), sha256(rollup_path)
    frozen_sources = [row for row in rollup.get("source_artifacts", []) if Path(str(row.get("path", ""))).name == "frozen_model.json"]
    if len(frozen_sources) != 1 or frozen_sources[0].get("sha256") != model_hash:
        raise ValueError("FROZEN_MODEL_HASH_MISMATCH")
    if model.get("features") != FEATURES or rollup.get("model_freeze_evidence", {}).get("features") != FEATURES:
        raise ValueError("FROZEN_MODEL_FEATURE_LIST_MISMATCH")
    if model.get("estimator") != "sklearn.tree.DecisionTreeClassifier" or not isinstance(model.get("nodes"), list) or not isinstance(model.get("train_medians"), dict):
        raise ValueError("FROZEN_MODEL_SCHEMA_MISMATCH")
    return model, rollup, model_hash, rollup_hash


def predict_tree(model: dict, features: dict[str, float]) -> float:
    nodes = {int(node["node"]): node for node in model["nodes"]}; current = 0
    while True:
        node = nodes.get(current)
        if node is None:
            raise ValueError("FROZEN_MODEL_NODE_MISSING")
        if node.get("feature") is None:
            probability = float(node["severe_loss_probability"])
            if not 0.0 <= probability <= 1.0:
                raise ValueError("FROZEN_MODEL_PROBABILITY_INVALID")
            return probability
        name = str(node["feature"]); raw = features.get(name); value = float(model["train_medians"][name]) if raw is None or not math.isfinite(float(raw)) else float(raw)
        current = int(node["left"] if value <= float(node["threshold"]) else node["right"])


def query_candidates(db_path: Path, confirmed_ymd: int) -> list[dict[str, Any]]:
    with duckdb.connect(str(db_path), read_only=True) as con:
        ranking_date = con.execute("select max(dt) from ranking_appearance_daily").fetchone()[0]
        feature_date = con.execute("select max(cast(strftime(to_timestamp(dt),'%Y%m%d') as int)) from ml_feature_daily").fetchone()[0]
        if int(ranking_date or 0) != confirmed_ymd:
            raise ValueError(f"RANKING_CONFIRMED_DATE_MISMATCH:ranking={ranking_date}:confirmed={confirmed_ymd}")
        if int(feature_date or 0) != confirmed_ymd:
            raise ValueError(f"FEATURE_CONFIRMED_DATE_MISMATCH:feature={feature_date}:confirmed={confirmed_ymd}")
        rows = con.execute("""
            with ranked as (
              select dt,dir,rank,cast(code as varchar) code,name,anchor_price_close,
                     row_number() over(partition by dir order by rank,code) side_row
              from ranking_appearance_daily where dt=? and dir in ('up','down')
            ), feature as (
              select cast(code as varchar) code,close,ma7,ma20,ma60,ma7_prev1,ma20_prev1,ma60_prev1,
                     candle_body_ratio,candle_upper_wick_ratio,candle_lower_wick_ratio,close_ret2,close_ret3,
                     gap_pct,vol_ratio5_20,atr14_pct,range_pct
              from ml_feature_daily where cast(strftime(to_timestamp(dt),'%Y%m%d') as int)=?
            )
            select r.*,f.* exclude(code) from ranked r left join feature f using(code) where r.side_row<=5
            order by case r.dir when 'up' then 0 else 1 end,r.rank,r.code
        """, [confirmed_ymd, confirmed_ymd]).fetchdf().to_dict("records")
    if len(rows) != 10 or {str(row["dir"]) for row in rows} != {"up", "down"}:
        raise ValueError(f"RANKING_TOP5_COVERAGE_MISSING:n={len(rows)}")
    return rows


def feature_vector(row: dict[str, Any], side: str) -> dict[str, float]:
    values = {"side_code": 1.0 if side == "buy" else -1.0}
    for ma in (7, 20, 60):
        values[f"close_vs_ma{ma}"] = float(row["close"]) / float(row[f"ma{ma}"]) - 1.0
        values[f"ma{ma}_slope1_pct"] = float(row[f"ma{ma}"]) / float(row[f"ma{ma}_prev1"]) - 1.0
    for name in FEATURES:
        if name not in values:
            values[name] = float(row[name])
    if any(not math.isfinite(value) for value in values.values()):
        raise ValueError(f"FEATURE_COVERAGE_MISSING:{row.get('code')}")
    return values


def build_board(rows: list[dict[str, Any]], model: dict, confirmed_iso: str, model_path: Path, model_hash: str, rollup_path: Path, rollup_hash: str, db_path: Path) -> dict:
    scored = []
    for row in rows:
        side = "buy" if row["dir"] == "up" else "sell"; probability = predict_tree(model, feature_vector(row, side)); baseline_order = int(row["rank"]) * 2 - (1 if side == "buy" else 0)
        scored.append({"side": side, "code": str(row["code"]), "name": row.get("name"), "price": float(row["anchor_price_close"]) if row.get("anchor_price_close") is not None else float(row["close"]), "probability": probability, "baseline_order": baseline_order})
    scored.sort(key=lambda row: (row["probability"], row["baseline_order"], row["code"])); actionable, watch = [], []
    for priority, row in enumerate(scored, 1):
        high = row["probability"] >= 0.5; is_actionable = priority <= 3
        reason = "形状上の大損リスクが相対的に低い" if is_actionable else ("形状上の大損リスクが高いため見送り" if high else "優先3銘柄外")
        item = {"side": row["side"], "code": row["code"], "name": row["name"], "rule": "severe_loss_shape_guard", "integrated_rank": priority, "data_state": "official_close", "rule_state": "DisplayOnlyCurrentRegime", "rule_score": 1.0 - row["probability"], "rule_priority": priority, "global_priority": priority, "decision": "review_entry" if is_actionable else "watch_not_routed", "reason": reason, "avoid_reason": None if is_actionable else reason, "severe_loss_probability": row["probability"], "price": row["price"], "automatic_trade": False, "model_source": str(model_path)}
        (actionable if is_actionable else watch).append(item)
    return {"schema_version": f"{AXIS_ID}.board.v1", "artifact_role": "authoritative", "generated_at": datetime.now(timezone.utc).isoformat(), "confirmed_as_of": confirmed_iso, "current_regime": "display_only_current_regime", "directional_bias": "unified_low_severe_loss_priority", "actionable_count": len(actionable), "watch_count": len(watch), "actionable": actionable, "watch": watch, "decision": "review_entries_present", "ranking_contract": "severe-loss probability ascending, then fixed interleave order; unified top3", "boundary": {"display_only": True, "production": False, "automatic_trading": False, "official_and_provisional_not_mixed": True}, "freshness": {"runtime_db": str(db_path), "confirmed_date_matches_ranking": True, "confirmed_date_matches_features": True, "feature_coverage": 1.0}, "model_hash": model_hash, "rollup_hash": rollup_hash, "sources": {"frozen_model": str(model_path), "authoritative_rollup": str(rollup_path)}, "runtime_db_write": False, "production_ranking_changed": False}


def generate(db_path: Path | None, model_path: Path, rollup_path: Path, out_root: Path, status: dict[str, Any] | None = None) -> Path:
    status = dict(status or runtime_status()); selected_db = Path(db_path or status.get("selected_runtime_db_path", ""))
    if not status.get("validated") or status.get("stale") or status.get("freshness_blocked") or not selected_db.is_file():
        raise ValueError("RUNTIME_DB_NOT_FRESH_AND_VALIDATED")
    confirmed_iso = status.get("latest_confirmed_daily_bars_date_iso"); confirmed_ymd = int(str(confirmed_iso).replace("-", "")) if confirmed_iso else 0
    if not confirmed_ymd:
        raise ValueError("RUNTIME_CONFIRMED_DATE_MISSING")
    model, _, model_hash, rollup_hash = verify_contract(model_path, rollup_path)
    rows = query_candidates(selected_db, confirmed_ymd)
    payload = build_board(rows, model, str(confirmed_iso), model_path, model_hash, rollup_path, rollup_hash, selected_db)
    root = out_root / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"; root.mkdir(parents=True, exist_ok=False); path = root / "integrated_entry_board.json"; path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"); return path


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--db", type=Path); parser.add_argument("--model", type=Path, default=None); parser.add_argument("--rollup", type=Path, default=None); parser.add_argument("--out", type=Path, default=DEFAULT_OUT); args = parser.parse_args(); print(generate(args.db, args.model or latest(MODEL_ROOT, "frozen_model.json"), args.rollup or latest(ROLLUP_ROOT, "session_leaderboard_rollup.json"), args.out))


if __name__ == "__main__": main()
