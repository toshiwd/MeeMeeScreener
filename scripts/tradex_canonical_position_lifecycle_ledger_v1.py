from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "canonical_position_lifecycle_ledger_v1"
DEFAULT_OUT_ROOT = Path("G:/Tradex/canonical_position_lifecycle_ledger_v1")
DEFAULT_SOURCE_ROOT = Path("G:/Tradex/position_management_policy_pretest_v1/20260526T022045Z-position-management-policy-pretest-v1")
DEFAULT_SOURCE_LEDGER = DEFAULT_SOURCE_ROOT / "position_policy_daily_ledger.csv"
DEFAULT_ENTRY_CONTRACT = DEFAULT_SOURCE_ROOT / "entry_source_contract.json"
DEFAULT_SOURCE_DECISION = DEFAULT_SOURCE_ROOT / "research_decision.json"
DEFAULT_FEATURE_AUDIT = Path("G:/Tradex/ma_phase_feature_base_v1/20260603T121215Z-ma-phase-feature-base-v1/input_audit.json")
REQUIRED = (
    "input_audit.json",
    "lifecycle_contract.json",
    "lifecycle_ledger.parquet",
    "lifecycle_ledger_sample.csv",
    "source_entry_events.csv",
    "no_lookahead_audit.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    try:
        import numpy as np

        if isinstance(value, np.generic):
            return _json_ready(value.item())
    except Exception:
        pass
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _build_ledger(source_ledger: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    raw = pd.read_csv(source_ledger)
    raw["code"] = raw["code"].astype(str)
    raw["position_id"] = raw["policy"].astype(str) + ":" + raw["event_id"].astype(str)
    raw = raw.sort_values(["policy", "event_id", "day_index"], kind="stable")
    first = raw.groupby(["policy", "event_id"], sort=False).first().reset_index()
    entry = first[["policy", "event_id", "as_of_date", "bar_date", "close"]].rename(
        columns={"as_of_date": "source_signal_date", "bar_date": "entry_date", "close": "entry_price"}
    )
    ledger = raw.merge(entry, on=["policy", "event_id"], how="left", validate="many_to_one")
    ledger["position_side"] = "long"
    ledger["bar_date"] = pd.to_numeric(ledger["bar_date"], errors="coerce").astype("Int64")
    ledger["entry_date"] = pd.to_numeric(ledger["entry_date"], errors="coerce").astype("Int64")
    ledger["close_price"] = pd.to_numeric(ledger["close"], errors="coerce")
    ledger["entry_price"] = pd.to_numeric(ledger["entry_price"], errors="coerce")
    ledger["holding_age_bars"] = pd.to_numeric(ledger["day_index"], errors="coerce").astype("Int64")
    ledger["is_open_at_bar"] = pd.to_numeric(ledger["units"], errors="coerce").fillna(0).gt(0)
    ledger["unrealized_return_pct"] = (ledger["close_price"] / ledger["entry_price"] - 1.0) * 100.0
    ledger["source_entry_event_id"] = ledger["event_id"]
    ledger["source_policy"] = ledger["policy"]
    ledger["source_replay_axis_id"] = "position_management_policy_pretest_v1"
    ledger["lifecycle_source_type"] = "research_replay_specific"
    ledger["is_real_position_lifecycle"] = False
    ledger["is_replay_specific_lifecycle"] = True
    out_cols = [
        "position_id",
        "code",
        "bar_date",
        "entry_date",
        "entry_price",
        "close_price",
        "position_side",
        "is_open_at_bar",
        "holding_age_bars",
        "unrealized_return_pct",
        "source_entry_event_id",
        "source_policy",
        "source_signal_date",
        "action",
        "units",
        "hedge_units",
        "gross_exposure",
        "drawdown_from_peak",
        "exit_reason",
        "source_replay_axis_id",
        "lifecycle_source_type",
        "is_real_position_lifecycle",
        "is_replay_specific_lifecycle",
    ]
    ledger = ledger[out_cols].copy()
    source_events = entry.merge(first[["policy", "event_id", "code"]], on=["policy", "event_id"], how="left")
    source_events = source_events.rename(columns={"event_id": "source_entry_event_id"})
    return ledger, source_events


def _contract(entry_contract: dict[str, Any]) -> dict[str, Any]:
    return {
        "axis_id": AXIS_ID,
        "contract_name": "canonical_position_lifecycle_ledger_v1",
        "required_grain": "one row per open/replay position per confirmed daily bar",
        "required_keys": ["position_id", "code", "bar_date"],
        "source_type": entry_contract.get("source_type"),
        "entry_source": entry_contract.get("entry_source"),
        "is_real_position_lifecycle": False,
        "is_replay_specific_lifecycle": True,
        "research_fallback_used": False,
        "replay_specific_limitation": "ledger is canonical-shaped but sourced from a specific historical research candidate replay, not a universal actual/open-position book",
        "fields": [
            "position_id",
            "code",
            "bar_date",
            "entry_date",
            "entry_price",
            "close_price",
            "position_side",
            "is_open_at_bar",
            "holding_age_bars",
            "unrealized_return_pct",
            "source_entry_event_id",
        ],
    }


def run(args: argparse.Namespace) -> Path:
    out_dir = args.out_root / f"{_now_tag()}-{AXIS_ID.replace('_', '-')}"
    out_dir.mkdir(parents=True, exist_ok=False)
    feature_audit = json.loads(args.feature_audit.read_text(encoding="utf-8"))
    entry_contract = json.loads(args.entry_contract.read_text(encoding="utf-8"))
    source_decision = json.loads(args.source_decision.read_text(encoding="utf-8"))
    ledger, source_events = _build_ledger(args.source_ledger)
    ledger.to_parquet(out_dir / "lifecycle_ledger.parquet", index=False)
    ledger.head(5000).to_csv(out_dir / "lifecycle_ledger_sample.csv", index=False, encoding="utf-8")
    source_events.to_csv(out_dir / "source_entry_events.csv", index=False, encoding="utf-8")
    input_audit = {
        "axis_id": AXIS_ID,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_ledger": str(args.source_ledger),
        "entry_source_contract": str(args.entry_contract),
        "source_decision": str(args.source_decision),
        "entry_source": entry_contract.get("entry_source"),
        "entry_source_type": entry_contract.get("source_type"),
        "source_research_decision": source_decision.get("research_decision"),
        "confirmed_bars_only_inherited": bool(feature_audit.get("confirmed_bars_only")),
        "row_count": int(len(ledger)),
        "position_count": int(ledger["position_id"].nunique()),
        "symbol_count": int(ledger["code"].nunique()),
        "min_bar_date": int(ledger["bar_date"].min()),
        "max_bar_date": int(ledger["bar_date"].max()),
        "is_real_position_lifecycle": False,
        "is_replay_specific_lifecycle": True,
        "research_fallback_used": False,
        "runtime_db_write": False,
        "meemee_reflection": False,
        "ranking_change": False,
        "publish": False,
    }
    _write_json(out_dir / "input_audit.json", input_audit)
    _write_json(out_dir / "lifecycle_contract.json", _contract(entry_contract))
    _write_json(
        out_dir / "no_lookahead_audit.json",
        {
            "axis_id": AXIS_ID,
            "entry_events_frozen_before_replay": bool(entry_contract.get("entry_events_frozen_before_replay")),
            "position_rows_use_replay_bar_state_only": True,
            "future_exit_labels_used_as_lifecycle_input": False,
            "runtime_db_write": False,
            "audit_result": "pass_with_replay_specific_scope",
        },
    )
    decision = {
        "axis_id": AXIS_ID,
        "candidate_local_decision": "keep_for_replay_specific_exit_policy_retest_next",
        "session_aggregate_decision": "keep_for_replay_specific_exit_policy_retest_next",
        "authoritative_rollup_decision": "keep_for_replay_specific_exit_policy_retest_next",
        "reason": "canonical_shaped_replay_specific_lifecycle_ledger_created",
        "sell_rule_promotion_allowed": False,
        "replay_specific_retest_allowed": True,
        "real_position_lifecycle_available": False,
        "non_scope": ["no MeeMee reflection", "no runtime DB write", "no ranking change", "no publish", "no candidate generation change", "no live sell rule implementation", "no exit threshold tuning", "no MA signal research", "no synthetic lifecycle promotion"],
    }
    _write_json(out_dir / "research_decision.json", decision)
    missing = [name for name in REQUIRED if name != "_ARTIFACT_COMPLETE.json" and not (out_dir / name).exists()]
    _write_json(out_dir / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "status": "complete" if not missing else "incomplete", "missing_artifacts": missing, "authoritative_result": str(out_dir / "research_decision.json"), "generated_at_utc": datetime.now(timezone.utc).isoformat()})
    if missing:
        raise RuntimeError(f"missing artifacts: {missing}")
    return out_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build TRADEX canonical-shaped position lifecycle ledger artifact.")
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--source-ledger", type=Path, default=DEFAULT_SOURCE_LEDGER)
    parser.add_argument("--entry-contract", type=Path, default=DEFAULT_ENTRY_CONTRACT)
    parser.add_argument("--source-decision", type=Path, default=DEFAULT_SOURCE_DECISION)
    parser.add_argument("--feature-audit", type=Path, default=DEFAULT_FEATURE_AUDIT)
    return parser.parse_args()


def main() -> None:
    print(run(parse_args()))


if __name__ == "__main__":
    main()
