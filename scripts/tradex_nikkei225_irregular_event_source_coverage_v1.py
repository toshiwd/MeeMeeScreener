from __future__ import annotations

import argparse
import bisect
import hashlib
import json
import re
import unicodedata
from datetime import datetime, time, timezone
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd


AXIS_ID = "tradex_nikkei225_irregular_event_source_coverage_v1"
WHITELIST_VERSION = "jp_material_irregular_v1"
MATERIAL_PATTERN = re.compile(
    r"決算短信|業績予想.{0,8}(修正|差異)|配当予想.{0,8}修正|増資|第三者割当|新株予約権|"
    r"株式分割|株式併合|自己株式.{0,8}(取得|処分)|TOB|公開買付|合併|株式交換|株式移転|"
    r"会社分割|上場廃止|監理|特別損益|特別利益|特別損失|重大訴訟"
)
HORIZONS = (1, 3, 5, 10)
PERIODS = {
    "diagnostic_2019_2021": (20190101, 20211231),
    "calibration_2022": (20220101, 20221231),
    "evaluation_2023_2025": (20230101, 20251231),
    "observed_2026": (20260101, 20260713),
}


def _normalize(value: Any) -> str:
    return re.sub(r"\s+", " ", unicodedata.normalize("NFKC", str(value or ""))).strip()


def _event_session(published_at: pd.Timestamp, sessions: list[int]) -> tuple[int | None, str]:
    ymd = int(published_at.strftime("%Y%m%d"))
    position = bisect.bisect_left(sessions, ymd)
    same_session = position < len(sessions) and sessions[position] == ymd
    before_close = published_at.time() <= time(15, 30)
    if same_session and before_close:
        return ymd, "same_session_before_or_at_close"
    next_position = position + 1 if same_session else position
    return (sessions[next_position], "next_session_after_close_or_nontrading") if next_position < len(sessions) else (None, "no_later_session_in_ledger")


def _snapshot_id(source: str, fetched_at: Any) -> str:
    return hashlib.sha256(f"{source}|{fetched_at}".encode()).hexdigest()[:20]


def run(feature_parquet: Path, runtime_db: Path, output_root: Path) -> Path:
    feature = pd.read_parquet(feature_parquet, columns=["code", "ymd"])
    feature["code"] = feature["code"].astype(str)
    sessions = sorted(int(value) for value in feature["ymd"].unique())
    codes = set(feature["code"].unique())
    conn = duckdb.connect(str(runtime_db), read_only=True)
    try:
        earnings = conn.execute("SELECT code,planned_date,kind,company_name,source,fetched_at FROM earnings_planned").fetchdf()
        rights = conn.execute("SELECT code,ex_date,record_date,category,last_rights_date,source,fetched_at FROM ex_rights").fetchdf()
        tdnet = conn.execute("SELECT disclosure_id,sec_code,company_name,title,category,published_at,fetched_at FROM tdnet_disclosures").fetchdf()
    finally:
        conn.close()

    earnings["code"] = earnings["code"].astype(str).str[:4]
    rights["code"] = rights["code"].astype(str).str[:4]
    tdnet["code"] = tdnet["sec_code"].astype(str).str[:4]
    earnings = earnings[earnings["code"].isin(codes)].copy()
    rights = rights[rights["code"].isin(codes)].copy()
    tdnet = tdnet[tdnet["code"].isin(codes)].copy()

    provenance: list[dict[str, Any]] = []
    for row in earnings.itertuples(index=False):
        event_ymd = int(pd.Timestamp(row.planned_date).strftime("%Y%m%d"))
        provenance.append({
            "source": "earnings_planned", "source_row_id": f"{row.code}|{event_ymd}", "code": row.code,
            "event_time": pd.Timestamp(row.planned_date), "event_session": event_ymd if event_ymd in sessions else None,
            "session_mapping": "scheduled_date", "known_at": pd.Timestamp(row.fetched_at),
            "snapshot_id": _snapshot_id("earnings_planned", row.fetched_at), "title_normalized": _normalize(row.kind),
            "category_normalized": "earnings", "material_whitelist_match": True,
            "pit_eligible_for_ledger": False, "pit_status_reason": "current_snapshot_not_archival_asof_decision",
        })
    for row in rights.itertuples(index=False):
        event_ymd = int(pd.Timestamp(row.ex_date).strftime("%Y%m%d"))
        provenance.append({
            "source": "ex_rights", "source_row_id": f"{row.code}|{event_ymd}", "code": row.code,
            "event_time": pd.Timestamp(row.ex_date), "event_session": event_ymd if event_ymd in sessions else None,
            "session_mapping": "scheduled_date", "known_at": pd.Timestamp(row.fetched_at),
            "snapshot_id": _snapshot_id("ex_rights", row.fetched_at), "title_normalized": _normalize(row.category),
            "category_normalized": "ex_rights", "material_whitelist_match": True,
            "pit_eligible_for_ledger": False, "pit_status_reason": "current_snapshot_not_archival_asof_decision",
        })
    material_sessions_by_code: dict[str, list[int]] = {}
    for row in tdnet.itertuples(index=False):
        title = _normalize(row.title)
        category = _normalize(row.category)
        material = bool(MATERIAL_PATTERN.search(title) or MATERIAL_PATTERN.search(category))
        event_session, mapping = _event_session(pd.Timestamp(row.published_at), sessions)
        if material and event_session is not None:
            material_sessions_by_code.setdefault(row.code, []).append(event_session)
        provenance.append({
            "source": "tdnet_disclosures", "source_row_id": row.disclosure_id, "code": row.code,
            "event_time": pd.Timestamp(row.published_at), "event_session": event_session,
            "session_mapping": mapping, "known_at": pd.Timestamp(row.fetched_at),
            "snapshot_id": _snapshot_id("tdnet_disclosures", row.fetched_at), "title_normalized": title,
            "category_normalized": category, "material_whitelist_match": material,
            "pit_eligible_for_ledger": False,
            "pit_status_reason": "system_ingestion_completeness_not_proven",
        })
    provenance_frame = pd.DataFrame(provenance)

    session_position = {ymd: index for index, ymd in enumerate(sessions)}
    ledger = feature.copy()
    ledger["decision_cutoff"] = ledger["ymd"].astype(str) + "T15:30:00+09:00"
    ledger["scheduled_snapshot_status"] = "unknown_no_archival_snapshot"
    ledger["tdnet_system_status"] = "unknown_ingestion_completeness_unproven"
    ledger["event_mask_union"] = "unknown"
    ledger["event_exclude"] = False
    ledger["event_eligible"] = False
    ledger["reason_bitset"] = "SCHEDULED_UNKNOWN|TDNET_INGESTION_UNKNOWN"
    for horizon in HORIZONS:
        realized = np.zeros(len(ledger), dtype=bool)
        for code, indices in ledger.groupby("code", sort=False).indices.items():
            event_sessions = sorted(set(material_sessions_by_code.get(str(code), [])))
            if not event_sessions:
                continue
            event_positions = [session_position[value] for value in event_sessions if value in session_position]
            for row_index in indices:
                current_position = session_position[int(ledger.at[row_index, "ymd"])]
                candidate = bisect.bisect_right(event_positions, current_position)
                realized[row_index] = candidate < len(event_positions) and event_positions[candidate] <= current_position + horizon
        ledger[f"mask_h{horizon}"] = "unknown"
        ledger[f"realized_unanticipated_tdnet_h{horizon}"] = realized
        ledger[f"realized_unanticipated_tdnet_h{horizon}_use"] = "evaluation_sensitivity_only_never_adoption"

    inventory = {
        "earnings_planned": {
            "rows_all": int(len(earnings)), "codes": int(earnings["code"].nunique()),
            "min_event": str(earnings["planned_date"].min()), "max_event": str(earnings["planned_date"].max()),
            "min_fetched_at": str(earnings["fetched_at"].min()), "max_fetched_at": str(earnings["fetched_at"].max()),
            "archival_snapshot_available": False,
        },
        "ex_rights": {
            "rows_all": int(len(rights)), "codes": int(rights["code"].nunique()),
            "min_event": str(rights["ex_date"].min()) if len(rights) else None,
            "max_event": str(rights["ex_date"].max()) if len(rights) else None,
            "archival_snapshot_available": False,
        },
        "tdnet_disclosures": {
            "rows_all": int(len(tdnet)), "codes": int(tdnet["code"].nunique()),
            "material_rows": int(provenance_frame.query("source == 'tdnet_disclosures' and material_whitelist_match").shape[0]),
            "min_published_at": str(tdnet["published_at"].min()), "max_published_at": str(tdnet["published_at"].max()),
            "ingestion_completeness_proven": False,
        },
    }
    coverage: dict[str, Any] = {}
    for period, (start, end) in PERIODS.items():
        part = ledger[ledger["ymd"].between(start, end)]
        coverage[period] = {
            "rows": int(len(part)), "codes": int(part["code"].nunique()),
            "known_rows": 0, "known_rate": 0.0, "unknown_rows": int(len(part)),
            "coverage_gate_95pct": False,
            "realized_tdnet_sensitivity_rates": {
                f"h{horizon}": float(part[f"realized_unanticipated_tdnet_h{horizon}"].mean()) if len(part) else None
                for horizon in HORIZONS
            },
        }

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = output_root / f"{stamp}-{AXIS_ID}"
    output.mkdir(parents=True, exist_ok=False)
    ledger_path = output / "irregular_event_three_state_ledger.parquet"
    provenance_path = output / "irregular_event_source_provenance.parquet"
    ledger.to_parquet(ledger_path, index=False, compression="zstd")
    provenance_frame.to_parquet(provenance_path, index=False, compression="zstd")
    whitelist_spec = {"version": WHITELIST_VERSION, "regex": MATERIAL_PATTERN.pattern}
    payload = {
        "schema_version": f"{AXIS_ID}.compare.v1", "artifact_role": "authoritative",
        "research_phase": "infrastructure_stabilization", "source_feature_parquet": str(feature_parquet),
        "runtime_db": str(runtime_db), "source_inventory": inventory, "coverage": coverage,
        "whitelist": {**whitelist_spec, "hash": hashlib.sha256(json.dumps(whitelist_spec, sort_keys=True).encode()).hexdigest()},
        "outputs": {"three_state_ledger": str(ledger_path), "source_provenance": str(provenance_path)},
        "fixed_conditions": {
            "mask_states": ["exclude", "eligible", "unknown"], "unknown_is_not_eligible": True,
            "session_close": "15:30 Asia/Tokyo", "tdnet_before_close": "same session", "tdnet_after_close_or_holiday": "next trading session",
            "scheduled_sources": "archival snapshot with fetched_at<=decision cutoff required",
            "tdnet_primary": "system ingestion PIT and completeness proof required",
            "future_tdnet": "realized_unanticipated sensitivity only; prohibited from action/adoption",
            "model_retrained": False, "features_changed": False, "probabilities_changed": False, "thresholds_changed": False,
        },
        "decision": {"candidate_local_decision": "hold_source_pit_unavailable", "authoritative_rollup_decision": "review_only"},
        "boundary": {"owner": "TRADEX", "meemee_changed": False, "runtime_db_write": False, "production_ranking_changed": False},
        "remaining_risks": ["2019-2025 scheduled archival snapshots absent", "TDnet ingestion completeness not proven", "2026 observed rows cannot repair historical coverage"],
    }
    compare = output / "compare.json"
    compare.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "compare": str(compare), "ledger": str(ledger_path), "provenance": str(provenance_path)}, indent=2) + "\n", encoding="utf-8")
    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--feature-parquet", required=True, type=Path)
    parser.add_argument("--runtime-db", required=True, type=Path)
    parser.add_argument("--output-root", type=Path, default=Path(r"G:\Tradex\tradex_nikkei225_irregular_event_source_coverage_v1"))
    args = parser.parse_args()
    print(run(args.feature_parquet, args.runtime_db, args.output_root))


if __name__ == "__main__":
    main()
