"""Build the fixed gap-stop case ledger for targeted historical disclosure review."""
import argparse, hashlib, json
from pathlib import Path

import duckdb
import pandas as pd


def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--natural", type=Path, required=True)
    ap.add_argument("--db", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    a = ap.parse_args(); a.output.mkdir(parents=True, exist_ok=False)

    data = pd.read_parquet(a.natural)
    cases = data.loc[data.base_regime.eq("BOX") & (data.close_pos <= .20) & data.exit_reason_fixed3.eq("gap_stop")].copy()
    cases = cases.sort_values(["ymd", "code"]).reset_index(drop=True)
    cases["case_id"] = [f"GAP{i:03d}" for i in range(1, len(cases) + 1)]
    cases["signal_ymd"] = cases.ymd.astype(int)
    cases["entry_ymd"] = cases.entry_ymd.astype(int)
    cases["gap_stop_ymd"] = cases.exit_ymd.astype(int)

    con = duckdb.connect(str(a.db), read_only=True)
    coverage = con.execute("select min(published_at),max(published_at),count(*) from tdnet_disclosures").fetchone()
    price_dates = con.execute(
        "select code,strftime(to_timestamp(date),'%Y%m%d')::integer ymd from daily_bars where code in (select unnest(?)) order by code,date",
        [cases.code.unique().tolist()],
    ).fetchdf()
    price_dates.code = price_dates.code.astype(str).str.zfill(4)
    sessions = {code: list(group.ymd.astype(int)) for code, group in price_dates.groupby("code")}
    coverage_start = int(coverage[0].strftime("%Y%m%d")) if coverage[0] else None
    coverage_end = int(coverage[1].strftime("%Y%m%d")) if coverage[1] else None
    rows, disclosures = [], []
    for row in cases.itertuples():
        calendar = sessions[row.code]
        holding_sessions = calendar.index(row.gap_stop_ymd) - calendar.index(row.entry_ymd) + 1
        inside = coverage_start is not None and row.entry_ymd >= coverage_start and row.gap_stop_ymd <= coverage_end
        event_rows = []
        if inside:
            event_rows = con.execute(
                """select d.disclosure_id,d.published_at,d.title,d.category,f.event_type,f.earnings,
                          f.forecast_revision,f.dividend_revision,f.share_buyback,f.share_split
                   from tdnet_disclosures d join tdnet_disclosure_features f using(disclosure_id)
                   where d.sec_code=? and cast(strftime(d.published_at,'%Y%m%d') as integer) between ? and ?
                   order by d.published_at""",
                [row.code, row.entry_ymd, row.gap_stop_ymd],
            ).fetchall()
        status = "local_event_found" if event_rows else "no_local_event" if inside else "event_unknown_outside_local_coverage"
        for event in event_rows:
            disclosures.append({"case_id": row.case_id, "disclosure_id": event[0], "published_at": str(event[1]), "title": event[2],
                                "category": event[3], "event_type": event[4], "earnings": bool(event[5]),
                                "forecast_revision": event[6], "dividend_revision": event[7],
                                "share_buyback": bool(event[8]), "share_split": bool(event[9])})
        rows.append({"case_id": row.case_id, "code": row.code, "signal_ymd": row.signal_ymd, "entry_ymd": row.entry_ymd,
                     "gap_stop_ymd": row.gap_stop_ymd, "holding_sessions": holding_sessions,
                     "return_fixed3_pct": float(row.return_fixed3_pct), "year": int(row.year), "half": row.half,
                     "local_tdnet_status": status, "local_disclosure_count": len(event_rows),
                     "backfill_required": status == "event_unknown_outside_local_coverage"})
    con.close()
    board = pd.DataFrame(rows)
    board.to_csv(a.output / "gap_stop_event_backfill_board.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(disclosures).to_csv(a.output / "local_tdnet_matches.csv", index=False, encoding="utf-8-sig")
    result = {
        "schema_version": "tradex_gap_stop_event_backfill_board_v1.compare.v1",
        "artifact_role": "authoritative_gap_stop_event_backfill_board",
        "review_only": True, "research_phase": "infrastructure_stabilization",
        "fixed_conditions": {"selector": "BOX and close_pos<=0.20", "outcome": "exit_reason_fixed3=gap_stop",
                             "event_window": "entry_ymd through gap_stop_ymd inclusive",
                             "local_tdnet_coverage": {"start_ymd": coverage_start, "end_ymd": coverage_end, "rows": int(coverage[2])},
                             "unknown_contract": "outside local coverage is event_unknown, never no_event"},
        "authoritative_result": {"cases": int(len(board)), "years": {str(k): int(v) for k, v in board.year.value_counts().sort_index().items()},
                                 "status_counts": {str(k): int(v) for k, v in board.local_tdnet_status.value_counts().items()},
                                 "backfill_required": int(board.backfill_required.sum()), "local_disclosures": int(len(disclosures))},
        "observed_branching": {"changed_top5_members_count": None, "changed_top10_members_count": None, "changed_rank_count": None,
                               "selection_divergence_reason": "gap-stop cases fixed for external event review"},
        "judgment": {"candidate_local_decision": "hold", "session_aggregate_decision": "historical_backfill_required",
                     "authoritative_rollup_decision": "gap_stop_case_board_ready_external_tdnet_backfill_required",
                     "reason_type": "local_tdnet_coverage_does_not_overlap_cases"},
        "not_changed": ["selector", "sizing", "MeeMee", "runtime DB", "production logic"],
    }
    cp = a.output / "compare.json"; cp.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {"sources": {"natural": {"path": str(a.natural.resolve()), "sha256": sha(a.natural)},
                         "db": {"path": str(a.db.resolve()), "read_only": True}},
             "case_rows": int(len(board)), "unique_cases": int(board.case_id.nunique()),
             "board_sha256": sha(a.output / "gap_stop_event_backfill_board.csv"),
             "matches_sha256": sha(a.output / "local_tdnet_matches.csv"),
             "compare_sha256": sha(cp)}
    (a.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (a.output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(cp)}, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(a.output), "result": result["authoritative_result"], "judgment": result["judgment"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__": main()
