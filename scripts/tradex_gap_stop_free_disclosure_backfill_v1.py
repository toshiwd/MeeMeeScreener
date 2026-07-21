"""Best-effort free historical TDNET-index backfill for fixed gap-stop cases."""
import argparse, hashlib, html, json, re, time
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin
from urllib.request import Request, urlopen

import pandas as pd

BASE = "https://irbank.net"
ENTRY_RE = re.compile(r"<dt>(\d{4}/\d{2}/\d{2})</dt>(.*?)(?=<dt>|<dd id=\"loading\"|</dl>)", re.S)
LINK_RE = re.compile(r'<a[^>]+title="([^"]*)"[^>]+href="([^"]+)"[^>]*>(.*?)</a>', re.S)
NEXT_RE = re.compile(r'data-nx="([^"]+)"')


def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def fetch(url):
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=20) as response:
        return response.read().decode("utf-8", errors="replace")


def clean(value):
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", "", value))).strip()


def classify(title):
    if "決算短信" in title or "決算発表" in title: return "earnings"
    if "上方修正" in title: return "upward_revision"
    if "業績予想" in title or "予想の修正" in title: return "forecast_revision"
    if "増配" in title or "配当予想" in title or "剰余金の配当" in title: return "dividend"
    if "自己株式の取得" in title or "自己株式取得" in title or "自社株買い" in title: return "buyback"
    if any(word in title for word in ("株式分割", "公開買付", "TOB", "資本提携", "子会社化", "合併", "株式交換")): return "corporate_action"
    return "other_disclosure"


def parse_timestamp(date_text, visible, title_attr):
    match = re.search(r"[（(](\d{1,2}):(\d{2})[）)]", visible)
    if not match:
        match = re.search(r"(\d{1,2}):(\d{2})提出", title_attr)
    hour, minute = (int(match.group(1)), int(match.group(2))) if match else (23, 59)
    return datetime.strptime(date_text, "%Y/%m/%d").replace(hour=hour, minute=minute)


def load_code_history(code, oldest_needed):
    url = f"{BASE}/{code}0/tdnet"
    visited, events, complete = set(), [], False
    for _ in range(30):
        if url in visited: break
        visited.add(url)
        page = fetch(url)
        page_dates = []
        for date_text, block in ENTRY_RE.findall(page):
            page_dates.append(datetime.strptime(date_text, "%Y/%m/%d").date())
            for title_attr, href, visible in LINK_RE.findall(block):
                title = clean(visible)
                events.append({"published_at": parse_timestamp(date_text, title, html.unescape(title_attr)),
                               "title": title, "url": urljoin(BASE, href)})
        if page_dates and min(page_dates) <= oldest_needed:
            complete = True
            break
        next_match = NEXT_RE.search(page)
        if not next_match:
            complete = bool(page_dates and min(page_dates) <= oldest_needed)
            break
        url = urljoin(BASE, html.unescape(next_match.group(1)))
        time.sleep(.15)
    unique = {(event["published_at"], event["url"]): event for event in events}
    return sorted(unique.values(), key=lambda event: event["published_at"]), complete, len(visited)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--board", type=Path, required=True)
    ap.add_argument("--parent-compare", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    a = ap.parse_args(); a.output.mkdir(parents=True, exist_ok=False)
    board = pd.read_csv(a.board, encoding="utf-8-sig", dtype={"code": str})
    board.code = board.code.str.zfill(4)
    histories, fetch_meta = {}, {}
    for code, group in board.groupby("code"):
        oldest = datetime.strptime(str(group.entry_ymd.min()), "%Y%m%d").date()
        try:
            histories[code], complete, pages = load_code_history(code, oldest)
            fetch_meta[code] = {"complete_to_oldest_case": complete, "pages": pages, "error": None}
        except Exception as exc:
            histories[code] = []
            fetch_meta[code] = {"complete_to_oldest_case": False, "pages": 0, "error": str(exc)}

    matches, updates = [], []
    for row in board.itertuples():
        start = datetime.strptime(str(row.entry_ymd), "%Y%m%d")
        gap_day = datetime.strptime(str(row.gap_stop_ymd), "%Y%m%d").replace(hour=9)
        found = [event for event in histories[row.code] if start <= event["published_at"] < gap_day]
        complete = fetch_meta[row.code]["complete_to_oldest_case"]
        status = "event_found" if found else "no_event_found_secondary_index" if complete else "event_unknown_fetch_incomplete"
        classes = sorted({classify(event["title"]) for event in found})
        updates.append({"case_id": row.case_id, "free_backfill_status": status, "event_count": len(found),
                        "event_classes": "|".join(classes), "evidence_level": "secondary_tdnet_index" if complete else "unverified"})
        for event in found:
            matches.append({"case_id": row.case_id, "code": row.code, "published_at": event["published_at"].isoformat(" "),
                            "event_class": classify(event["title"]), "title": event["title"], "url": event["url"],
                            "evidence_level": "secondary_tdnet_index"})
    enriched = board.merge(pd.DataFrame(updates), on="case_id", validate="one_to_one")
    enriched.to_csv(a.output / "gap_stop_event_backfill_enriched.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(matches, columns=["case_id", "code", "published_at", "event_class", "title", "url", "evidence_level"]).to_csv(
        a.output / "free_disclosure_matches.csv", index=False, encoding="utf-8-sig")
    status_counts = {str(k): int(v) for k, v in enriched.free_backfill_status.value_counts().items()}
    class_counts = {str(k): int(v) for k, v in pd.DataFrame(matches).event_class.value_counts().items()} if matches else {}
    result = {
        "schema_version": "tradex_gap_stop_free_disclosure_backfill_v1.compare.v1",
        "artifact_role": "authoritative_gap_stop_free_disclosure_backfill",
        "review_only": True, "research_phase": "comparison_stabilization",
        "fixed_conditions": {"cases": "fixed 31 gap-stop cases", "event_window": "entry timestamp 00:00 through gap-stop day 09:00 exclusive",
                             "source": "free IRBANK TDNET index mirror", "evidence_level": "secondary_tdnet_index",
                             "no_event_contract": "no_event_found only when pagination reached the oldest required case date"},
        "authoritative_result": {"cases": int(len(enriched)), "status_counts": status_counts, "matched_disclosures": int(len(matches)),
                                 "event_class_counts": class_counts, "fetch_meta": fetch_meta},
        "observed_branching": {"changed_top5_members_count": None, "changed_top10_members_count": None, "changed_rank_count": None,
                               "selection_divergence_reason": "gap-stop cases classified by free historical disclosure evidence"},
        "judgment": {"candidate_local_decision": "hold", "session_aggregate_decision": "free_backfill_completed_best_effort",
                     "authoritative_rollup_decision": "event_contribution_ready_for_review" if matches else "no_free_event_matches_or_fetch_incomplete",
                     "reason_type": "secondary_index_evidence_requires_primary_confirmation"},
        "not_changed": ["selector", "sizing", "MeeMee", "runtime DB", "production logic"],
    }
    cp = a.output / "compare.json"; cp.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    audit = {"sources": {"board": {"path": str(a.board.resolve()), "sha256": sha(a.board)},
                         "parent_compare": {"path": str(a.parent_compare.resolve()), "sha256": sha(a.parent_compare)}},
             "enriched_rows": int(len(enriched)), "unique_cases": int(enriched.case_id.nunique()),
             "enriched_sha256": sha(a.output / "gap_stop_event_backfill_enriched.csv"),
             "matches_sha256": sha(a.output / "free_disclosure_matches.csv"), "compare_sha256": sha(cp)}
    (a.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (a.output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(cp)}, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(a.output), "result": result["authoritative_result"], "judgment": result["judgment"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__": main()
