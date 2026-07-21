from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


AXIS_ID = "tradex_integrated_entry_board_v1"
ROUTER_ROOT = Path(r"G:\Tradex\adaptive_rule_router_v1")
SHORT_ROUTER_ROOT = Path(r"G:\Tradex\adaptive_short_rule_router_v1")
INTRADAY_SHORT = Path(r"G:\Tradex\intraday_short_preview_v1\latest_intraday_short_preview.json")
OUT = Path(r"G:\Tradex\integrated_entry_board_v1")


def latest(root: Path, name: str) -> Path:
    paths = sorted(root.glob(f"*/{name}"), key=lambda path: path.stat().st_mtime)
    if not paths:
        raise FileNotFoundError(f"{name} not found under {root}")
    return paths[-1]


def read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def build_board(router: dict[str, Any], short_preview: dict[str, Any], short_router: dict[str, Any] | None = None) -> dict[str, Any]:
    actionable: list[dict[str, Any]] = []
    watch: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for row in router.get("current_candidates", []):
        rule = str(row.get("router_rule") or row.get("rule") or "")
        key = ("buy", str(row.get("code")), rule)
        if key in seen:
            continue
        seen.add(key)
        item = {
            "side": "buy", "code": str(row.get("code")), "rule": rule,
            "signal_date": row.get("signal_date"), "price": row.get("confirmed_close"),
            "data_state": "official_close", "rule_state": row.get("router_state"),
            "rule_score": row.get("router_score"), "rule_priority": row.get("router_priority_rank"),
            "entry_condition": row.get("entry_condition") or "review_next_session_open",
            "decision": row.get("router_verdict"), "automatic_trade": False,
        }
        (actionable if row.get("router_verdict") == "review_entry" else watch).append(item)

    for row in (short_router or {}).get("current_candidates", []):
        item = {
            "side": "sell", "code": str(row.get("code")), "rule": str(row.get("rule") or ""),
            "signal_date": row.get("signal_date"), "price": row.get("confirmed_close"),
            "data_state": "official_close", "rule_state": row.get("rule_state"),
            "rule_score": row.get("rule_score"), "rule_priority": row.get("family_rank"),
            "entry_condition": row.get("entry_condition") or "next_session_trigger_review",
            "decision": row.get("decision"), "automatic_trade": False,
        }
        (actionable if row.get("decision") == "sell_condition_confirmed" else watch).append(item)

    if short_preview.get("intraday_available"):
        for row in short_preview.get("candidates", []):
            actionable.append({
                "side": "sell", "code": str(row.get("code")), "rule": "support_break_breadth40",
                "signal_date": row.get("provisional_ymd"), "price": row.get("price"),
                "data_state": "provisional_intraday", "rule_state": "Provisional",
                "rule_score": row.get("volume_vs20"), "rule_priority": row.get("intraday_rank"),
                "entry_condition": "official close confirmation, then next-session signal-low break",
                "decision": "preclose_review", "automatic_trade": False,
            })
        for row in short_preview.get("near_matches", []):
            watch.append({
                "side": "sell", "code": str(row.get("code")), "rule": "support_break_breadth40",
                "signal_date": row.get("provisional_ymd"), "price": row.get("price"),
                "data_state": "provisional_intraday", "rule_state": "NearMatch",
                "rule_score": row.get("passed_gate_count"), "rule_priority": None,
                "entry_condition": "watch only", "decision": "watch_not_routed", "automatic_trade": False,
            })

    actionable.sort(key=lambda row: (0 if row["data_state"] == "official_close" else 1, row.get("rule_priority") or 999, row["side"], row["code"]))
    watch.sort(key=lambda row: (row["side"], -(float(row.get("rule_score") or 0)), row["code"]))
    for rank, row in enumerate(actionable, start=1):
        row["integrated_rank"] = rank

    regime = str(router.get("current_regime") or "unknown")
    if short_preview.get("intraday_available") and short_preview.get("market_gate_pass"):
        directional_bias = "two_sided_review"
    elif regime == "broad_up":
        directional_bias = "buy_priority"
    elif regime == "risk_off":
        directional_bias = "sell_or_reversal_priority"
    else:
        directional_bias = "selective_mixed"
    return {
        "schema_version": f"{AXIS_ID}.board.v1", "artifact_role": "authoritative",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "confirmed_as_of": router.get("current_as_of"), "current_regime": regime,
        "directional_bias": directional_bias,
        "intraday_short_status": short_preview.get("status"),
        "intraday_short_available": bool(short_preview.get("intraday_available")),
        "actionable_count": len(actionable), "watch_count": len(watch),
        "actionable": actionable, "watch": watch,
        "decision": "review_entries_present" if actionable else "wait_no_entry",
        "ranking_contract": "official Active entries first; provisional pre-close entries second; each by point-in-time rule priority",
        "boundary": {"holdings_ignored": True, "capital_not_used": True, "automatic_trading": False, "official_and_provisional_not_mixed": True},
        "runtime_db_write": False, "production_ranking_changed": False,
    }


def run() -> Path:
    router_path = latest(ROUTER_ROOT, "compare.json")
    router = read(router_path)
    short_router_path = latest(SHORT_ROUTER_ROOT, "compare.json")
    short_router = read(short_router_path)
    short = read(INTRADAY_SHORT) if INTRADAY_SHORT.exists() else {"status": "preview_artifact_missing", "intraday_available": False}
    payload = build_board(router, short, short_router)
    payload["sources"] = {"adaptive_router": str(router_path), "adaptive_short_router": str(short_router_path), "intraday_short_preview": str(INTRADAY_SHORT)}
    output = OUT / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    output.mkdir(parents=True)
    path = output / "integrated_entry_board.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(path)
    return path


if __name__ == "__main__":
    run()
