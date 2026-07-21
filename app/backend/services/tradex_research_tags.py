from __future__ import annotations

import json
from pathlib import Path
from typing import Any


DEFAULT_SHORT_TAG_REPORT_PATH = Path(
    r"G:\Tradex\three_window_side_rule_probe_v1\latest_three_window_side_rule_report.json"
)


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        if not path.is_file():
            return None
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return loaded if isinstance(loaded, dict) else None


def _tag_rows(contract: dict[str, Any]) -> list[dict[str, Any]]:
    scan = contract.get("latest_current_scan")
    if not isinstance(scan, dict):
        return []
    rows: list[dict[str, Any]] = []
    for bucket, status in (
        ("triggered_previous_signal_rows", "triggered"),
        ("waiting_latest_signal_rows", "waiting"),
    ):
        values = scan.get(bucket)
        if isinstance(values, list):
            for value in values:
                if isinstance(value, dict):
                    rows.append({**value, "research_tag_status": status})
    return rows


def build_short_research_tags_by_code(
    *,
    report_path: Path = DEFAULT_SHORT_TAG_REPORT_PATH,
) -> dict[str, list[str]]:
    report = _read_json(report_path)
    if not report:
        return {}
    contract = report.get("meemee_display_contract")
    if not isinstance(contract, dict):
        return {}
    if contract.get("meemee_reflectable") is not True:
        return {}
    if contract.get("display_status") != "research_match_not_trade_signal":
        return {}
    label = str(contract.get("display_label_ja") or "").strip()
    if not label:
        return {}

    tags_by_code: dict[str, list[str]] = {}
    for row in _tag_rows(contract):
        code = str(row.get("code") or "").strip().upper()
        if not code:
            continue
        status = row.get("research_tag_status")
        suffix = "発動" if status == "triggered" else "待ち"
        tags_by_code.setdefault(code, []).append(f"{label}:{suffix}")
    return tags_by_code
