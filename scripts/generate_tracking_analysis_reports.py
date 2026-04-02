from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.services import signal_tracking_service


OUTPUT_DIR = Path("output/analysis")


def _safe_percent(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "--"
    percent = float(value) * 100.0
    return f"{percent:+.2f}%"


def _pick_first(rows: list[dict[str, Any]] | None, *, key: str, value: str) -> dict[str, Any] | None:
    for row in rows or []:
        if str(row.get(key) or "") == value:
            return row
    return None


def _json_dump(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2)


def main() -> None:
    stamp = datetime.now().strftime("%Y%m%d")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    buy_validation = signal_tracking_service.get_signal_tracking_validation(side="buy", logic_version="latest")
    sell_validation = signal_tracking_service.get_signal_tracking_validation(side="sell", logic_version="latest")
    ranking_analysis = signal_tracking_service.get_ranking_history_analysis(ranking_logic_version="latest")
    leakage_audit = signal_tracking_service.get_signal_tracking_leakage_audit(logic_version="latest")

    buy_decision = dict(buy_validation.get("decision_level") or {})
    sell_decision = dict(sell_validation.get("decision_level") or {})
    buy_campaign = dict(buy_validation.get("campaign_level") or {})
    ranking_up = _pick_first(ranking_analysis.get("by_dir") or [], key="dir", value="up") or {}
    ranking_down = _pick_first(ranking_analysis.get("by_dir") or [], key="dir", value="down") or {}
    buy_single = _pick_first(buy_campaign.get("by_signal_count") or [], key="bucket", value="single") or {}
    buy_repeated = _pick_first(buy_campaign.get("by_signal_count") or [], key="bucket", value="repeated") or {}
    top_break_reasons = (buy_validation.get("decision_level") or {}).get("by_break_reason") or []
    top_regimes = sorted(
        list((buy_validation.get("decision_level") or {}).get("by_regime") or []),
        key=lambda item: float(item.get("lift_vs_same_date_universe_30") or -999),
        reverse=True,
    )[:3]

    signal_quality_report = {
        "generated_at": datetime.now().isoformat(),
        "buy_validation": buy_validation,
        "sell_validation": sell_validation,
        "ranking_analysis": ranking_analysis,
        "highlights": {
            "buy_30d_directional_hit_rate": buy_decision.get("qualified_directional_hit_rate_30"),
            "buy_30d_average_directional_return": buy_decision.get("average_directional_return_30"),
            "buy_30d_lift_vs_universe": buy_decision.get("lift_vs_same_date_universe_30"),
            "sell_30d_directional_hit_rate": sell_decision.get("qualified_directional_hit_rate_30"),
            "sell_30d_average_directional_return": sell_decision.get("average_directional_return_30"),
            "sell_30d_lift_vs_universe": sell_decision.get("lift_vs_same_date_universe_30"),
            "ranking_up_30d_average_directional_return": ranking_up.get("average_directional_return_30"),
            "ranking_down_30d_average_directional_return": ranking_down.get("average_directional_return_30"),
            "buy_single_average_final_directional_return": buy_single.get("average_final_directional_return"),
            "buy_repeated_average_final_directional_return": buy_repeated.get("average_final_directional_return"),
        },
    }
    leakage_report = {
        "generated_at": datetime.now().isoformat(),
        "leakage_audit": leakage_audit,
    }

    signal_quality_json = OUTPUT_DIR / f"signal_quality_report_{stamp}.json"
    signal_quality_md = OUTPUT_DIR / f"signal_quality_report_{stamp}.md"
    leakage_json = OUTPUT_DIR / f"leakage_audit_{stamp}.json"
    leakage_md = OUTPUT_DIR / f"leakage_audit_{stamp}.md"

    signal_quality_json.write_text(_json_dump(signal_quality_report), encoding="utf-8")
    leakage_json.write_text(_json_dump(leakage_report), encoding="utf-8")

    signal_lines = [
        f"# Signal Quality Report {stamp}",
        "",
        "## Buy",
        f"- 30d 方向勝率: {_safe_percent(buy_decision.get('qualified_directional_hit_rate_30'))}",
        f"- 30d 平均方向リターン: {_safe_percent(buy_decision.get('average_directional_return_30'))}",
        f"- 30d lift vs same-date universe: {_safe_percent(buy_decision.get('lift_vs_same_date_universe_30'))}",
        "",
        "## Sell",
        f"- 30d 方向勝率: {_safe_percent(sell_decision.get('qualified_directional_hit_rate_30'))}",
        f"- 30d 平均方向リターン: {_safe_percent(sell_decision.get('average_directional_return_30'))}",
        f"- 30d lift vs same-date universe: {_safe_percent(sell_decision.get('lift_vs_same_date_universe_30'))}",
        "",
        "## Buy Failure",
        f"- 単発 campaign 平均最終: {_safe_percent(buy_single.get('average_final_directional_return'))}",
        f"- 再判定あり campaign 平均最終: {_safe_percent(buy_repeated.get('average_final_directional_return'))}",
        "- 上位 break reason:",
    ]
    for row in top_break_reasons[:5]:
        signal_lines.append(
            f"  - {row.get('break_reason')}: 件数 {row.get('count')} / 30d 平均 {_safe_percent(row.get('average_directional_return_30'))}"
        )
    signal_lines.extend(
        [
            "",
            "## Regime",
            "- 上位 regime lift:",
        ]
    )
    for row in top_regimes:
        signal_lines.append(
            f"  - {row.get('regime')}: lift {_safe_percent(row.get('lift_vs_same_date_universe_30'))} / 30d 勝率 {_safe_percent(row.get('directional_hit_rate_30'))}"
        )
    signal_lines.extend(
        [
            "",
            "## Ranking",
            f"- up 30d 平均方向リターン: {_safe_percent(ranking_up.get('average_directional_return_30'))}",
            f"- down 30d 平均方向リターン: {_safe_percent(ranking_down.get('average_directional_return_30'))}",
        ]
    )
    signal_quality_md.write_text("\n".join(signal_lines) + "\n", encoding="utf-8")

    leakage_lines = [
        f"# Leakage Audit {stamp}",
        "",
        "## Basis Provenance",
        f"- total rows: {leakage_audit.get('basis_provenance', {}).get('total_rows', 0)}",
        f"- future source_as_of: {leakage_audit.get('basis_provenance', {}).get('future_source_as_of_count', 0)}",
        f"- future pred_dt: {leakage_audit.get('basis_provenance', {}).get('future_pred_dt_count', 0)}",
        f"- prohibited payload rows: {leakage_audit.get('basis_provenance', {}).get('prohibited_payload_count', 0)}",
        "",
        "## Latest Parity",
        f"- available: {leakage_audit.get('latest_signal_parity', {}).get('available')}",
    ]
    for row in leakage_audit.get("latest_signal_parity", {}).get("per_side") or []:
        leakage_lines.append(
            f"  - {row.get('side')}: qualified match {_safe_percent(row.get('qualified_match_rate'))} / setup match {_safe_percent(row.get('setup_match_rate'))}"
        )
    leakage_lines.extend(
        [
            "",
            "## External Labels",
            f"- available: {leakage_audit.get('label_policy_audit', {}).get('available')}",
            "",
            "## External Replay",
            f"- available: {leakage_audit.get('external_replay_audit', {}).get('available')}",
        ]
    )
    leakage_md.write_text("\n".join(leakage_lines) + "\n", encoding="utf-8")

    print(signal_quality_json)
    print(signal_quality_md)
    print(leakage_json)
    print(leakage_md)


if __name__ == "__main__":
    main()
