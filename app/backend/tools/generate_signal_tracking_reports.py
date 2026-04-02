from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb

from app.backend.services import signal_tracking_service
from app.db.schema import ensure_schema


def _json_dump(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)


def _format_percent(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "--"
    return f"{value * 100:.1f}%"


def _format_compare_metric(value: Any, *, percent: bool = False, signed_percent: bool = False) -> str:
    if not isinstance(value, dict):
        if signed_percent:
            return _format_percent(value) if isinstance(value, (int, float)) else "--"
        return _format_percent(value) if percent else str(value)
    base = value.get("base")
    target = value.get("target")
    delta = value.get("delta")
    if percent or signed_percent:
        base_text = _format_percent(base)
        target_text = _format_percent(target)
        delta_text = _format_percent(delta) if isinstance(delta, (int, float)) else "--"
    else:
        base_text = str(base) if base is not None else "--"
        target_text = str(target) if target is not None else "--"
        delta_text = str(delta) if delta is not None else "--"
    return f"base {base_text} -> target {target_text} (delta {delta_text})"


def _format_report_markdown(
    *,
    signal_buy: dict[str, Any],
    signal_sell: dict[str, Any],
    ranking: dict[str, Any],
    leakage: dict[str, Any],
    sell_compare: dict[str, Any] | None = None,
) -> str:
    buy_summary = signal_buy.get("summary") or {}
    sell_summary = signal_sell.get("summary") or {}
    buy_decision = signal_buy.get("decision_level") or {}
    sell_decision = signal_sell.get("decision_level") or {}
    sell_subsets = ((signal_sell.get("sell_subset_comparison") or {}).get("subsets") or [])[:4]
    ranking_up = next((item for item in (ranking.get("by_dir") or []) if item.get("dir") == "up"), {})
    ranking_down = next((item for item in (ranking.get("by_dir") or []) if item.get("dir") == "down"), {})
    buy_breaks = list(buy_decision.get("by_break_reason") or [])[:5]
    buy_regimes = list(buy_decision.get("by_regime") or [])[:5]
    buy_patterns = list(buy_decision.get("profit_timing_patterns") or [])[:3]
    sell_patterns = list(sell_decision.get("profit_timing_patterns") or [])[:3]
    lines = [
        "# Signal Quality Report",
        "",
        f"- generated_at: {signal_buy.get('generated_at') or signal_sell.get('generated_at') or ranking.get('generated_at')}",
        "",
        "## Buy",
        f"- qualified_decisions: {buy_summary.get('qualified_decisions') or buy_decision.get('qualified_decisions')}",
        f"- directional_hit_rate_30: {_format_percent(buy_decision.get('qualified_directional_hit_rate_30'))}",
        f"- average_directional_return_30: {_format_percent(buy_decision.get('average_directional_return_30'))}",
        f"- lift_vs_same_date_universe_30: {_format_percent(buy_decision.get('lift_vs_same_date_universe_30'))}",
        f"- median_days_to_max_favorable_30: {buy_decision.get('median_days_to_max_favorable_30') if buy_decision.get('median_days_to_max_favorable_30') is not None else '--'}",
        f"- median_days_to_max_adverse_30: {buy_decision.get('median_days_to_max_adverse_30') if buy_decision.get('median_days_to_max_adverse_30') is not None else '--'}",
        "",
        "## Sell",
        f"- qualified_decisions: {sell_summary.get('qualified_decisions') or sell_decision.get('qualified_decisions')}",
        f"- directional_hit_rate_30: {_format_percent(sell_decision.get('qualified_directional_hit_rate_30'))}",
        f"- average_directional_return_30: {_format_percent(sell_decision.get('average_directional_return_30'))}",
        f"- lift_vs_same_date_universe_30: {_format_percent(sell_decision.get('lift_vs_same_date_universe_30'))}",
        f"- median_days_to_max_favorable_30: {sell_decision.get('median_days_to_max_favorable_30') if sell_decision.get('median_days_to_max_favorable_30') is not None else '--'}",
        f"- median_days_to_max_adverse_30: {sell_decision.get('median_days_to_max_adverse_30') if sell_decision.get('median_days_to_max_adverse_30') is not None else '--'}",
        "",
        "## Ranking",
        f"- up_average_directional_return_30: {_format_percent(ranking_up.get('average_directional_return_30'))}",
        f"- up_directional_win_rate_30: {_format_percent(ranking_up.get('directional_win_rate_30'))}",
        f"- up_median_days_to_max_favorable_30: {ranking_up.get('median_days_to_max_favorable_30') if ranking_up.get('median_days_to_max_favorable_30') is not None else '--'}",
        f"- down_average_directional_return_30: {_format_percent(ranking_down.get('average_directional_return_30'))}",
        f"- down_directional_win_rate_30: {_format_percent(ranking_down.get('directional_win_rate_30'))}",
        f"- down_median_days_to_max_favorable_30: {ranking_down.get('median_days_to_max_favorable_30') if ranking_down.get('median_days_to_max_favorable_30') is not None else '--'}",
        "",
        "## Buy Failure Top Reasons",
    ]
    if buy_breaks:
        lines.extend(
            [
                f"- {item.get('break_reason')}: count={item.get('count')} avg30={_format_percent(item.get('average_directional_return_30'))} win={_format_percent(item.get('directional_win_rate'))}"
                for item in buy_breaks
            ]
        )
    else:
        lines.append("- none")
    lines.extend(["", "## Buy By Regime"])
    if buy_regimes:
        lines.extend(
            [
                f"- {item.get('regime')}: count={item.get('qualified_decisions')} avg30={_format_percent(item.get('average_directional_return_30'))} lift={_format_percent(item.get('lift_vs_same_date_universe_30'))}"
                for item in buy_regimes
            ]
        )
    else:
        lines.append("- unavailable")
    lines.extend(["", "## Profit Timing Patterns"])
    if buy_patterns:
        lines.append("- buy")
        lines.extend(
            [
                f"  - {item.get('bucket')}: count={item.get('count')} share={_format_percent(item.get('share'))} 10d={_format_percent(item.get('average_directional_return_10'))} 20d={_format_percent(item.get('average_directional_return_20'))} 30d={_format_percent(item.get('average_directional_return_30'))}"
                for item in buy_patterns
            ]
        )
    else:
        lines.append("- buy: unavailable")
    if sell_patterns:
        lines.append("- sell")
        lines.extend(
            [
                f"  - {item.get('bucket')}: count={item.get('count')} share={_format_percent(item.get('share'))} 10d={_format_percent(item.get('average_directional_return_10'))} 20d={_format_percent(item.get('average_directional_return_20'))} 30d={_format_percent(item.get('average_directional_return_30'))}"
                for item in sell_patterns
            ]
        )
    else:
        lines.append("- sell: unavailable")
    lines.extend(["", "## Sell Subset Comparison"])
    if sell_subsets:
        lines.extend(
            [
                f"- {item.get('label')}: count={item.get('count')} hit={_format_percent(item.get('directional_hit_rate'))} return={_format_percent(item.get('average_directional_return'))} lift={_format_percent(item.get('lift_vs_same_date_universe'))} break={_format_percent(item.get('break_rate'))}"
                for item in sell_subsets
            ]
        )
    else:
        lines.append("- unavailable")
    lines.extend(
        [
            "",
            "## Sell v1 vs v2 (primary)",
        ]
    )
    if sell_compare:
        decision = sell_compare.get("decision") or {}
        campaign = sell_compare.get("campaign") or {}
        lines.extend(
            [
                f"- primary_horizon: {sell_compare.get('primary_horizon')}",
                f"- qualified_decisions: {_format_compare_metric(decision.get('qualified_decisions'))}",
                f"- directional_hit_rate: {_format_compare_metric(decision.get('directional_hit_rate'), percent=True)}",
                f"- average_directional_return: {_format_compare_metric(decision.get('average_directional_return'), signed_percent=True)}",
                f"- lift_vs_same_date_universe: {_format_compare_metric(decision.get('lift_vs_same_date_universe'), signed_percent=True)}",
                f"- campaign_win_rate: {_format_compare_metric(campaign.get('evaluated_directional_win_rate'), percent=True)}",
                f"- campaign_average_final_directional_return: {_format_compare_metric(campaign.get('average_final_directional_return'), signed_percent=True)}",
            ]
        )
    else:
        lines.append("- unavailable")
    lines.extend(
        [
            "",
            "## Leakage Audit",
            f"- basis_future_source_as_of_count: {((leakage.get('basis_provenance') or {}).get('future_source_as_of_count'))}",
            f"- basis_future_pred_dt_count: {((leakage.get('basis_provenance') or {}).get('future_pred_dt_count'))}",
            f"- prohibited_payload_count: {((leakage.get('basis_provenance') or {}).get('prohibited_payload_count'))}",
            f"- latest_signal_parity_available: {((leakage.get('latest_signal_parity') or {}).get('available'))}",
            f"- latest_signal_parity_mismatch_samples: {len(((leakage.get('latest_signal_parity') or {}).get('mismatch_samples') or []))}",
            f"- label_policy_audit_available: {((leakage.get('label_policy_audit') or {}).get('available'))}",
            f"- external_replay_audit_available: {((leakage.get('external_replay_audit') or {}).get('available'))}",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate signal tracking quality and leakage reports.")
    parser.add_argument("--db-path", default=None)
    parser.add_argument("--logic-version", default="latest")
    parser.add_argument("--ranking-logic-version", default="latest")
    parser.add_argument("--from", dest="from_ymd", default=None)
    parser.add_argument("--to", dest="to_ymd", default=None)
    parser.add_argument("--output-dir", default="output/analysis")
    args = parser.parse_args()

    output_dir = Path(str(args.output_dir)).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d")
    if args.db_path:
        with duckdb.connect(str(args.db_path)) as conn:
            ensure_schema(conn)

    buy_validation = signal_tracking_service.get_signal_tracking_validation(
        side="buy",
        logic_version=str(args.logic_version),
        from_ymd=args.from_ymd,
        to_ymd=args.to_ymd,
        db_path=args.db_path,
    )
    sell_validation = signal_tracking_service.get_signal_tracking_validation(
        side="sell",
        logic_version=str(args.logic_version),
        from_ymd=args.from_ymd,
        to_ymd=args.to_ymd,
        db_path=args.db_path,
    )
    ranking_analysis = signal_tracking_service.get_ranking_history_analysis(
        ranking_logic_version=str(args.ranking_logic_version),
        db_path=args.db_path,
    )
    sell_compare = signal_tracking_service.get_signal_tracking_comparison(
        side="sell",
        base_logic_version=signal_tracking_service.DEFAULT_LOGIC_VERSION,
        target_logic_version=signal_tracking_service.SELL_TIGHTENED_LOGIC_VERSION,
        primary_horizon=10,
        from_ymd=args.from_ymd,
        to_ymd=args.to_ymd,
        db_path=args.db_path,
    )
    leakage_audit = signal_tracking_service.get_signal_tracking_leakage_audit(
        side="buy",
        logic_version=str(args.logic_version),
        from_ymd=args.from_ymd,
        to_ymd=args.to_ymd,
        db_path=args.db_path,
    )

    signal_ranking_payload = {
        "generated_at": buy_validation.get("generated_at"),
        "logic_version": buy_validation.get("logic_version"),
        "ranking_logic_version": ranking_analysis.get("ranking_logic_version"),
        "buy_validation": buy_validation,
        "sell_validation": sell_validation,
        "ranking_analysis": ranking_analysis,
    }
    signal_quality_payload = {
        "generated_at": buy_validation.get("generated_at"),
        "buy": buy_validation,
        "sell": sell_validation,
        "ranking": ranking_analysis,
        "sell_compare": sell_compare,
    }

    (output_dir / f"signal_ranking_analysis_{stamp}.json").write_text(_json_dump(signal_ranking_payload), encoding="utf-8")
    (output_dir / f"signal_quality_report_{stamp}.json").write_text(_json_dump(signal_quality_payload), encoding="utf-8")
    (output_dir / f"leakage_audit_{stamp}.json").write_text(_json_dump(leakage_audit), encoding="utf-8")

    markdown = _format_report_markdown(
        signal_buy=buy_validation,
        signal_sell=sell_validation,
        ranking=ranking_analysis,
        leakage=leakage_audit,
        sell_compare=sell_compare,
    )
    (output_dir / f"signal_ranking_analysis_{stamp}.md").write_text(markdown, encoding="utf-8")
    (output_dir / f"signal_quality_report_{stamp}.md").write_text(markdown, encoding="utf-8")
    (output_dir / f"leakage_audit_{stamp}.md").write_text(markdown, encoding="utf-8")

    print(
        _json_dump(
            {
                "ok": True,
                "output_dir": str(output_dir),
                "files": [
                    f"signal_ranking_analysis_{stamp}.json",
                    f"signal_ranking_analysis_{stamp}.md",
                    f"signal_quality_report_{stamp}.json",
                    f"signal_quality_report_{stamp}.md",
                    f"leakage_audit_{stamp}.json",
                    f"leakage_audit_{stamp}.md",
                ],
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
