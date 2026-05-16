from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts import portfolio_agent_replay_v1 as replay


AXIS_ID = "portfolio_agent_replay_c1506_fixed_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\portfolio_agent_replay_c1506_fixed_v1")
C1506_STOP_LOSS = -0.07
C1506_PROFIT_TARGET = float("inf")
C1506_MAX_HOLDING_TRADING_DAYS = 30


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, float) and value == float("inf"):
        return "disabled"
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")


def run_c1506_fixed_replay(
    *,
    source_db: str | Path | None = None,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
    start_ymd: int = 20210101,
    end_ymd: int = 20220101,
) -> dict[str, Any]:
    old_profit_target = replay.PROFIT_TARGET
    old_stop_loss = replay.STOP_LOSS
    old_max_holding = replay.MAX_HOLDING_TRADING_DAYS
    try:
        replay.PROFIT_TARGET = C1506_PROFIT_TARGET
        replay.STOP_LOSS = C1506_STOP_LOSS
        replay.MAX_HOLDING_TRADING_DAYS = C1506_MAX_HOLDING_TRADING_DAYS
        result = replay.run_portfolio_agent_replay_v1(
            source_db=source_db,
            output_root=output_root,
            run_id=run_id,
            start_ymd=start_ymd,
            end_ymd=end_ymd,
        )
    finally:
        replay.PROFIT_TARGET = old_profit_target
        replay.STOP_LOSS = old_stop_loss
        replay.MAX_HOLDING_TRADING_DAYS = old_max_holding

    output_dir = Path(result["output_dir"])
    summary = {
        "schema_version": "tradex_portfolio_agent_replay_c1506_fixed_v1_summary_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "source_replay_axis": "portfolio_agent_replay_v1",
        "fixed_candidate_source": "portfolio_exit_parameter_optimizer_v1:c1506",
        "period": {"start_ymd": int(start_ymd), "end_ymd": int(end_ymd)},
        "fixed_exit_rules": {
            "stop_loss": C1506_STOP_LOSS,
            "profit_target": None,
            "profit_target_disabled": True,
            "max_holding_trading_days": C1506_MAX_HOLDING_TRADING_DAYS,
        },
        "optimization_mode": False,
        "in_sample_only": False,
        "parameter_sweep": False,
        "policy_promotion_allowed": False,
        "meemee_ui_changed": False,
        "runtime_db_written": False,
        "ranking_changed": False,
        "publish_registry_changed": False,
        "result": result,
    }
    _write_json(output_dir / "c1506_fixed_challenger_summary.json", summary)
    return {**result, "c1506_fixed_summary": str(output_dir / "c1506_fixed_challenger_summary.json")}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run fixed c1506 exit challenger replay.")
    parser.add_argument("--source-db", default="")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id", default="")
    parser.add_argument("--start-ymd", type=int, default=20210101)
    parser.add_argument("--end-ymd", type=int, default=20220101)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    print(
        json.dumps(
            _json_ready(
                run_c1506_fixed_replay(
                    source_db=args.source_db.strip() or None,
                    output_root=args.output_root,
                    run_id=args.run_id.strip() or None,
                    start_ymd=args.start_ymd,
                    end_ymd=args.end_ymd,
                )
            ),
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
