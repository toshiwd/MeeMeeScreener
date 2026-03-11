from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.services.toredex_config import load_toredex_config
from app.backend.services.toredex_policy import build_decision
from app.backend.services.toredex_snapshot_service import build_snapshot


DEFAULT_VARIANTS: list[dict[str, Any]] = [
    {
        "name": "live_current",
        "override": {},
    },
    {
        "name": "ml_reference",
        "override": {
            "rankingMode": "ml",
            "sides": {"longEnabled": True, "shortEnabled": True},
            "thresholds": {
                "entryMinUpProb": 0.65,
                "entryMinEv": 0.03,
                "entryMaxRevRisk": 0.55,
                "maxNewEntriesPerDay": 1.0,
                "newEntryMaxRank": 1.0,
            },
        },
    },
    {
        "name": "hybrid_balanced",
        "override": {
            "rankingMode": "hybrid",
            "sides": {"longEnabled": True, "shortEnabled": True},
            "thresholds": {
                "entryMinUpProb": 0.56,
                "entryMinEv": -0.01,
                "entryMaxRevRisk": 0.70,
                "maxNewEntriesPerDay": 2.0,
                "newEntryMaxRank": 10.0,
            },
        },
    },
    {
        "name": "hybrid_aggressive",
        "override": {
            "rankingMode": "hybrid",
            "sides": {"longEnabled": True, "shortEnabled": True},
            "thresholds": {
                "entryMinUpProb": 0.55,
                "entryMinEv": -0.03,
                "entryMaxRevRisk": 0.72,
                "maxNewEntriesPerDay": 2.0,
                "newEntryMaxRank": 10.0,
            },
        },
    },
]


@dataclass(frozen=True)
class Variant:
    name: str
    override: dict[str, Any]


def _parse_date(text: str) -> date:
    return datetime.strptime(text, "%Y-%m-%d").date()


def _iter_days(start_date: date, end_date: date) -> list[date]:
    out: list[date] = []
    cur = start_date
    while cur <= end_date:
        out.append(cur)
        cur = cur + timedelta(days=1)
    return out


def _load_variants(path: str | None) -> list[Variant]:
    payload: Any
    if path:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    else:
        payload = DEFAULT_VARIANTS

    if isinstance(payload, dict):
        payload = payload.get("variants")
    if not isinstance(payload, list):
        raise ValueError("variants must be a list or {\"variants\": [...]}")

    variants: list[Variant] = []
    for idx, raw in enumerate(payload):
        if not isinstance(raw, dict):
            raise ValueError(f"variants[{idx}] must be an object")
        name = str(raw.get("name") or "").strip()
        if not name:
            raise ValueError(f"variants[{idx}].name is required")
        override = raw.get("override")
        if override is None:
            override = {}
        if not isinstance(override, dict):
            raise ValueError(f"variants[{idx}].override must be an object")
        variants.append(Variant(name=name, override=override))
    if not variants:
        raise ValueError("variants is empty")
    return variants


def _top_counts(counter: Counter[str], *, limit: int = 10) -> list[dict[str, Any]]:
    pairs = sorted(counter.items(), key=lambda item: (-int(item[1]), str(item[0])))
    return [{"ticker": ticker, "count": int(count)} for ticker, count in pairs[:limit]]


def _action_row(action: dict[str, Any]) -> dict[str, Any]:
    return {
        "ticker": str(action.get("ticker") or ""),
        "side": str(action.get("side") or "").upper(),
        "delta_units": int(action.get("deltaUnits") or 0),
        "reason_id": str(action.get("reasonId") or ""),
        "notes": str(action.get("notes") or ""),
    }


def _scan_variant(
    *,
    variant: Variant,
    days: list[date],
    include_daily: bool,
) -> dict[str, Any]:
    cfg = load_toredex_config(override=variant.override)

    action_days = 0
    total_actions = 0
    long_entries = 0
    short_entries = 0
    crash_boost_entries = 0
    short_tickers: Counter[str] = Counter()
    daily_rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    for as_of in days:
        try:
            snapshot = build_snapshot(
                season_id=f"scan_{variant.name}",
                as_of=as_of,
                config=cfg,
                positions=[],
            )
            decision = build_decision(snapshot=snapshot, config=cfg, prev_metrics=None, mode="LIVE")
        except Exception as exc:
            errors.append({"as_of": as_of.isoformat(), "error": str(exc)})
            continue

        actions = [a for a in (decision.get("actions") or []) if isinstance(a, dict)]
        if actions:
            action_days += 1
            total_actions += len(actions)

        day_long_entries = 0
        day_short_entries = 0
        for action in actions:
            delta_units = int(action.get("deltaUnits") or 0)
            if delta_units <= 0:
                continue
            side = str(action.get("side") or "").upper()
            ticker = str(action.get("ticker") or "")
            if side == "LONG":
                long_entries += 1
                day_long_entries += 1
            elif side == "SHORT":
                short_entries += 1
                day_short_entries += 1
                if ticker:
                    short_tickers[ticker] += 1
                notes = str(action.get("notes") or "").upper()
                if "CRASH_DIP_BOOST" in notes:
                    crash_boost_entries += 1

        if include_daily:
            daily_rows.append(
                {
                    "as_of": as_of.isoformat(),
                    "action_count": len(actions),
                    "long_entries": day_long_entries,
                    "short_entries": day_short_entries,
                    "actions": [_action_row(a) for a in actions],
                }
            )

    out: dict[str, Any] = {
        "name": variant.name,
        "config_hash": cfg.config_hash,
        "override": variant.override,
        "action_days": int(action_days),
        "total_actions": int(total_actions),
        "long_entries": int(long_entries),
        "short_entries": int(short_entries),
        "crash_boost_entries": int(crash_boost_entries),
        "top_short_tickers": _top_counts(short_tickers),
        "errors": errors,
    }
    if include_daily:
        out["daily"] = daily_rows
    return out


def run_scan(
    *,
    start_date: date,
    end_date: date,
    variants: list[Variant],
    include_daily: bool,
) -> dict[str, Any]:
    days = _iter_days(start_date, end_date)
    results = [_scan_variant(variant=v, days=days, include_daily=include_daily) for v in variants]
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "window": {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "days": len(days),
        },
        "variants": results,
    }


def _resolve_output_path(raw: str | None, *, start_date: date, end_date: date) -> Path:
    if raw:
        return Path(raw)
    return Path("tmp") / f"toredex_short_rollout_scan_{start_date.isoformat()}_{end_date.isoformat()}.json"


def main() -> None:
    parser = argparse.ArgumentParser(description="Scan TOREDEX short rollout variants with snapshot/decision replay.")
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--variants-file", default="", help="JSON file path. Supports list or {\"variants\": [...]}")
    parser.add_argument("--output", default="", help="Output JSON path (default: tmp/...)")
    parser.add_argument("--include-daily", action="store_true", help="Include per-day action rows.")
    args = parser.parse_args()

    start_date = _parse_date(str(args.start_date))
    end_date = _parse_date(str(args.end_date))
    if end_date < start_date:
        raise ValueError("end-date must be >= start-date")

    variants = _load_variants(str(args.variants_file).strip() or None)
    payload = run_scan(
        start_date=start_date,
        end_date=end_date,
        variants=variants,
        include_daily=bool(args.include_daily),
    )
    out_path = _resolve_output_path(str(args.output).strip() or None, start_date=start_date, end_date=end_date)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[toredex_short_rollout_scan] wrote {out_path}")


if __name__ == "__main__":
    main()
