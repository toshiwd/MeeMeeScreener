from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from .toredex_config import load_toredex_config
from .toredex_hash import hash_payload
from .toredex_policy import build_decision
from .toredex_repository import ToredexRepository


def _parse_as_of(value: str) -> date:
    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    raise ValueError("as_of must be YYYY-MM-DD or YYYYMMDD")


def replay_decision(*, season_id: str, as_of: str) -> dict[str, Any]:
    as_of_date = _parse_as_of(as_of)
    repo = ToredexRepository()
    cfg = load_toredex_config()

    snapshot = repo.get_snapshot_payload(season_id, as_of_date)
    decision_row = repo.get_decision_row(season_id, as_of_date)
    decision_payload = repo.get_decision_payload(season_id, as_of_date)
    if not snapshot or not decision_row:
        raise RuntimeError("missing snapshot/decision payload for replay")

    prev_metric = repo.get_latest_metrics(season_id, before_or_equal=as_of_date - timedelta(days=1))
    mode = str((decision_payload or {}).get("mode") or "LIVE").upper()
    rebuilt = build_decision(snapshot=snapshot, config=cfg, prev_metrics=prev_metric, mode=mode)
    rebuilt_hash = hash_payload(rebuilt, exclude_fields={"createdAt", "runtime", "path", "realPath", "host"})
    saved_hash = str(decision_row.get("decision_hash") or "")

    if rebuilt_hash != saved_hash:
        raise RuntimeError("replay mismatch: decision_hash is not equal")

    return {
        "ok": True,
        "season_id": season_id,
        "asOf": as_of_date.isoformat(),
        "decision_hash": saved_hash,
        "replay_hash": rebuilt_hash,
        "matched": True,
    }
