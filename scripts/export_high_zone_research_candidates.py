from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.services.ml import rankings_cache

SCHEMA_VERSION = "meemee_high_zone_research_candidates_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\meemee_high_zone_research_candidates")
REQUIRED_ARTIFACTS = (
    "high_zone_research_candidates.json",
    "_ARTIFACT_COMPLETE.json",
)


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def build_high_zone_research_candidate_artifacts(
    *,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    tf: rankings_cache.RankTimeframe = "D",
    which: rankings_cache.RankWhich = "latest",
    direction: rankings_cache.RankDir = "up",
    limit: int = 200,
    risk_mode: rankings_cache.RankRiskMode = "balanced",
    include_provisional: bool = False,
    session_id: str | None = None,
) -> dict[str, Any]:
    if direction != "up":
        raise ValueError("high-zone research export is long-side only")
    session_name = session_id or _utc_stamp()
    session_root = Path(output_root) / session_name
    payload = rankings_cache.get_rankings(
        tf,
        which,
        direction,
        limit,
        mode="trade",
        risk_mode=risk_mode,
        include_provisional=include_provisional,
    )
    candidates = list(payload.get("high_zone_research_candidates") or [])
    candidate_snapshot = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "session_root": str(session_root),
        "boundary": "TRADEX_REVIEW_ONLY",
        "source": "MeeMee rankings high_zone_research_candidates",
        "no_runtime_mutation": True,
        "ranking_request": {
            "tf": tf,
            "which": which,
            "direction": direction,
            "mode": "trade",
            "risk_mode": risk_mode,
            "limit": int(limit),
            "include_provisional": bool(include_provisional),
        },
        "ranking_snapshot": {
            "snapshot_as_of": payload.get("snapshot_as_of"),
            "provisional_snapshot_as_of": payload.get("provisional_snapshot_as_of"),
            "is_provisional": bool(payload.get("is_provisional")),
            "freshness_state": payload.get("freshness_state"),
            "freshness_days": payload.get("freshness_days"),
        },
        "candidate_count": len(candidates),
        "candidates": candidates,
    }
    _write_json(session_root / "high_zone_research_candidates.json", candidate_snapshot)
    complete = {
        "schema_version": f"{SCHEMA_VERSION}_artifact_complete_v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "session_root": str(session_root),
        "required_artifacts": list(REQUIRED_ARTIFACTS),
        "all_present": all((session_root / artifact).exists() for artifact in REQUIRED_ARTIFACTS if artifact != "_ARTIFACT_COMPLETE.json"),
        "artifact_refs": {artifact: str(session_root / artifact) for artifact in REQUIRED_ARTIFACTS},
        "candidate_count": len(candidates),
        "boundary": "TRADEX_REVIEW_ONLY",
        "no_runtime_mutation": True,
    }
    _write_json(session_root / "_ARTIFACT_COMPLETE.json", complete)
    return {
        "session_root": str(session_root),
        "candidate_count": len(candidates),
        "artifacts": {
            "candidate_snapshot": str(session_root / "high_zone_research_candidates.json"),
            "complete": str(session_root / "_ARTIFACT_COMPLETE.json"),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Export MeeMee high-zone research candidates as TRADEX review-only artifacts.")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--session-id", default=None)
    parser.add_argument("--include-provisional", action="store_true")
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--risk-mode", default="balanced", choices=["defensive", "balanced", "aggressive"])
    args = parser.parse_args()
    result = build_high_zone_research_candidate_artifacts(
        output_root=args.output_root,
        session_id=args.session_id,
        include_provisional=bool(args.include_provisional),
        limit=int(args.limit),
        risk_mode=args.risk_mode,
    )
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
