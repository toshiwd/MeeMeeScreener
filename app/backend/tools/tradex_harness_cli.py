from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import Any

from app.backend.services import tradex_research_contracts as contracts
from app.backend.services.tradex_experiment_store import read_json, run_manifest_file
from app.backend.tools import tradex_data_smoke_check as data_smoke
from app.backend.tools import tradex_research_runner as runner


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    return value


def _print_json(payload: dict[str, Any]) -> None:
    print(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True))


def _run_subprocess(command: list[str]) -> int:
    completed = subprocess.run(command, check=False)
    return int(completed.returncode)


def _run_pytest(args: list[str]) -> int:
    return _run_subprocess([sys.executable, "-m", "pytest", "-q", *args])


def _verify_artifact(path: Path, validator) -> dict[str, Any]:
    payload = read_json(path)
    validator(payload)
    return {"path": str(path), "schema_version": payload.get("schema_version"), "exists": path.exists()}


def _validate_session_outputs(session_id: str) -> dict[str, Any]:
    manifest_path = run_manifest_file(session_id)
    manifest = read_json(manifest_path)
    contracts.validate_run_manifest(manifest)

    compare_path = runner._session_compare_file(session_id)
    compare = read_json(compare_path)
    contracts.validate_compare_artifact(compare)

    family_leaderboard_path = runner._session_family_leaderboard_file(session_id)
    family_leaderboard = read_json(family_leaderboard_path)
    contracts.validate_family_leaderboard_artifact(family_leaderboard)

    session_rollup_path = runner._session_leaderboard_rollup_file()
    session_rollup = read_json(session_rollup_path)
    if session_rollup:
        contracts.validate_session_rollup_artifact(session_rollup)

    scope_rollup_path = runner._scope_stability_rollup_file()
    scope_rollup = read_json(scope_rollup_path)
    if scope_rollup:
        contracts.validate_scope_rollup_artifact(scope_rollup)

    return {
        "session_id": session_id,
        "manifest_path": str(manifest_path),
        "compare_path": str(compare_path),
        "family_leaderboard_path": str(family_leaderboard_path),
        "session_rollup_path": str(session_rollup_path),
        "scope_rollup_path": str(scope_rollup_path),
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m app.backend.tools.tradex_harness_cli")
    sub = parser.add_subparsers(dest="cmd", required=True)

    contract_parser = sub.add_parser("contract-tests", help="Run TRADEX contract tests.")
    contract_parser.add_argument("--test-path", default="tests/contracts/test_tradex_research_contracts.py")

    integration_parser = sub.add_parser("integration-tests", help="Run TRADEX integration tests.")
    integration_parser.add_argument(
        "--test-paths",
        nargs="*",
        default=["tests/test_tradex_experiment_family_api.py", "tests/integration/test_tradex_research_integration.py"],
    )

    compare_parser = sub.add_parser("compare-session", help="Run a TRADEX research session and write artifacts.")
    compare_parser.add_argument("--session-id", required=True)
    compare_parser.add_argument("--random-seed", type=int, required=True)
    compare_parser.add_argument("--universe-size", type=int, default=30)
    compare_parser.add_argument("--max-candidates-per-family", type=int, default=2)
    compare_parser.add_argument(
        "--ret20-source-mode",
        choices=["precomputed", "derived_from_daily_bars"],
        default="precomputed",
    )
    compare_parser.add_argument("--session-scope-id", default=None)

    artifact_parser = sub.add_parser("artifact-verify", help="Validate the session artifacts written by a research run.")
    artifact_parser.add_argument("--session-id", required=True)

    rollup_parser = sub.add_parser("rollup-verify", help="Validate the rollup artifacts written by research runs.")
    rollup_parser.add_argument("--session-id", required=False)

    env_parser = sub.add_parser("environment-eval-smoke", help="Run a small environment smoke check and validate the manifest contract.")
    env_parser.add_argument("--session-id", default="tradex-environment-smoke")

    smoke_parser = sub.add_parser("image-rerank-smoke", help="Verify TRADEX image rerank CLI startup paths.")
    smoke_parser.add_argument("--as-of-date", default=date.today().isoformat())

    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    if args.cmd == "contract-tests":
        return _run_pytest([args.test_path])

    if args.cmd == "integration-tests":
        return _run_pytest(list(args.test_paths))

    if args.cmd == "compare-session":
        payload = runner.run_tradex_research_session(
            session_id=str(args.session_id),
            random_seed=int(args.random_seed),
            universe_size=int(args.universe_size),
            max_candidates_per_family=int(args.max_candidates_per_family),
            session_scope_id=args.session_scope_id,
            ret20_source_mode=str(args.ret20_source_mode),
        )
        manifest_path = run_manifest_file(args.session_id)
        manifest = read_json(manifest_path)
        contracts.validate_run_manifest(manifest)
        _print_json(
            {
                "status": payload.get("status"),
                "session_id": args.session_id,
                "manifest_path": str(manifest_path),
                "session_state_path": str(runner._session_state_file(args.session_id)),
                "compare_path": str(runner._session_compare_file(args.session_id)),
                "family_leaderboard_path": str(runner._session_family_leaderboard_file(args.session_id)),
            }
        )
        return 0

    if args.cmd == "artifact-verify":
        _print_json(_validate_session_outputs(str(args.session_id)))
        return 0

    if args.cmd == "rollup-verify":
        payload: dict[str, Any] = {
            "session_rollup_path": str(runner._session_leaderboard_rollup_file()),
            "scope_rollup_path": str(runner._scope_stability_rollup_file()),
        }
        session_rollup = read_json(Path(payload["session_rollup_path"]))
        if session_rollup:
            contracts.validate_session_rollup_artifact(session_rollup)
            payload["session_rollup_schema_version"] = session_rollup.get("schema_version")
        scope_rollup = read_json(Path(payload["scope_rollup_path"]))
        if scope_rollup:
            contracts.validate_scope_rollup_artifact(scope_rollup)
            payload["scope_rollup_schema_version"] = scope_rollup.get("schema_version")
        _print_json(payload)
        return 0

    if args.cmd == "environment-eval-smoke":
        smoke = data_smoke.collect_tradex_data_smoke()
        if int(smoke.get("confirmed_universe_count") or 0) <= 0:
            raise RuntimeError("confirmed universe is empty")
        manifest = contracts.build_run_manifest(
            session_id=str(args.session_id),
            seed=7,
            random_seed=7,
            input_artifacts=[{"name": "data_smoke", "path": "internal"}],
            asof=date.today().isoformat(),
            config={"mode": "environment-smoke"},
            universe=["SMOKE"],
            period={"start_date": date.today().isoformat(), "end_date": date.today().isoformat()},
            horizon="20d",
            artifact_detail_level=contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
            fallback_status=contracts.TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
            cost_model=contracts.TRADEX_DEFAULT_COST_MODEL,
        )
        contracts.validate_run_manifest(manifest)
        _print_json({"smoke": smoke, "manifest": manifest})
        return 0

    if args.cmd == "image-rerank-smoke":
        commands = [
            [sys.executable, "-m", "external_analysis", "image-rerank-run", "--help"],
            [sys.executable, "-m", "external_analysis", "image-rerank-research-run", "--help"],
            [sys.executable, "-m", "external_analysis", "image-rerank-disposition-run", "--help"],
        ]
        for command in commands:
            rc = _run_subprocess(command)
            if rc != 0:
                return rc
        _print_json({"status": "ok", "checked_commands": ["image-rerank-run", "image-rerank-research-run", "image-rerank-disposition-run"]})
        return 0

    raise RuntimeError(f"unknown command: {args.cmd}")


if __name__ == "__main__":
    raise SystemExit(main())
