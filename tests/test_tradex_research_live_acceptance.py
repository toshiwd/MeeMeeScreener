from __future__ import annotations

import json
import os
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from app.backend.services import tradex_research_environment_readiness as readiness_service
from app.backend.services import tradex_research_trader_foundation as foundation_service
from app.backend.tools import tradex_research_os_runner as os_runner
from shared import tradex_storage


def _live_verify_enabled() -> bool:
    return os.getenv("MEEMEE_ENABLE_TRADEX_LIVE_VERIFY", "").strip() == "1"


def _live_llm_verify_enabled() -> bool:
    return os.getenv("MEEMEE_ENABLE_TRADEX_LIVE_LLM_VERIFY", "").strip() == "1"


def _env(name: str) -> str:
    return os.getenv(name, "").strip()


def _is_test_temp_path(path: Path) -> bool:
    parts = {part.lower() for part in path.parts}
    return ".tmp-tests" in parts or ".tmp-pytest-fixtures" in parts or ".tmp-pytest-root" in parts


def _live_runtime_root() -> Path:
    explicit = _env("TRADEX_LIVE_RUNTIME_ROOT") or _env("MEEMEE_TRADEX_ROOT")
    if explicit:
        candidate = Path(explicit).expanduser().resolve(strict=False)
        if candidate.exists() and not _is_test_temp_path(candidate):
            return candidate
    return Path(r"G:\Tradex")


def _prepared_db_candidates() -> list[Path]:
    candidates: list[Path] = []
    for raw in (
        _env("TRADEX_LIVE_STOCKS_DB_PATH"),
        _env("STOCKS_DB_PATH"),
        r"G:\Tradex\db\stocks.duckdb",
        str(Path.home() / "AppData" / "Local" / "MeeMeeScreener-dev" / "data" / "stocks.duckdb"),
        str(Path.home() / "AppData" / "Local" / "MeeMeeScreener" / "data" / "stocks.duckdb"),
    ):
        if not raw:
            continue
        candidate = Path(raw).expanduser().resolve(strict=False)
        if candidate not in candidates:
            candidates.append(candidate)
    return candidates


def _has_required_live_tables(db_path: Path) -> bool:
    if not db_path.exists():
        return False
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        tables = {
            str(row[0]).strip()
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).fetchall()
            if str(row[0]).strip()
        }
        return {"daily_bars", "market_regime_daily"} <= tables
    finally:
        conn.close()


def _live_stocks_db_path() -> Path:
    explicit = _env("TRADEX_LIVE_STOCKS_DB_PATH") or _env("STOCKS_DB_PATH")
    if explicit:
        return Path(explicit).expanduser().resolve(strict=False)
    for candidate in _prepared_db_candidates():
        if _has_required_live_tables(candidate):
            return candidate
    raise RuntimeError("prepared live stocks DuckDB not found; set STOCKS_DB_PATH or TRADEX_LIVE_STOCKS_DB_PATH")


def _as_yyyymmdd(value: int) -> str:
    if value >= 1_000_000_000:
        return datetime.fromtimestamp(value, tz=UTC).strftime("%Y%m%d")
    return str(int(value))


def _discover_live_target(db_path: Path) -> tuple[str, str]:
    explicit_code = _env("TRADEX_LIVE_HYPOTHESIS_CODE")
    explicit_date = _env("TRADEX_LIVE_HYPOTHESIS_DATE")
    if explicit_code and explicit_date:
        return explicit_code, explicit_date

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        row = conn.execute(
            """
            WITH per_code AS (
                SELECT
                    code,
                    MAX(date) AS max_date,
                    COUNT(*) AS bar_count
                FROM daily_bars
                GROUP BY code
            )
            SELECT code, max_date
            FROM per_code
            WHERE bar_count >= 260
            ORDER BY max_date DESC, bar_count DESC, code ASC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()
    if not row or not row[0] or row[1] is None:
        raise RuntimeError("unable to auto-discover live hypothesis code/as-of-date from daily_bars")
    return str(row[0]).strip(), _as_yyyymmdd(int(row[1]))


def _live_hypothesis_path(runtime_root: Path, suffix: str) -> Path:
    scratch_temp = runtime_root / "scratch" / "temp" / "live_acceptance"
    scratch_temp.mkdir(parents=True, exist_ok=True)
    return scratch_temp / f"{suffix}-{uuid.uuid4().hex[:10]}.json"


def _live_hypothesis_payload(*, adapter_ids: list[str] | None = None, primary_adapter_id: str | None = None) -> dict[str, object]:
    db_path = _live_stocks_db_path()
    code, as_of_date = _discover_live_target(db_path)
    active_adapter_ids = list(adapter_ids or ["numeric_baseline_v1"])
    active_primary_adapter = str(primary_adapter_id or active_adapter_ids[0]).strip()
    default_session_id = "tradex-live-llm" if active_primary_adapter == "structured_reasoner_v1" else "tradex-live-num"
    return {
        "schema_version": "tradex_research_os_hypothesis_v1",
        "hypothesis_id": f"live-trader-foundation-{code}-{as_of_date}-{active_primary_adapter}",
        "hypothesis_type": "candidate-family-comparison",
        "changed_axis": "regime_adaptation",
        "fixed_contracts": ["same_condition", "authoritative_compare", "single_session"],
        "expected_effect": "live unshimmed trader foundation acceptance",
        "metrics_to_watch": ["changed_top5_members_count", "changed_rank_count", "hold_end_return_20d"],
        "acceptance_gate": {"mode": "authoritative", "criteria": ["artifact_generation_complete"]},
        "rejection_gate": {"mode": "authoritative", "criteria": ["preflight_failed", "missing_family_compare"]},
        "notes": "live acceptance verify",
        "status": "ready",
        "target_method_family": _env("TRADEX_LIVE_TARGET_METHOD_FAMILY") or "regime-aware",
        "execution": {
            "runner": "tradex_research_session",
            "session_id": _env("TRADEX_LIVE_SESSION_ID") or default_session_id,
            "random_seed": int(_env("TRADEX_LIVE_RANDOM_SEED") or "7"),
            "session_scope_id": _env("TRADEX_LIVE_SESSION_SCOPE_ID") or "scope-a",
            "universe_size": int(_env("TRADEX_LIVE_UNIVERSE_SIZE") or "30"),
            "max_candidates_per_family": int(_env("TRADEX_LIVE_MAX_CANDIDATES_PER_FAMILY") or "2"),
            "ret20_source_mode": _env("TRADEX_LIVE_RET20_SOURCE_MODE") or "derived_from_daily_bars",
        },
        "strategy_target": {
            "code": code,
            "as_of_date": int(as_of_date),
            "side": _env("TRADEX_LIVE_SIDE") or "long",
            "judgement_type": "close_based_daily_buy_v1",
        },
        "strategy_judgement": {
            "primary_adapter_id": active_primary_adapter,
            "adapter_ids": active_adapter_ids,
            "observation_lookback_bars": int(_env("TRADEX_LIVE_OBSERVATION_LOOKBACK_BARS") or "120"),
            "teacher_horizon_bars": int(_env("TRADEX_LIVE_TEACHER_HORIZON_BARS") or "20"),
        },
    }


def _run_live_hypothesis(*, hypothesis_payload: dict[str, object], runtime_root: Path) -> dict[str, object]:
    hypothesis_path = _live_hypothesis_path(runtime_root, "tradex-live-hypothesis")
    hypothesis_path.write_text(json.dumps(hypothesis_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return os_runner.run_hypothesis(hypothesis_path)


def test_live_stocks_db_path_prefers_explicit_env_without_probing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    explicit = (tmp_path / "prepared.duckdb").resolve()
    monkeypatch.setenv("STOCKS_DB_PATH", str(explicit))
    monkeypatch.delenv("TRADEX_LIVE_STOCKS_DB_PATH", raising=False)
    monkeypatch.setattr(sys.modules[__name__], "_has_required_live_tables", lambda path: (_ for _ in ()).throw(AssertionError(f"unexpected probe: {path}")))
    assert _live_stocks_db_path() == explicit


def test_live_runtime_root_ignores_test_temp_roots(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fake_test_root = (tmp_path / ".tmp-tests" / "tradex-live").resolve()
    fake_test_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(fake_test_root))
    monkeypatch.delenv("TRADEX_LIVE_RUNTIME_ROOT", raising=False)
    assert _live_runtime_root() == Path(r"G:\Tradex")


def test_discover_live_target_prefers_explicit_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRADEX_LIVE_HYPOTHESIS_CODE", "6963")
    monkeypatch.setenv("TRADEX_LIVE_HYPOTHESIS_DATE", "20260403")
    monkeypatch.setattr(duckdb, "connect", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("unexpected duckdb connect")))
    code, as_of_date = _discover_live_target(Path(r"C:\unused.duckdb"))
    assert code == "6963"
    assert as_of_date == "20260403"


@pytest.mark.live_acceptance
@pytest.mark.skipif(not _live_verify_enabled(), reason="set MEEMEE_ENABLE_TRADEX_LIVE_VERIFY=1 to run live acceptance")
def test_live_unshimmed_single_session_acceptance(monkeypatch: pytest.MonkeyPatch) -> None:
    runtime_root = _live_runtime_root()
    stocks_db_path = _live_stocks_db_path()
    assert runtime_root.exists(), f"runtime root missing: {runtime_root}"

    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(runtime_root))
    monkeypatch.setenv("STOCKS_DB_PATH", str(stocks_db_path))
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "0")
    tradex_storage.resolve_tradex_root.cache_clear()

    readiness = readiness_service.evaluate_environment_readiness()
    assert readiness["ready"] is True, json.dumps(readiness, ensure_ascii=False, indent=2)

    result = _run_live_hypothesis(
        hypothesis_payload=_live_hypothesis_payload(),
        runtime_root=runtime_root,
    )
    assert result["status"] == "ok"
    assert Path(result["preflight_report_path"]).exists()
    assert Path(result["experiment_manifest_path"]).exists()
    assert Path(result["observation_snapshot_path"]).exists()
    assert Path(result["strategy_judgement_path"]).exists()
    assert Path(result["teacher_evaluation_row_path"]).exists()
    assert Path(result["judge_input_path"]).exists()
    assert Path(result["judge_decision_path"]).exists()
    assert Path(result["authoritative_decision_path"]).exists()
    assert Path(result["research_memory_path"]).exists()
    assert Path(result["family_compare_path"]).name == "compare.json"
    assert "research_families" in result["family_compare_path"]


@pytest.mark.live_acceptance
@pytest.mark.skipif(
    not (_live_verify_enabled() and _live_llm_verify_enabled()),
    reason="set MEEMEE_ENABLE_TRADEX_LIVE_VERIFY=1 and MEEMEE_ENABLE_TRADEX_LIVE_LLM_VERIFY=1 to run live LLM acceptance",
)
def test_live_unshimmed_llm_adapter_acceptance(monkeypatch: pytest.MonkeyPatch) -> None:
    runtime_root = _live_runtime_root()
    stocks_db_path = _live_stocks_db_path()
    assert runtime_root.exists(), f"runtime root missing: {runtime_root}"

    missing = [
        env_name
        for env_name in (
            foundation_service.TRADEX_TRADER_LLM_ENDPOINT_ENV,
            foundation_service.TRADEX_TRADER_LLM_MODEL_ENV,
            foundation_service.TRADEX_TRADER_LLM_API_KEY_ENV,
        )
        if not _env(env_name)
    ]
    assert not missing, f"missing live LLM env: {', '.join(missing)}"

    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(runtime_root))
    monkeypatch.setenv("STOCKS_DB_PATH", str(stocks_db_path))
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "0")
    tradex_storage.resolve_tradex_root.cache_clear()

    readiness = readiness_service.evaluate_environment_readiness()
    assert readiness["ready"] is True, json.dumps(readiness, ensure_ascii=False, indent=2)

    result = _run_live_hypothesis(
        hypothesis_payload=_live_hypothesis_payload(
            adapter_ids=["numeric_baseline_v1", "structured_reasoner_v1"],
            primary_adapter_id="structured_reasoner_v1",
        ),
        runtime_root=runtime_root,
    )
    assert result["status"] == "ok"
    strategy_judgement_path = Path(result["strategy_judgement_path"])
    assert strategy_judgement_path.exists()
    payload = json.loads(strategy_judgement_path.read_text(encoding="utf-8"))
    assert payload["primary_adapter_id"] == "structured_reasoner_v1"
    assert [row["adapter_id"] for row in payload["adapter_outputs"]] == [
        "numeric_baseline_v1",
        "structured_reasoner_v1",
    ]
