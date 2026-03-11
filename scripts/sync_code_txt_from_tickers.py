from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import tempfile
from datetime import datetime
from pathlib import Path

import duckdb


CODE_PATTERN = re.compile(r"^\d{4}[A-Z]?$")


def _resolve_db_path(cli_value: str | None) -> Path:
    if cli_value:
        path = Path(cli_value).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"DB not found: {path}")
        return path
    env = os.getenv("STOCKS_DB_PATH")
    if env:
        path = Path(env).expanduser().resolve()
        if path.exists():
            return path
    default = Path(os.getenv("LOCALAPPDATA", str(Path.home()))) / "MeeMeeScreener" / "data" / "stocks.duckdb"
    if default.exists():
        return default
    raise FileNotFoundError("Could not resolve DB path. Pass --db-path or set STOCKS_DB_PATH.")


def _resolve_code_txt_path(cli_value: str | None) -> Path:
    if cli_value:
        return Path(cli_value).expanduser().resolve()
    local = Path(os.getenv("LOCALAPPDATA", str(Path.home()))) / "MeeMeeScreener" / "data" / "code.txt"
    return local.resolve()


def _read_lines(path: Path) -> list[str]:
    if not path.exists():
        return []
    try:
        return path.read_text(encoding="utf-8").splitlines()
    except UnicodeDecodeError:
        return path.read_text(encoding="cp932", errors="ignore").splitlines()


def _normalize_code(raw: str) -> str | None:
    txt = (raw or "").strip().upper().replace(" ", "")
    if txt.endswith(".T"):
        txt = txt[:-2]
    if CODE_PATTERN.fullmatch(txt):
        return txt
    return None


def _parse_code_txt(lines: list[str]) -> tuple[set[str], list[str]]:
    codes: set[str] = set()
    others: list[str] = []
    for line in lines:
        stripped = (line or "").strip()
        if not stripped:
            continue
        normalized = _normalize_code(stripped)
        if normalized is not None:
            codes.add(normalized)
        else:
            others.append(line.rstrip("\r\n"))
    return codes, others


def _load_ticker_codes(db_path: Path) -> set[str]:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = con.execute("SELECT DISTINCT CAST(code AS VARCHAR) FROM tickers").fetchall()
        out: set[str] = set()
        for row in rows:
            if not row or row[0] is None:
                continue
            normalized = _normalize_code(str(row[0]))
            if normalized is not None:
                out.add(normalized)
        return out
    finally:
        con.close()


def _write_code_txt(path: Path, codes: set[str], others: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    sorted_codes = sorted(codes)
    lines = list(sorted_codes)
    if others:
        lines.append("")
        lines.append("# --- Preserved Non-Code Lines ---")
        lines.extend(others)
    content = "\n".join(lines) + "\n"

    if path.exists():
        backup = path.with_suffix(path.suffix + f".bak.{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        shutil.copy2(path, backup)

    fd, tmp_path = tempfile.mkstemp(dir=str(path.parent), prefix="code_sync_", suffix=".tmp", text=True)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(content)
        shutil.move(tmp_path, path)
    except Exception:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync code.txt with tickers table from DuckDB.")
    parser.add_argument("--db-path", default="", help="Path to stocks.duckdb")
    parser.add_argument("--code-txt", default="", help="Path to code.txt")
    parser.add_argument("--apply", action="store_true", help="Write synced code.txt")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Replace with DB tickers only. Default keeps existing extra codes.",
    )
    parser.add_argument(
        "--output",
        default="tmp/code_txt_sync_report.json",
        help="Output JSON report path",
    )
    args = parser.parse_args()

    db_path = _resolve_db_path(args.db_path or None)
    code_txt_path = _resolve_code_txt_path(args.code_txt or None)

    db_codes = _load_ticker_codes(db_path)
    lines = _read_lines(code_txt_path)
    current_codes, others = _parse_code_txt(lines)

    missing_in_code = sorted(db_codes - current_codes)
    extra_in_code = sorted(current_codes - db_codes)
    if args.strict:
        target_codes = set(db_codes)
    else:
        target_codes = set(db_codes | current_codes)

    changed = target_codes != current_codes
    if args.apply and changed:
        _write_code_txt(code_txt_path, target_codes, others)

    report = {
        "db_path": str(db_path),
        "code_txt_path": str(code_txt_path),
        "db_codes": len(db_codes),
        "current_codes": len(current_codes),
        "target_codes": len(target_codes),
        "missing_in_code": len(missing_in_code),
        "extra_in_code": len(extra_in_code),
        "missing_samples": missing_in_code[:30],
        "extra_samples": extra_in_code[:30],
        "strict": bool(args.strict),
        "changed": bool(changed),
        "applied": bool(args.apply and changed),
    }

    out = Path(args.output).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False))
    print(str(out))


if __name__ == "__main__":
    main()
