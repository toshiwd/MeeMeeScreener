import os
import re
import sys
import tempfile
import zipfile

REQUIRED_PATHS = [
    "_internal/app/main.py",
    "_internal/app/backend/main.py",
    "_internal/app/backend/api/__init__.py",
    "_internal/app/backend/api/routers/__init__.py",
    "_internal/app/backend/api/routers/market.py",
    "_internal/app/backend/trade_parser.py",
    "_internal/app/backend/static/index.html",
    "_internal/app/desktop/launcher.py",
]

SIMILARITY_PATH_CANDIDATES = [
    "_internal/app/backend/similarity.py",
    "_internal/app/backend/domain/similarity.py",
]

SIM_MUST_CONTAIN_ANY = [
    "MEEMEE_DATA_STORE",
    "MEEMEE_DATA_DIR",
]

SIM_MUST_CONTAIN_ALL = [
    "SHORT_TERM_WINDOW = 24",
]

SIM_MUST_NOT_CONTAIN = [
    ".iloc[query_idx]",
    "SHORT_TERM_WINDOW = 12",
]


def _fail(msg: str) -> int:
    print(msg)
    return 1


def _collect_dir_names(root: str) -> set[str]:
    names: set[str] = set()
    for base, _, files in os.walk(root):
        for name in files:
            full = os.path.join(base, name)
            rel = os.path.relpath(full, root).replace("\\", "/")
            names.add(rel)
    return names


def _read_text_from_dir(root: str, rel_path: str) -> str:
    full = os.path.join(root, rel_path.replace("/", os.sep))
    with open(full, "r", encoding="utf-8", errors="replace") as handle:
        return handle.read()


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        return _fail("Usage: python scripts/verify_portable_zip.py <portable.zip|onedir>")

    target = argv[1]

    if os.path.isdir(target):
        root = target
        names = _collect_dir_names(root)
        read_text = lambda p: _read_text_from_dir(root, p)
        read_binary = lambda p: open(os.path.join(root, p.replace("/", os.sep)), "rb").read()
        in_zip = False
    else:
        zip_path = target
        zf = zipfile.ZipFile(zip_path, "r")
        names = set(zf.namelist())
        read_text = lambda p: zf.read(p).decode("utf-8", errors="replace")
        read_binary = lambda p: zf.read(p)
        in_zip = True

    try:
        missing = [p for p in REQUIRED_PATHS if p not in names]
        if missing:
            print("NG: required paths missing")
            for p in missing:
                print(f"  - {p}")
            return 1

        # Frontend static assets gate (index.html + referenced assets)
        index_path = "_internal/app/backend/static/index.html"
        index_text = read_text(index_path)
        asset_refs = re.findall(r"/(assets/[^\"'\s>]+)", index_text)
        if not asset_refs:
            print("NG: no static asset references found in index.html")
            print(f"  - {index_path}")
            return 1
        missing_assets = []
        for ref in asset_refs:
            full_path = f"_internal/app/backend/static/{ref}"
            if full_path not in names:
                missing_assets.append(full_path)
        if missing_assets:
            print("NG: static assets missing")
            for p in missing_assets:
                print(f"  - {p}")
            return 1
        has_js = any(ref.endswith(".js") for ref in asset_refs)
        has_css = any(ref.endswith(".css") for ref in asset_refs)
        if not (has_js and has_css):
            print("NG: static assets missing js/css references")
            print(f"  js={has_js} css={has_css}")
            return 1

        # Launcher logging markers gate (backend.log + logs dir + MEEMEE_DATA_DIR)
        launcher_path = "_internal/app/desktop/launcher.py"
        launcher_text = read_text(launcher_path)
        required_markers = ["backend.log", "logs_dir", "MEEMEE_DATA_DIR"]
        missing_markers = [m for m in required_markers if m not in launcher_text]
        if missing_markers:
            print("NG: launcher logging markers missing")
            for m in missing_markers:
                print(f"  - {m}")
            return 1

        sim_path = None
        for p in SIMILARITY_PATH_CANDIDATES:
            if p in names:
                sim_path = p
                break

        if sim_path is None:
            print("NG: similarity.py not found")
            for p in SIMILARITY_PATH_CANDIDATES:
                print(f"  - {p}")
            return 1

        sim_text = read_text(sim_path)

        if not any(s in sim_text for s in SIM_MUST_CONTAIN_ANY):
            print("NG: similarity storage env priority missing")
            print(f"  file={sim_path}")
            return 1

        for s in SIM_MUST_CONTAIN_ALL:
            if s not in sim_text:
                print("NG: similarity expected string missing")
                print(f"  missing='{s}' file={sim_path}")
                return 1

        for s in SIM_MUST_NOT_CONTAIN:
            if s in sim_text:
                print("NG: similarity legacy marker found")
                print(f"  found='{s}' file={sim_path}")
                return 1

        # TradeParser import wiring
        positions_path = "_internal/app/backend/positions.py"
        import_positions_path = "_internal/app/backend/import_positions.py"
        if positions_path not in names:
            print("NG: positions.py missing")
            print(f"  missing='{positions_path}'")
            return 1
        if import_positions_path not in names:
            print("NG: import_positions.py missing")
            print(f"  missing='{import_positions_path}'")
            return 1
        positions_text = read_text(positions_path)
        if "from app.backend.trade_parser import TradeParser" not in positions_text:
            print("NG: positions.py missing TradeParser import")
            print(f"  file={positions_path}")
            return 1
        import_positions_text = read_text(import_positions_path)
        if "from app.backend.positions" not in import_positions_text:
            print("NG: import_positions.py no longer references app.backend.positions")
            print(f"  file={import_positions_path}")
            return 1

        # Verify industry_master table exists in bundled DuckDB
        db_path = "_internal/app/backend/stocks.duckdb"
        if db_path not in names:
            print("NG: bundled stocks.duckdb missing")
            print(f"  missing='{db_path}'")
            return 1

        try:
            import duckdb  # type: ignore
        except Exception:
            print("NG: duckdb module unavailable for gate check")
            return 1

        if in_zip:
            fd, temp_path = tempfile.mkstemp(suffix=".duckdb")
            os.close(fd)
            try:
                with open(temp_path, "wb") as dst:
                    dst.write(read_binary(db_path))
                with duckdb.connect(temp_path, read_only=True) as conn:
                    row = conn.execute(
                        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'industry_master'"
                    ).fetchone()
                    if not row or row[0] == 0:
                        print("NG: industry_master table missing in bundled DuckDB")
                        return 1
            finally:
                try:
                    os.remove(temp_path)
                except OSError:
                    pass
        else:
            full_db = os.path.join(root, db_path.replace("/", os.sep))
            with duckdb.connect(full_db, read_only=True) as conn:
                row = conn.execute(
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'industry_master'"
                ).fetchone()
                if not row or row[0] == 0:
                    print("NG: industry_master table missing in bundled DuckDB")
                    return 1
    finally:
        if "zf" in locals():
            zf.close()

    print("OK: portable gate passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
