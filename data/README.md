# Data directory policy

- data/ contains example files only.
- Real broker CSV/SQLite/duckdb files must be kept in data_store/ (gitignored).
- If you need a fresh SQLite/duckdb, run the setup/init scripts instead of committing data files.
