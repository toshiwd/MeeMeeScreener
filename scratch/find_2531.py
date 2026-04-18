import duckdb

# Check all DBs for 2531 data
dbs = [
    'G:/Tradex/db/stocks.duckdb',
    'G:/Tradex/db/result.duckdb',
    'G:/Tradex/db/export.duckdb',
    'G:/Tradex/db/ops.duckdb',
    'G:/Tradex/db/label.duckdb',
]

for db in dbs:
    print(f"\n=== {db} ===")
    try:
        con = duckdb.connect(db, read_only=True)
        tables = con.execute('SHOW TABLES').fetchall()
        for t in tables:
            name = t[0]
            try:
                # Check if table has 'code' column with 2531
                cols = con.execute(f'DESCRIBE "{name}"').fetchall()
                col_names = [c[0] for c in cols]
                if 'code' in col_names:
                    cnt = con.execute(f'SELECT COUNT(*) FROM "{name}" WHERE code=\'2531\'').fetchone()[0]
                    if cnt > 0:
                        print(f'  {name}: {cnt} rows for code=2531')
                        sample = con.execute(f'SELECT * FROM "{name}" WHERE code=\'2531\' LIMIT 3').df()
                        print(f'    columns: {list(sample.columns)}')
                        print(sample.to_string())
            except Exception as e:
                pass
        con.close()
    except Exception as e:
        print(f'  ERROR: {e}')
