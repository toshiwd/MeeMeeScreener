import duckdb

con = duckdb.connect('G:/Tradex/db/stocks.duckdb', read_only=True)
tables = con.execute('SHOW TABLES').fetchall()
for t in tables:
    name = t[0]
    try:
        cnt = con.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
        if cnt > 0:
            print(f'{name}: {cnt} rows')
    except Exception as e:
        print(f'{name}: ERROR {e}')
