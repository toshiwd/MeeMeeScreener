import duckdb
con = duckdb.connect(r'C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb')
print('SHORT:', con.execute("SELECT COUNT(*) FROM signal_decision_daily WHERE upper(side)='SHORT'").fetchall())
print('short rows:', con.execute("SELECT code, score_snapshot_json, forward_return_20, max_favorable_30, max_adverse_30 FROM signal_decision_daily WHERE upper(side)='SHORT' LIMIT 5").fetchall())
