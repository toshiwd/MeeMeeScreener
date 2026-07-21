import pandas as pd
from scripts.tradex_meemee_baseline_ledger_diff_v1 import diagnose
def test_common_fields_and_one_sided_rows():
 a=pd.DataFrame({'code':[1,2],'date':[10,20],'next_entry_date':[11,21],'next_open_return':[.1,.2],'fill_price':[100,200],'exit_date':[12,22],'pnl_yen':[10,20],'entry_price':[99,199],'shares':[1,1],'invested_yen':[100,200]})
 b=a.iloc[:1].assign(signal_year=2026);d,r=diagnose(a,b);assert d['row_counts']['both']==1 and d['row_counts']['legacy_only']==1 and d['field_differences_on_common_rows']['pnl_yen']['changed_rows']==0
def test_contract_judgment_is_typed():
 a=pd.DataFrame(columns=['code','date','next_entry_date','next_open_return','fill_price','exit_date','pnl_yen','entry_price','shares','invested_yen']);b=a.assign(signal_year=pd.Series(dtype=int));d,_=diagnose(a,b);assert d['typed_judgment']['reason']=='LEGACY_MEE_MEE_RANK_ORDER_REVERSED_IN_PORTFOLIO_REPLAY'
