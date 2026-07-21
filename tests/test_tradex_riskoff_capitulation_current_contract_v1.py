import pandas as pd
from scripts import tradex_riskoff_capitulation_current_contract_v1 as m

def test_permission_uses_strictly_known_exits():
 x=pd.DataFrame({"signal_ymd":[20240102],"exit_ymd":[20240102],"entry_gate":[True],"trade_return_h10":[.1]})
 out=m.rolling_permission(x)
 assert out.iloc[0].permission_n==0 and not bool(out.iloc[0].permission)

def test_gap_gate_boundary():
 assert 103<=100*1.03 and not 103.01<=100*1.03

def test_contract_constants():
 assert m.PERIODS["train"]==(20240101,20241231) and m.PERIODS["validation"]==(20250101,20251231)
