import pandas as pd

from scripts.tradex_short_rolling_permission_compare_v1 import permission_at, route_b3


def _events(n=35, ret=.02):
    rows=[]
    for i in range(n):
        d=pd.Timestamp("2020-01-01")+pd.Timedelta(days=i*10)
        rows.append({"code":str(4000+i),"signal_date":d,"entry_date":d,"ret":ret,"rule":"r","outcome_known_date":d+pd.Timedelta(days=1)})
    return pd.DataFrame(rows)


def test_permission_explicitly_denies_insufficient_history():
    result=permission_at(_events(29),pd.Timestamp("2022-01-01"))
    assert result["permission"] is False
    assert result["permission_status"] == "insufficient_history"


def test_permission_uses_only_strictly_prior_known_results():
    events=_events(30)
    events.loc[events.index % 2 == 1, "ret"] = -.01
    as_of=pd.Timestamp("2022-01-01")
    same={"code":"9999","signal_date":as_of,"entry_date":as_of,"ret":-10.0,"rule":"r","outcome_known_date":as_of}
    result=permission_at(pd.concat([events,pd.DataFrame([same])],ignore_index=True),as_of)
    assert result["known_n"] == 30
    assert result["permission"] is True


def test_future_return_change_does_not_change_prior_permission_or_route():
    events=_events(40)
    future=events.signal_date.max()+pd.Timedelta(days=10)
    row={"code":"9999","signal_date":future,"entry_date":future,"ret":-.08,"rule":"r","outcome_known_date":future+pd.Timedelta(days=20)}
    left=pd.concat([events,pd.DataFrame([row])],ignore_index=True); right=left.copy(); right.loc[right.code=="9999","ret"]=.5
    la,lr=route_b3(left); ra,rr=route_b3(right)
    pd.testing.assert_frame_equal(la[la.signal_date<=row["outcome_known_date"]].reset_index(drop=True),ra[ra.signal_date<=row["outcome_known_date"]].reset_index(drop=True))
    pd.testing.assert_frame_equal(lr[lr.signal_date<future].reset_index(drop=True),rr[rr.signal_date<future].reset_index(drop=True))
