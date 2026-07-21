import pandas as pd

from scripts.tradex_short_rolling_permission_1y_compare_v1 import permission_at, route_b4


def _events(n=35):
    rows=[]
    for i in range(n):
        d=pd.Timestamp("2024-01-01")+pd.Timedelta(days=i*9)
        rows.append({"code":str(5000+i),"signal_date":d,"entry_date":d,"ret":.03 if i%2==0 else -.01,"rule":"r","outcome_known_date":d+pd.Timedelta(days=1)})
    return pd.DataFrame(rows)


def test_one_year_permission_allows_qualified_family():
    result=permission_at(_events(35),pd.Timestamp("2025-01-01"))
    assert result["known_n"] == 35
    assert result["permission"] is True


def test_one_year_window_explicitly_rejects_too_few_recent_events():
    events=_events(35); result=permission_at(events,pd.Timestamp("2025-07-01"))
    assert result["known_n"] < 30
    assert result["permission_status"] == "insufficient_history"


def test_future_return_does_not_change_pre_outcome_permission_or_route():
    events=_events(40); future=events.signal_date.max()+pd.Timedelta(days=9)
    row={"code":"9999","signal_date":future,"entry_date":future,"ret":-.08,"rule":"r","outcome_known_date":future+pd.Timedelta(days=20)}
    left=pd.concat([events,pd.DataFrame([row])],ignore_index=True); right=left.copy(); right.loc[right.code=="9999","ret"]=.5
    la,lr=route_b4(left); ra,rr=route_b4(right)
    pd.testing.assert_frame_equal(la[la.signal_date<=row["outcome_known_date"]].reset_index(drop=True),ra[ra.signal_date<=row["outcome_known_date"]].reset_index(drop=True))
    pd.testing.assert_frame_equal(lr[lr.signal_date<future].reset_index(drop=True),rr[rr.signal_date<future].reset_index(drop=True))
