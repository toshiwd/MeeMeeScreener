from __future__ import annotations

import pandas as pd

from scripts.tradex_high_zone_temporal_validation_v1 import _full_exposure, paired_month_bootstrap


def _frame():
    rows=[]
    for i in range(12):
        exposure=.5 if i%2 else 1.0; raw=.1 if i%3 else -.02
        row={"policy":"initial_expansion25","code":str(i),"signal_ymd":20240101+i,"exposure":exposure,"ret5":raw*exposure,"ret10":raw*exposure,"ret20":raw*exposure,"mae5":-.03*exposure,"mae10":-.03*exposure,"mae20":-.03*exposure}
        rows.append(row)
    return pd.DataFrame(rows)


def test_full_exposure_reverses_position_scaling():
    source=_frame(); full=_full_exposure(source)
    assert abs(full.iloc[1].ret10-.1)<1e-12
    assert full.iloc[1].exposure==1.0


def test_month_bootstrap_is_deterministic():
    champion=_frame();full=_full_exposure(champion)
    one=paired_month_bootstrap(champion,full,iterations=50,seed=1);two=paired_month_bootstrap(champion,full,iterations=50,seed=1)
    assert one==two
