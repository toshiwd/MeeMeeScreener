from __future__ import annotations

import numpy as np
import pandas as pd

from scripts import tradex_contraction_dryup_breakout_sequence_v1 as m


def _observables() -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=8).strftime("%Y%m%d").astype(int)
    return pd.DataFrame({"signal_ymd":dates,"code":"1000","o":100.0,"h":101.0,"l":99.0,"c":100.0,"v":1000,
      "prior10_width":[.10,.05,.05,.07,.07,.07,.07,.07],"vol_ratio5_20":[1,.9,.7,.7,.9,.9,.9,.9],"prior20_high":101.0,
      "range_pct":[.01,.01,.01,.01,.01,.01,.03,.01],"prior5_range_median":.02,"dry_hit":[False,False,True,True,False,False,False,False],
      "breakout_hit":[False,False,False,False,False,False,True,False]})


def test_sequence_records_ordered_first_stage_dates() -> None:
    x=_observables();x.loc[6,"c"]=102.0
    out=m.sequence_variant(x,"width_6pct").sort_values("signal_ymd")
    hit=out[out.sequence_hit].iloc[0]
    assert int(hit.contraction_first_ymd)==int(x.signal_ymd.iloc[1])
    assert int(hit.dry_first_ymd)==int(x.signal_ymd.iloc[2])
    assert int(hit.breakout_first_ymd)==int(x.signal_ymd.iloc[6])


def test_wrong_order_does_not_create_sequence() -> None:
    x=_observables();x.loc[0,"breakout_hit"]=True;x.loc[6,"breakout_hit"]=False
    out=m.sequence_variant(x,"width_6pct")
    assert not out.sequence_hit.any()


def test_all_rows_ranked_and_variants_only_change_width() -> None:
    x=pd.concat([_observables(),_observables().assign(code="2000")],ignore_index=True)
    out=m.sequence_variant(x,"width_4pct")
    assert out.groupby("signal_ymd").size().eq(2).all()
    assert out.groupby("signal_ymd")["rank"].max().eq(2).all()
    assert list(m.WIDTH_VARIANTS.values())==[.04,.06,.08]
