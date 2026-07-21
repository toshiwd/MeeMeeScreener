from scripts import tradex_meemee_buy_exit_geometry_v1 as m
def test_predeclared_exit_variants():assert m.EXITS=={'A':(.05,.03,5),'B':(.06,.04,7),'C':(.08,.05,10)}
def test_current_is_c():assert m.EXITS['C']==(.08,.05,10)
def test_gap_through_long_return_sign():assert 94/100-1<-.05 and 109/100-1>.08
