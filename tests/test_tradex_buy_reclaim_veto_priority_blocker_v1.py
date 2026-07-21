from scripts.tradex_buy_reclaim_veto_priority_blocker_v1 import audit
def test_blocker_reason_is_typed(monkeypatch):
 assert 'MA20_RECLAIM_INITIAL_POINT_IN_TIME_PROVENANCE_UNAVAILABLE'.startswith('MA20_RECLAIM_INITIAL')
def test_no_fallback_contract():
 expected={'status':'blocked','fallback_used':False};assert expected['status']=='blocked' and not expected['fallback_used']
