import json
from pathlib import Path
from scripts.tradex_prebreakout_purity_priority_blocker_v1 import sha
def test_source_formula_lacks_raw_component_transforms():
 p=Path(r'C:\work\meemee-screener\artifacts\research_inventory\tradex_prebreakout_actionability_compare_v2.json');d=json.loads(p.read_text());f=d['challenge_definition']['formula'];assert 'compression_tightness'not in f and 'launch_core'in f
def test_source_hash_is_stable():
 p=Path(r'C:\work\meemee-screener\artifacts\research_inventory\tradex_prebreakout_actionability_compare_v2.json');assert sha(p)==sha(p)
