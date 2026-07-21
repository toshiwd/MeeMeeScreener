from pathlib import Path
import argparse
from scripts.tradex_strict_pit_router_entry_eligibility_v1 import generate
AXIS_ID='tradex_family_hit_only_rolling_gate_v1';OUT=Path(r'G:\Tradex\tradex_family_hit_only_rolling_gate_v1')
def main():
 p=argparse.ArgumentParser();p.add_argument('--db',type=Path,required=True);p.add_argument('--out',type=Path,default=OUT);a=p.parse_args();print(generate(a.db,a.out,hit_only=True,axis_id=AXIS_ID))
if __name__=='__main__':main()
