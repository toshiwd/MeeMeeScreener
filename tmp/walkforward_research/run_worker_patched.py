import json
import os
import random
import sys
from collections import deque
from dataclasses import asdict
from pathlib import Path

ROOT = Path(r'C:\work\meemee-screener')
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault('MEEMEE_DATA_DIR', r'C:\work\meemee-screener\tmp\research_data')

from app.backend.tools import walkforward_research_worker as worker

_RECENT_KEYS = deque(maxlen=24)


def _config_key(cfg) -> str:
    return json.dumps(asdict(cfg), ensure_ascii=False, sort_keys=True)


def _candidate_weight(cfg):
    long_setups = {str(v) for v in (cfg.allowed_long_setups or ()) if str(v).strip()}
    if 'long_breakout_p2' in long_setups:
        if (
            bool(cfg.use_regime_filter)
            and bool(cfg.require_decision_for_long)
            and bool(cfg.require_ma_bull_stack_long)
            and float(cfg.min_volume_ratio_long or 0.0) >= 1.0
            and cfg.max_atr_pct_long is not None
            and float(cfg.max_atr_pct_long) <= 0.08
        ):
            return 6.0
        if bool(cfg.use_regime_filter) and bool(cfg.require_decision_for_long):
            return 3.0
        if int(cfg.max_positions) > 1:
            return 0.75
        if not bool(cfg.use_regime_filter):
            return 0.5
        return 1.5
    if 'long_reversal_p1' in long_setups:
        return 0.35
    if 'long_pullback_p3' in long_setups:
        return 0.35
    if 'long_decision_up' in long_setups:
        return 0.25
    return 0.25


def _sample_config(rng: random.Random):
    candidates = list(worker._research_candidate_configs())
    if not candidates:
        raise RuntimeError('no research candidates')
    filtered = [cfg for cfg in candidates if _config_key(cfg) not in _RECENT_KEYS]
    active = filtered or candidates
    weights = [_candidate_weight(cfg) for cfg in active]
    chosen = rng.choices(active, weights=weights, k=1)[0]
    _RECENT_KEYS.append(_config_key(chosen))
    return chosen


worker._sample_config = _sample_config
sys.argv = [
    'walkforward_research_worker',
    '--progress-jsonl', r'C:\work\meemee-screener\tmp\walkforward_research\progress_isolated.jsonl',
    '--best-json', r'C:\work\meemee-screener\tmp\walkforward_research\best_isolated.json',
    '--stop-file', r'C:\work\meemee-screener\tmp\walkforward_research\STOP_isolated',
    '--done-file', r'C:\work\meemee-screener\tmp\walkforward_research\DONE_isolated.json',
    '--max-runs', '0',
    '--step-months', '12',
    '--max-codes-choices', '150,250,350,500',
]
raise SystemExit(worker.main())
