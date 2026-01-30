import json
import os
from typing import Dict, Any, Optional

class ConfigRepository:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.state_path = os.path.join(data_dir, "update_state.json")
        self.rank_config_path = os.path.join(data_dir, "config", "rank_config.json")

    def load_update_state(self) -> Dict[str, Any]:
        if not os.path.exists(self.state_path):
            return {}
        try:
            with open(self.state_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    def save_update_state(self, state: Dict[str, Any]):
        os.makedirs(os.path.dirname(self.state_path), exist_ok=True)
        with open(self.state_path, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)

    def load_rank_config(self) -> Dict[str, Any]:
        # Fallback to bundled if not found? 
        if not os.path.exists(self.rank_config_path):
             return {}
        try:
             with open(self.rank_config_path, "r", encoding="utf-8") as f:
                 return json.load(f)
        except Exception:
             return {}
