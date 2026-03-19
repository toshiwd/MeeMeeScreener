import json
import os
import tempfile
from typing import Dict, Any

LOGIC_SELECTION_SCHEMA_VERSION = "logic_selection_v1"

class ConfigRepository:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.state_path = os.path.join(data_dir, "update_state.json")
        self.rank_config_path = os.path.join(data_dir, "config", "rank_config.json")
        self.logic_selection_path = os.path.join(data_dir, "config", "logic_selection.json")

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

    def load_logic_selection_state(self) -> Dict[str, Any]:
        if not os.path.exists(self.logic_selection_path):
            return {}
        try:
            with open(self.logic_selection_path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            if isinstance(payload, dict):
                return payload
        except Exception:
            return {}
        return {}

    def _atomic_write_json(self, path: str, payload: Dict[str, Any]) -> str:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(prefix=".logic_selection.", suffix=".tmp", dir=os.path.dirname(path))
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
                f.write("\n")
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, path)
        except Exception:
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass
            raise
        return path

    def save_logic_selection_state(self, state: Dict[str, Any]) -> str:
        payload = dict(state or {})
        payload["schema_version"] = str(payload.get("schema_version") or LOGIC_SELECTION_SCHEMA_VERSION)
        return self._atomic_write_json(self.logic_selection_path, payload)
