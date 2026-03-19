import json
import os
import hashlib
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any

LOGIC_SELECTION_SCHEMA_VERSION = "logic_selection_v1"
LOGIC_SELECTION_AUDIT_PATH = "logic_selection_audit.jsonl"
PUBLISH_PROMOTION_AUDIT_PATH = "publish_promotion_audit.jsonl"
PUBLISH_REGISTRY_SCHEMA_VERSION = "publish_registry_v1"
LAST_KNOWN_GOOD_DIRNAME = "last_known_good"
LAST_KNOWN_GOOD_ARTIFACT_SCHEMA_VERSION = "last_known_good_v1"

class ConfigRepository:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.state_path = os.path.join(data_dir, "update_state.json")
        self.rank_config_path = os.path.join(data_dir, "config", "rank_config.json")
        self.logic_selection_path = os.path.join(data_dir, "config", "logic_selection.json")
        self.publish_registry_path = os.path.join(data_dir, "config", "publish_registry.json")
        self.runtime_selection_dir = os.path.join(data_dir, "runtime_selection")
        self.logic_selection_audit_path = os.path.join(self.runtime_selection_dir, LOGIC_SELECTION_AUDIT_PATH)
        self.publish_promotion_audit_path = os.path.join(self.runtime_selection_dir, PUBLISH_PROMOTION_AUDIT_PATH)
        self.last_known_good_root = os.path.join(self.runtime_selection_dir, LAST_KNOWN_GOOD_DIRNAME)

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

    def load_publish_registry_state(self) -> Dict[str, Any]:
        if not os.path.exists(self.publish_registry_path):
            return {}
        try:
            with open(self.publish_registry_path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            if isinstance(payload, dict):
                return payload
        except Exception:
            return {}
        return {}

    def save_publish_registry_state(self, state: Dict[str, Any]) -> str:
        payload = dict(state or {})
        payload["schema_version"] = str(payload.get("schema_version") or PUBLISH_REGISTRY_SCHEMA_VERSION)
        return self._atomic_write_json(self.publish_registry_path, payload)

    def _logic_key_dirname(self, logic_key: str) -> str:
        safe = str(logic_key or "").strip().replace(":", "__")
        return safe or "unknown_logic"

    def _artifact_checksum(self, path: str | os.PathLike[str]) -> str:
        digest = hashlib.sha256()
        with open(path, "rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def resolve_last_known_good_artifact_path(
        self,
        *,
        logic_key: str,
        checksum: str,
        captured_at: str | None = None,
    ) -> str:
        stamp = str(captured_at or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
        checksum_part = str(checksum or "").strip()[:16] or "nochecksum"
        filename = f"{stamp}__{checksum_part}.json"
        return os.path.join(self.last_known_good_root, self._logic_key_dirname(logic_key), filename)

    def write_last_known_good_artifact(self, payload: Dict[str, Any], *, artifact_path: str) -> str:
        return self._atomic_write_json(artifact_path, payload)

    def read_json_file(self, path: str) -> Dict[str, Any]:
        if not os.path.exists(path):
            return {}
        try:
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}

    def append_audit_event(self, event: Dict[str, Any]) -> str:
        os.makedirs(os.path.dirname(self.logic_selection_audit_path), exist_ok=True)
        payload = dict(event or {})
        payload.setdefault("schema_version", LOGIC_SELECTION_SCHEMA_VERSION)
        payload.setdefault("event_at", datetime.now(timezone.utc).isoformat())
        payload.setdefault("artifact_schema_version", LAST_KNOWN_GOOD_ARTIFACT_SCHEMA_VERSION)
        line = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        with open(self.logic_selection_audit_path, "a", encoding="utf-8") as f:
            f.write(line)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        return self.logic_selection_audit_path

    def append_publish_promotion_audit_event(self, event: Dict[str, Any]) -> str:
        os.makedirs(os.path.dirname(self.publish_promotion_audit_path), exist_ok=True)
        payload = dict(event or {})
        payload.setdefault("schema_version", PUBLISH_REGISTRY_SCHEMA_VERSION)
        payload.setdefault("event_at", datetime.now(timezone.utc).isoformat())
        line = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        with open(self.publish_promotion_audit_path, "a", encoding="utf-8") as f:
            f.write(line)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        return self.publish_promotion_audit_path
