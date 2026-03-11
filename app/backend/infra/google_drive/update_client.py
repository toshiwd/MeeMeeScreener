from __future__ import annotations
import urllib.request
import json
import logging
import os
import tempfile
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

@dataclass
class UpdateInfo:
    version: str
    url: str
    mandatory: bool
    notes: str = ""

class UpdateClient:
    def __init__(self):
        # Placeholder URL - User needs to provide real Drive public link
        self.version_url = "https://raw.githubusercontent.com/toshiwd/MeeMeeScreener/main/release/version.json" 
        # Using a raw github link as a placeholder which acts like a file hosting
        # Ideally this is a Google Drive direct download link

    def check_for_updates(self, current_version: str) -> Optional[UpdateInfo]:
        try:
            logger.info(f"Checking updates from {self.version_url}...")
            with urllib.request.urlopen(self.version_url, timeout=5) as response:
                data = json.loads(response.read().decode("utf-8"))
            
            remote_ver = data.get("version")
            if not remote_ver:
                return None
                
            if self._compare_versions(remote_ver, current_version) > 0:
                return UpdateInfo(
                    version=remote_ver,
                    url=data.get("url", ""),
                    mandatory=data.get("mandatory", False),
                    notes=data.get("notes", "")
                )
            return None
        except Exception as e:
            logger.warning(f"Update check failed: {e}")
            return None

    def download_update(self, url: str) -> str | None:
        """Downloads update to a temp file and returns path."""
        try:
            # Create a temp file path
            fd, path = tempfile.mkstemp(suffix=".zip")
            os.close(fd)
            
            logger.info(f"Downloading update from {url} to {path}...")
            # For Google Drive large files, urllib might not handle the confirm warning.
            # But if it's a direct link to small zip/exe, it works.
            # Real implementation might need 'requests' library with cookies.
            urllib.request.urlretrieve(url, path)
            return path
        except Exception as e:
            logger.error(f"Download failed: {e}")
            return None

    def _compare_versions(self, v1: str, v2: str) -> int:
        """
        Returns 1 if v1 > v2, -1 if v1 < v2, 0 if equal.
        Assumes semantic versioning (x.y.z)
        """
        def normalize(v):
            return [int(x) for x in v.split(".")]
        
        try:
            p1 = normalize(v1)
            p2 = normalize(v2)
            if p1 > p2: return 1
            if p1 < p2: return -1
            return 0
        except:
            return 0
