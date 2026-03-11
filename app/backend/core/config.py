from __future__ import annotations

# Compatibility wrapper:
# `app.core.config` is the canonical configuration source.
# Keep this module to preserve legacy imports from backend packages.
from app.core.config import *  # noqa: F401,F403

