from __future__ import annotations

# Re-export the real events routes (prefix: /api/events/*)
from app.backend.api.events_routes import router  # noqa: F401
