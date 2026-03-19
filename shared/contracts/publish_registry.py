from __future__ import annotations

from typing import Final

PUBLISH_REGISTRY_SCHEMA_VERSION: Final[str] = "publish_registry_v1"
PUBLISH_PROMOTION_ACTION_PROMOTE: Final[str] = "promote"
PUBLISH_PROMOTION_ACTION_DEMOTE: Final[str] = "demote"
PUBLISH_PROMOTION_ACTION_ROLLBACK: Final[str] = "rollback"

PUBLISH_ROLE_CHAMPION: Final[str] = "champion"
PUBLISH_ROLE_CHALLENGER: Final[str] = "challenger"
PUBLISH_ROLE_RETIRED: Final[str] = "retired"
PUBLISH_ROLE_BLOCKED: Final[str] = "blocked"
PUBLISH_ROLE_DEMOTED: Final[str] = "demoted"
PUBLISH_ROLE_QUEUED: Final[str] = "queued"

PUBLISH_BOOTSTRAP_RULE_EXPLICIT_CHAMPION: Final[str] = "explicit_champion_flag"
PUBLISH_BOOTSTRAP_RULE_DEFAULT_POINTER: Final[str] = "default_logic_pointer"
PUBLISH_BOOTSTRAP_RULE_LAST_STABLE_PROMOTED: Final[str] = "last_stable_promoted_entry"
PUBLISH_BOOTSTRAP_RULE_EMPTY: Final[str] = "empty_safe_state"
