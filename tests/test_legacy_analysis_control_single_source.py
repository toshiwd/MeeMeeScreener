from app.backend.core.legacy_analysis_control import (
    is_legacy_analysis_disabled as core_is_disabled,
    legacy_analysis_disabled_log_value as core_log_value,
)
from app.backend.services.legacy_analysis_control import (
    is_legacy_analysis_disabled as services_is_disabled,
    legacy_analysis_disabled_log_value as services_log_value,
)


def test_legacy_analysis_control_services_module_reexports_core(monkeypatch):
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")
    assert services_is_disabled() is core_is_disabled()
    assert services_log_value() == core_log_value()
