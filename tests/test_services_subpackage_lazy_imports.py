from app.backend.services.analysis import analysis_backfill_service, swing_expectancy_service
from app.backend.services.data import taisyaku_import, yahoo_provisional
from app.backend.services.ml import ml_service, rankings_cache


def test_subpackage_lazy_imports_keep_module_access():
    assert type(ml_service).__name__ in {"_LazyModule", "module"}
    assert type(rankings_cache).__name__ in {"_LazyModule", "module"}
    assert type(analysis_backfill_service).__name__ in {"_LazyModule", "module"}
    assert type(swing_expectancy_service).__name__ in {"_LazyModule", "module"}
    assert type(taisyaku_import).__name__ in {"_LazyModule", "module"}
    assert type(yahoo_provisional).__name__ in {"_LazyModule", "module"}


def test_lazy_module_patch_forwards_to_real_module(monkeypatch):
    monkeypatch.setattr(ml_service, "train_models", lambda **_: {"ok": True})
    monkeypatch.setattr(analysis_backfill_service, "get_conn", lambda: None)

    assert ml_service.train_models() == {"ok": True}
    assert analysis_backfill_service.get_conn() is None
