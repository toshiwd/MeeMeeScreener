from __future__ import annotations

from typing import Any, Callable


def train_models(
    *,
    start_dt: int | None = None,
    end_dt: int | None = None,
    dry_run: bool = False,
    progress_cb: Callable[[int, str], None] | None = None,
) -> dict[str, Any]:
    from . import ml_service as _ml

    cfg = _ml.load_ml_config()

    def _notify(progress: int, message: str) -> None:
        if progress_cb is None:
            return
        try:
            progress_cb(max(0, min(100, int(progress))), str(message))
        except Exception:
            return

    _notify(2, "Preparing ML training...")
    with _ml.get_conn() as conn:
        _ml._ensure_ml_schema(conn)
        active_row = _ml._load_active_model_row(conn)
        has_active_model = bool(active_row)
        active_objective = str(active_row[2]) if active_row and len(active_row) > 2 and active_row[2] is not None else None
        compare_to_champion = bool(has_active_model and active_objective == _ml.OBJECTIVE)
        champion_wf_metrics = _ml._extract_walk_forward_metrics_from_registry_row(active_row)
        _notify(6, "Refreshing feature table...")
        feature_rows = _ml.refresh_ml_feature_table(
            conn,
            feature_version=_ml.FEATURE_VERSION,
            start_dt=start_dt,
            end_dt=end_dt,
        )
        _notify(14, f"Feature table refreshed ({int(feature_rows)} rows).")
        _notify(18, "Refreshing label table...")
        label_rows = _ml.refresh_ml_label_table(
            conn,
            cfg=cfg,
            label_version=_ml.LABEL_VERSION,
            start_dt=start_dt,
            end_dt=end_dt,
        )
        _notify(26, f"Label table refreshed ({int(label_rows)} rows).")
        _notify(30, "Refreshing monthly labels...")
        monthly_label_rows = _ml.refresh_ml_monthly_label_table(
            conn,
            label_version=_ml.MONTHLY_LABEL_VERSION,
            start_dt=start_dt,
            end_dt=end_dt,
        )
        _notify(36, "Loading training datasets...")
        df = _ml._load_training_df(conn, start_dt=start_dt, end_dt=end_dt)
        if df.empty:
            raise RuntimeError("No joined rows for ML training")
        monthly_df = _ml._load_monthly_training_df(conn, start_dt=start_dt, end_dt=end_dt)
        monthly_models = None
        monthly_gate_recommendation: dict[str, Any] = {}
        monthly_ret20_lookup: dict[str, Any] = {}
        monthly_train_error: str | None = None

        _notify(40, f"Running walk-forward ({int(len(df))} rows)...")
        wf_start = 40
        wf_span = 30

        def _on_wf_progress(done: int, total: int) -> None:
            pct = wf_start + int(wf_span * max(0, done) / max(1, total))
            _notify(pct, f"Walk-forward {int(done)}/{int(total)}")

        wf_metrics = _ml._walk_forward_eval(df, cfg, progress_cb=_on_wf_progress)
        _notify(72, "Fitting production models...")
        models = _ml._fit_models(df, cfg)
        _notify(80, "Fitting monthly models...")
        if monthly_df.empty:
            monthly_train_error = "No joined rows for monthly ML training"
        else:
            try:
                monthly_models = _ml._fit_monthly_models(monthly_df, cfg)
                if monthly_models.abs_cls is not None:
                    monthly_pred_train = _ml._predict_monthly_frame(monthly_df, monthly_models)
                    monthly_pred_train["ret1m"] = _ml.pd.to_numeric(monthly_df.get("ret1m"), errors="coerce")
                    monthly_ret20_lookup = _ml._derive_monthly_ret20_lookup(
                        monthly_df,
                        monthly_models,
                        monthly_pred_train=monthly_pred_train,
                    )
                    monthly_gate_recommendation = _ml._recommend_monthly_target_gate(
                        monthly_pred_train=monthly_pred_train,
                        monthly_ret20_lookup=monthly_ret20_lookup,
                    )
            except Exception as exc:
                monthly_train_error = str(exc)

        _notify(88, "Evaluating promotion policy...")
        promotion = _ml._evaluate_promotion_policy(
            wf_metrics=wf_metrics,
            cfg=cfg,
            has_active_model=has_active_model,
            compare_to_champion=compare_to_champion,
            champion_wf_metrics=champion_wf_metrics,
        )
        if dry_run:
            _notify(100, "Dry run completed.")
            return {
                "ok": True,
                "dry_run": True,
                "feature_rows": int(feature_rows),
                "label_rows": int(label_rows),
                "monthly_label_rows": int(monthly_label_rows),
                "wf_metrics": wf_metrics,
                "promotion": promotion,
                "monthly_train_error": monthly_train_error,
                "monthly_gate_recommendation": monthly_gate_recommendation,
                "monthly_ret20_lookup": monthly_ret20_lookup,
            }

        _notify(92, "Saving model registry...")
        saved = _ml._save_models(
            conn,
            models=models,
            wf_metrics=wf_metrics,
            promotion=promotion,
            monthly_models=monthly_models,
            monthly_gate_recommendation=monthly_gate_recommendation,
            monthly_ret20_lookup=monthly_ret20_lookup,
            monthly_train_error=monthly_train_error,
        )
        _notify(100, "ML training completed.")
        return {
            "ok": True,
            "dry_run": False,
            "feature_rows": int(feature_rows),
            "label_rows": int(label_rows),
            "monthly_label_rows": int(monthly_label_rows),
            "wf_metrics": wf_metrics,
            "promotion": promotion,
            "saved": saved,
            "monthly_train_error": monthly_train_error,
            "monthly_gate_recommendation": monthly_gate_recommendation,
            "monthly_ret20_lookup": monthly_ret20_lookup,
        }
