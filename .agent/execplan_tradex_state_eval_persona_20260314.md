# Tradex State Eval Persona ExecPlan

## Goal

`external_analysis` の state evaluation を、`side + 保有帯 + 手法タグ` で判定できる形へ拡張し、短文3理由・50件 promotion gate・失敗例保存・research load control を最小変更で入れる。

## Decisions

- state eval の出力語彙は `enter / wait / skip` を使う。
- buy は `buy_5_20 / buy_21_60`、sell は `sell_5_10 / sell_11_20` に分ける。
- 手法タグは初期は `価格 + MA + 出来高` だけで広めに付与する。
- promotion gate は `50件`, `期待値改善`, `含み損悪化なし`, `teacher alignment 悪化なし` を必須にする。
- failure sample は `latest_bucket 10件 + worst_bucket 10件` を保存する。
- MeeMee 同一PC運用なので publish job は load control を見て `full / throttled / deferred` を決める。

## Implemented

- `external_analysis/models/state_eval_baseline.py`
  - holding band / strategy tags / short reason texts / 50件 gate / failure samples
- `external_analysis/results/result_schema.py`
  - `state_eval_daily` に `holding_band`, `strategy_tags`, `reason_text_top3`
- `external_analysis/ops/ops_schema.py`
  - teacher profile / shadow / readiness の拡張
  - `external_state_eval_failure_samples` 追加
- `external_analysis/runtime/load_control.py`
  - 時間帯と foreground window を見る軽量 load control
- `app/backend/core/external_analysis_publish_job.py`
  - deferred 時は publish job を skip
- `app/backend/services/analysis_bridge/contracts.py`
  - public state-eval payload を拡張

## Verification

- `python -m pytest tests/test_external_analysis_result_schema.py`
- `python -m pytest tests/test_external_analysis_candidate_baseline.py`
- `python -m pytest tests/test_analysis_bridge_api.py tests/test_external_analysis_load_control.py tests/test_external_analysis_publish_job.py`
- `python -m pytest tests/test_phase2_slice_f_nightly_pipeline.py`
- `python -m pytest tests/test_phase3_similarity_nightly_pipeline.py`
- `python -m pytest tests/test_phase5_historical_replay.py`
