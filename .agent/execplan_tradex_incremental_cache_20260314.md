# Tradex Incremental Cache ExecPlan

## Goal

`external_analysis` の nightly / replay で、同じ元データに対する `label` / `anchor` / `similarity` の全再構築を減らし、`skip / partial / full` を自動選択できるようにする。

## Decisions

- export 側の `meta_export_runs.source_signature` を再利用の正本にする。
- `label_generation_manifest` と `similarity_generation_manifest` を追加し、依存 version と cache state を保持する。
- `rolling_labels` は dirty date 単位、`anchor_windows` は dirty code 単位、`case_library` は dirty code 単位で再計算する。
- relevant table に delete が含まれる場合は安全側で full rebuild に戻す。
- physical hot/cold 分離は次段階とし、今回は ops retention を追加して論理分離だけ先に入れる。

## Implemented

- `external_analysis/runtime/incremental_cache.py`
  - latest export run 読み出し
  - label / similarity probe
  - dirty range 算出
  - generation manifest upsert
- `external_analysis/labels/store.py`
  - `label_generation_manifest` 追加
- `external_analysis/similarity/store.py`
  - `similarity_generation_manifest` 追加
- `external_analysis/labels/rolling_labels.py`
  - skip / partial / full 対応
- `external_analysis/labels/anchor_windows.py`
  - skip / partial / full 対応
- `external_analysis/similarity/baseline.py`
  - case library の partial rebuild 対応
- `external_analysis/ops/store.py`
  - teacher profile / state eval shadow の retention 追加

## Verification

- `python -m pytest tests/test_external_analysis_rolling_labels.py`
- `python -m pytest tests/test_external_analysis_anchor_windows.py`
- `python -m pytest tests/test_phase3_similarity_baseline.py`
- `python -m pytest tests/test_phase3_similarity_nightly_pipeline.py`
- `python -m pytest tests/test_phase5_historical_replay.py`
- `python -m pytest tests/test_phase2_slice_f_nightly_pipeline.py`
