# PHASE1_SLICE_B_PLAN

## 目的

この文書は `docs/PHASE1_IMPLEMENTATION_PLAN.md` を親として、Vertical Slice B を実装単位として固定する。Slice B の目的は、diff export、JPX calendar、rolling labels、anchor windows を成立させ、後続の publish に必要な入力素材を作ることである。

Slice B は Slice A 完了後にのみ着手できる。Slice B 完了前に Slice C へ進んではならない。

## 対象

Slice B の対象は次のとおりである。

- source DB からの diff export
- JPX calendar loader
- rolling label 生成
- anchor window 生成
- label / export generation manifest

## 非対象

Slice B では次を実装しない。

- candidate model 本格実装
- similarity embedding 本格実装
- `state_eval_daily` 実データ生成
- UI 完全切替
- 旧解析系停止
- 旧コード物理削除

Slice A の result DB / bridge / degrade 契約は維持し、変更しない。

## 変更対象ファイル

最低限、次のファイルを追加または変更する。

- `external_analysis/exporter/source_reader.py`
- `external_analysis/exporter/diff_export.py`
- `external_analysis/exporter/jpx_calendar.py`
- `external_analysis/labels/rolling_labels.py`
- `external_analysis/labels/anchor_windows.py`
- `external_analysis/exporter/export_schema.py`
- `tests/test_external_analysis_diff_export.py`
- `tests/test_external_analysis_jpx_calendar.py`
- `tests/test_external_analysis_rolling_labels.py`
- `tests/test_external_analysis_anchor_windows.py`

必要に応じて `external_analysis/__main__.py` に `export_sync`, `label_build`, `anchor_window_build` 相当の最小コマンドを追加してよい。

## DB schema

Slice B で新たに使う schema は次のとおりである。

### export DB

- `bars_daily_export`
- `bars_monthly_export`
- `indicator_daily_export`
- `pattern_state_export`
- `ranking_snapshot_export`
- `trade_event_export`
- `position_snapshot_export`
- `meta_export_runs`

### label store

- `label_daily_h5`
- `label_daily_h10`
- `label_daily_h20`
- `label_daily_h40`
- `label_daily_h60`
- `label_aux_monthly`
- `anchor_window_master`
- `anchor_window_bars`
- `label_generation_runs`

## 実装タスク

### Task B1: JPX calendar 実装

作業:

- JPX 営業日カレンダー reader を実装する
- 営業日 index API を用意する

完了条件:

- 5/10/20/40/60 horizon が自然日ではなく営業日で計算できる
- 祝日またぎのケースで営業日数が正しく進む

依存関係:

- Slice A 完了

### Task B2: diff export 実装

作業:

- source signature と row hash に基づく差分抽出を実装する
- export DB へ対象 code/date だけを反映する

完了条件:

- 同一 source で再実行しても全量再投入しない
- source 修正時に対象範囲だけ再 export する
- `meta_export_runs` に diff reason が残る

依存関係:

- Task B1

### Task B3: rolling labels 実装

作業:

- `ret_5/10/20/40/60`
- `mfe_20`, `mae_20`
- `days_to_mfe_20`, `days_to_stop_20`
- `rank_ret_20`, `top_1pct_20`, `top_3pct_20`, `top_5pct_20`

を生成する。

完了条件:

- label テーブルが JPX 営業日基準で埋まる
- purge / embargo policy version が `label_generation_runs` に残る

依存関係:

- Task B1
- Task B2

### Task B4: anchor windows 実装

作業:

- 初期標準 anchor の検出
- `anchor_window_master`, `anchor_window_bars` 保存
- collision / overlap / embargo group 付与

完了条件:

- `-20..+20` 営業日窓が保存される
- overlap を持つ anchor が group 化される

依存関係:

- Task B1
- Task B2

## 受入条件

- JPX 営業日基準の horizon 計算が成立する
- diff export が差分反映で動く
- rolling labels が生成される
- anchor windows が生成される
- Slice A の publish/read-only 契約を壊していない

## 手動確認手順

作業ディレクトリは `C:\work\meemee-screener` とする。

1. JPX calendar 読込コマンドまたはテストを実行し、祝日をまたぐ horizon 計算を確認する。
2. source DB の同一スナップショットに対して diff export を 2 回実行し、2 回目が全量再投入にならないことを確認する。
3. 特定 code のみ更新した source を用意し、その code/date だけが再 export されることを確認する。
4. rolling labels を生成し、`label_daily_h20` に `ret_20` などが入ることを確認する。
5. anchor window 生成を実行し、`anchor_window_master` と `anchor_window_bars` に `-20..+20` 窓が保存されることを確認する。
6. Slice A の read-only bridge を再確認し、new label/export store を MeeMee が直接読んでいないことを確認する。

## Slice C への進行条件

Slice C へ進んでよいのは、Slice B の受入条件と手動確認手順がすべて満たされた後だけである。
