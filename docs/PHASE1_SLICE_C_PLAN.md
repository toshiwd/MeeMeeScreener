# PHASE1_SLICE_C_PLAN

## 目的

この文書は `docs/PHASE1_IMPLEMENTATION_PLAN.md` を親として、Vertical Slice C を実装単位として固定する。Slice C の目的は、旧解析系の起動停止、更新停止、最小限の並走確認を行い、Phase 1 の土台と旧経路の干渉を断つことである。

Slice C は Slice B 完了後にのみ着手できる。

## 対象

Slice C の対象は次のとおりである。

- 旧解析系の起動停止
- 旧解析系の更新停止
- 新経路と旧経路の並走確認

## 非対象

Slice C では次を実装しない。

- UI 完全切替
- candidate model 本格実装
- similarity embedding 本格実装
- `state_eval_daily` 実データ生成
- 旧コード物理削除

旧経路の比較参照や監視のための存在は許容するが、再起動や更新は許容しない。

## 変更対象ファイル

最低限、次の領域を変更対象とする。

- 旧解析 worker / job の自動起動箇所
- 旧 `ml_pred_20d`, `phase_pred_daily`, `sell_analysis_daily` 更新箇所
- 起動時フックまたはスケジュール登録箇所
- `tests/test_phase1_legacy_analysis_disabled.py`
- `tests/test_phase1_legacy_updates_disabled.py`
- `tests/test_phase1_parallel_run_guard.py`

具体的な既存ファイル名は実装時に確定してよいが、変更範囲は「起動停止」「更新停止」に限定し、UI 完全切替や削除まで広げない。

## DB / runtime 対象

Slice C では新しい schema を増やさない。既存旧解析系テーブルの扱いだけを変える。

- 旧解析テーブルは読み取り比較用に残してよい
- 新規更新は禁止する
- `publish_pointer` と result DB 公開経路を Phase 1 の基準経路とする

## 実装タスク

### Task C1: 起動停止

作業:

- 通常起動時の旧解析 worker / job 自動起動を止める
- バックグラウンド更新にぶら下がる旧解析 hook を無効化する

完了条件:

- MeeMee 通常起動で旧解析 worker が起動しない
- external_analysis の Phase 1 経路には影響しない

依存関係:

- Slice A 完了
- Slice B 完了

### Task C2: 更新停止

作業:

- `ml_pred_20d`
- `phase_pred_daily`
- `sell_analysis_daily`

の新規更新経路を止める。

完了条件:

- source 更新や通常操作で旧解析テーブルが新規更新されない
- MeeMee 本体の通常機能は継続する

依存関係:

- Task C1

### Task C3: 並走確認

作業:

- Phase 1 の新経路が旧経路停止後も壊れないことを確認する
- 旧経路停止により MeeMee が degrade 可能であることを確認する

完了条件:

- old path 無効化後も `publish_pointer` 起点の read-only bridge が動く
- 旧経路テーブル未更新でも MeeMee が例外終了しない

依存関係:

- Task C2

## 受入条件

- 旧解析 worker の自動起動が止まる
- 旧解析テーブルの更新が止まる
- Slice A と Slice B の成果が維持される
- MeeMee が `publish_pointer` 起点の read-only / degrade 経路で動く
- UI 完全切替や物理削除をしていない

## 手動確認手順

作業ディレクトリは `C:\work\meemee-screener` とする。

1. MeeMee を通常起動し、旧解析 worker / job が起動していないことをログまたは状態確認で確認する。
2. source 更新を 1 回実行し、旧 `ml_pred_20d`, `phase_pred_daily`, `sell_analysis_daily` に新規更新が入らないことを確認する。
3. `publish_pointer` を起点に bridge が latest successful publish を読めることを確認する。
4. 旧解析テーブルが更新されない状態で MeeMee を操作し、通常機能が継続することを確認する。
5. Slice A の degrade ケースを 1 つ再実行し、旧経路停止後も解析パネルだけが degrade することを確認する。

## Phase 1 完了条件への接続

Slice C 完了時点で、Phase 1 の対象である `publish_pointer`, `publish_manifest`, result DB empty schema, read-only bridge, graceful degrade, diff export, rolling labels, anchor windows, 旧解析系の起動停止と更新停止が一通り実装・確認済みでなければならない。

candidate model 本格実装、similarity embedding 本格実装、`state_eval_daily` 実データ生成、UI 完全切替、旧コード物理削除は引き続き Phase 1 非対象のままとする。
