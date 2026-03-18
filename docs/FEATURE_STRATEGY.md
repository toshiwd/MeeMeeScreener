# FEATURE_STRATEGY

## 目的

この文書は、external_analysis が保持・再計算・学習利用する特徴量の責務を固定する。各特徴量または特徴群について、`source`, `recalc_owner`, `leakage_guard`, `MeeMee_visible` を明示し、MeeMee 本体内で特徴量再計算や推論を行わない前提を実装レベルで閉じる。

この文書は `docs/REBUILD_MASTER_PLAN.md`、`docs/ARCHITECTURE_EXTERNAL_ANALYSIS.md`、`docs/DATA_EXPORT_SPEC.md`、`docs/LABELING_STRATEGY.md`、`docs/ROADMAP_PHASES.md`、`docs/RESOURCE_POLICY.md` に従う。特に `result DB only`、`MeeMee read-only`、`Parquet internal only`、`publish_pointer table 主体`、`graceful degrade` を破らない。

## 固定原則

MeeMee 本体は解析計算を一切行わない。MeeMee 本体は Parquet を直接参照しない。MeeMee 本体は feature store、label store、export DB、ops DB、model registry を読まない。MeeMee 本体は旧解析 worker を再起動しない。bridge は補完計算や代替推論を行わない。

特徴量は external_analysis の feature store にのみ保存する。MeeMee 本体や result DB は、特徴量そのものではなく publish 済みの解析結果だけを扱う。feature store は internal 専用であり、MeeMee から直接読めない。

Phase 1 を重くしないため、feature 群は二段構成に固定する。Phase 1 では差分 export から素直に構築できる日次要約特徴を中心に作る。類似形検索向けの系列特徴や高コスト特徴は schema を先に固定し、重い実装は Phase 2 以降で段階投入する。

## feature store 物理構成

feature store は最低でも次の 2 層を持つ。

- `feature_frame_daily`
- `feature_window_seq`

`feature_frame_daily` は `code + trade_date` 単位の要約特徴を持つ。`feature_window_seq` は固定長の近生系列特徴を持ち、類似形検索や sequence encoder の入力に使う。どちらも internal store であり、MeeMee 本体は読まない。

`feature_generation_runs` を別に持ち、`feature_version`, `calendar_version`, `source_snapshot_id`, `generated_at`, `row_counts`, `null_counts`, `source_hash`, `recalc_scope`, `dependency_versions` を記録する。

## 特徴群一覧

最低限、次の特徴群を固定する。

- 価格系列そのもの
- MA 距離、傾き、収束拡散
- 実体、ヒゲ、ATR、GAP
- 出来高の圧縮と解放
- box 滞在、box ブレイク状態
- PPP、ABC、本数系状態
- 横断相対強弱
- sector 相対比較
- 信用需給
- イベント残日数

各特徴群は raw-near 特徴と summary 特徴を併記する。raw-near は後日の類似形や埋め込み用、summary は candidate baseline 用である。

## source / recalc_owner / leakage_guard / MeeMee_visible

### 1. 価格系列そのもの

対象は `close`, `open`, `high`, `low`, `volume`, `log_return_1d`, `return_5d_back`, `return_20d_back`, 正規化価格列、window 内の close/volume series である。

`source` は `bars_daily_export`。`recalc_owner` は external_analysis。`leakage_guard` は forward 窓や future extrema を使わず、`trade_date` 当日までで閉じること。`MeeMee_visible` は `false`。

### 2. MA 距離、傾き、収束拡散

対象は `ma7`, `ma20`, `ma60`, `ma100`, `ma200`, `dist_ma20`, `dist_ma60`, `slope_ma20`, `slope_ma60`, `ma_spread_20_60`, `ma_compression_score` である。

`ma7`, `ma20`, `ma60` の `source` は `indicator_daily_export` 内の MeeMee 流用列とし、`ma100`, `ma200` および派生距離・傾きは external_analysis 再計算とする。`recalc_owner` は MeeMee 流用列では `MeeMee`, 派生列では `external_analysis`。`leakage_guard` は moving average 計算窓を過去方向に限定し、将来バーを参照しない。`MeeMee_visible` は `false`。

### 3. 実体、ヒゲ、ATR、GAP

対象は `body_pct`, `upper_wick_pct`, `lower_wick_pct`, `true_range`, `atr14`, `gap_up_pct`, `gap_down_pct`, `bar_range_pct` である。

`source` は `bars_daily_export` と `indicator_daily_export`。`recalc_owner` は external_analysis。`leakage_guard` は当日 bar と過去 ATR 窓のみを使うこと。将来のボラ情報を派生に混ぜない。`MeeMee_visible` は `false`。

### 4. 出来高の圧縮と解放

対象は `vol_ratio_5`, `vol_ratio_20`, `vol_compression_score`, `vol_release_score`, `dry_up_flag`, `volume_spike_flag` である。

`source` は `bars_daily_export`。`recalc_owner` は external_analysis。`leakage_guard` は volume spike 判定でも future 平均を使わず、当日以前だけを使うこと。`MeeMee_visible` は `false`。

### 5. box 滞在、box ブレイク状態

対象は `box_position_20`, `box_position_60`, `box_range_pct`, `box_breakout_flag`, `box_reclaim_flag`, `days_in_box`, `days_since_box_break` である。

`source` は `pattern_state_export` の MeeMee 流用 state と external_analysis 再計算 state の組み合わせとする。`recalc_owner` は primary 定義が MeeMee 由来でも、feature 化した派生列は external_analysis。`leakage_guard` は box 上下限を当日以前の window で確定すること。未来の高値・安値を使った再定義は禁止する。`MeeMee_visible` は `false`。

### 6. PPP、ABC、本数系状態

対象は `ppp_state`, `abc_state`, `count_above_ma20`, `count_above_ma60`, `trend_leg_count`, `phase_early_flag`, `phase_late_flag` である。

`source` は `pattern_state_export`。`recalc_owner` は state の owner に従うが、学習用 encode は external_analysis。`leakage_guard` は状態遷移が当日までの情報で確定していること。後日確定イベントの逆流は禁止する。`MeeMee_visible` は `false`。

### 7. 横断相対強弱

対象は `cross_section_ret_rank_5_back`, `cross_section_ret_rank_20_back`, `breadth_relative_score`, `universe_strength_percentile` である。

`source` は `bars_daily_export` と universe 定義。`recalc_owner` は external_analysis。`leakage_guard` は横断比較の対象を同一 `trade_date` のみとし、future return 順位を feature に使わないこと。`MeeMee_visible` は `false`。

### 8. sector 相対比較

対象は `sector33_code`, `sector_ret_rank_5_back`, `sector_ret_rank_20_back`, `sector_breadth_score`, `stock_vs_sector_gap` である。

`source` は `ranking_snapshot_export` または sector 正規化 source と `bars_daily_export`。canonical sector は東証 33 業種。`recalc_owner` は external_analysis。`leakage_guard` は sector 集計も同一 `trade_date` までで閉じること。future sector move を使わない。`MeeMee_visible` は `false`。

### 9. 信用需給

対象は `margin_buy_sell_ratio`, `short_interest_score`, `borrow_pressure_flag` などである。

`source` は optional source。`recalc_owner` は external_analysis。`leakage_guard` は公表遅延を尊重し、入手日より未来へ遡及適用しないこと。`MeeMee_visible` は `false`。

### 10. イベント残日数

対象は `days_since_event_x`, `days_to_next_event_x`, `event_cluster_score`, `recent_event_flags` である。

`source` は optional event source と `pattern_state_export`。`recalc_owner` は external_analysis。`leakage_guard` は事前に確定していない future event を使わないこと。公知日ベースで計算する。`MeeMee_visible` は `false`。

## Phase 1 最小実装

Phase 1 では重い系列特徴を完成させない。最低限実装するのは `feature_frame_daily` の日次要約特徴であり、次を必須にする。

- 価格系列の basic return と normalized close
- MA 7/20/60/100/200 と距離
- ATR、GAP、body/wick
- volume ratio と basic compression/release
- box position と breakout/reclaim 基本 flag
- PPP/ABC / 本数系の基本 encode
- 東証 33 業種コード

`feature_window_seq` は schema を先に固定し、重い埋め込み用系列の本格生成は Phase 2 以降でよい。

## leakage guard の共通規則

次の情報を feature に入れてはならない。

- future return
- future MFE/MAE
- future highest high / lowest low
- future cross-sectional rank
- test 期間を含む rolling 集計

特徴量生成器は、各列に `feature_asof_date <= trade_date` を満たすことを invariant として実装する。optional source は public availability date を持ち、availability date より前へ値を流してはならない。

## テスト観点

最低限、次のテストを実装する。

- MeeMee 本体が feature store を読まないこと
- `feature_frame_daily` の全列が `trade_date` 当日以前だけで計算されること
- `ma100`, `ma200` が external_analysis 再計算であること
- 東証 33 業種が sector canonical として使われること
- 公表遅延のある optional source が遡及適用されないこと
- Phase 1 で `feature_window_seq` が空でも schema 固定されること

## 受入条件

実装完了後、開発者は feature generator を実行し、Phase 1 必須特徴が `feature_frame_daily` に生成されること、future 情報が feature に混入しないこと、MeeMee 本体が feature store を読まずとも UI が成立することを確認できなければならない。

この文書と上位文書の競合時の優先順位は `REBUILD_MASTER_PLAN.md > ARCHITECTURE_EXTERNAL_ANALYSIS.md > DATA_EXPORT_SPEC.md > LABELING_STRATEGY.md > FEATURE_STRATEGY.md` とする。競合時は `result DB only`、`MeeMee read-only`、`Parquet internal only`、`publish_pointer table 主体`、`graceful degrade` を優先する。
