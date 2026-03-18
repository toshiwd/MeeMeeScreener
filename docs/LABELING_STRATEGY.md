# LABELING_STRATEGY

## 目的

この文書は、external_analysis が生成する教師ラベルの定義を固定する。主ラベルを rolling horizon に統一し、anchor window を別系統の評価単位として持ち、JPX 営業日基準、purged walk-forward + embargo、collision / overlap / leakage guard を明文化する。

この文書は `docs/REBUILD_MASTER_PLAN.md`、`docs/ARCHITECTURE_EXTERNAL_ANALYSIS.md`、`docs/DATA_EXPORT_SPEC.md` に従う。特に `result DB only`、`MeeMee read-only`、`Parquet internal only`、`publish_pointer table 主体`、`graceful degrade` の契約を破らない。MeeMee 本体はラベル計算を行わず、label store は external_analysis 内部専用である。

## 固定前提

営業日定義は JPX 取引カレンダーを canonical とする。`5/10/20/40/60` horizon、anchor window の `-20..+20`、purge 幅、embargo 幅、freshness 補助評価に使う営業日数はすべて同一の JPX カレンダーに基づく。

主ラベルは rolling horizon であり、月固定ラベルは補助評価専用である。月固定ラベルを主学習ラベルへ戻してはならない。label store は Parquet と DuckDB の internal store に置き、MeeMee 本体や result DB から直接参照させない。

## 主ラベル

主ラベルは日次 `code + trade_date` 単位で定義する。最低限、次の rolling horizon ラベルを持つ。

- `ret_5`
- `ret_10`
- `ret_20`
- `ret_40`
- `ret_60`
- `mfe_20`
- `mae_20`
- `days_to_mfe_20`
- `days_to_stop_20`
- `rank_ret_20`
- `top_1pct_20`
- `top_3pct_20`
- `top_5pct_20`

`ret_h` は `trade_date` の close から JPX 営業日 `h` 本先の close までの forward return とする。`mfe_20` は 20 営業日窓内の最大有利方向 return、`mae_20` は同窓内の最大不利方向 return とする。`days_to_mfe_20` は 20 営業日窓内で MFE を記録した最初の営業日オフセット、`days_to_stop_20` は stop 条件を初回に満たした営業日オフセットとする。

`rank_ret_20` は同一 `trade_date` における全銘柄横断の `ret_20` 順位である。`top_1pct_20`, `top_3pct_20`, `top_5pct_20` は同一 `trade_date` 横断順位に基づく二値ラベルであり、母集団は当日有効 universe のみを使う。

## 補助評価ラベル

補助評価ラベルは主学習ラベルではなく、モデル評価と月次レビューにのみ使う。最低限、次を保持する。

- `monthly_top5`
- `monthly_top10`
- `monthly_rank_ret`

これらは月次ランキング再現や monthly capture 評価には使ってよいが、candidate engine の主目的関数に使ってはならない。補助評価ラベルは主ラベルと物理テーブルを分けて保持する。

## anchor window 定義

anchor window はイベント起点の局面ラベルであり、日次 rolling horizon とは別テーブルで管理する。初期標準 anchor は次のとおり。

- `20MA_cross_up`
- `20MA_reclaim`
- `box_breakout`
- `prev_high_break`
- `volume_spike`
- `prev_low_break`
- `big_bear_full_reclaim`

各 anchor は `anchor_type + code + anchor_date` を主キーとし、`anchor_date` を中心に JPX 営業日 `-20..+20` の窓を持つ。保存単位は 2 層とする。第一に `anchor_window_master` で anchor 1 件の概要を保持する。第二に `anchor_window_bars` で relative day ごとの系列を保持する。

`anchor_window_master` の最低列は `anchor_type`, `code`, `anchor_date`, `window_start_date`, `window_end_date`, `anchor_strength`, `anchor_source_version`, `future_ret_5`, `future_ret_10`, `future_ret_20`, `future_mfe_20`, `future_mae_20`, `future_outcome_class`, `failure_reason`, `embargo_group_id` とする。

`anchor_window_bars` の最低列は `anchor_type`, `code`, `anchor_date`, `rel_day`, `trade_date`, `close_norm`, `volume_norm`, `ma20_gap`, `ma60_gap`, `box_state`, `event_flags` とする。

## JPX 営業日基準

ラベル計算で用いる先読み・窓切り・overlap 判定はすべて JPX 営業日 index を使う。自然日ベースの前後日数計算は禁止する。例えば `ret_20` は 20 自然日後ではなく 20 JPX 営業日後である。anchor window の `+20` も 20 営業日先である。

欠損営業日がある場合は、その日のラベルを無効とする。`trade_date` の先に horizon を満たす営業日が足りない場合、当該 `ret_h` とその派生ラベルは `NULL` とし、学習サンプルへ入れない。

## purged walk-forward + embargo

学習・評価の標準分割法は purged walk-forward + embargo とする。walk-forward では train, validation, test を時間順に並べ、未来情報が近接窓から漏れないよう purge と embargo を適用する。

rolling horizon ラベルでは、任意のサンプル `trade_date = t` が `h` 営業日先まで未来を参照するため、test 区間に対して train/validation 側の `t ... t+h` が重なるサンプルを purge する。最低 purge 幅は対象 horizon と同じ営業日数とする。したがって `ret_20` を主目的に使う学習では、test 境界前後の 20 営業日を purge 対象とする。

embargo は split 境界の直後に追加で設ける保護帯である。初期既定値は `max(5, horizon/4)` 営業日とし、各タスクで次を採用する。

- 5 日系: purge 5, embargo 5
- 10 日系: purge 10, embargo 5
- 20 日系: purge 20, embargo 5
- 40 日系: purge 40, embargo 10
- 60 日系: purge 60, embargo 15

anchor window では、同一銘柄かつ future 区間が重なる anchor 同士を同一 `embargo_group_id` に束ねる。test anchor の future 区間と 1 営業日でも重なる train/validation anchor は purge する。さらに test anchor 境界の後ろに 5 営業日の embargo を置く。

## collision / overlap / leakage guard

同一 `code + trade_date` で複数 anchor が発火する場合、anchor は落とさず保持する。ただし学習時には `anchor_type` ごとに別サンプルとして扱うか、または `multi_anchor` フラグを立てて別モデルへ分ける。安易に 1 件へ潰してはならない。

rolling horizon ラベル同士の leakage guard として、同一 split 内で future 参照区間が test と重なるサンプルを purge する。cross-sectional rank ラベルでは、当日 universe のみを用いるが、future return で順位が決まるため purge 規則は `ret_20` と同じにする。

anchor window の overlap guard として、同一 `code` で `anchor_date` が近接し、future 区間が重なる anchor は学習時に同一 fold へ閉じ込めるか purge する。train と test にまたがる重複は許容しない。

MFE/MAE 系 leakage guard として、未来窓内の最高値・最安値に直接依存する派生列を feature として入れてはならない。これらは label のみであり、feature store 側へ流してはならない。

## label store 物理構成

label store は internal 専用であり、MeeMee 本体は読まない。最低物理テーブルは次のとおり。

- `label_daily_h5`
- `label_daily_h10`
- `label_daily_h20`
- `label_daily_h40`
- `label_daily_h60`
- `label_aux_monthly`
- `anchor_window_master`
- `anchor_window_bars`
- `label_generation_runs`

`label_generation_runs` には `label_version`, `calendar_version`, `source_snapshot_id`, `generated_at`, `row_counts`, `null_counts`, `purge_policy_version`, `embargo_policy_version` を保持する。

## 実装単位

Phase 1 で最低限実装するのは、JPX カレンダーを使った rolling horizon ラベル生成、anchor window 保存、補助評価ラベルの分離保存、label generation manifest 出力である。Phase 1 では model 学習自体を必須にしないが、後続 phase がそのまま学習へ接続できる schema を先に固定する。

## テスト観点

最低限、次のテストを実装する。

- 20 営業日 horizon が JPX カレンダー基準で計算されること
- 祝日をまたぐ場合でも自然日ではなく営業日で `ret_h` が決まること
- horizon を満たさない末端データでは label が `NULL` になること
- `rank_ret_20` と `top_xpct_20` が同日横断順位で一貫すること
- 同一銘柄で future 区間が重なる anchor が train/test にまたがらないこと
- purged walk-forward + embargo の split で leakage が起きないこと
- 補助評価ラベルが主ラベルテーブルへ混入しないこと

## 受入条件

実装完了後、開発者は label generator を実行し、JPX 営業日基準で `label_daily_h20` と `anchor_window_master` が生成されることを確認できなければならない。purge と embargo の設定が manifest に残り、テストで overlap と leakage が否定できる状態を合格とする。

この文書と上位文書の競合時の優先順位は `REBUILD_MASTER_PLAN.md > ARCHITECTURE_EXTERNAL_ANALYSIS.md > DATA_EXPORT_SPEC.md > LABELING_STRATEGY.md` とする。競合時は `result DB only`、`MeeMee read-only`、`Parquet internal only`、`publish_pointer table 主体`、`graceful degrade` を優先する。
