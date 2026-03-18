# DATA_EXPORT_SPEC

## 目的

この文書は、MeeMee source DB から external_analysis へ渡す公開契約と、external_analysis から MeeMee 本体へ返す result DB 契約を固定する。主眼は source of truth の列単位明示、差分 export 規則、publish 契約、read-only 契約、failure/stale semantics の固定にある。

この文書で定義する内容は、後続の labeling と feature 戦略の前提となる。後続文書はここで固定した canonical key、calendar、sector basis、publish 契約を破ってはならない。

## export scope

external_analysis が source DB から受け取る最低対象は次のテーブル群である。

- `daily_bars`
- `daily_ma`
- `monthly_bars`
- `monthly_ma`
- `feature_snapshot_daily`
- ranking 関連の既存スナップショット
- position / trade history 関連の既存正規化テーブル
- AI export metadata に相当する watermarks と更新メタ情報

これらを external_analysis 側では、最低でも次の export テーブルへ正規化する。

- `bars_daily_export`
- `bars_monthly_export`
- `indicator_daily_export`
- `pattern_state_export`
- `ranking_snapshot_export`
- `trade_event_export`
- `position_snapshot_export`
- `meta_export_runs`

信用需給やイベント系テーブルが source DB に存在する場合は optional source として取り込む。存在しない環境では export 失敗にしない。

## canonical keys

日次価格系の主キーは `code + trade_date` とする。月次系の主キーは `code + month_key` とする。ranking snapshot は `trade_date + code + ranking_family` とする。trade history は `account_or_broker + code + event_ts + seq` を標準主キーとする。anchor window は `anchor_type + code + anchor_date` を主キーとする。

publish の識別子は `publish_id` とする。生成規則は `pub_{as_of_date}_{utc_timestamp}_{sequence}` に固定する。`as_of_date` は publish 対象の取引日、`utc_timestamp` は publish 実行時刻、`sequence` は同一時刻衝突回避用である。

同一 `as_of_date` での再 publish を許容する。latest successful publish として参照されるのは常に result DB 内の `publish_pointer` テーブルが指す 1 件のみである。JSON pointer は採用しない。

## canonical calendar

営業日計算は JPX 取引カレンダーを canonical とする。rolling horizon の `5/10/20/40/60` はすべて JPX 営業日数で定義し、anchor window の `-20..+20` も同じ JPX カレンダーで数える。

土日祝の単純除外や自然日ベースの代替計算は禁止する。future leakage を防ぐため、学習・評価・window 切り出し・freshness 判定のすべてで同一の JPX カレンダーを使う。

## sector classification

sector relative の基準分類は、初期実装では東証 33 業種を canonical とする。source DB 側に sector 情報がある場合はそれを正規化して使い、欠損や未対応銘柄は `UNKNOWN_33` を明示的に割り当てる。

将来ほかの分類系を追加しても、初期実装の canonical が東証 33 業種であることは変えない。

## table contracts

### export DB

`bars_daily_export` は OHLCV と source metadata を持つ日次正規化テーブルである。

`bars_monthly_export` は月次バーと月次補助情報を持つ。

`indicator_daily_export` は MA、ATR、GAP、PPP/ABC、box、volume 系などの指標を持つ。MeeMee 既存列を流用するものと external_analysis で再計算するものが混在するため、列単位 source of truth を明示する。

`pattern_state_export` は PPP/ABC、本数系、box state、event flag のような状態表現を持つ。

`ranking_snapshot_export` は既存 ranking 情報を解析側で参照可能な形に正規化する。

`trade_event_export` と `position_snapshot_export` は trade history と現ポジション関連の正規化済み入力とする。

`meta_export_runs` は export 実行時刻、source signature、row count、max trade date、diff target、schema version を記録する。

### result DB

result DB の公開 schema は Phase 1 から固定する。最低テーブルは次のとおり。

- `publish_pointer`
- `publish_runs`
- `publish_manifest`
- `candidate_daily`
- `candidate_component_scores`
- `state_eval_daily`
- `similar_cases_daily`
- `similar_case_paths`
- `regime_daily`

Phase 1 では `candidate_daily` 以降の実データが未実装でもよいが、空テーブルとして存在しなければならない。これにより MeeMee 側の read-only 契約と schema compatibility を先に固定する。

`publish_pointer` は result DB 内の単一テーブルであり、MeeMee が読む起点である。最小列は `pointer_name`, `publish_id`, `as_of_date`, `published_at`, `schema_version`, `contract_version`, `freshness_state` とする。

## column-level source of truth

列の source of truth は、`MeeMee 流用`, `external_analysis 再計算`, `互換参照のみ` の 3 区分で管理する。各列には `owner`, `primary source`, `read visibility`, `recompute trigger`, `deprecated?` を持たせる。

固定文言として、MeeMee 本体は解析計算を一切行わない。MeeMee 本体は Parquet を直接参照しない。MeeMee 本体は feature store、label store、export DB、ops DB、model registry を読まない。MeeMee 本体は旧解析 worker を再起動しない。bridge は補完計算や代替推論を行わない。

### MeeMee 流用

`OHLCV`, `daily_ma(7/20/60)`, `monthly_bars`, `monthly_ma`, 既存 ranking snapshot の元列、position/trade history の正規化列、AI export metadata は MeeMee source DB を primary source とする。external_analysis はこれらを受け取り、意味を書き換えずに使う。

### external_analysis 再計算

`ma100`, `ma200`, rolling return 系、`mfe/mae`, `days_to_mfe`, `days_to_stop`, cross-sectional rank labels, top-x% labels, sector relative features, regime features, anchor outcomes, candidate scores, similarity embeddings, similarity index 派生列は external_analysis を owner とする。

### 互換参照のみ

旧 `ml_pred_20d`, `phase_pred_daily`, `sell_analysis_daily`, ranking cache 内の合成スコア列は、新基盤の source of truth ではない。移行比較、監視、UI 切替確認のために互換参照してよいが、新規設計の正本として使わない。これらは `deprecated = true` で扱う。

## diff export rules

差分 export の基本単位は `trade_date` と `source row hash` である。新規日付の追加、既存日付の修正、対象 code の追加、ranking snapshot の更新、trade event の追記を検出し、該当範囲だけ再 export する。

`meta_export_runs` には `last_successful_export_at`, `source_db_signature`, `source_max_trade_date`, `table_row_counts`, `diff_reason` を保存する。source signature が変化し、差分判定が安全にできない場合は fail-closed で再同期を要求する。

## recompute triggers

`OHLCV` 修正時は、該当 code の feature、label、anchor window、embedding を invalid とする。`MA/PPP/ABC/box` 計算ロジックの version 更新時は依存列だけ再計算する。trade history の追記時は trade / position 依存特徴だけを再計算する。

optional source の遅延到着時は backfill job を別投入し、通常の daily publish を巻き込まない。再計算トリガは列 owner に基づいて管理する。

## publish contracts

publish は `publish_id + manifest + publish_pointer` の 3 点で管理する。publish 実行時は staging 結果を作成し、必須テーブル、schema version、`as_of_date` 整合、row count、freshness metadata を validation する。validation 成功後に `publish_manifest` を書き込み、最後に `publish_pointer` を atomic に更新する。

MeeMee 本体は `publish_pointer` が指す publish だけを読む。validation 未通過の publish、staging 中の publish、失敗 publish は読まない。staging publish と failed publish は pointer 更新前のため不可視である。

`publish_runs` には `publish_id`, `as_of_date`, `contract_version`, `schema_version`, `status`, `created_at`, `published_at`, `row_counts`, `validation_summary` を保持する。`publish_manifest` には MeeMee 側が表示判定に必要な最小 metadata を保持する。

## MeeMee read contract

MeeMee 本体が読む対象は result DB と `publish_pointer` のみである。Parquet や feature store や label store は読まない。MeeMee は read-only bridge を介して pointer を解決し、該当 publish の result テーブルだけを読む。

MeeMee が読んでよいテーブルは `publish_pointer`, `publish_manifest`, `candidate_daily`, `state_eval_daily`, `similar_cases_daily`, `similar_case_paths`, `regime_daily` のみである。MeeMee が読んではいけない対象は `candidate_component_scores`, `publish_runs`, model registry, ops DB, feature store, label store, export DB, Parquet, external_analysis 内部メタである。

bridge は pointer 解決、publish_id フィルタ、freshness 判定、degrade 分岐のみを行う。補完計算、代替推論、結果の穴埋め、派生スコア生成は行わない。read-only bridge は schema version を確認し、互換性のない publish を採用しない。採用不可の場合は graceful degrade へ落とす。

## failure and stale semantics

no latest successful publish の場合、MeeMee は「外付け解析結果は未公開」を表示し、候補一覧、類似事例、state evaluation を表示しない。CTA は抑制する。通常の DB/UI/閲覧機能は継続する。

freshness の既定閾値は JPX 営業日基準で、2 営業日超過を warning stale、5 営業日超過を hard stale とする。

warning stale の場合、MeeMee は候補一覧、類似事例、state evaluation を表示継続してよい。「解析結果は最新ではありません」を表示する。CTA は抑制する。通常の DB/UI/閲覧機能は継続する。

hard stale の場合、MeeMee は候補一覧と類似事例の表示継続を許容するが、state evaluation は表示しない。「解析結果が古いため参考表示に切替中」を表示する。CTA は抑制する。通常の DB/UI/閲覧機能は継続する。

pointer corruption の場合、MeeMee は候補一覧、類似事例、state evaluation を表示しない。「解析結果ポインタが破損しています」を表示する。CTA は抑制する。通常の DB/UI/閲覧機能は継続する。

manifest mismatch の場合、MeeMee は候補一覧、類似事例、state evaluation を表示しない。「解析結果 manifest が不整合です」を表示する。CTA は抑制する。通常の DB/UI/閲覧機能は継続する。

schema mismatch の場合、MeeMee は候補一覧、類似事例、state evaluation を表示しない。「解析結果 schema が非互換です」を表示する。CTA は抑制する。通常の DB/UI/閲覧機能は継続する。

result DB missing の場合、MeeMee は候補一覧、類似事例、state evaluation を表示しない。「解析結果 DB が見つかりません」を表示する。CTA は抑制する。通常の DB/UI/閲覧機能は継続する。

この文書と上位文書の競合時の優先順位は `REBUILD_MASTER_PLAN.md > ARCHITECTURE_EXTERNAL_ANALYSIS.md > DATA_EXPORT_SPEC.md` とする。競合時は `result DB only`、`MeeMee read-only`、`Parquet internal only`、`publish_pointer table 主体`、`graceful degrade` を優先する。
