# CANDIDATE_ENGINE_SPEC

## 目的

この文書は、candidate engine の責務を `retrieval`, `ranking`, `state evaluation` に分離し、Phase 2 で必要な最小出力を固定する。目的は「毎営業日、20 日前後で期待値の高い候補群を安定供給する」ことであり、MeeMee 本体に特徴量再計算や推論を戻さない。

この文書は既存 6 文書と親仕様に従う。特に `result DB only`、`MeeMee read-only`、`Parquet internal only`、`publish_pointer table 主体`、`graceful degrade` を破らない。

## 固定原則

MeeMee 本体は学習・検証・特徴量再構築を行わない。MeeMee 本体は Parquet を直接参照しない。MeeMee 本体は feature store、label store、export DB、ops DB、model registry を読まない。MeeMee 本体は旧解析 worker を再起動しない。bridge は補完計算や代替推論を行わない。配布ユーザー向けの売買判定更新要求は許容するが、重い評価や研究実行は本体へ戻さない。これらの開発用スタックの正式名称は `Tradex` とし、重い DB と成果物は `G:\Tradex` 配下に保存する。

candidate engine は Tradex 側だけで実行する。MeeMee が受け取るのは publish 済みの `candidate_daily` と `regime_daily` だけである。`candidate_component_scores` は internal / debug 用であり MeeMee は読まない。既存コードに残る `external_analysis` や `toredex` は互換識別子であり、この仕様上の正式名称ではない。

## engine の責務分離

### retrieval

retrieval の責務は、日次 universe から ranking 対象候補を絞り込むことである。最終候補を直接返さず、ranking の入力集合を作る。

retrieval の入力は `feature_frame_daily`, universe 定義, optional regime 情報である。retrieval の出力は `publish_id`, `as_of_date`, `code`, `side_candidate`, `retrieval_score`, `retrieval_reason_codes` を持つ internal テーブルとする。

Phase 2 では retrieval を最小限に保つ。まずは rule-based prefilter + simple learned filter を許容し、全銘柄から long/short 各 300〜800 銘柄程度へ絞る。

### ranking

ranking の責務は、retrieval で絞った候補群を long/short それぞれ期待値順に並べ、公開候補 Top20 を確定することである。

ranking の入力は retrieval 出力、feature、label history、採用中 model である。ranking の公開出力は `candidate_daily` とする。公開順位は long/short 別で 1 から付番する。

### state evaluation

state evaluation の責務は、候補に対して `仕込む`, `待つ`, `見送る` の状態判断を返すことである。ただし Phase 2 の必須公開対象にはしない。Phase 2 では schema 固定のみで、実データ生成は optional とする。

state evaluation が未実装でも candidate engine は成立しなければならない。hard stale 時は state evaluation を MeeMee に表示しない。

## Phase 2 最小出力

Phase 2 で result DB に必須公開するのは次である。

- `candidate_daily`
- `regime_daily`

`state_eval_daily` は schema は存在させるが、Phase 2 の完成条件には実データを含めない。

`candidate_daily` の最小列は次のとおりとする。

- `publish_id`
- `as_of_date`
- `code`
- `side`
- `rank_position`
- `candidate_score`
- `expected_horizon_days`
- `primary_reason_codes`
- `regime_tag`
- `freshness_state`

`regime_daily` の最小列は次のとおりとする。

- `publish_id`
- `as_of_date`
- `regime_tag`
- `regime_score`
- `breadth_score`
- `volatility_state`

## candidate_component_scores

`candidate_component_scores` は ranking の内部内訳を保持する internal 公開補助テーブルである。最低列は `publish_id`, `as_of_date`, `code`, `side`, `retrieval_score`, `ranking_score`, `risk_penalty`, `regime_adjustment`, `reason_codes` とする。

このテーブルは MeeMee が読んではいけない。デバッグ、評価、shadow compare のみで使う。

## model と scoring

Phase 2 の baseline は過剰に複雑化しない。最小構成は、retrieval 用の軽量フィルタと ranking 用の expected return / risk adjusted score でよい。state evaluation は後続 phase へ送る。

ranking が最適化すべき対象は accuracy ではない。最低限、`Recall@20`, `Recall@10`, `月間トップ5捕捉率`, `上位候補の平均ret_20`, `上位候補の平均MFE/MAE`, `DD`, `turnover`, `regime別成績` を評価し、採用モデルはこれらを基準に決める。

## training / evaluation

candidate engine の学習・評価は `LABELING_STRATEGY.md` の purged walk-forward + embargo をそのまま使う。`ret_20`, `rank_ret_20`, `top_1pct_20`, `top_3pct_20`, `top_5pct_20`, `mfe_20`, `mae_20` を評価軸として扱い、future leakage を起こす split は禁止する。

Phase 2 で state evaluation を未実装にしても、candidate ranking の評価は nightly に保存する。保存先は model registry と internal evaluation artifact であり、MeeMee へは渡さない。

## retrieval / ranking / state evaluation の分離条件

実装上、次を禁止する。

- retrieval と ranking を MeeMee 本体内で実行すること
- ranking と state evaluation を単一の UI 依存ロジックへ混ぜること
- state evaluation 未実装を理由に candidate publish を止めること
- `candidate_component_scores` を MeeMee の公開契約に含めること

Phase 2 では ranking 公開を優先し、state evaluation は schema 固定だけに留める。

## result DB 公開規則

MeeMee が読んでよいのは `publish_pointer`, `publish_manifest`, `candidate_daily`, `state_eval_daily`, `similar_cases_daily`, `similar_case_paths`, `regime_daily` のみである。candidate engine 由来で MeeMee に直接見せてよいのは Phase 2 では `candidate_daily` と `regime_daily` だけである。

warning stale の場合、MeeMee は `candidate_daily` を表示継続してよい。hard stale の場合も `candidate_daily` は参考表示として継続してよいが、CTA は抑制する。pointer corruption, manifest mismatch, schema mismatch, result DB missing の場合は表示しない。

## テスト観点

最低限、次のテストを実装する。

- retrieval が日次 universe から絞り込みを行い、ranking へ渡すこと
- ranking が long/short 各 Top20 を `candidate_daily` に保存すること
- `candidate_component_scores` が生成されても MeeMee が読まないこと
- warning stale / hard stale / schema mismatch で candidate 表示挙動が契約どおり分かれること
- state evaluation 未実装でも Phase 2 の candidate publish が成立すること
- purged walk-forward + embargo が candidate 評価で守られること

## 受入条件

実装完了後、開発者は candidate pipeline を実行し、毎営業日の `candidate_daily` と `regime_daily` が publish され、MeeMee が `publish_pointer` 経由でそれらだけを read-only 表示できることを確認できなければならない。MeeMee 本体内での推論や補完計算が不要であることも確認条件に含む。

この文書と上位文書の競合時の優先順位は `REBUILD_MASTER_PLAN.md > ARCHITECTURE_EXTERNAL_ANALYSIS.md > DATA_EXPORT_SPEC.md > LABELING_STRATEGY.md > ROADMAP_PHASES.md > CANDIDATE_ENGINE_SPEC.md` とする。競合時は `result DB only`、`MeeMee read-only`、`Parquet internal only`、`publish_pointer table 主体`、`graceful degrade` を優先する。
