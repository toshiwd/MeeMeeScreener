# SIMILARITY_ENGINE_SPEC

## 目的

この文書は、similarity engine の設計を固定する。`case_library`, `success/failure separation`, `future-path aligned embedding`, `top-k public output` を明示し、MeeMee が「今の形に似た過去事例」と「その後どう動いたか」を read-only で見られるようにする。

この文書は既存 6 文書と親仕様に従う。特に `result DB only`、`MeeMee read-only`、`Parquet internal only`、`publish_pointer table 主体`、`graceful degrade` を破らない。MeeMee 本体内で特徴量再計算や推論を行う案は採用しない。

## 固定原則

MeeMee 本体は解析計算を一切行わない。MeeMee 本体は Parquet を直接参照しない。MeeMee 本体は feature store、label store、export DB、ops DB、model registry を読まない。MeeMee 本体は旧解析 worker を再起動しない。bridge は補完計算や代替推論を行わない。

similarity engine は external_analysis 側だけで動く。MeeMee が受け取るのは publish 済みの `similar_cases_daily` と `similar_case_paths` のみである。

## case_library

case_library は similarity engine の正本である。case は成功例だけでなく失敗例も必ず保持する。最低物理テーブルは次のとおりとする。

- `case_library`
- `case_window_bars`
- `case_embedding_store`
- `case_generation_runs`

`case_library` の最小列は次のとおりとする。

- `case_id`
- `case_type`
- `anchor_type`
- `code`
- `anchor_date`
- `asof_start_date`
- `asof_end_date`
- `outcome_class`
- `success_flag`
- `failure_reason`
- `future_path_signature`
- `embedding_version`
- `source_snapshot_id`

`case_type` は少なくとも `pre_big_up`, `pre_big_down`, `failed_setup` を区別する。`outcome_class` は long/short 双方向で比較可能な outcome bucket とする。

## success / failure separation

success/failure separation は必須である。success case だけを近傍集合にしてはならない。failed setup は独立 class として保持し、public output でも success/failure を区別して返す。

同一 chart 形状でもその後に失敗した窓を別 case として残すことで、MeeMee で「似ているが失敗しやすい形」を説明可能にする。failure はノイズとして捨てず、学習上も検索上も一次情報として扱う。

## future-path aligned embedding

embedding は見た目類似だけでなく未来軌道整合を優先する。初期定義では、window 形状特徴に加えて `future_ret_5`, `future_ret_10`, `future_ret_20`, `future_mfe_20`, `future_mae_20`, `future_path_signature` を teacher 側へ使い、近い embedding 空間に「その後の軌道が似る窓」を寄せる。

future-path aligned embedding の入力は `feature_window_seq` と `anchor_window_bars` の internal 系列である。MeeMee 本体は embedding 生成も検索も行わない。

Phase 2 でこの embedding を完成させる必要はない。Phase 2 では schema と case_library 土台まで、Phase 3 で実運用検索へ進める。

## 検索単位

検索単位は次の 2 系統を持つ。

- `daily_window_query`
- `anchor_window_query`

`daily_window_query` は `code + as_of_date` に紐づく固定長系列窓である。`anchor_window_query` は event 起点窓である。検索時は query type と case type を混ぜてよいが、public output では origin を区別して返す。

## top-k public output

MeeMee に公開する最小出力は次の 2 テーブルとする。

- `similar_cases_daily`
- `similar_case_paths`

`similar_cases_daily` の最小列は次のとおりとする。

- `publish_id`
- `as_of_date`
- `code`
- `query_type`
- `query_anchor_type`
- `neighbor_rank`
- `case_id`
- `neighbor_code`
- `neighbor_anchor_date`
- `case_type`
- `outcome_class`
- `success_flag`
- `similarity_score`
- `reason_codes`

`similar_case_paths` の最小列は次のとおりとする。

- `publish_id`
- `as_of_date`
- `code`
- `case_id`
- `rel_day`
- `path_return_norm`
- `path_volume_norm`

public output の `top-k` は初期既定で 10 件とする。MeeMee はこの 10 件だけを read-only で表示する。

## internal score と MeeMee 非公開項目

embedding vector、ANN index、neighbor graph、prototype centroid、training loss は internal 専用である。MeeMee は読まない。debug 内訳を返したい場合も `candidate_component_scores` と同様に internal 補助テーブルへ置き、公開契約には含めない。

## Phase 3 完成条件との接続

Phase 3 で similarity engine が満たすべき最低条件は次のとおりである。

- case_library が success/failure を分離保持している
- future-path aligned embedding が存在する
- current query に対する top-k 類似事例を返せる
- `similar_cases_daily`, `similar_case_paths` を result DB に publish できる
- MeeMee が read-only でそれを表示できる

Phase 1 と Phase 2 では similarity を重くしない。schema と case library の基本契約は先に固定してよいが、重い embedding 学習や近傍索引の本格構築は Phase 3 まで持ち込まない。

## leakage guard

similarity engine の leakage guard は 2 層ある。第一に query 構築時の feature が当日以前で閉じること。第二に embedding 学習と検索評価で purged walk-forward + embargo を守ること。

同一 future 区間を共有する case を train/test に跨がせてはならない。anchor 由来 case は `embargo_group_id` に従って fold を分ける。daily window case も future 20 営業日が test と重なる train 窓を purge する。

## result DB 公開規則

MeeMee が読んでよい similarity 系テーブルは `similar_cases_daily` と `similar_case_paths` のみである。warning stale の場合は表示継続してよい。hard stale の場合も参考表示として継続してよい。pointer corruption, manifest mismatch, schema mismatch, result DB missing の場合は表示しない。

success/failure を UI で区別できるように、`success_flag` と `case_type` は public output に必須である。MeeMee 側で embedding から再計算してはならない。

## テスト観点

最低限、次のテストを実装する。

- case_library に success case と failed_setup case の両方が保存されること
- top-k public output が `similar_cases_daily` と `similar_case_paths` に限定されること
- MeeMee が embedding vector や internal index を読まないこと
- daily window と anchor window の query type が区別されること
- purged walk-forward + embargo が similarity 評価で守られること
- hard stale や schema mismatch で similarity パネルだけが degrade すること

## 受入条件

実装完了後、開発者は similarity pipeline を実行し、success と failure を分離した case_library が生成され、`similar_cases_daily` と `similar_case_paths` が publish され、MeeMee が `publish_pointer` 経由で top-k 類似事例だけを read-only 表示できることを確認できなければならない。

この文書と上位文書の競合時の優先順位は `REBUILD_MASTER_PLAN.md > ARCHITECTURE_EXTERNAL_ANALYSIS.md > DATA_EXPORT_SPEC.md > LABELING_STRATEGY.md > ROADMAP_PHASES.md > SIMILARITY_ENGINE_SPEC.md` とする。競合時は `result DB only`、`MeeMee read-only`、`Parquet internal only`、`publish_pointer table 主体`、`graceful degrade` を優先する。
