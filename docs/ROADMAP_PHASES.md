# ROADMAP_PHASES

## 目的

この文書は、外付け解析刷新を Phase 1 から Phase 4 まで段階的に実装するための完成条件、依存関係、非対象、移行順、rollback 条件を固定する。各 phase は「何ができれば完了か」を具体的に定義し、旧解析 worker の延命を挟まずに前進できる状態を作る。

この文書は上位 3 文書の契約に従う。特に `result DB only`、`MeeMee read-only`、`Parquet internal only`、`publish_pointer table 主体`、`graceful degrade` を破る phase 設計は不採用とする。

## 共通原則

MeeMee 本体は解析計算を一切行わない。MeeMee 本体は Parquet を直接参照しない。MeeMee 本体は feature store、label store、export DB、ops DB、model registry を読まない。MeeMee 本体は旧解析 worker を再起動しない。bridge は補完計算や代替推論を行わない。

各 phase は、前 phase の契約を壊さないことを完了条件に含む。staging publish や failed publish は `publish_pointer` 更新前で不可視であり、MeeMee 本体から見えるのは latest successful publish のみである。

## Phase 1

Phase 1 の目的は、外付け解析の骨格と公開契約を成立させることである。実装完了後に新しい学習結果がなくても、MeeMee が `publish_pointer` を起点に read-only で結果公開層へ接続できる状態を作る。

Phase 1 の必須成果は次のとおりである。

- `external_analysis` runtime 骨格
- export DB と差分 export
- JPX 営業日基準の rolling label 生成
- anchor window 保存
- result DB schema 固定
- `publish_pointer` による atomic publish
- MeeMee read-only bridge
- graceful degrade 基本動作

Phase 1 の完成条件は次のとおりである。result DB に `publish_pointer`, `publish_manifest`, `candidate_daily`, `state_eval_daily`, `similar_cases_daily`, `similar_case_paths`, `regime_daily` が存在すること。未実装テーブルは空でよいが schema は固定されていること。MeeMee 本体が `publish_pointer` の 1 行から latest successful publish を解決できること。publish 不在時、pointer 破損時、result DB 欠損時に graceful degrade すること。

Phase 1 の非対象は、candidate baseline、similarity embedding、champion/challenger、nightly retrain、state evaluation 実データ生成である。

## Phase 2

Phase 2 の目的は、毎営業日の候補抽出を実運用可能にすることである。候補抽出 engine の最初の baseline を作り、result DB を通じて MeeMee に long/short 候補を返せるようにする。

Phase 2 の必須成果は次のとおりである。

- candidate retrieval/ranking baseline
- `candidate_daily` 実データ生成
- `regime_daily` 実データ生成
- candidate 評価指標の nightly 保存
- MeeMee で候補一覧を表示

Phase 2 の完成条件は、毎営業日 `上昇候補 Top20` と `下落候補 Top20` が `candidate_daily` へ publish されること、MeeMee が `publish_pointer` 経由で表示できること、warning stale と hard stale の分岐で CTA 抑制が効くこと、主要評価指標が保存されることとする。

Phase 2 の非対象は、類似形検索、失敗窓ライブラリ、champion/challenger、nightly retrain 自動昇格である。

## Phase 3

Phase 3 の目的は、類似形検索を追加し、「今の形に似た過去事例」と「その後の軌道」を MeeMee で閲覧できるようにすることである。

Phase 3 の必須成果は次のとおりである。

- similarity embedding 生成
- case library 保存
- failure window library 保存
- `similar_cases_daily`, `similar_case_paths` 実データ生成
- MeeMee 詳細画面での類似事例表示

Phase 3 の完成条件は、current 窓に対して成功例と失敗例を含む top-k 類似事例が返せること、MeeMee が read-only でそれを表示できること、hard stale や schema mismatch で適切に degrade することとする。

Phase 3 の非対象は、champion/challenger 自動昇格、nightly retrain の本番採用、state evaluation 高度化である。

## Phase 4

Phase 4 の目的は、外付け解析基盤の継続改善サイクルを確立することである。nightly retrain、champion/challenger、promotion gate、rollback を運用可能にする。

Phase 4 の必須成果は次のとおりである。

- nightly retrain
- champion/challenger registry 運用
- shadow publish
- promotion gate
- rollback 実行手順

Phase 4 の完成条件は、challenger が評価を通過した場合のみ champion に昇格できること、昇格後も MeeMee から見える publish は `publish_pointer` で 1 件に固定されること、rollback により直前安定 publish へ戻せることとする。

Phase 4 の非対象は、RL や自律戦略執行そのものの導入である。それらは将来拡張として残すが、本ロードマップの完了条件には含めない。

## 依存関係

Phase 2 は Phase 1 の publish/read-only 契約と result schema 固定に依存する。Phase 3 は Phase 1 の anchor window 保存と Phase 2 の日次公開経路に依存する。Phase 4 は Phase 2 の baseline candidate engine と Phase 3 の保存契約が安定していることに依存する。

逆方向の依存は禁止する。例えば similarity embedding のために Phase 1 で MeeMee 本体を重くすることや、candidate baseline の都合で `publish_pointer` 契約を変更することは許容しない。

## 移行順

移行順は固定する。

最初に external_analysis 側を新設する。次に差分 export と label 基盤を作る。次に result DB と `publish_pointer` を通じた公開経路を立てる。次に MeeMee UI の読取り先を新しい read-only bridge へ切り替える。最後に旧解析系を監視付きで廃止する。

この順序を守る理由は、本体の軽さと可観測性を保つためである。旧解析 worker の延命や共存は移行順に含めない。

## rollback 条件

各 phase には rollback 条件を持たせる。

Phase 1 rollback は、`publish_pointer` が安定して解決できない、MeeMee が graceful degrade せず例外終了する、result schema が確定しない場合に発動する。この場合はコードを戻すのではなく、新経路の採用を停止し、旧 UI 参照切替を保留する。

Phase 2 rollback は、候補一覧 publish は成立しても MeeMee 表示が壊れる、freshness 分岐が誤る、Top20 生成が継続不能な場合に発動する。rollback 後は latest stable publish を指す `publish_pointer` を維持し、不安定 publish を採用しない。

Phase 3 rollback は、similarity 結果が schema 不整合を起こす、MeeMee 詳細画面で degrade 不能な例外を起こす、failure case を返せない場合に発動する。rollback 後は candidate 経路を維持し、similarity 公開を止める。

Phase 4 rollback は、champion/challenger 昇格後の評価悪化、publish 不安定化、pointer 誤更新、rollback 手順不備が検出された場合に発動する。rollback では stable publish に pointer を戻し、不安定 challenger を採用しない。

## テスト観点

各 phase で最低限次を確認する。

- Phase 1: empty schema を含む result DB で MeeMee が起動し、pointer 不在時に degrade する
- Phase 1: staging / failed publish が MeeMee から不可視である
- Phase 2: Top20 long/short が publish され、MeeMee で表示される
- Phase 2: warning stale と hard stale で CTA 抑制が分かれる
- Phase 3: 類似事例が成功例と失敗例を含む
- Phase 3: similarity 非互換時に解析パネルのみ degrade する
- Phase 4: challenger 採用前後で `publish_pointer` が 1 行起点のまま維持される
- Phase 4: rollback 後に stable publish が再表示される

## 受入条件

この文書の受入条件は、実装者が各 phase の完了条件、非対象、依存関係、移行順、rollback 条件を追加判断なしで実装できることである。特に、いつ UI を切り替えてよいか、いつ rollback すべきか、何を次 phase へ持ち越してよいかが文書だけで分かる状態を合格とする。

この文書と上位文書の競合時の優先順位は `REBUILD_MASTER_PLAN.md > ARCHITECTURE_EXTERNAL_ANALYSIS.md > DATA_EXPORT_SPEC.md > ROADMAP_PHASES.md` とする。競合時は `result DB only`、`MeeMee read-only`、`Parquet internal only`、`publish_pointer table 主体`、`graceful degrade` を優先する。
