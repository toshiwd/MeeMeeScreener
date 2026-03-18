# REBUILD_MASTER_PLAN

## 目的

この文書は、MeeMee の既存解析 worker を延命せず破棄し、外付け解析基盤へ全面刷新するための親仕様である。ここで固定するのは、なぜ作り直すのか、どこまでを MeeMee 本体の責務とし、どこからを外付け解析の責務とするか、どの契約を最優先で守るか、そして旧解析系をどう段階廃止するかである。

この親仕様の目的は、実装者が以後の詳細設計で方針を迷わない状態を作ることにある。後続の戦略文書は、この文書と `docs/ARCHITECTURE_EXTERNAL_ANALYSIS.md`、`docs/DATA_EXPORT_SPEC.md` を破ってはならない。

## 背景と破棄判断

既存解析 worker は、MeeMee 本体に責務が寄り過ぎている。学習、特徴量生成、ランキング計算、補助評価、キャッシュ更新が本体コードと強く結びつき、MeeMee を軽い DB/UI/閲覧ソフトとして保つ設計になっていない。また、月固定ラベルを中核に置いた設計は、上昇や下落の初動兆候を拾いにくく、将来の継続学習や類似局面検索にも不向きである。

そのため、この刷新は部分改修ではなく作り直しとする。旧解析系のコードは仕様理解や移行確認のために参照してよいが、設計判断の基準にはしない。既存構造を前提にした増改築案、互換維持を優先する延命案、MeeMee 本体へ解析責務を戻す案は採用しない。

## 固定前提

MeeMee 本体は DB/UI/閲覧ソフトであり続ける。重い学習、長時間ジョブ、GPU 占有処理、特徴量の大量再計算、埋め込み生成、索引再構築は外付け解析基盤で実行する。MeeMee 本体は解析結果を読むだけに限定する。

MeeMee 本体が読む主契約は result DB のみである。Parquet は external_analysis 内部専用とし、本体 UI は直接参照しない。publish は `publish_id + manifest + publish_pointer` による原子的切替で行い、MeeMee 本体は result DB 内の単一テーブル `publish_pointer` の 1 行を起点に latest successful publish に紐づく結果のみを表示する。JSON pointer は採用しない。

営業日定義は JPX 取引カレンダーを canonical とする。rolling horizon と anchor 窓の営業日数計算は同一カレンダーで統一する。sector relative の基準分類は、初期実装では東証 33 業種を canonical とする。

解析結果が存在しない場合や stale な場合でも、MeeMee 本体は停止しない。graceful degrade を必須とし、解析パネルのみを劣化表示に落とし、本体の DB/UI/閲覧機能は維持する。staging publish と failed publish は `publish_pointer` 更新前であるため、MeeMee からは不可視でなければならない。

## 非目標

この刷新で目指さないものを先に固定する。

本体内での重学習や重特徴量生成はしない。本体 DB への解析結果の常時書戻しはしない。月固定ラベルを主学習ラベルへ戻さない。旧 worker の互換維持を前提にしない。Parquet を MeeMee が直接参照する方式は採用しない。MeeMee 本体と外付け解析を一体化した共存アーキテクチャも採用しない。

固定文言として、MeeMee 本体は解析計算を一切行わない。MeeMee 本体は Parquet を直接参照しない。MeeMee 本体は feature store、label store、export DB、ops DB、model registry を読まない。MeeMee 本体は旧解析 worker を再起動しない。bridge は補完計算や代替推論を行わない。

また、現時点で主目的は候補抽出と類似形検索の土台づくりであり、強化学習や高度な自律最適化そのものを先行実装することではない。それらは将来拡張点として残すにとどめる。

## 最終到達像

最終的な MeeMee は、軽く安定した DB/UI/閲覧ソフトとして動作し続ける。一方、外付け解析基盤は別プロセス・別 DB・差分更新・checkpoint・再開可能・低優先度実行を備え、PC の余剰 CPU/GPU を使って継続的に進化する。

外付け解析基盤は三層の能力を持つ。第一に、20 営業日前後のスイングで期待値の高い候補群を毎日順位付けする候補抽出。第二に、過去の大相場や大下落の直前局面に似た現在の銘柄を見つける類似形検索。第三に、将来的に候補へ対して仕込む、待つ、見送るを状態評価できる拡張性である。

MeeMee 本体はこれらの重い処理を実行しない。publish 済みの最新成功結果だけを読み、UI へ表示する。

## システム責務境界

MeeMee 本体の責務は、source DB の保持、通常の画面操作、チャート閲覧、検索、銘柄詳細、result DB の read-only 読取り、publish freshness の表示に限定する。本体は解析ジョブを起動しない。学習や再学習も行わない。

external_analysis の責務は、source DB からの差分 export、feature store と label store の生成、anchor window 保存、候補抽出モデルの学習と評価、類似形検索用の埋め込み生成と検索索引管理、result DB への publish、ジョブスケジューリング、checkpoint と監視である。

公開契約の境界は result DB と `publish_pointer` テーブルにある。MeeMee 本体は source DB と result DB を読むが、外付け解析の内部 store や Parquet を直接参照しない。MeeMee が読む起点は `publish_pointer` の 1 行のみである。

## 公開契約の優先順位

最優先の契約は「MeeMee は result DB のみ読む」である。次に「publish は publish_id と manifest と `publish_pointer` テーブルによる atomic switch である」。次に「営業日計算は JPX canonical」「sector relative は東証 33 業種 canonical」「graceful degrade は必須」「旧解析系は段階廃止である」が続く。

これらの契約を破る詳細設計は不採用とする。後続文書に矛盾が生じた場合の優先順位は `REBUILD_MASTER_PLAN.md > ARCHITECTURE_EXTERNAL_ANALYSIS.md > DATA_EXPORT_SPEC.md` とする。競合時は `result DB only`、`MeeMee read-only`、`Parquet internal only`、`publish_pointer table 主体`、`graceful degrade` を優先する。

## 段階廃止方針

旧解析系は即時物理削除しない。段階廃止の順序を固定する。

最初に起動を止める。次に更新を止める。次に MeeMee UI の参照先を新しい read-only bridge と result DB に切り替える。その後、freshness と表示品質を監視し、新経路のみで運用可能と確認できた時点で削除へ進む。

この順序を飛ばして物理削除しない。逆に、監視期間中に旧 worker の延命案を差し込むこともしない。旧解析系は比較対象か移行確認用にだけ残し、最終的には撤去する。

## 後続文書への委譲

`docs/ARCHITECTURE_EXTERNAL_ANALYSIS.md` は、この親仕様を具体的な責務境界、データストア分離、publish/read-only フローへ落とす文書である。

`docs/DATA_EXPORT_SPEC.md` は、この親仕様を export/result 契約、source of truth、publish_id 生成規則、freshness、graceful degrade の公開仕様へ落とす文書である。

その後に作成する `LABELING_STRATEGY`、`FEATURE_STRATEGY`、`ROADMAP_PHASES` は、ここで定義した責務境界と公開契約を前提にのみ展開してよい。特に、MeeMee 本体の重量化、旧 worker 延命、Parquet 直接参照、月固定ラベル主軸への回帰は認めない。
