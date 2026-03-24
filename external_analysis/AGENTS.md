# external_analysis AGENTS.md

## 責務
- MeeMee 本体と分析基盤の境界を守る。
- source export、result DB、publish manifest/pointer、read-only consumption を分離する。
- analysis 側の内部 artifact は analysis 側に閉じる。
- MeeMee は publish 済み contract だけを読む。

## 境界
- export は source data から analysis 用データを作る。
- result DB は analysis の staging/result store。
- manifest / pointer は MeeMee へ渡す契約面。
- read-only consumption は MeeMee が契約済み成果のみ読む入口。
- internal artifact、intermediate table、temp file、staging row は MeeMee の入力にしない。
- publish 切替は契約単位で原子的に行う。
- 部分更新や途中状態を MeeMee に見せない。

## 禁止
- MeeMee 本体から internal artifact へ直接依存する。
- publish 前のデータを MeeMee に読ませる。
- 中間テーブルや一時生成物を公開契約として扱う。
- analysis の内部都合を app 側へ押し出す。
- 切替不能な暫定 fallback を本番契約にする。

## 変更前
- 症状、原因仮説、変更対象、非対象、影響範囲、回帰リスク、検証手順を整理する。
- どの成果物が publish contract で、どれが内部 artifact かを先に区別する。
- 3 ファイル以上や public contract 変更は plan 先行。

## 計画が必要な時
- export / result / publish / consumption のどこかを変える時。
- publish 形式や pointer の切替方法を変える時。
- MeeMee 本体との契約を変える時。
- read-only bridge の入力や出力を変える時。

## 停止条件
- 内部 artifact を読ませないと進めない時。
- publish の原子的切替が守れない時。
- 中途半端な状態を MeeMee に見せるしかない時。
- 契約境界が曖昧なまま実装するしかない時。

## 検証
- publish 済み contract だけで MeeMee が動くことを確認する。
- 内部 artifact への直接依存が増えていないことを確認する。
- 切替が原子的で、中間状態を見せないことを確認する。
- 未検証は未検証と明記する。

## Done
- MeeMee が読む契約面が明確になっている。
- 内部 artifact と公開契約が分離されている。
- 検証結果と残件を明記した。
