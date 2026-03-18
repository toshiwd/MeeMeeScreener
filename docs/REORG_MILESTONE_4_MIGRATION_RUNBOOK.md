# Reorg Milestone 4: Migration Runbook

## Goal

大規模再編を安全に進めるための実行順、停止順、確認手順、rollback 手順を固定する。この文書は code change の前に順番を固定し、レート制限で作業が中断しても次の着手点が分かるようにする。

## Execution Order

1. DB boundary の固定
   - `ml_feature_daily` など旧解析テーブルの扱いを確定する。
2. 再膨張経路の停止
   - `refresh_ml_feature_table()` 自動呼び出しを止める。
3. 本体参照の切替
   - 旧 `ml_*` 参照を external bridge または degrade に寄せる。
4. runtime boundary の切替
   - app / external_analysis / research / scripts の責務を実装へ反映する。
5. repo layout の再配置
   - ファイル移動、削除、runbook 更新を行う。
6. 最終 cleanup
   - compatibility-only schema や不要スクリプトを削る。

## Checkpoints

checkpoint は大きめ milestone 単位で切る。各 checkpoint で必ず行うことは次である。

- 親 ExecPlan の `Progress` を更新する。
- `Decision Log` に変更多発点を記録する。
- rollback 先が残っていることを確認する。
- 最小 smoke を回す。

## Required Smoke Checks

各 checkpoint 後の最小確認は次である。

    cd C:\work\meemee-screener
    python -c "import app.main"

    cd C:\work\meemee-screener\app\frontend
    npm run build

変更範囲が external_analysis に及ぶときは、その milestone に関係する pytest を最小単位で回す。

## Rollback

危険な変更の rollback は次で固定する。

- live DB を触る前に full backup を別ドライブへ退避する。
- 新しい compact DB や schema 変更は別パスで試し、本番位置へは起動確認後に入れ替える。
- 起動異常が出たら full backup の DB へ戻す。
- graceful degrade が expected か、破壊的例外かを切り分ける。

## Acceptance

この runbook の完了条件は次である。

- implementer が「次に何を止めるか」を迷わない。
- checkpoint ごとの検証手順がある。
- live DB と code change の rollback が明記されている。
- レート制限で中断しても再開順が崩れない。

