# TOREDEX Runbook

最終更新: 2026-02-18

## 1. 目的
- TOREDEX (MeeMee連携 paper trading) を、**ネット損益（コスト控除後）**で評価・改善する。
- 本番運用（Champion）と研究運用（Challenger）を分離し、研究設定が本番に混入しないようにする。
- 判断は deterministic policy を維持し、再現可能なログ・設定ハッシュ・コミットで追跡可能にする。

## 2. 運用モード
- Champion（本番）:
  - ロールバック基準は厳格（`max_drawdown_pct <= -8` を含む）。
  - 日次実行の主対象。運用中に大きな閾値緩和はしない。
- Challenger（研究）:
  - ロールバック閾値は緩和可能。
  - 自己改善ループで探索し、Stage2の結果が昇格条件を満たした候補のみChampionへ反映する。

## 3. 主要設定（toredex_config.json）
- `costModel`:
  - `feesBps`, `slippageBps`, `slippageLiquidityFactorBps`, `borrowShortBpsAnnual`
  - `sensitivityBps`（5/10/15bpsの感度）
- `riskGates`:
  - `champion` / `challenger` ごとの閾値
- `portfolioConstraints`:
  - `grossUnitsCap`, `maxNetUnits`, `maxUnitsPerTicker`, `maxPerSector`, `minLiquidity20d`
- `optimization`:
  - Stage0/1/2 の期間月数、試行数、Stage2進出件数

## 4. 連続実行（時刻非依存）
固定時刻実行を前提にせず、目標に到達するまで自己改善ループを連続で回す。

1. 単発サイクル（従来）
   - `python -m toredex self-improve --mode challenger --iterations 12 --stage2-topk 3`
2. 連続サイクル（目標到達まで）
   - `python -m toredex self-improve-loop --mode challenger --iterations 12 --stage2-topk 3 --max-cycles 30 --target-net-return-pct 2.0 --target-score-objective 0.0`
3. 生成物確認
   - `runs/<season_id>/daily/<YYYY-MM-DD>/snapshot.json`
   - `runs/<season_id>/daily/<YYYY-MM-DD>/decision.json`
   - `runs/<season_id>/daily/<YYYY-MM-DD>/ledger_after.json`
   - `runs/<season_id>/daily/<YYYY-MM-DD>/metrics.json`
4. ループ停止条件
   - Stage2合格 + `target-net-return-pct` / `target-score-objective` 達成
   - または `max-cycles` 到達
5. 異常確認
   - `K_POLICY_INCONSISTENT` が 0 件
   - `K_NO_SNAPSHOT` が想定外で増えていないこと

## 5. スケジューラ運用（保留）
19:05固定の外部スケジューラ接続は本フェーズでは必須にしない。必要になった時点で再導入する。

## 6. 週次レビュー（毎週金曜）
- 必須確認:
  - `net_cum_return_pct`（`cum_return_pct` と同値で互換維持）
  - `gross_cum_return_pct`
  - `fees/slippage/borrow` の累積
  - `max_drawdown_pct`
  - `risk_gate_pass`
  - `reasonTop3`
- 集計:
  - `python -m tools.analytics.toredex_reason_scorecard --season-id <season_id>`

## 7. 自己改善ループ（多段評価）
- 実行:
  - `python -m toredex self-improve --mode challenger --iterations 12 --stage2-topk 3`
  - `python -m toredex self-improve-loop --mode challenger --iterations 12 --stage2-topk 3 --max-cycles 30 --target-net-return-pct 2.0 --target-score-objective 0.0`
- 段階:
  - Stage0: 1-2か月（即死条件で落とす）
  - Stage1: 6-12か月（候補絞り込み）
  - Stage2: 3年（正式採点）
- 永続化:
  - `toredex_optimization_runs` テーブルに `config_hash`, `git_commit`, `stage`, `data_range`, `metrics_json`, `artifact_path` を保存
  - 同一 `config_hash + stage + range + mode` は重複実行を避ける

## 8. ロールバック基準
- Champion（厳格）:
  - `cum_return_pct <= 0`
  - `max_drawdown_pct <= -8`
  - `K_POLICY_INCONSISTENT` が1件以上
- Challenger（研究）:
  - `riskGates.challenger` で閾値を設定（Championより緩和可）
  - 昇格は Champion 閾値で再評価してから実施

## 9. 参考コマンド
- 1ヶ月バックテスト:
  - `python -m toredex run-backtest --season-id <season_id> --start-date 2026-01-01 --end-date 2026-01-31 --operating-mode champion`
- 3年バックテスト（Challenger例）:
  - `python -m toredex run-backtest --season-id <season_id> --start-date 2023-02-17 --end-date 2026-02-16 --operating-mode challenger`
- Replay:
  - `python -m toredex replay --season-id <season_id> --asof 2026-01-15`
- Reason集計:
  - `python -m tools.analytics.toredex_reason_scorecard --season-id <season_id>`
