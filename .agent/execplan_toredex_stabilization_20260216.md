# TOREDEX Stabilization and Profitability Improvement (2026-02-16)

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` are updated while implementation proceeds.

If `.agent/PLANS.md` exists in this repository, this ExecPlan follows it.

## Purpose / Big Picture

ユーザー価値は「TRADEX/TOREDEX がリークなしで毎日回り、根拠付きで意思決定し、検証可能な形で利益改善サイクルを回せる」ことです。

今回の変更後は、ユーザーが `run-backtest` を実行すると、従来の結果悪化要因（低品質エントリー、遅い損切り、無根拠な追加）が抑制された状態で、同じ月次ログと KPI を再生成できます。動作確認は、同期間再実行で KPI の改善・再現性（replay hash 一致）・制約違反ゼロを確認することで行います。

## Context and Orientation

主対象は `app/backend/services/toredex_*.py` と `toredex/__main__.py` です。

`SnapshotService` が MeeMee 出力から日次スナップショットを作り、`Policy` が `decision.json` のアクションを作り、`Execution` が約定を反映して `positions` / `daily_metrics` を更新し、`Runner` が Live/Backtest の日次ループを統括します。

改善は次の接続で実施します。

1. 診断を追加して「なぜ負けるか」を定量化する。
2. Policy に品質ゲートと段階的リスク回避を実装する。
3. Execution 側でハード制約（-10%/ -20%）を再確認し、日次の一貫性を保つ。
4. Backtest で複数期間を再実行し、改善の有無を検証する。

## Milestones

### Milestone 1: Evidence (敗因の確定)

既存ランのスナップショットと理由別損益を再集計して、敗因を固定する。ここで新しい仮説を決める。

実行コマンド（作業ディレクトリ `C:\work\meemee-screener`）:

    python tools/analytics/toredex_reason_scorecard.py --season-id tradex_demo_202601_v2

受け入れシグナル:

- 理由 ID ごとの損益寄与が CSV/JSON で取得できる。
- 改善対象のルールを 2-4 個に絞れる。

### Milestone 2: Policy/Runner 修正 (最小で効く変更)

Policy に「品質不足時のエントリー抑制」「保有中の早期悪化撤退」「追加条件の厳格化」を実装する。Runner/Execution 側で整合チェックを壊さない。

受け入れシグナル:

- decision が引き続き deterministic で replay 一致。
- 2/3/5 ユニット、最大3銘柄、-10%/-20% ルールが継続して守られる。

### Milestone 3: Verify (1か月級バックテスト)

少なくとも 2026-01 の1か月、可能なら 2025-10〜2026-01 の連続期間で再実行して KPI を比較する。

実行コマンド例:

    python -m toredex run-backtest --season-id tradex_demo_202601_v6b --start-date 2026-01-01 --end-date 2026-01-31

受け入れシグナル:

- 実行が完走し、runs 配下に daily/monthly 出力が揃う。
- 以前より DD か累積損益のどちらかが改善。
- replay で decision hash が一致する。

## Concrete Steps

1. 既存診断と 2026-01 ランの reason 別損益を再確認し、主要敗因を確定する。
2. `toredex_policy.py` の判定順序を見直し、低品質エントリー抑制と早期撤退条件を追加する。
3. 必要最小限で `toredex_config.json` に閾値を追加し、policyVersion を更新する。
4. `run-backtest` で新 season を実行し、KPI と reason scorecard を再生成して差分確認する。
5. replay 検証で deterministic 性を確認する。

## Validation and Acceptance

検証コマンド（作業ディレクトリ `C:\work\meemee-screener`）:

    python -m toredex run-backtest --season-id tradex_demo_202601_v6b --start-date 2026-01-01 --end-date 2026-01-31
    python -m toredex replay --season-id tradex_demo_202601_v6b --asof 2026-01-31
    python tools/analytics/toredex_reason_scorecard.py --season-id tradex_demo_202601_v6b

期待する観測:

- replay で decision hash が一致する。
- monthly KPI が生成され、`cum_return_pct`, `max_drawdown_pct` が確認できる。
- reason scorecard で `R_CUT_LOSS_HARD` 依存が減る、または総損失が縮小する。

## Idempotence and Recovery

同じ `season_id` に対して同日再実行しても二重約定しないことを前提にする。

失敗時は新しい `season_id`（例: `_v6b_retry1`）で再実行し、既存 season の結果は保持する。既存ランは破壊的に上書きしない。

## Artifacts and Notes

本実装で主に更新される候補:

- `.agent/execplan_toredex_stabilization_20260216.md`
- `app/backend/services/toredex_execution.py`
- `app/backend/services/toredex_policy.py`
- `app/backend/services/toredex_config.py`
- `toredex_config.json`
- `.local/meemee/runs/<season_id>/...`（検証出力）

## Progress

- [x] (2026-02-16 00:00 UTC) ExecPlan を新規作成し、実行方針を固定。
- [x] (2026-02-16 02:15 UTC) `tradex_demo_202601_v2` の reason scorecard 再集計を実施し、主要敗因を確定。
- [x] (2026-02-16 03:40 UTC) `toredex_execution.py` の実現損益反映バグ、`toredex_policy.py` の追加/撤退/資金制約ロジック、`toredex_config.json` の閾値を更新。
- [x] (2026-02-16 06:10 UTC) 2026-01 バックテストと replay 検証を実施し、決定性一致を確認。
- [x] (2026-02-16 11:25 UTC) 2025-10〜2026-01 の追加検証（v6b）を実施し、結果を集計。
- [x] (2026-02-16 11:40 UTC) 成果と残課題を反映して ExecPlan を更新。

## Surprises & Discoveries

- Observation: クローズ約定で実現損益が cash に反映されず、equity が実質リセットされる欠陥があった。
  Evidence: `tradex_demo_202601_v2` では reason scorecard 上で `R_CUT_LOSS_HARD=-2,040,208` が出る一方、monthly KPI が `cum_return_pct=-8.229236` と不整合で、約定ロジック確認で cash 更新式が元本のみ加減算だった。
- Observation: 追加時の既存含み益が高すぎるケースで負けが集中した。
  Evidence: `tradex_demo_202601_v2` の A_ADD 系分析で `pnl_at_add >= 15%` は 2件とも大幅マイナス、`0〜5%` 帯は高勝率。
- Observation: `ENTRY_OK_FALLBACK` が 100% で、upstream の `entryQualified/setupType/entryScore` が欠落していた。
  Evidence: 2026-01 snapshot 1550行を集計し `fallback_ratio=1.0`。
- Observation: 新規を上位Kまで広く取ると `E_NEW_TOPK_GATE_OK` の損失寄与が残る。
  Evidence: v4/v5 集計で同 reason が主要損失源、`newEntryMaxRank=1` に絞った v6b では当該損失が実質消失。

## Decision Log

- Decision: Execution の cash 更新を「クローズ時は exit/avg 比率で実現損益反映」に変更した。
  Rationale: 月次KPIと理由別実現損益の不整合を解消し、-20% 判定や stage 判定を正しく機能させるため。
  Date/Author: 2026-02-16 / Codex
- Decision: 追加ロジックに `addMaxPnlPct` と「同日 reduce 後の再追加禁止」を導入した。
  Rationale: 伸び切り追撃と同日 churn が損失源だったため。
  Date/Author: 2026-02-16 / Codex
- Decision: ランキング外銘柄を `X_EXIT_GATE_NG` で撤退するルールを導入した。
  Rationale: snapshot 上で評価不能になった保有を放置すると hard cut に到達しやすかったため。
  Date/Author: 2026-02-16 / Codex
- Decision: 新規は `maxNewEntriesPerDay=1` かつ `newEntryMaxRank=1` に絞る構成（v6b）を採用した。
  Rationale: `E_NEW_TOPK_GATE_OK` の損失寄与を抑制しつつ、月次の勝率を改善できたため。
  Date/Author: 2026-02-16 / Codex

## Outcomes & Retrospective

主要不具合（実現損益の未反映）を解消し、Policy の過剰追加と低品質新規を抑制した結果、再現性を維持したまま成績が改善した。

最終検証セット（v6b, 2025-10〜2026-01）では、月次リターンは `[-5.721, +33.825, +6.352, +17.162]`、平均 `+12.904%`、プラス月 3/4 だった。

replay は `tradex_demo_202601_v6b` で hash 一致を確認済み。

残課題は 2点ある。1つ目は 2025-11 の `max_drawdown_pct=-20.282` で、累積損益はプラスでもドローダウンが大きい。2つ目は snapshot 側の `ENTRY_OK_FALLBACK` 依存で、MeeMee の gate 入力品質を上げない限り Policy 側だけでは改善限界がある。

