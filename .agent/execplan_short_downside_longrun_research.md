# 短期空売りの長時間解析を再現可能にする

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

If `.agent/PLANS.md` exists in this repository, this ExecPlan must be maintained in accordance with it.

## Purpose / Big Picture

空売り判定の改善に向けて、手動実行していた分析を再現可能なCLIへ統合する。変更後は、単発のメモ分析ではなく、同じコマンドで `60上60` の反転確率、下落パターン探索、レジーム別確率校正、2段階エントリー検証、失敗ケース逆解析、保有期間ハザード、ウォークフォワード閾値検証を一括生成できる。成果物CSVを `tmp` に出力し、次回以降の再検証と比較を可能にする。

## Progress

- [x] (2026-02-16 08:20Z) 既存出力CSVと既存分析スクリプトを確認し、互換対象の出力名を確定。
- [x] (2026-02-16 09:05Z) 新規分析スクリプト `tools/analytics/short_downside_research.py` を実装。
- [x] (2026-02-16 09:09Z) スモーク実行で主要CSV出力の生成を確認。
- [x] (2026-02-16 09:11Z) 実装結果を本ExecPlanへ反映。

## Surprises & Discoveries

- Observation: `app/backend/analysis` には過去分析コードがあるが、再現性のあるCLIや成果物管理は未整備。
  Evidence: `analyze_60up60.py` は `report.txt` 出力のみ、他分析も個別テキスト出力。

- Observation: 初回スモークでSQLの別名参照ミス（`joined`内で`b.*`参照）により即失敗した。
  Evidence: `_duckdb.BinderException: Referenced table "b" not found!`

- Observation: `groupby` の `observed` 既定値に関する `FutureWarning` が出たため明示指定が必要だった。
  Evidence: `FutureWarning: The default of observed=False is deprecated ...`

## Decision Log

- Decision: 新機能はバックエンドAPIではなく `tools/analytics` のCLIとして追加する。
  Rationale: 長時間の研究用処理であり、既存運用APIへの影響を避ける方が安全。
  Date/Author: 2026-02-16 / Codex

- Decision: 既存成果物名（`short_rule_search_*.csv` など）を維持しつつ、新規分析CSVを追加する。
  Rationale: 既存の比較フローを壊さず、追加分析だけ拡張できる。
  Date/Author: 2026-02-16 / Codex

- Decision: パネル抽出は `daily_bars` を起点にし、将来3/5/7/10/15/20日窓を同時計算して全分析で再利用する。
  Rationale: 同じ計算の重複を避け、長時間分析の実行コストを一定にするため。
  Date/Author: 2026-02-16 / Codex

## Outcomes & Retrospective

`tools/analytics/short_downside_research.py` の追加により、従来の `60上60`/ルール探索/パターン比較に加え、レジーム校正、2段階エントリー、失敗要因分析、保有ハザード、ウォークフォワード閾値検証を単一CLIで再実行できるようになった。スモークでは全16 CSVを出力し、最終的に警告なしで完走した。

## Context and Orientation

対象は `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb`。主要入力テーブルは `daily_bars`, `ml_feature_daily`, `ml_pred_20d`。`daily_bars` は価格系列、`ml_feature_daily` は移動平均や出来高などの特徴量、`ml_pred_20d` は下落確率予測を持つ。既存の `tools/analytics/full_retrace_impact.py` は単機能CLIで、同様に今回も独立CLIを作る。

この実装で触るファイルは以下。

- `tools/analytics/short_downside_research.py`（新規）
- `.agent/execplan_short_downside_longrun_research.md`（本書更新）

## Plan of Work

新規CLIはまずDuckDBから分析用パネルを1回抽出する。抽出時に将来5/10/20日の最安値・最高値・終値をウィンドウ関数で計算し、短期空売りの利益条件（20日内-10%）と逆行条件（+5%）を再現可能にする。次に `cnt_60_above` を連続日数として算出し、既存要件の `60上60` 検証CSVを再生成する。

続いて、確率合意ルール探索（`p_down` と `p_turn_down`）をグリッドで実施し、`short_rule_search_all.csv` と `short_rule_search_practical.csv` を出力する。さらに定義済みの代表パターン群（R1〜R6）を同一指標で採点し、`short_patterns_curated.csv` と `short_patterns_risk_reward.csv` を出力する。

提案済みの長時間分析5件は追加CSVとして実装する。レジーム別確率校正、2段階エントリー、失敗ケース逆解析、保有期間ハザード、ウォークフォワード閾値最適化をそれぞれ関数化し、CLI実行で一括保存する。

## Concrete Steps

作業ディレクトリは `c:\work\meemee-screener`。

1) 実装:

    python tools/analytics/short_downside_research.py --out-dir tmp --min-sample 120 --walkforward-train-months 18

2) 期待される標準出力例:

    wrote: tmp/reversal_by_cnt60_exact.csv
    wrote: tmp/short_rule_search_practical.csv
    wrote: tmp/short_walkforward_thresholds.csv

3) 失敗時確認:

    python tools/analytics/short_downside_research.py --help

## Validation and Acceptance

受け入れ条件は以下。

- `tmp/reversal_by_cnt60_exact.csv` と `tmp/short_hit_by_cnt60_bin.csv` が再生成される。
- `tmp/short_patterns_curated.csv` に `R1_model_consensus` 行が存在する。
- 新規分析 `tmp/short_regime_calibration.csv`, `tmp/short_two_stage_entry.csv`, `tmp/short_failure_forensics.csv`, `tmp/short_holding_hazard.csv`, `tmp/short_walkforward_thresholds.csv` が生成される。
- CLIが終了コード0で完了する。

## Idempotence and Recovery

本CLIは読み取り主体で、出力先CSVを上書きするため再実行可能。失敗時は部分出力を削除して再実行してもよいし、上書き再実行でも整合性を保つ。DBロックが発生した場合は `stocks.duckdb` を掴んでいるプロセスを停止して再試行する。

## Artifacts and Notes

スモーク実行:

    python tools/analytics/short_downside_research.py --out-dir tmp --start-dt 1735689600 --min-sample 50 --walkforward-train-months 3

生成確認:

    wrote: .../tmp/reversal_by_cnt60_exact.csv
    wrote: .../tmp/short_patterns_curated.csv
    wrote: .../tmp/short_regime_calibration.csv
    wrote: .../tmp/short_two_stage_entry.csv
    wrote: .../tmp/short_walkforward_thresholds.csv

## Interfaces and Dependencies

実装言語は Python。依存は既存環境の `duckdb`, `pandas`, `numpy` を利用する。新規CLIは以下インターフェースを提供する。

    python tools/analytics/short_downside_research.py \
      --db-path <optional duckdb path> \
      --out-dir tmp \
      --min-sample 120 \
      --walkforward-train-months 18

変更履歴:
- 2026-02-16 初版作成。
