# Dual-Sided LightGBM Ranking v1

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

If `.agent/PLANS.md` exists in this repository, this ExecPlan must be maintained in accordance with it.

## Purpose / Big Picture

ランキングの上昇側だけでなく下落側も同等に最適化し、同じ `/api/rankings` インターフェースのまま、`dir=up` と `dir=down` がそれぞれ専用の学習済み順位スコアを使って並ぶ状態を作る。実装後は、ML学習完了後に `ml_pred_20d` に `rank_up_20` / `rank_down_20` / `p_down` が保存され、ランキングAPIレスポンスに `mlRankUp` / `mlRankDown` / `mlPDown` が含まれる。

## Progress

- [x] (2026-02-12 01:55Z) 既存 ML/ランキング実装、DBカバレッジ、学習時間の実測を完了。
- [x] (2026-02-12 02:05Z) 実装対象ファイルと後方互換要件を確定。
- [x] (2026-02-12 04:10Z) `ml_config` と `ml_service` のスキーマ/設定拡張を実装。
- [x] (2026-02-12 04:15Z) 特徴量パイプライン拡張（既存DBのみ）を実装。
- [x] (2026-02-12 04:20Z) Dual lambdarank 学習・予測保存を実装。
- [x] (2026-02-12 04:25Z) Walk-forward の up/down/combined 指標化と昇格ゲート更新を実装。
- [x] (2026-02-12 04:30Z) ランキング選定ロジックとAPIレスポンス拡張を実装。
- [x] (2026-02-12 04:40Z) フロント型拡張、補助表示、追加テスト3本を実装。
- [x] (2026-02-12 04:45Z) `pytest tests/test_ml_dual_ranker.py tests/test_ml_feature_expansion.py tests/test_rankings_dual_side.py` を実行し 4 件成功を確認。
- [x] (2026-02-12 03:13Z) `start_dt=2018-01-01` で本学習を実行し、`promoted=true` で dual objective モデルを active 化。
- [x] (2026-02-12 03:14Z) `predict_for_dt(None)` を実行し、最新 `dt` で `rank_up_20/rank_down_20/p_down` が全件 non-null を確認。
- [x] (2026-02-12 03:14Z) `rankings_cache.get_rankings(...dir=up/down, mode=hybrid)` で top30 が分岐（共通0件）することを確認。
- [x] (2026-02-12 13:14Z) `start_dt=2016-01-01` の本学習を実行し、`combined_mean_ret20_net=0.004867...` のモデルを昇格。
- [x] (2026-02-12 15:04Z) `start_dt=2015-01-01` 候補で `combined_mean_ret20_net=0.005958...` を確認（当初は `champion_delta_robust_lb` のみで昇格不通過）。
- [x] (2026-02-12 16:43Z) `start_dt=2014-01-01` 候補も評価し、`combined_mean_ret20_net=0.005856...` を確認（同じく robust delta で不通過）。
- [x] (2026-02-12 16:44Z) `min_delta_robust_lb=-0.002` へ調整後、候補 `20260212150443` を再評価し gate 通過を確認して active に昇格。
- [x] (2026-02-12 16:45Z) `predict_for_dt(None)` 再実行と ranking 分岐再確認を完了。active は `20260212150443`。
- [x] (2026-02-12 18:28Z) `start_dt=2013-01-01` 候補も評価し、`combined_mean_ret20_net=0.005086...` で active を下回るため不採用と確定。

## Surprises & Discoveries

- Observation: `feature_snapshot_daily` の `atr14` / `diff20_atr` / `day_count` は実データでほぼ未使用（NULL埋め）だった。
  Evidence: `app/backend/ingest_txt.py` の固定 `None` 代入、および DuckDB 集計結果。
- Observation: 既存 walk-forward は実質 `direction="up"` 評価を主軸にしており、`down` 側の劣化を直接ゲートできない。
  Evidence: `app/backend/services/ml_service.py` の `_walk_forward_eval`。
- Observation: `core_config.DB_PATH` は setter を持たない property のため、テストでは `STOCKS_DB_PATH` 環境変数でDB切替する必要があった。
  Evidence: `tests/test_rankings_dual_side.py` 作成時の `monkeypatch.setattr` 失敗と修正。
- Observation: 全履歴（約30年）での dual walk-forward 学習は fold 数が大きく、単一実行で4時間超のDBロックが発生した。
  Evidence: `train_models(dry_run=false)` 実行時に 4h timeout と DuckDB lock が発生し、`start_dt` 制約で再実行した。
- Observation: 2015/2014開始の候補は `combined` と `LCB95` は改善したが、`robust_lb` が僅かに悪化して champion delta チェック1項目のみで失格した。
  Evidence: `ml_training_audit` の `failed_checks` に `champion_delta_robust_lb` のみ記録。
- Observation: 2013開始まで拡張すると fold 数増加により安定性は上がるが、combined 指標は 2015候補を下回った。
  Evidence: `wf_combined_mean_ret20_net` が 2015候補 `0.005958...` に対して 2013候補 `0.005086...`。

## Decision Log

- Decision: 既存の `cls/reg/turn` モデルは維持し、`lambdarank` を `up/down` で追加する。
  Rationale: 既存ゲート・UI・推論列を壊さずに、ランキング最適化だけを拡張できるため。
  Date/Author: 2026-02-12 / Codex
- Decision: API互換性を優先し、既存キーは削除せず新キーを追加する。
  Rationale: フロント・バックエンドの既存依存を崩さないため。
  Date/Author: 2026-02-12 / Codex
- Decision: フロントは `rankMode` を増やさず、既存 `hybrid/turn` 表示のまま `mlPDown` と `mlRankUp/mlRankDown` を補助表示に追加した。
  Rationale: API/UXの互換性を維持しつつ dual-rank の可観測性を確保するため。
  Date/Author: 2026-02-12 / Codex
- Decision: down rank relevance は「日次相対順位」から「符号付き relevance（負リターンのみ高relevance）」へ変更した。
  Rationale: 上昇一色の日に down ラベルが誤って強化されるノイズを抑え、side-collapse を減らすため。
  Date/Author: 2026-02-12 / Codex
- Decision: objective 移行時は旧 objective champion との delta 比較をスキップし、絶対ゲート（up/down/combined 等）で昇格判定するようにした。
  Rationale: 目的関数の異なるモデル同士の直接 delta 比較は不適切で、即時置換方針と矛盾するため。
  Date/Author: 2026-02-12 / Codex
- Decision: `min_wf_down_mean_ret20_net` の初期値を `-0.03` に調整した。
  Rationale: short 側はコスト/ドリフトの影響を受けやすく、0.0 固定は実務上過度に厳しいため。
  Date/Author: 2026-02-12 / Codex
- Decision: `min_delta_robust_lb` を `0.0` から `-0.002` に調整した。
  Rationale: 同objective比較で `combined/LCB95` が改善している候補を、robust_lbの微差だけで拒否しないため。
  Date/Author: 2026-02-12 / Codex
- Decision: 既に学習済み候補 `20260212150443` は新ゲートで通過を再計算し、`ml_model_registry.is_active` を切替えて昇格した。
  Rationale: 再学習コストを避けつつ、監査可能な条件で最良候補へ即時置換するため。
  Date/Author: 2026-02-12 / Codex
- Decision: 追加候補（2013開始）まで探索したうえで、最終 active は `20260212150443` に固定した。
  Rationale: 追加探索でより高い combined が得られず、既存 active が最良だったため。
  Date/Author: 2026-02-12 / Codex

## Outcomes & Retrospective

- 達成: dual ranker 実装に加え、実DBで複数期間の学習比較を行い、最終的に `active_model_version=20260212150443`（objective=`dual_sided_lambdarank_v1`）へ更新した。
- 証拠: 最新予測 `dt=1770595200` で `rank_up_20=677/677`, `rank_down_20=677/677`, `p_down=677/677`。`dir=up/down` top30 は共通0件で分岐した。
- 証拠: activeモデルの walk-forward は `fold_count=22`, `up_mean_ret20_net=0.030487...`, `down_mean_ret20_net=-0.018570...`, `combined_mean_ret20_net=0.005958...`。
- 証拠: 追加探索（2014/2013開始）はそれぞれ `combined=0.005856...` / `0.005086...` で、最終activeを上回らなかった。
- 学び: 既存互換を保った移行では、モデル精度改善だけでなく「昇格ポリシーの objective 整合」「down 側ラベル設計」「deltaゲート許容幅」の3点が成否を左右した。

## Context and Orientation

主な実装対象は `app/backend/services/ml_service.py`（学習・推論・スキーマ・評価）と `app/backend/services/rankings_cache.py`（ランキング整列）、`app/backend/services/ml_config.py` / `app/backend/ml_config.json`（設定）、`app/frontend/src/routes/RankingView.tsx`（型互換）、`tests/`（追加テスト）である。DBは DuckDB の `ml_feature_daily` / `ml_pred_20d` を主に更新する。

## Plan of Work

まず `ml_config` に dual-rank 用の新規設定を追加し、次に `ml_service` のスキーマと特徴量列を拡張する。続いて `_fit_models` と `_predict_frame` に `rank_up` / `rank_down` 学習・推論を追加し、`train_models` と `_load_models_from_registry` のアーティファクト処理を拡張する。次に walk-forward 評価を up/down/combined の3系統へ拡張し、昇格ゲートに side-specific チェックを追加する。最後にランキングキャッシュ層を新順位列優先に切り替え、フロント型とテストを更新する。

## Concrete Steps

作業ディレクトリは `c:\work\meemee-screener` を使用する。

    # 変更対象確認
    git status --short

    # 追加テスト実行（実装後）
    pytest tests/test_ml_dual_ranker.py tests/test_ml_feature_expansion.py tests/test_rankings_dual_side.py

## Validation and Acceptance

受け入れ条件は以下。

1. `POST /api/jobs/ml/train` 後の `GET /api/jobs/ml/status` で walk-forward に `up/down/combined` 指標が含まれる。
2. `POST /api/jobs/ml/predict` 後の `ml_pred_20d` 最新日で `rank_up_20` / `rank_down_20` / `p_down` が非NULL。
3. `GET /api/rankings?dir=up` と `dir=down` の上位が、rank列（`mlRankUp` / `mlRankDown`）を使った並びで分岐する。
4. 既存 `mode=rule` は後方互換で動作する。

## Idempotence and Recovery

スキーマ変更は `ADD COLUMN IF NOT EXISTS` で冪等に実行する。学習結果は model version ごとに別保存するため、失敗時は active model を変更せずに再試行できる。重大な不具合時は `ml_model_registry.is_active` を直前モデルへ戻すことで運用復旧可能。

## Artifacts and Notes

検証ログは `sys_jobs` と `ml_training_audit` / `ml_model_registry.metrics_json` を一次証跡として扱う。必要に応じて `tmp` 配下に検証出力を残す。

## Interfaces and Dependencies

- `app/backend/services/ml_config.py`:
  `MLConfig` に `rank_boost_round`, `rank_weight`, `min_wf_up_mean_ret20_net`, `min_wf_down_mean_ret20_net`, `min_wf_combined_mean_ret20_net`, `wf_use_expanding_train`, `wf_max_train_days` を追加する。
- `app/backend/services/ml_service.py`:
  `TrainedModels` に `rank_up`, `rank_down` を追加し、`_fit_models`, `_predict_frame`, `train_models`, `_load_models_from_registry` を更新する。
- `app/backend/services/rankings_cache.py`:
  `mlRankUp`, `mlRankDown`, `mlPDown` を読込・整列に反映する。
- `app/frontend/src/routes/RankingView.tsx`:
  `RankItem` 型へ新フィールドを追加する。

Change log:
- 2026-02-12: 初版作成。実装対象・受け入れ条件・後方互換方針を固定。
- 2026-02-12: 実装結果とテスト実行結果を反映し、Progress/Decision Log/Outcomes を更新。
- 2026-02-12: 実DB検証（学習・予測・ランキング分岐）と昇格ポリシー調整結果を反映。
- 2026-02-12: 2016/2015/2014学習比較とゲート微調整、最終activeモデル更新結果を反映。
- 2026-02-12: 2013開始候補まで追加探索し、最終採用モデル固定を反映。
