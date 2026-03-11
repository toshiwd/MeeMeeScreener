# Candlestick Cluster + Weekly/Monthly Regime Integration

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

If `.agent/PLANS.md` exists in this repository, this ExecPlan must be maintained in accordance with it.

## Purpose / Big Picture

ユーザーがランキングを見たときに、単純な確率だけでなく「足形の勢い」と「週足・月足の場面（レンジか、抜ける局面か）」を同時に判断できる状態にする。変更後は、ランキング計算がローソク足3本の塊シグナルと週足・月足のレジーム推定を取り込み、画面にもその情報が表示される。

確認方法はランキング画面を開き、各タイルに3本塊/月抜け/月レンジのバッジが表示され、`hybrid` スコアにも反映されることを観察する。

## Progress

- [x] (2026-02-11 04:20Z) 既存実装の調査を完了。ランキング選定ロジックと表示ロジックの接続点を特定。
- [x] (2026-02-11 04:33Z) バックエンドに「3本ローソク塊」「週足/⽉足レジーム推定」を追加し、ランキング項目へ保存。
- [x] (2026-02-11 04:37Z) `hybrid/turn` エントリースコアへ塊/レジーム加点を反映。
- [x] (2026-02-11 04:40Z) フロントのランキングカードに `3本買い/売り`、`月抜け/月下抜け`、`月レンジ` を追加。
- [x] (2026-02-11 04:42Z) `python -m py_compile` と `git diff` で最低限検証を完了。

## Surprises & Discoveries

- Observation: 現行のMLは日足由来特徴量が中心で、週足・月足レジームはランキング時の追加判定にほぼ依存している。
  Evidence: `app/backend/services/ml_service.py` の `FEATURE_COLUMNS` と `app/backend/services/rankings_cache.py` の `snapshot_map`。
- Observation: ランキング画面の「上昇確率」表示は短期確率と長期確率のどちらかを意識して選ばないと、選定ロジックとの見え方がズレる。
  Evidence: `RankingView.tsx` で `mlPUpShort` 優先表示へ変更済み。
- Observation: 月足の取得本数上限が6本だったため、12か月窓のレジーム推定が成立しない銘柄が多かった。
  Evidence: `rankings_cache.py` の `_MONTHLY_LIMIT=6` を `24` に変更して解消。

## Decision Log

- Decision: 今回は再学習必須の大改修ではなく、ランキング層への追加特徴量とスコア反映を先行実装する。
  Rationale: ユーザーが即時に使える改善を先に提供でき、既存DB/学習資産を壊さないため。
  Date/Author: 2026-02-11 / Codex

## Outcomes & Retrospective

- 3本ローソク塊と週/月レジーム推定をランキング計算に追加し、スコア反映とUI表示まで接続した。
- 既存ML再学習に依存しないため、更新直後から場面認識の補助情報を確認できる。
- 今後の拡張としては、同指標を `ml_feature_daily` に取り込み、学習時の説明変数に昇格させると予測精度改善の再現性を評価しやすい。

## Implementation Details

`app/backend/services/rankings_cache.py` に以下を追加する。

まず、日足3本から塊シグナル（上昇塊確率・下落塊確率）を作る関数を実装する。入力は `daily_rows`（`date, o, h, l, c, v`）で、各バーの実体比率・ヒゲ比率・連続性を使って0.0〜1.0のスコアを返す。

次に、クローズ系列からレジーム推定を行う関数を実装する。週足は直近20週、月足は直近12か月を既定窓とし、上抜け確率・下抜け確率・レンジ継続確率を返す。

`_build_cache` で各銘柄の `daily/weekly/monthly` 生成後に新指標を算出し、各timeframeのitem辞書へ保存する。これにより `/api/rankings` のレスポンスへ同梱される。

`_apply_ml_mode` では既存の `bonus` 算出に新指標を加点する。方向が `up` なら上昇塊/週上抜け/月上抜けを、`down` なら下落側を参照する。月レンジ確率が高い時はブレイク優位に小さい減点を入れ、過度なブレイク判定を抑える。

フロントは `app/frontend/src/routes/RankingView.tsx` の `RankItem` 型へ新フィールドを追加し、カード右上に `3本塊`、`月抜け`、`月レンジ` バッジを追加する。表示は `dir` に応じて上方向/下方向確率を切り替える。

## Verification

作業ディレクトリ `c:\work\meemee-screener` で以下を実行する。

    python -m py_compile app/backend/services/rankings_cache.py

期待結果はエラーなし終了。次に差分を確認する。

    git diff -- app/backend/services/rankings_cache.py app/frontend/src/routes/RankingView.tsx .agent/execplan_candle_weekly_monthly.md

期待結果は、バックエンドに新シグナル計算とボーナス反映、フロントにバッジ表示追加、ExecPlan更新が含まれる。

## Rollback

想定外の挙動が出た場合は、今回編集した3ファイルのみを個別に元に戻す。

    git checkout -- app/backend/services/rankings_cache.py
    git checkout -- app/frontend/src/routes/RankingView.tsx
    git checkout -- .agent/execplan_candle_weekly_monthly.md
