# Candlestick Combo Precision Uplift Research (2026-02-27)

このExecPlanは、売買シグナル精度を上げるために「既存のローソク足3種の再評価」と「追加候補の組み合わせ研究」を、再現可能な手順で実行するための仕様書である。`.agent/execplan_candle_weekly_monthly.md` で導入済みの足形・週足/月足レジーム基盤を前提に、今回は精度改善の研究設計と最小実装までを扱う。

変更後にユーザーが得られる価値は、ランキングの加点/減点が経験則ではなく最新の実証結果に沿うこと、そして四半期ごとの精度ぶれを抑えた採用ルールになることだ。動作確認は、研究スクリプト出力とランキングAPIの差分で行う。

## Repository Orientation

この作業で主に触る場所は次の3系統である。`scripts/` は研究実行、`tmp/` は研究結果、`app/backend/services/rankings_cache.py` は実運用スコア反映である。

- 研究基盤:
  - `scripts/short_horizon_pattern_study.py`
  - `scripts/month_end_pattern_mining.py`
- 研究結果（既存エビデンス）:
  - `tmp/short_horizon_pattern_study_latest.json`
  - `tmp/month_end_pattern_mining.json`
  - `tmp/pullback_candlestick_incremental_scoring.csv`
  - `tmp/pullback_candlestick_combo_with_strong.csv`
  - `tmp/monthly_signal_pairwise_synergy_6m.csv`
  - `tmp/long_short_precision_fullwindow_20260227.json`
- 実運用反映:
  - `app/backend/services/rankings_cache.py`
  - `app/backend/api/routers/ticker.py`

## Scope

対象は「ローソク足/形状シグナルの重み最適化」と「追加候補の採用/不採用判定」である。学習モデル本体の再学習や、新規テーブル追加は対象外とする。

## Evidence Baseline (固定値)

今回の計画は次の既存証拠を固定ベースラインにして進める。

1. `short` の長期品質はマイナス傾向で、`long` はプラス。
   `tmp/short_horizon_pattern_study_latest.json` の `selection` と `tmp/month_end_pattern_mining.json` の `overall` を根拠とする。

2. 直近の短期ショート精度は高い期間と崩れる期間の差が大きい。
   `tmp/long_short_precision_fullwindow_20260227.json` では `down/aggressive` が全体 precision 0.760 だが、2025Q2 は 0.424。

3. 足形候補のうち、サンプル数と改善幅の両立が確認できるのは主に次。
   - 既存採用: `three_white_soldiers`, `bull_engulfing`, `shooting_star_like`
   - 追加研究候補: `morning_star`, `three_black_crows`, `sakata_bullish`, `bear_harami`, `bear_marubozu`

4. 形状の相乗候補として、6か月集計で `f_touch20+f_s60_down` と `f_s20_up+f_s60_down` の `synergy_vs_additive` が正かつ母数が十分。

## Acceptance Criteria

採用判定は「単発の高勝率」ではなく、次の全条件を満たす場合のみ通す。

1. 母数条件:
   候補シグナル単体で `n_flag >= 800`、強コンテキスト内で `n_flag_in_strong >= 1200` を満たす。

2. 改善条件:
   `diff_ret3 > 0` かつ `diff_win3 > 0`。加えて悪化回避として `diff_dd3 >= 0`。

3. 安定性条件:
   四半期分割で `mean_pnl_20d` がマイナスになる四半期を 1 つ以内に制限する。

4. 実運用条件:
   A/B比較で `down` 側の `precision` がベースライン比 +1.5pt 以上、かつ `mean_pnl_20d` を悪化させない。

## Milestone 1: Research Dataset Lock

最初に、評価窓と評価指標を固定する。窓は `2025-01-01` から最新まで、比較窓として `2025Q4` と `recent` を別集計する。指標は `precision`, `mean_pnl_20d`, `precision_top1`, `worst`, `q05` を共通採用する。

この段階の成果物は、研究で使う固定入力セットと、採用閾値を明文化したメモである。これにより後続の実験で「条件の後出し」を防ぐ。

実行コマンド（作業ディレクトリ: `c:\work\meemee-screener`）:

    python scripts/short_horizon_pattern_study.py --output tmp/short_horizon_pattern_study_latest.json
    python scripts/month_end_pattern_mining.py --output tmp/month_end_pattern_mining.json

受け入れシグナルは、両JSONが更新され、`meta.events` と `selection/overall` が取得できることである。

## Milestone 2: Candlestick Candidate Screening

候補足形を3層でふるいにかける。第1層は単体改善、第2層は強コンテキスト内改善、第3層は相乗効果である。第1層と第2層の両方を通過した足形のみを「本命候補」にする。

この段階で、既存3種を維持するか、追加候補を加えるかを決める。採用の判定理由は必ず `tmp/` にCSVまたはJSONで残す。

実行コマンド:

    @'
    import pandas as pd
    c = pd.read_csv("tmp/pullback_candlestick_incremental_scoring.csv")
    s = c[(c["n_flag"]>=800) & (c["diff_ret3"]>0) & (c["diff_win3"]>0) & (c["diff_dd3"]>=0)]
    s.sort_values(["diff_ret3","diff_win3"], ascending=False).to_csv("tmp/candlestick_screen_stage1_20260227.csv", index=False)
    k = pd.read_csv("tmp/pullback_candlestick_combo_with_strong.csv")
    t = k[(k["n_flag_in_strong"]>=1200) & (k["diff_ret3_in_strong"]>0) & (k["diff_win3_in_strong"]>0)]
    t.to_csv("tmp/candlestick_screen_stage2_20260227.csv", index=False)
    '@ | python -

受け入れシグナルは `tmp/candlestick_screen_stage1_20260227.csv` と `tmp/candlestick_screen_stage2_20260227.csv` が生成されること。

## Milestone 3: Pairwise Synergy Integration Plan

`monthly_signal_pairwise_synergy_6m.csv` から母数が十分な相乗ペアのみを抽出し、加点ではなく「重み補正係数」として扱う。理由は、単純加点だと過剰最適化しやすく、四半期ぶれを増やすためである。

この段階では、実装前に補正方針を固定する。

- `f_touch20+f_s60_down`: short側補正を優先。
- `f_s20_up+f_s60_down`: side判定の曖昧局面でのみ補正。

## Milestone 4: Minimal Runtime Implementation

実装は最小変更とし、`rankings_cache.py` の固定加点を「設定可能な候補集合 + 重み」に置き換える。初期値は現行互換にして、候補追加はフラグで段階投入する。`ticker.py` の説明用ボーナス表示も同じ定義を参照する。

編集対象:

- `app/backend/services/rankings_cache.py`
  - 既存 `_ENTRY_BONUS_CANDLE_PATTERN` の単一運用を、`dict[str,float]` ベースへ移行。
  - `shooting_star_like`, `three_white_soldiers`, `bull_engulfing` の既定値は維持。
  - 追加候補は off-by-default。
- `app/backend/api/routers/ticker.py`
  - `candlestickPatternBonus` の内訳を候補別に返せるようにする。

## Milestone 5: Verification and Rollout Decision

検証は「研究結果」と「API挙動」の2系統で行う。最低限、研究再集計とAPIレスポンス差分で採否判断できる状態にする。

実行コマンド:

    python scripts/short_horizon_pattern_study.py --output tmp/short_horizon_pattern_study_latest.json
    python scripts/month_end_pattern_mining.py --output tmp/month_end_pattern_mining.json
    python scripts/verify_sell_signal_loop.py

期待する観察結果:

- `down` 側の `precision` がベースラインより改善、または同等で `mean_pnl_20d` 改善。
- `candlestickPatternBonus` が候補別に追跡可能。
- 追加候補を有効化しても、四半期単位の極端な悪化が増えない。

## Progress

- [x] (2026-02-27) 既存エビデンスの収集を完了。ベースラインJSON/CSVと実装ポイントを特定。
- [x] (2026-02-27) 採用閾値（母数・改善・安定性）を文書化。
- [x] (2026-02-27) Milestone 1 の再実行を完了。`tmp/short_horizon_pattern_study_latest.json` と `tmp/month_end_pattern_mining.json` を更新。
- [x] (2026-02-27) Milestone 2 の候補スクリーニングを完了。`tmp/candlestick_screen_stage1_20260227.csv`（8件）と `tmp/candlestick_screen_stage2_20260227.csv`（2件）を出力。
- [x] (2026-02-27) Milestone 3 の相乗補正仕様を確定。`f_touch20+f_s60_down` と `f_s20_up+f_s60_down` を優先候補に固定。
- [x] (2026-02-27) Milestone 4 の最小実装を完了。`rankings_cache.py` / `ticker.py` に候補別重み計算と内訳返却を追加。
- [x] (2026-02-27) Milestone 5 の準備として `scripts/verify_sell_signal_loop.py` を評価DBで実行し、基準値（legacy/tuned）を記録。
- [x] (2026-02-27) Milestone 5 の最終比較を完了。`tmp/candlestick_ab_q4_up_balanced_20260227.json` と `tmp/candlestick_ab_recent_up_balanced_20260227.json` を作成し、追加候補の有効化は差分 0 のため据え置きと判断。

## Surprises & Discoveries

- `short` は「勝つ条件追加」より「負けやすいレジームを弾く」方が効く構造が強い。
  根拠は `tmp/month_end_pattern_mining.json` の short上位ルール群で、平均リターンが負のルールが大量かつ母数が大きいこと。

- 既存採用の `shooting_star_like` は `diff_ret3` 改善が大きい一方、`diff_win3` 改善が小さいため、勝率軸だけで評価すると過小評価される。

- 実行時に本番DB（`%LOCALAPPDATA%\\MeeMeeScreener\\data\\stocks.duckdb`）がアプリプロセスにロックされ、研究スクリプトが失敗した。回避として `STOCKS_DB_PATH=c:\\work\\meemee-screener\\.local\\meemee\\tmp_eval\\stocks.duckdb` を使う運用に切り替えた。
- 既存の `short_long_precision_*` JSON との差分比較は生成条件の不一致で再現困難だったため、同一評価器でのA/B比較に切り替えた。

## Decision Log

- Decision: 今回は追加候補を即時で本番固定しない。
  Rationale: 四半期ぶれ（特に2025Q2）への耐性を先に確認し、過剰適合を避けるため。
  Date/Author: 2026-02-27 / Codex

- Decision: 候補評価を「単体」「強コンテキスト」「相乗」の3層で行う。
  Rationale: 単体の見かけ優位が本番で消えるリスクを下げるため。
  Date/Author: 2026-02-27 / Codex

- Decision: 追加足形（`morningStar`, `threeBlackCrows`）はフラグ算出のみ先行実装し、重みは0.0でオフにした。
  Rationale: 実運用互換を維持しながら、次回のA/B比較を即実行できるようにするため。
  Date/Author: 2026-02-27 / Codex

- Decision: `morningStar`, `threeBlackCrows` の重みを 0.005 で試験投入して A/B 比較したが、Q4/recent とも差分 0 だったため本採用を見送った。
  Rationale: 指標改善が観測できない状態で重みだけ増やすと、将来の過剰適合リスクだけが増えるため。
  Date/Author: 2026-02-27 / Codex

## Outcomes & Retrospective

Milestone 1〜5 を完了した。研究データ再生成、候補抽出、最小実装、A/B比較まで実施した。結論として、追加候補の有効化は現時点で改善寄与が確認できず、既定重み（既存3種のみ）維持が妥当である。

## Rollback

このExecPlan追加のみの段階ではコード挙動変更はない。以後の実装段階で問題が出た場合は、`rankings_cache.py` と `ticker.py` の変更を個別に戻し、`tmp/` の比較結果を保持したまま再評価する。
