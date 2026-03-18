# Purpose

Tradex の state evaluation は既に teacher profile と similarity を使っているが、日次研究で蓄積した `strategy_tag` と `candle combo` の成績を判定重みに戻していない。これを入れると、昨日までに効いていたタグや足形の傾向を、今日の buy/sell 判定へ直接反映できる。

この変更後は、`AI Research` で見えていた tag rollup の強弱が `state_eval_daily` の confidence と理由文にも反映される。動作確認は nightly candidate run と `tests/test_external_analysis_candidate_baseline.py` で行う。

# Progress

- [x] 既存の state eval / teacher / similarity / tag rollup の接続を調査
- [x] 過去 publish の tag rollup を prior として読むローダーを追加
- [x] prior を champion/challenger score と readiness summary に反映
- [x] prior を理由文と summary_json に反映
- [x] targeted pytest と build/import で確認

# Milestones

## Milestone 1

`external_state_eval_tag_rollups` から `as_of_date` より前の実績だけを読み、`side + holding_band + strategy_tag` 単位の prior signal を作る。`needs_samples` や `risk_heavy` のような readiness も signal に織り込む。

完了後は、state eval が今日の特徴量だけでなく、過去研究のタグ成績も参照できる。

## Milestone 2

long/short の champion/challenger score に `tag_prior_signal` と `combo_prior_signal` を追加し、理由文でも `Historically strong combo` のような説明を返せるようにする。

完了後は、似た局面の similarity とタグ研究の両方が confidence に効く。

## Milestone 3

promotion review と shadow summary に prior signal を残し、候補昇格の判断で「過去研究と整合しているか」を追えるようにする。

完了後は、readiness の summary_json から champion/challenger の prior 差分を確認できる。

# Surprises & Discoveries

- `external_state_eval_tag_rollups` は当日 run 後に保存されるので、当日スコアへ直接使うと leakage になる。`as_of_date` より前だけを使う必要がある。
- candle combo は独立テーブルではなく `strategy_tag` として保存されているため、prior も同じキーで扱うのが最小変更になる。

# Decision Log

- 2026-03-14: prior のデータ源は新規 DB ではなく既存の `external_state_eval_tag_rollups` を使う。理由は leakage を避けつつ既存研究結果を再利用できるため。
- 2026-03-14: combo 専用 signal は新規スキーマを増やさず、`strategy_tag` 名から candle/combo 判定して集約する。理由は migration を最小化するため。

# Outcomes & Retrospective

`external_state_eval_tag_rollups` の履歴を使って `tag_prior_signal` と `combo_prior_signal` を作り、state eval の confidence と readiness summary に反映した。これで `AI Research` に出ていたタグ研究が、翌日以降の判定重みに戻るようになった。

テストでは、前日までの combo rollup を seed した時に `state_eval_daily` の理由文と `external_state_eval_readiness.summary_json` に prior 情報が出ることを確認した。次に進めるなら、この prior を UI 側の review 表示にも明示して、昇格判断で見やすくするのが自然。
