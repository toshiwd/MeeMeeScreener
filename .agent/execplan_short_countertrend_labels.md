# Short Countertrend Label Expansion

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

If `.agent/PLANS.md` exists in this repository, this ExecPlan must be maintained in accordance with it.

## Purpose / Big Picture

売り（下落）側の機械学習ラベルが、ユーザー運用方針である「逆張り空売り（高値圏で積み増し）+ サポート割れ追撃」に整合していなかった。  
この変更により、下落ラベルをパターン別に分解し、`p_down` も `turn_down` 系シグナルを反映した値に更新する。  
結果として、ランキングと詳細分析で下落側の判定が「1-p_up の補完値」依存から改善される。

## Progress

- [x] (2026-02-15 12:40Z) `ml_label_20d` に下落パターン別ラベル列を追加するスキーマ変更を実施。
- [x] (2026-02-15 12:52Z) 逆張り高値圏/サポート割れ追撃の条件を使って `turn_down_label_*` を再定義。
- [x] (2026-02-15 13:05Z) 推論時の `p_down` を `turn_down` と合成するロジックに更新。
- [x] (2026-02-15 13:12Z) `/ticker/analysis` が `ml_pred_20d.p_down` を参照するよう取得列とインデックスを更新。
- [ ] 実データで再学習とウォークフォワード評価を実行して、下落指標改善を確認。

## Surprises & Discoveries

- Observation: ローカル `app/backend/stocks.duckdb` の `daily_bars` が空で、ラベル分布の事前検証ができなかった。  
  Evidence: `SELECT count(*) FROM daily_bars` が `0`。

- Observation: `get_ml_analysis_pred` は `p_down` 列を取得しておらず、API 側で `1-p_up` を再計算していた。  
  Evidence: `app/backend/infra/duckdb/stock_repo.py` の select list に `p_down` が未定義だった。

## Decision Log

- Decision: 上昇側 `turn_up_label` の閾値は維持し、下落側のみ新方針を適用。  
  Rationale: 依頼は下落精度改善であり、買い側性能の回帰リスクを避けるため。  
  Date/Author: 2026-02-15 / Codex

- Decision: 下落成功条件を `+5%逆行で失敗`、`10日/20日で-10%成功`、`5日で-5%成功` とした。  
  Rationale: ユーザー要件（撤退5%、短期5日利確、主成功10%）を満たしつつ5日ラベルの枯渇を避ける。  
  Date/Author: 2026-02-15 / Codex

- Decision: `p_down` は `1-p_up` と `p_turn_down_10` の加重平均（0.40/0.60）にする。  
  Rationale: 既存API/DB互換を保ったまま、下落継続確率を判定へ反映できる最小変更。  
  Date/Author: 2026-02-15 / Codex

## Outcomes & Retrospective

ラベル設計と推論出力は下落戦略に合わせて更新できたが、実DBが空のため性能検証は未実施。  
次の最重要タスクは、実データ再学習後に `down_mean_ret20_net` と `down_win_rate` の改善を確認し、必要なら逆張り条件の閾値を再調整すること。

## Context and Orientation

本変更の中核は `app/backend/services/ml_service.py`。ここでラベル生成（`refresh_ml_label_table`）と予測（`_predict_frame`）を行う。  
`app/backend/infra/duckdb/stock_repo.py` の `get_ml_analysis_pred` は詳細API用の予測取得を担当し、`app/backend/api/routers/ticker.py` がレスポンス整形を行う。  
「逆張り高値圏」は直近レンジ上部かつMA20近辺上側を指し、「サポート割れ追撃」は直近安値帯を下抜けた初動を指す。

## Plan of Work

`ml_service.py` に下落パターン別ラベル列（reversion/break）を追加し、`turn_down_label_5/10/20` はそれらのORで生成する。  
ラベル判定に必要な高値/安値を `daily_bars` から取得するため、ラベル作成クエリを `h/l` 付きに変更する。  
予測時は `p_down` を `1-p_up` 固定にせず、`p_turn_down_10` と合成して下落側の信号を強化する。  
最後に詳細API取得で `p_down` を直接返し、フロントでの解釈が新ロジックを反映するようにする。

## Concrete Steps

作業ディレクトリ: `C:\work\meemee-screener`

1. `app/backend/services/ml_service.py` を編集して、スキーマ拡張、ラベル定義追加、予測時 `p_down` 合成を実装する。  
2. `app/backend/infra/duckdb/stock_repo.py` の `get_ml_analysis_pred` に `p_down` 選択列を追加する。  
3. `app/backend/api/routers/ticker.py` の `get_analysis_pred` で列インデックスを更新し、`p_down` を優先採用する。  
4. `git diff -- <path>` で列順/値順とインデックス整合を目視確認する。

## Validation and Acceptance

この変更単体では再学習実行が必要なため、受け入れ条件は次の通り。  
`predict_for_dt` 実行後に `ml_pred_20d.p_down` が `1-p_up` と完全一致しない銘柄が存在すること。  
`/api/ticker/analysis` で返る `item.pDown` が `ml_pred_20d.p_down` を反映していること。  
`ml_label_20d` に `turn_down_reversion_label_*` と `turn_down_break_label_*` 列が存在し、値が投入されること。

## Idempotence and Recovery

スキーマ変更は `ADD COLUMN IF NOT EXISTS` のため再実行安全。  
ラベル再生成は `refresh_ml_label_table` が対象期間を削除して再投入するため冪等。  
ロジックを戻す場合は、`ml_service.py` の下落ラベル条件と `p_down` 合成を旧式（`1-p_up`）へ戻せばよい。

## Artifacts and Notes

主要差分:

    app/backend/services/ml_service.py
    - LABEL_VERSION 4
    - turn_down_reversion_label_* / turn_down_break_label_* 追加
    - refresh_ml_label_table の下落条件を逆張り+追撃へ変更
    - _predict_frame で p_down を turn_down と合成

    app/backend/infra/duckdb/stock_repo.py
    - get_ml_analysis_pred で p_down 列を取得

    app/backend/api/routers/ticker.py
    - get_analysis_pred の列インデックス更新
    - pDown を row の p_down 優先で返却

## Interfaces and Dependencies

既存依存のまま `duckdb`, `numpy`, `pandas` を利用する。  
新しい外部ライブラリは追加しない。  
永続化インターフェースは既存テーブル `ml_label_20d`, `ml_pred_20d` を拡張して互換運用する。
