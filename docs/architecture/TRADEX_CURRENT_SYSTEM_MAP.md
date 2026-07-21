# TRADEX 現行システムマップ

## 責務境界

- TRADEX: 仮説生成、バックテスト、比較、画像ラベルの集計、JSON成果物、GO/HOLD/REJECT。
- MeeMee: チャート表示と既存画面経由のスクリーンショット取得、運用画面での確認。
- 今回の研究は TRADEX 所有。MeeMeeへの反映・ランキング変更・ランタイムDB書込みは非対象。

## 主要構成

| 領域 | 確認済みの入口 | 出力 / 役割 |
| --- | --- | --- |
| 実行データ | `%LOCALAPPDATA%\MeeMeeScreener\data\stocks.duckdb` | `daily_bars`、月足、特徴量、相場環境、銘柄・業種・貸借関連 |
| 汎用研究 | `app\backend\tools\tradex_research_runner.py` | champion/challenger比較・セッション成果物 |
| 個別仮説 | `scripts\tradex_*.py` | 原則 `G:\Tradex\<axis_id>\...\*.json` |
| 画像取得計画 | `scripts\tradex_detail_clean_screenshot_purpose_plan_v1.py` | 画像取得対象とバッチコマンド |
| 画像取得 | `scripts\meemee_detail_clean_screenshot_batch_v1.mjs` | MeeMeeが描画した日足詳細画像 |
| 画像索引 | `scripts\tradex_labeled_visual_ledger_index_v1.py` | 既存ラベル付き画像のJSONL索引 |
| 画像の利用 | `docs\features\tradex-image-rerank.md` | 数値候補への補助的rerank/veto/boostのみ |

## 現在利用可能な画像証拠

- 短期形状ラベルの既存索引は224画像。ラベル付き短期サンプルと独立holdoutが含まれる。
- この索引の権威JSONは画像単独スコアを `failed_or_dropped_on_holdout` とし、特徴仮説の材料に限定している。
- 買い候補の目的別スクリーンショット計画と、空売り候補の目的別スクリーンショット計画がある。画像取得の新規APIは不要と判断するまで作らない。

## 比較上の注意

- 直近の `chart_entry_geometry_research_v1` は候補発見用の固定契約を持つが、当日終値約定・コスト除外のものがある。最終GO判定のベースラインとは別成果物として扱う。
- すべての次工程で、比較対象・期間・上位K・相場環境・コスト・成果物粒度を固定し、JSONを一次情報とする。
