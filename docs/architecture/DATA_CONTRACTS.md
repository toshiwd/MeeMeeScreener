# Data Contracts v3

## 目的

MeeMee Screener と TradeX の境界を、データ契約の段階で固定する。

- MeeMee Screener は使う製品
- TradeX は育てる製品
- ranking は見る順番
- execution は入るかどうか
- provisional は表示補助
- confirmed は解析基準
- `published_ranking_snapshot` は runtime cache / audit artifact であり source of truth ではない

## 基本原則

- 契約名は役割で決める
- data source は `source` field で表現する
- provider 名だけで意味を固定しない
- 表示用データと解析用データを分ける
- heavy analysis 用の入力は confirmed 系に限定する
- runtime は declared artifact を読む

## confirmed_market_bars

確定済みの市場バーを表す。

- 主用途: 解析基準、ランキング再計算、チャート表示の基準値
- 必須フィールド:
  - `code`
  - `market_date`
  - `open`
  - `high`
  - `low`
  - `close`
  - `volume`
  - `source`
  - `confirmation_state`
  - `quality`
- `source` は PAN, Yahoo, CSV, manual などの来歴を表す
- `confirmation_state` は confirmed / provisional / unknown のような semantic state を表す
- `quality` は analysis 可否を決める補助判定に使う

### 解析可否

最終的な解析可否は provider 名ではなく semantic fields で決める。

- `confirmation_state` が confirmed であること
- `quality` が分析に十分であること
- provisional 補助は `display_only` 条件で除外すること

provider 名による除外は移行期の互換ガードに留める。

## provisional_intraday_overlay

場中の補助表示専用データを表す。

- 主用途: チャート補助、画面上の参考表示
- 必須フィールド:
  - `code`
  - `overlay_at`
  - `source`
  - `display_only`
  - `freshness_state`
- 付随フィールド:
  - `open`
  - `high`
  - `low`
  - `close`
  - `volume`
  - `fetched_at`
- `display_only` は true を前提とする
- 解析 path には入れない

## financial_facts

共通参照可能な補助情報。

- 主用途: 決算、財務、イベント、品質観点の補助参照
- 取得・保存・API 呼び出しは shared では扱わない
- shared は正規化の pure function だけを持つ

## trade_history_normalized

MeeMee で取り込んだ取引履歴を TradeX が検証・分析できる形に正規化した契約。

- 主用途: import, replay, walk-forward, validation
- broker 固有の表現は正規化で落とす
- raw payload は別管理に分ける

## ranking_output

TradeX の検証・比較・監査用成果物。

- runtime の唯一入力ではない
- MeeMee Screener は local confirmed data + published_logic_artifact から必要に応じて再計算できることを優先する
- execution 情報は含めない

## logic_artifact 系

### published_logic_artifact

実行可能 code 前提ではない declarative artifact。

最低限の field:

- `artifact_version`
- `logic_id`
- `logic_version`
- `logic_family`
- `feature_spec_version`
- `required_inputs`
- `scorer_type`
- `params`
- `thresholds`
- `weights`
- `output_spec`
- `checksum`

### published_logic_manifest

runtime が参照する metadata と pointer 解決用の manifest。

- `logic_id`
- `logic_version`
- `logic_family`
- `status`
- `input_schema_version`
- `output_schema_version`
- `trained_at`
- `published_at`
- `artifact_uri`
- `checksum`

### published_ranking_snapshot

runtime cache / audit artifact。

- source of truth ではない
- boot 時の参照高速化や比較監査のために残す
- MeeMee は snapshot ではなく artifact を主入力にする

### validation_summary

TradeX 側の採用判定 summary。

- backtest
- replay
- walk-forward
- champion / challenger 比較
- publish 可否

## 互換期間の扱い

- provider 名だけで provisional を判断するコードは移行期の互換ガードとして扱う
- 最終的には `confirmation_state` / `quality` / `display_only` の semantic gate に寄せる
- `published_ranking_snapshot` は残してよいが、runtime source of truth にしない

## 関連ドキュメント

- `docs/architecture/RUNTIME_SELECTION.md`
- `docs/features/tradex-publish-flow.md`
- `docs/features/yahoo-provisional-overlay.md`

