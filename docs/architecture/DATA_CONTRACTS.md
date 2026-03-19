# Data Contracts

## 目的

MeeMee と TradeX の境界で、どのデータを「正」とみなし、どのデータを表示専用・派生専用として扱うかを固定する。

## 全体方針

- 生データは捨てない
- 正規化テーブルを分ける
- 画面表示用の派生テーブルやキャッシュは別管理にする
- 日付、銘柄コード、足種は追跡可能なキーにする
- 欠損時の扱いを先に決める
- 重い検証用途の集計は MeeMee 本体ではなく外付けに寄せる

## confirmed_market_bars

確定した株価データの正規系列である。

- 解析、判定、根拠表示に使ってよい本線データ
- 主キー候補は `code + date + interval`
- 日足、週足、月足を扱う
- 調整済み OHLC を本線にする
- 必要に応じて `raw_close` と `adj_factor` を補助列として保持できる
- PAN 取り込み後に更新される確定系列として扱う

運用上の注意:

- 欠損は補完せず、欠損として残す
- 価格の正当性を曖昧にする一時値はここへ混ぜない

## provisional_intraday_overlay

場中の暫定データの重ね合わせである。

- 表示専用
- 解析、判定、ランキング根拠には使わない
- Yahoo 等の値を表示している時だけ UI 上で「暫定」を明示する
- 取得失敗時は `confirmed_market_bars` にフォールバックする
- フォールバック理由は詳細ステータスに残す

運用上の注意:

- 暫定値で分析結果を更新しない
- 暫定値と確定値を同一系列として扱わない

## financial_facts

1 物理テーブルではなく、論理統合ビューとして扱う。

物理分割の想定:

- `edinet / filings`
- `financial_metrics`
- `event_calendar`
- `lending / short interest`

真実ソースの優先順位:

- 法定財務: EDINET
- 適時開示: TDNET
- 権利付き / 権利落ち系: JPX
- J-Quants 等: 補助取得層

予定と実績がずれる場合:

- 予定は予定として残す
- 実績は別レコードまたは別状態で上書きではなく追加する
- 変更履歴が必要なものは、最新状態と差分理由を両方残す
- UI は「予定」「実績」「変更済み」を区別して表示する

## trade_history_normalized

楽天 / SBI の取引履歴 CSV を正規化したテーブルである。

- 現物 / 信用、買い / 売り、約定日、受渡日、手数料、数量、単価などを正規化する
- MeeMee 内部の建玉表記は `売-買` に統一する
- 既存コードの暗黙ルールを壊さない

運用上の注意:

- 元 CSV は別保管し、正規化結果だけで再現できる形にする
- broker 固有の列名や並び替えに依存しない

## position_snapshot_daily

取引履歴から日次建玉を復元した派生契約である。

- trade_history_normalized と役割を分ける
- 現在建玉だけでなく、過去日時点の建玉確認にも使う
- 売り数量、買い数量、平均単価などを持てる形にする

運用上の注意:

- これは履歴の正規化そのものではなく、履歴から作る派生物である
- 再計算タイミングは未確定のため、実装ごとに明示する

## published_logic_artifact / logic_manifest

TradeX で研究・publish されたロジック成果物である。

- MeeMee は publish 済みのものだけを表示・参照する
- `version`, `input_spec`, `threshold`, `description`, `published_at` などを持てる形にする
- `ranking_output` / `published_ranking_snapshot` は監査・比較用であり、MeeMee 本体の解析基準そのものではない

責務分離:

- logic artifact は研究成果の実体
- logic manifest は「どの成果物を公開対象にしたか」を示す最小メタデータ
- MeeMee は manifest を入口にして、公開済み artifact のみを読む

## 類似チャート検索の最低契約

現在局面に近い過去局面を探し、未来のシナリオ候補を観測する。

- 予言ではなく、シナリオ候補提示である
- 左は基銘柄、右は過去の類似銘柄
- 右端は類似起点日基準でそろえる
- 類似度は少なくとも以下を分解して保持する
  - `sim_ma_daily`
  - `sim_ma_weekly`
  - `sim_ma_monthly`
  - `sim_candle_daily`
  - `sim_candle_weekly`
  - `sim_candle_monthly`
  - `total_similarity`

補足:

- 類似銘柄の未来側チャートは、比較用にドラッグ確認できる前提とする
- 次の比較銘柄も保持できる構造にする

## 縮退方針

- provisional 取得失敗時: `confirmed_market_bars` を表示し、失敗理由をステータスに出す
- financial_facts 欠損時: 閲覧継続を優先し、欠損部分だけを非表示または欠損表示にする
- artifact 不整合時: publish 済みの整合が取れた成果物のみを採用し、不整合 artifact は表示しない
- データ 0 件時: 空状態を明示し、無理な補完や推測で埋めない

## Open Questions / TODO

- `financial_facts` の物理分割の粒度
- `position_snapshot_daily` の再計算タイミング
- raw 系列をどこまで保持するか
- 類似度の各成分をどの単位で再集計するか
