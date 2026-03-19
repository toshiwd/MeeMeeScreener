# Yahoo Provisional Overlay

## 目的

場中の補助表示として Yahoo の暫定値を重ねる契約を固定する。

## ルール

- provisional は表示専用
- Yahoo 値を表示している時だけ暫定表示を出す
- 解析、判定、ランキング根拠には使わない
- confirmed 系列を壊さない

## 表示条件

- その日の場中データが取れたときだけ overlay を使う
- 取れない場合は confirmed を表示する
- 暫定値であることは UI 上で明示する

## フォールバック

- provisional 取得失敗時は `confirmed_market_bars` を表示する
- 失敗理由は詳細ステータスに出す
- 失敗を理由に通常の分析表示を止めない

## 実装上の注意

- provisional と confirmed を同一系列として扱わない
- 座標や表示だけを差し替え、判定データには流さない
- キャッシュがある場合も、分析側へは渡さない

## Open Question / TODO

- Yahoo の取得失敗時に出す詳細ステータス文言の最終形
- 半日立会や取引時間外の扱い
