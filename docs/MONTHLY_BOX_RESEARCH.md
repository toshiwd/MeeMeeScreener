# 月足ボックス研究

## 目的

- 月足ボックスを MeeMee の中核研究テーマとして恒久化する。
- 初版の主眼は `月足ボックスの上抜け` と `上抜け失敗` に置く。
- `底値買い` は同じ基盤で secondary phase として扱う。

## source of truth

- box 定義の source of truth は [month_end_shape_study.py](/C:/work/meemee-screener/scripts/month_end_shape_study.py) の `_detect_body_box`
- 月足ボックス研究の実行スクリプトは [monthly_box_breakout_research.py](/C:/work/meemee-screener/scripts/monthly_box_breakout_research.py)
- 5541 の既存研究は [NOTE_TRADE_REPRO_STUDY.md](/C:/work/meemee-screener/docs/NOTE_TRADE_REPRO_STUDY.md) を参照元として残す

## box 定義

- body box
- `min_months=4`
- `max_months=14`
- `range_pct <= 0.20`
- wild wick は除外せず `box_wild` として flag 化する
- `box_month_index` は box 開始月から signal 月までの経過月数で数える
- bucket は `4-5 / 6-8 / 9-12 / 13-14`

## signal 定義

### bottom_entry

- active monthly box 内
- `box_zone in lower/mid`
- 週足が `flat/up`
- 日足は `下ヒゲコマ / 小陽線 / ma20 回復` 系

### breakout_entry

- active monthly box の `upper` または `breakout`
- 週足支持維持
- 日足は `HB` を含む break 系
- climactic day は除外せず flag で保持する

### failed_breakout_exit

- breakout_entry 後 20 営業日以内
- `box upper` を明確に割り込み再侵入
- もしくは `up5_before_dn5` 失敗後の支持割れ

## 失敗類型

- `late_breakout`
- `climactic_exhaustion`
- `reentry_into_box`
- `support_break_after_breakout`
- `weak_volume_break`

優先順位:

- `climactic_exhaustion`
- `late_breakout`
- `weak_volume_break`
- `support_break_after_breakout`
- `reentry_into_box`

## 保存方針

- 再生成レポート:
  - [monthly_box_breakout_research.json](/C:/work/meemee-screener/tmp/monthly_box_breakout_research.json)
  - [monthly_box_breakout_research.md](/C:/work/meemee-screener/tmp/monthly_box_breakout_research.md)
- 恒久ケース表:
  - [monthly_box_research_cases.csv](/C:/work/meemee-screener/docs/monthly_box_research_cases.csv)

## 手動補足ルール

- 1 行 = 1 signal event
- `manual_note` は裁量判断や画像メモだけを書く
- `source_example` は代表例の出典ラベルを固定で残す
- 画像そのもののピクセル解析はしない
- DB で再現できる列だけをケース表へ入れる

## 代表例

- `1605`
  - `source_example=1605_monthly_box`
  - 月足 box 抜けと再上放れの代表例として使う
- `5541`
  - `source_example=5541_monthly_box`
  - 長期 base breakout と失敗抜けの比較対象として使う
