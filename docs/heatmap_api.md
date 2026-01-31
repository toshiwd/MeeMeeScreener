# Heatmap API

`GET /api/market/heatmap?period={1d|1w|1m}`

レスポンスは JSON オブジェクトで、主要なフィールドは次の通りです。

```
{
  "items": [
    {
      "sector33_code": "12",
      "name": "輸送用機器",
      "weight": 1.2345e8,
      "value": 1.8,
      "tickerCount": 24,
      "detailRoute": "/?sector=12&period=1d"
    }
  ],
  "period": "1d",
  "diagnostics": {
    "industry_master_present": true,
    "industry_master_rows": 2265,
    "tickers_rows": 8640,
    "computed_from": "industry_master",
    "period": "1d"
  }
}
```

- `items`: セクターごとのタイル（`weight` が面積、`value` が騰落率、 `tickerCount` が構成銘柄数）。
- `detailRoute`: タイルクリック時に画面遷移するためのパス（`sector` と `period` を含む）。
- `diagnostics`: `industry_master`/株価テーブルの健全性を示す。`computed_from` が `"fallback"` の場合、擬似データを返すため heatmap が実在しないことを意味します。

Front-end はこの JSON を受けて treemap を再描画し、`diagnostics` の `industry_master_present`/`ticker_rows` などを画面右上のステータスバッジや selftest の `heatmap-rendered` フラグに反映します。
