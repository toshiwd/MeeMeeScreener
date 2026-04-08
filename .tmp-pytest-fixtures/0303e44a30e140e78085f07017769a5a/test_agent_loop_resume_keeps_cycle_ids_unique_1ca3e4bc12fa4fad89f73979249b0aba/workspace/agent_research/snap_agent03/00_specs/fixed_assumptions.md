# Fixed Assumptions

- 価格は `daily.csv` の `close` を調整後終値として扱う。
- エントリーは当日終値固定、保有は最大60営業日。
- 売買コスト、逆日歩、イベント要因、ギャップ依存エントリーは初期研究から除外する。
- MeeMee本体、`app/`、`published/`、本番DBには書き込まない。
- walk-forward: train >= 10 years, valid 24 months, test 12 months, step 12 months.
