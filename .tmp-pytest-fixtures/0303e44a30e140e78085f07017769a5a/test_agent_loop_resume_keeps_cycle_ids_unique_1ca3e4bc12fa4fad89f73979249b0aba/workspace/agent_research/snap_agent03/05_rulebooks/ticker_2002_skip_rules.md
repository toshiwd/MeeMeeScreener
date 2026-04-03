# Skip Rulebook

## skip_noise_low_atr (hold)

- 短い要約: 2002 は state:low_atr / change:small_body / context:range の局面で見送り優先。
- どういう場面か: state:low_atr / change:small_body / context:range
- なぜ効くか: この銘柄は state:low_atr / change:small_body / context:range の局面では値幅が薄く、見送りが優先です。 long/short の期待値は伸びにくく、除外後の残集合期待値改善は 0.0023 でした。 有効レジーム: 特定レジーム未確定。無効レジーム: 特定レジーム未確定。
- どこで入るか: 当日終値基準。詳細は該当ルールの局面説明を参照。
- どこで利確か: takeprofit ルールがある場合はそちらを優先、なければ60営業日以内の終値管理。
- どこで損切りか: stop ルールがある場合はそちらを優先、なければ失敗理由シグナルを監視。
- どこは見送るか: このルール自体が見送り条件
- 何が出たら失敗しやすいか: 追加研究中
- サンプル数と信頼度: 56 / 0.84

### 詳細版

- effective_regimes: n/a
- ineffective_regimes: n/a
- stats: {"avg_move_potential": 0.058935503545428695, "improvement": 0.0023110549074355662, "long_expectancy": 0.0049795562585387265, "samples": 56, "short_expectancy": -0.0049795562585387265}
