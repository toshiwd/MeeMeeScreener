# Experiment 0002

- 実験ID: `0002`
- 仮説: Low ATR noise pocket
- 対象銘柄: snapshot `snap_agent03` / codes=3
- 対象期間: `snap_agent03` dataset cache
- 特徴量: state:low_atr, change:small_body, context:range
- ラベル定義: skip
- 検証方法: expanding walk-forward
- サンプル数: 171
- 結果: 保留
- 勝率: nan
- 期待値: 0.0
- 最大逆行: nan
- 平均保有日数: 20.0
- 有効レジーム: n/a
- 無効レジーム: n/a
- 失敗理由: {"avg_gain": NaN, "avg_loss": NaN, "improvement": 0.002311054907435566, "long_expectancy": 0.005034599055637363, "median_fold_expectancy": NaN, "median_hold": 20.0, "move_potential": 0.0590231738231496, "p90_close_mae": NaN, "pooled_expectancy": 0.0, "positive_fold_ratio": 1.0, "samples": 171, "short_expectancy": -0.005034599055637363, "win_rate": NaN}
- 採用/保留/破棄: 保留
- 次アクション: backlog の次仮説へ進む
