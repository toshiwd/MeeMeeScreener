# Experiment 0001

- 実験ID: `0001`
- 仮説: Box center with dry volume
- 対象銘柄: snapshot `snap_agent03` / codes=3
- 対象期間: `snap_agent03` dataset cache
- 特徴量: state:box_mid, state:volume_dry, context:ma_compressed
- ラベル定義: skip
- 検証方法: expanding walk-forward
- サンプル数: 0
- 結果: 破棄
- 勝率: nan
- 期待値: nan
- 最大逆行: nan
- 平均保有日数: 20.0
- 有効レジーム: n/a
- 無効レジーム: n/a
- 失敗理由: {"avg_gain": NaN, "avg_loss": NaN, "improvement": NaN, "long_expectancy": NaN, "median_fold_expectancy": NaN, "median_hold": 20.0, "move_potential": NaN, "p90_close_mae": NaN, "pooled_expectancy": NaN, "positive_fold_ratio": 0.0, "samples": 0, "short_expectancy": NaN, "win_rate": NaN}
- 採用/保留/破棄: 破棄
- 次アクション: backlog の次仮説へ進む
