# Regime x Pattern x Path Study

## Setup

- DBs: `.local\meemee\research_db\stocks_research_20160226_20191231.duckdb, .local\meemee\research_db\stocks_research_20200101_20221231.duckdb, .local\meemee\research_db\stocks_research_20230101_20260226.duckdb, data\stocks.duckdb`
- Codes: `685`
- Date range: `2016-05-02` to `2026-02-26`
- Round-trip cost: `0.002`
- Min samples per regime-pattern: `120`

## Top Pattern 2

| regime_key | pattern | n | ret10d | delta_vs_regime | win10d | mfe20d | mae20d | up5_before_dn5 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| up_init|wk_up|neutral|below20|above60|7d_1_3|20d_1_3|atr_mid|vol_mid|box_upper | DL-N-NG-LB>DL-N-NG-LB | 171 | 0.0172 | 0.0127 | 0.573 | 0.0792 | -0.0442 | 0.468 |
| up_init|wk_up|neutral|below20|above60|7d_1_3|20d_1_3|atr_mid|vol_mid|box_na | DL-N-NG-LB>DL-N-NG-LB | 193 | 0.0219 | 0.0088 | 0.637 | 0.1081 | -0.0612 | 0.627 |

## Top Pattern 3

| regime_key | pattern | n | ret10d | delta_vs_regime | win10d | mfe20d | mae20d | up5_before_dn5 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |

## Top Pattern 4

| regime_key | pattern | n | ret10d | delta_vs_regime | win10d | mfe20d | mae20d | up5_before_dn5 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |

## Notes

- `delta_vs_regime` is the key metric. Positive means the pattern beat the same regime baseline.
- `mfe20d` and `mae20d` show path quality. A pattern can have positive ret10d but poor path if `mae20d` is too deep.
- `up5_before_dn5_20d` is a simple path score for bottoming-style entries.
