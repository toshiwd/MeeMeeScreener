# TRADEX Export Snapshot Review Note

## confirmed
- exporter の重い実処理は exporter 側に残っており、`research_runner.py` は orchestration-only を維持している。
- progress artifact は `source` step と `export` step を分離しており、`indicator_daily_export` と `pattern_state_export` を独立観測できる。
- reusable probe は typed status と typed reason code を返し、`missing / incomplete / stale / mismatched / complete` の粒度は十分に明確。
- `pattern_state_export` を required にしている判断は妥当。candidate input が `pattern_state_export` を join し、`box_state` / `ppp_state` / `abc_state` を scoring に使うため。
- blocker artifact は precondition で止まった場合でも `export_probe`、`source_signature`、`expected_export_signature`、`blocker_reason_code` を持ち、原因切り分けの正本として機能する。

## provisional
- `bars_daily_export` の step 内 progress は coarse。table-unit progress は見えるが、row-level の残量は分からない。
- real full source に対する `mid-step kill -> resume` は synthetic では確認済みだが、実運用条件では未確認。
- `snapshot_status.json` と `snapshot_progress.json` の二系統 artifact は現状整合しているが、長時間 run での drift 監視は継続して必要。
- `meta_export_runs` を final success only にしている設計は正しいが、途中段階の運用可観測性は progress artifact へ依存する。

## remaining risks
- real full source の export 完走時間はまだ読めない。現時点の主ボトルネックは `bars_daily_export` に見えるが、`indicator_daily_export` や `pattern_state_export` まで到達していない。
- complete export snapshot が real full source で揃っても、その後の `image-rerank-research-run` 側で別 bottleneck が出る可能性がある。
- `run_diff_export()` の内部変更は exporter の広い経路に影響するため、nightly/export-sync 系の運用負荷は別確認が必要。

## 実装提案メモ
- `bars_daily_export` の step 内 progress が必要なら、次回は exporter 側の更なる粒度追加を検討する。
- real full source で一度 `mid-step kill -> resume` を再現確認できると、resume contract の信頼性が上がる。
- progress artifact の運用監視は docs/runbook に固定し、コード側に早い段階で監視前提を持ち込まない方が安全。
