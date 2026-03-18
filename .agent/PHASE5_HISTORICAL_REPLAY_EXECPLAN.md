# Phase 5 Historical Replay ExecPlan

## Purpose

この作業は、nightly を待たずに過去の `as_of_date` 群で candidate と similarity の champion/challenger 比較を一括実行し、昇格判断に必要な観測数を短時間で集めるための internal only 基盤を追加する。実装後は `historical-replay-run` を実行するだけで、過去日の replay、quality metrics 蓄積、promotion readiness 集計、replay summary 保存までをまとめて行える。MeeMee 本体の public contract は変わらず、`publish_pointer` が指す現在の公開結果も壊れない。

## Repo Orientation

replay runner 本体は `external_analysis/runtime/historical_replay.py` にある。candidate の internal 実行は `external_analysis/models/candidate_baseline.py`、similarity champion/challenger は `external_analysis/similarity/baseline.py`、ops 保存は `external_analysis/ops/store.py` と `external_analysis/ops/ops_schema.py`、CLI 入口は `external_analysis/__main__.py` でつながる。MeeMee 側の read-only bridge と public API は変更しない。

## Progress

- [x] `historical-replay-run` CLI を追加した
- [x] replay 用 ops schema を追加した
- [x] replay day / summary / readiness の保存を追加した
- [x] candidate / similarity を `publish_public=False` で internal replay 実行できるようにした
- [x] replay の idempotent / resume / quarantine を実装した
- [x] public bridge / API 不変のスモークテストを追加した

## Implementation Notes

replay は source DB の `daily_bars.date` の distinct 値を JPX 営業日の暫定 canonical として使い、`start-as-of-date` から `end-as-of-date` の範囲を順に再生する。各日ごとに `export-sync`, `label-build`, `anchor-window-build`, `candidate-baseline-run`, `similarity champion`, `similarity challenger shadow` を internal 実行するが、candidate と similarity champion は `publish_public=False` で走らせるので `publish_pointer` を更新しない。各日の publish id は `replay_<replay_id>_<YYYY-MM-DD>` で固定し、同じ replay を再実行しても rows と metrics は置換保存される。

ops DB には `external_replay_runs`, `external_replay_days`, `external_replay_summaries`, `external_replay_readiness` を追加した。successful day は `nightly_candidate_metrics` と `similarity_quality_metrics` を as_of_date ごとに蓄積し、rolling 20 / 40 / 60 run で readiness を集計する。summary は daily results と readiness windows を JSON で保存する。

## How To Run

作業ディレクトリは `C:\work\meemee-screener`。

    python -m external_analysis historical-replay-run --source-db-path <source.duckdb> --export-db-path <export.duckdb> --label-db-path <label.duckdb> --result-db-path <result.duckdb> --similarity-db-path <similarity.duckdb> --ops-db-path <ops.duckdb> --start-as-of-date 20260312 --end-as-of-date 20260316 --replay-id replay_demo --max-days 3 --max-codes 3

期待結果は次のとおり。

    - `external_replay_runs` に `replay_demo` の row が入る
    - `external_replay_days` に as_of_date ごとの row が入る
    - `external_replay_summaries` に replay summary が 1 row 入る
    - `external_replay_readiness` に rolling 20 / 40 / 60 の readiness row が入る
    - `publish_pointer` は replay 前の public publish を維持する

## Validation

最小検証コマンドは次のとおり。

    python -m pytest tests/test_phase5_historical_replay.py tests/test_phase4_slice_kl_nightly.py tests/test_phase4_slice_j_challenger.py tests/test_phase3_similarity_nightly_pipeline.py -q
    python -c "import external_analysis.__main__; import external_analysis.runtime.historical_replay; import external_analysis.similarity.baseline; print('ok')"

成功時の期待結果:

    14 passed
    ok

## Surprises & Discoveries

- `publish_public=False` を入れないと replay が `publish_pointer` を上書きして public API を壊す。replay 用 publish id を別にしても pointer が動く限り public contract は守れないため、candidate/similarity の internal 実行フラグを追加した。
- replay の summary を今回の実行結果だけで作ると resume 後に `success_days=0` になる。summary と readiness は replay 全体の persisted day rows を再集計する必要があった。
- `similarity_quality_metrics` を summary JSON に埋めると DuckDB から返る `DATE` が JSON 直列化に失敗した。`isoformat()` に正規化して保存する必要があった。

## Decision Log

- replay 用の評価結果は public table を増やさず、ops DB と既存 internal metrics store に閉じた。
- replay の per-day failure は即停止ではなく quarantine 記録して次の日へ進む。目的が観測の蓄積だからである。
- readiness は rolling 20 / 40 / 60 run で計算し、promotion gate に近い判定を internal only で保存するが、自動昇格はしない。

## Outcomes & Retrospective

Phase 5 で、nightly を待たずに過去日の champion/challenger 比較を一括で回し、candidate/similarity の quality metrics を短期間で蓄積できるようになった。public API と MeeMee read-only contract は維持され、replay は internal only で完結する。未実装なのは champion 置換、自動昇格、ANN 最適化、state evaluation、本体 UI 切替であり、これらは後段の Phase に送る。
