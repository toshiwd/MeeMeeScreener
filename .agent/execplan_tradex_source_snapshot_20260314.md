# Purpose

Tradex の重い研究ジョブが MeeMee 本体と同じ `stocks.duckdb` を直接開くと、Windows 上の DuckDB ファイルハンドル競合で MeeMee の一覧取得が `503` になる。ユーザーは「研究開始前に最新 DB のコピーを作って、そのコピーだけを Tradex が読む」ことを求めている。これを入れると、Codex が研究を開始しても MeeMee 本体の起動・一覧・操作が source DB 競合で止まりにくくなる。

この変更後は、`historical-replay-run`、`daily-research-run`、`nightly-candidate-run` が開始時に source DB snapshot を作り、以後は snapshot を入力にして export/label/candidate を進める。動作は、ops/job details に snapshot metadata が残ることと、テストで run entrypoint が元 DB ではなく snapshot path を下流へ渡すことで確認できる。

# Progress

- [x] source snapshot helper を追加して、DB 本体と `.wal` をコピーできるようにした
- [x] `run_nightly_candidate_pipeline` を snapshot input 化した
- [x] `run_daily_research_cycle` を単一 snapshot 共有にした
- [x] `run_historical_replay` を snapshot input 化した
- [x] CLI に snapshot 制御引数を追加した
- [x] helper / nightly / daily research / replay の回帰テストを追加した
- [ ] 実運用 replay を snapshot 前提で再起動したことを後続 runbook に反映する

# Surprises & Discoveries

- 503 の直接原因は Tradex result DB ではなく source DB の共有だった。`backend.log` に `Cannot open file "...stocks.duckdb"` と `File is already open in ... python.exe` が残っていた。
- `historical_replay` の日付バグを追う過程で、source DB には 9 桁 epoch 秒が混在していることが分かった。snapshot 化とは別に `normalize_market_date()` の閾値修正が必要だった。
- daily research は similarity/report 側では source DB を再読していなかった。snapshot を 1 回だけ作って candidate と latest-as-of 解決に使えば十分だった。

# Decision Log

- source snapshot は runtime helper に切り出し、各 entrypoint が必要時に 1 回だけ呼ぶ構成にした。これにより export/label 側へ snapshot 知識を広げずに済む。
- snapshot は default on とした。ユーザー要件が「解析をする際はコピー後に解析」であり、MeeMee と競合しないことが最優先だから。
- retention は helper 側で `keep_latest=2` にした。snapshot は数 GB 単位なので、無制限に残すとユーザーの「無駄に溜め込むな」に反する。
- daily research は nightly candidate に任せず、自分で snapshot を作って共有する方式にした。同一 run で snapshot を二重に作らないため。

# Implementation Notes

新規 helper は `external_analysis/runtime/source_snapshot.py` に置く。`create_source_snapshot(...)` は `resolve_source_db_path()` で source DB を解決し、`DATA_DIR/external_analysis/source_snapshots` に `label_timestamp.duckdb` をコピーする。`<db>.wal` が存在すればそれも `<snapshot>.duckdb.wal` にコピーし、metadata JSON に source/snapshot path, size, created_at を残す。古い metadata と対応する snapshot DB/WAL は `keep_latest` を超えた分だけ削除する。

`external_analysis/runtime/nightly_pipeline.py` では `run_nightly_candidate_pipeline(...)` に `snapshot_source` と `snapshot_root` を追加し、default で snapshot を作る。`run_diff_export()` に渡すのは元 DB ではなく snapshot path で、ops job details と戻り値に `source_snapshot` を含める。

`external_analysis/runtime/daily_research.py` では `run_daily_research_cycle(...)` に同じ引数を追加し、開始時に snapshot を 1 回だけ作る。`resolve_latest_daily_research_as_of_date()` も snapshot を読むようにし、`run_nightly_candidate_pipeline()` には `snapshot_source=False` を渡して二重 copy を避ける。report payload にも `source_snapshot` を残す。

`external_analysis/runtime/historical_replay.py` では `run_historical_replay(...)` に同じ引数を追加し、日付選択・銘柄選択・`run_diff_export()` のすべてを snapshot path 基準へ切り替える。`external_replay_runs.details_json` と戻り値に `source_snapshot` を残す。

`external_analysis/__main__.py` では `nightly-candidate-run`、`daily-research-run`、`historical-replay-run` に `--no-source-snapshot` と `--snapshot-root` を追加する。通常運用では引数不要で snapshot が有効になる。

テストは `tests/test_external_analysis_source_snapshot.py` にまとめる。helper では DB/WAL copy と prune を確認し、nightly/daily research/replay では monkeypatch で下流呼び出しに渡る source path が元 DB ではなく snapshot path であることを固定する。

# Validation

作業ディレクトリは repo root `C:\work\meemee-screener` とする。実行コマンドは次の通り。

    python -m pytest tests\test_external_analysis_source_snapshot.py
    python -m pytest tests\test_phase6_epoch_source_dates.py
    python -m pytest tests\test_phase5_historical_replay.py -k replay_cli_smoke
    python -c "import app.backend.main"

期待値は、snapshot テストが通り、epoch 日付の既存テストも維持され、backend import が成功すること。必要なら frontend は未変更なので build は不要だが、関連 UI を触った時だけ `cd app\frontend && npm run build` を実行する。

# Outcomes & Retrospective

MeeMee と Tradex の source DB 共有が 503 の根本原因だった。snapshot 化により、研究ジョブは開始時の短い copy だけ source DB に触れ、その後の重い replay/daily research はコピー済み DB だけを読む設計へ寄せられる。残る課題は、snapshot 作成時の短時間コピーでも競合が出るケースをどこまで許容するかと、運用 runbook に snapshot path の見方を追記すること。
