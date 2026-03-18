# MeeMee Research External Worker Separation v2

## Purpose

この変更の目的は、MeeMee 本体と research worker を保存先と受け渡し経路の両方で切り離すことです。変更後は、MeeMee 本体が保持する本番 DuckDB を research が直接共有せず、research は repo 外の専用ホームに作る mirror DuckDB と JSON bridge だけを使います。これにより、MeeMee 本体の更新や配布が長時間動く研究ジョブや研究成果物に波及しにくくなります。

ユーザーが観測できる変化は 3 つあります。`python -m research sync_source` で研究用 mirror を明示更新できること、`python -m research export_bridge_run --run-id ...` と `python -m research export_bridge_study --study-id ...` で MeeMee 本体へ渡す bridge JSON を repo 外に出力できること、そして MeeMee 本体が repo 直下 `published/latest` ではなく bridge を読むことです。

## Repository Orientation

research 側の入口は `research/__main__.py` です。ここで CLI を定義し、workspace や publish 先の既定値も `research/storage.py` 経由で決まります。mirror 同期は `research/source_sync.py`、bridge 出力は `research/bridge.py`、legacy publish gate は `research/publish.py` にあります。

MeeMee 本体側の research 連携は `app/backend/services/ml/rankings_cache.py` に集中しています。ここにある `_load_research_prior_snapshot()` が、ランキング補正に使う研究 prior を読み込む唯一の runtime 入口です。research の成果を本体へ見せるには、この関数が bridge を見ていることが必須です。

## Progress

- [x] `ResearchPaths` に research home, mirror, bridge, logs の既定ディレクトリを追加した。
- [x] `sync_source` CLI と DuckDB full mirror の atomic swap を実装した。
- [x] `study_build` と `study_loop` の CLI 入口で on-demand mirror 同期を必須化した。
- [x] `export_bridge_run` と `export_bridge_study` を追加した。
- [x] MeeMee 本体の prior 読み込み先を bridge に切り替えた。
- [x] `publish` を `--legacy-publish` 必須の opt-in に下げた。
- [x] 研究 worker 分離に関する最小テストを追加した。
- [ ] `research/README.md` の運用手順を新しい bridge/mirror 前提に更新する。

## Surprises & Discoveries

`study_build` 自体はまだ snapshot CSV を材料にしており、mirror DuckDB から直接 dataset を作る構成ではありません。そのため 이번変更では「source DB の共有をやめる境界」と「研究の既定保存先の分離」を優先し、study コマンドの入口で mirror freshness を fail-closed にする形に留めました。source signature が変わって mirror 更新に失敗したら、その研究 run は即座に失敗します。

MeeMee 本体の runtime coupling は想定より少なく、実質的な依存は `app/backend/services/ml/rankings_cache.py` の prior 読み込みでした。`app/backend/analysis/analyze_feature_importance.py` のような research import は補助スクリプトであり、runtime path ではありません。今回の主目的には含めず、後続整理に回しても本体分離は成立します。

## Decision Log

2026-03-12: research home の既定値は `%LOCALAPPDATA%/MeeMeeResearch` に固定し、workspace はその配下 `workspace/`、legacy publish は `legacy_published/` に置くことにした。これにより、repo 直下 `research_workspace/` や `published/` を既定値から外し、本体更新と研究成果物の衝突を減らす。

2026-03-12: source DB 同期は「差分コピー」ではなく `path + size + mtime` を署名とする full rebuild に決めた。row 差分や table 差分は実装しない。理由は、信頼性と復旧容易性を優先し、mirror の整合性を 1 回の atomic swap で完結させるため。

2026-03-12: MeeMee 本体への連携は file-based JSON bridge に一本化した。HTTP API や DB 共有は導入しない。現在の本体側消費コードが必要とする情報は run id, asof, codes, rank_map だけであり、JSON で十分だからである。

## Milestone 1: Research Home And Mirror

この段階で新しく存在すべきものは、repo 外の research home と `mirror/current/source.duckdb` です。`research/storage.py` で既定パスを定義し、`research/source_sync.py` で source DB を full mirror する実装を追加します。source DB の解決順は `--source-db`、`MEEMEE_SOURCE_DB`、`STOCKS_DB_PATH`、`%LOCALAPPDATA%/MeeMeeScreener/data/stocks.duckdb` です。

確認はリポジトリ root で次を実行します。

    python -m research sync_source --source-db C:\path\to\stocks.duckdb

成功時は JSON で `changed`, `mirror_db`, `mirror_manifest`, `table_count` が返り、`%LOCALAPPDATA%\MeeMeeResearch\mirror\current\mirror_manifest.json` が生成されます。再度同じ source で実行したとき `changed` が `false` なら、stale 判定が効いています。

## Milestone 2: Bridge Export And App Consumption

この段階で新しく存在すべきものは `bridge/latest/research_prior_snapshot.json`、`bridge/latest/adopted_hypotheses.json`、`bridge/latest/bridge_manifest.json` です。`research/bridge.py` に run と study の export を追加し、`app/backend/services/ml/rankings_cache.py` は bridge を読むように変更します。

確認は次の 2 段階です。

    python -m research export_bridge_run --run-id <run_id>
    python -m research export_bridge_study --study-id <study_id>

その後、MeeMee 本体で `_load_research_prior_snapshot()` を呼ぶと、repo 直下 `published/latest` が存在しても bridge の JSON が優先されます。bridge 不在時は空 payload を返し、本体動作は止まりません。

## Milestone 3: Legacy Publish Gate

この段階で新しく存在すべきものは、repo 直下 `published/` を誤って既定出力先にしない安全弁です。`research/publish.py` は `paths.published_root == repo_root/published` のとき `--legacy-publish` が無ければ失敗します。これにより、既存 publish は残しつつ、新しい正式経路を bridge export に寄せられます。

確認は次です。

    python -m research publish --run_id <run_id>

このコマンドは repo 直下 publish を既定にした場合に失敗し、`--legacy-publish` を付けたときだけ従来出力を許可します。

## Exact Files To Edit

`research/storage.py` で research home と bridge/mirror path を追加する。`research/source_sync.py` を新設して mirror 同期を実装する。`research/bridge.py` を新設して run/study の bridge export を実装する。`research/__main__.py` で `sync_source`, `export_bridge_run`, `export_bridge_study`, `--legacy-publish` を配線する。`research/publish.py` で legacy gate を入れる。`app/core/config.py` で `MEEMEE_RESEARCH_HOME` と `MEEMEE_RESEARCH_BRIDGE_DIR` を解決する。`app/backend/services/ml/rankings_cache.py` で bridge 読み込みへ切り替える。

テストは `tests/test_research_external_worker.py` と `tests/test_rankings_research_bridge.py` に追加し、`tests/test_research_agent_pipeline.py` と `tests/test_research_study_pipeline.py` の helper を temp research home 前提に揃える。

## Verification

lint や full test suite はこの計画の必須条件ではない。まず軽量 smoke で、import と CLI 入口が壊れていないことを確認する。リポジトリ root で次を使う。

    python -c "import research.source_sync, research.bridge, research.__main__; print('ok')"
    python -m research sync_source --help
    python -m research export_bridge_run --help
    python -m research export_bridge_study --help

可能なら一時ディレクトリに小さな DuckDB と run/study artifact を作り、`sync_source_mirror`, `export_bridge_run`, `export_bridge_study` を直接呼ぶ smoke を追加する。期待する出力は mirror manifest と bridge latest/history の生成である。

## Outcomes & Retrospective

2026-03-12 時点で、runtime coupling の中心だった `published/latest` 直読みは解消した。研究成果の MeeMee 本体への受け渡しは bridge JSON に絞られ、research の既定保存先も repo 外へ移せた。一方で、study dataset 自体はまだ snapshot CSV ベースであり、mirror DuckDB を直接材料にする設計には至っていない。必要なら後続で「mirror から snapshot を組み立てる ingest」または「study_build が mirror から直接読む経路」を追加する。
