# Publish Promotion / Rollback と Champion-Challenger 運用の実装

この ExecPlan は living document です。`Progress`、`Surprises & Discoveries`、`Decision Log`、`Outcomes & Retrospective` を作業中に更新してください。

この作業の目的は、MeeMee Screener から TradeX の publish 成果物を安全に昇格・退役できるようにし、現在採用中の champion と候補の challenger を明示的に管理できるようにすることです。ユーザーは、どのロジックが採用中か、どのロジックが候補か、どの条件で昇格または退役したかを API と docs で追えるようになります。既存の runtime selection、last_known_good、override、audit の挙動は維持し、そこに promotion / rollback を段階的に重ねます。

## Purpose / Big Picture

MeeMee Screener は日々使う runtime です。TradeX は研究・比較・検証・publish を行う基盤です。この変更で、TradeX 側で検証済みの logic_id:logic_version を、MeeMee 側の runtime selection と連動した形で promote / demote できるようになります。

変更後は、開発者または運用者が内部 API から champion と challenger を更新し、昇格条件を満たしたロジックだけを採用状態にできます。失敗時は audit が残り、rollback は current champion への戻しとして安全に実行できます。動作は `/api/system/runtime-selection` と新しい promotion API、そして `external_analysis` の既存 review tables で確認できます。

## Progress

- [x] (2026-03-19 14:20JST) 既存の runtime selection、publish catalog、analysis_bridge promotion review、ops schema を棚卸しした。
- [x] (2026-03-19 14:20JST) 本 ExecPlan を作成し、promotion / rollback と champion / challenger の実装範囲を固定した。
- [x] (2026-03-19 15:05JST) promotion state model を backend service に実装した。
- [x] (2026-03-19 15:05JST) promotion / demotion / rollback の内部 API を system router に追加した。
- [x] (2026-03-19 15:05JST) champion / challenger の比較条件と昇格条件を docs に追記した。
- [x] (2026-03-19 15:05JST) audit log と publish metadata の更新経路を固定した。
- [x] (2026-03-19 15:05JST) 既存の last_known_good / override / provisional gate を壊さないことをテストで確認した。
- [x] (2026-03-19 15:05JST) 変更範囲に対する pytest を実行し、回帰がないことを確認した。

## Surprises & Discoveries

- Observation: `external_analysis` には既に `external_state_eval_readiness`、`external_state_eval_shadow_runs`、`external_promotion_decisions` があり、promotion 判定の材料は揃っている。
  Evidence: `external_analysis/ops/ops_schema.py` と `app/backend/services/analysis_bridge/reader.py` を確認した。
- Observation: MeeMee 側の runtime selection は `selected_logic_override -> default_logic_pointer -> last_known_good -> safe fallback` の順で解決する実装が入っている。
  Evidence: `app/backend/services/runtime_selection_service.py` の resolver を確認した。
- Observation: `publish_manifest` には `logic_id`、`logic_version`、`logic_family`、`default_logic_pointer`、`logic_artifact_uri`、`logic_artifact_checksum` が既に入る。
  Evidence: `external_analysis/results/publish.py` と `external_analysis/results/result_schema.py` を確認した。
- Observation: Windows のテキスト書き込みは改行変換で checksum 照合を外しやすい。
  Evidence: publish promotion テストで artifact checksum mismatch が出たため、テスト fixture を bytes write に直した。

## Decision Log

- Decision: promotion / rollback は MeeMee 側の内部 API を正とし、TradeX 側の既存 review data を判定根拠として再利用する。
  Rationale: MeeMee は runtime の責務を持ち、TradeX は research / validation / publish を持つため、昇格操作だけを MeeMee 側の薄い制御面に置くのが境界に合う。
  Date/Author: 2026-03-19 / Codex
- Decision: champion / challenger の状態は `logic_id:logic_version` を identity として扱い、artifact_uri は locator として扱う。
  Rationale: 既に catalog と runtime selection がこの前提で実装されており、ファイル場所の変更に引きずられない。
  Date/Author: 2026-03-19 / Codex
- Decision: override UI と pure function の大規模移設は今回扱わない。
  Rationale: 依頼された優先順位に合わせ、API と内部ロジックだけに集中する。
  Date/Author: 2026-03-19 / Codex
- Decision: runtime selection の観測面には `publish_registry_state` という派生ビューを返す。
  Rationale: MeeMee runtime が champion / challenger / default pointer を追いやすくし、raw state と運用ビューを分離できる。
  Date/Author: 2026-03-19 / Codex

## Milestones

### Milestone 1: Promotion state model を固定する

まず、MeeMee 側で champion / challenger / retired / blocked の状態を保持する最小モデルを定義します。state model は既存の logic selection state に追加し、`logic_key`、`status`、`role`、`comparison`、`promotion_reason`、`retired_reason` を持てるようにします。比較基準は TradeX の review data を参照し、少なくとも sample_count、expectancy_delta、adverse_move、alignment、stable_window を使います。

この段階では UI を増やしません。内部 state と API の返却形だけを先に揃えます。実装後は、`/api/system/runtime-selection` に current champion と current challenger の概要が見え、promotion API がどの対象を操作するか分かる状態を目指します。

検証コマンドは次の通りです。

    python -m pytest tests/test_runtime_selection_system.py tests/test_analysis_bridge_api.py -q

合格条件は、runtime selection の既存挙動を壊さずに champion / challenger の状態を読み出せることです。

### Milestone 2: Promotion / demotion の内部 API を実装する

次に、`logic_id:logic_version` を指定した promotion と demotion の API を追加します。promotion は候補を champion に昇格させ、demotion は対象ロジックを retired に落とします。rollback は current champion を previous champion に戻す操作として扱い、必ず audit を残します。

この段階では、リクエスト検証として catalog に存在すること、artifact_uri が解決できること、manifest が一致すること、checksum が合うことを確認します。失敗時は HTTP 400 を返し、状態を変更しません。

検証コマンドは次の通りです。

    python -m pytest tests/test_runtime_selection_system.py tests/test_external_analysis_promotion_decision.py -q

合格条件は valid promotion が通り、invalid logic_key が拒否され、clear / demote / rollback の各操作に audit が残ることです。

### Milestone 3: Champion / challenger の比較と昇格条件を docs とコードで固定する

最後に、promotion 判断の基準を docs とコードの両方で明文化します。Promotion は単純なスコア比較ではなく、TradeX の review data に基づく安全条件が満たされた時だけ許可します。少なくとも、期待値改善、悪化指標の非悪化、十分な sample_count、stable_window、alignment_ok を確認します。退役条件は、失敗した validation、checksum 不一致、artifact 欠損、手動 demotion、または次の champion 昇格です。

この段階で `docs/architecture/` と `docs/pages/` に champion/challenger と publish promotion/rollback の説明を追加し、runtime selection との関係を明示します。MeeMee が何を読むか、TradeX が何を検証するか、rollback がどの state を戻すかを一目で追えるようにします。

検証コマンドは次の通りです。

    python -m pytest tests/test_runtime_selection_system.py tests/test_analysis_bridge_api.py tests/test_external_analysis_promotion_decision.py tests/test_external_analysis_daily_research.py -q

合格条件は docs と API の表現が一致し、既存の `last_known_good` / override / provisional gate が維持されることです。

## Exact Files to Touch

以下のファイル群を中心に変更します。必要なら関連テストも追加します。

`app/backend/services/runtime_selection_service.py`

`app/backend/api/routers/system.py`

`app/backend/infra/files/config_repo.py`

`app/backend/services/analysis_bridge/reader.py`

`external_analysis/ops/ops_schema.py`

`external_analysis/ops/store.py`

`external_analysis/results/publish.py`

`external_analysis/results/result_schema.py`

`docs/architecture/CHAMPION_CHALLENGER.md`

`docs/architecture/PUBLISH_PROMOTION.md`

`docs/pages/tradex-publish-flow.md`

`tests/test_runtime_selection_system.py`

`tests/test_analysis_bridge_api.py`

`tests/test_external_analysis_promotion_decision.py`

## Implementation Notes

promotion と rollback は、MeeMee の runtime selection を直接壊さずに上書きする必要があります。`selected_logic_override` が存在する場合は override が優先されるため、promotion は default pointer と champion registry を更新する操作として扱い、override の意味は変えません。`last_known_good` は壊れた場合にだけ復旧用として使い、promotion の source of truth にはしません。

comparison metrics は、TradeX の既存 review tables に合わせて定義します。最低限の基準は、`expectancy_delta` が非負、`adverse_move` が非悪化、`sample_count` が下限以上、`stable_window` が true、`alignment_ok` が true です。これより強い条件が必要になった場合は、Decision Log に追加し、docs を同時更新します。

audit log は、誰が、いつ、何を、なぜ変えたかを残します。内部 API で更新しても、後から inspection できることが重要です。保存先は MeeMee の `runtime_selection` ディレクトリ配下に統一し、既存の logic_selection audit と同じ保存規則に揃えます。

## Validation

実装途中では、少なくとも次の確認を毎回行います。

    python -m pytest tests/test_runtime_selection_system.py -q

    python -m pytest tests/test_analysis_bridge_api.py -q

必要に応じて:

    python -m pytest tests/test_external_analysis_promotion_decision.py -q

手動確認としては、`/api/system/runtime-selection` を叩いて `resolved_source` と `selected_logic_key` が追えること、promotion 後に champion が変わること、demotion 後にその logic が retired として扱われることを確認します。

## Outcomes & Retrospective

promotion / demotion / rollback の最小運用が、MeeMee の local publish registry と runtime selection に接続された。

残っているのは、external_analysis 側の full publish promotion、複数 challenger の明示的キュー管理、そして UI だ。今回の実装は「採用状態を安全に切り替えるための最小骨組み」であり、将来の publish 基盤に向けた前段としては十分だが、まだ運用完全版ではない。
