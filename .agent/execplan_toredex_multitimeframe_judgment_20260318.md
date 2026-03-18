# TOREDEX Multi-Timeframe Judgment Context (2026-03-18)

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` are updated while implementation proceeds.

## Purpose / Big Picture

TOREDEX の判断材料に週足と月足の情報を含めることで、日足だけでは見えないトレンド継続や逆風を踏まえた意思決定ができるようにします。ユーザーは、日次の `snapshot.json` と `decision.json` を見たときに、どの銘柄が日足だけでなく週足・月足でも支持されているかを追えるようになります。

今回の変更後は、`run-live` や `run-backtest` で生成される判断結果に、週足・月足の breakout / range / regime 情報が明示されます。動作確認は、同一入力での replay 一致に加えて、snapshot と decision の両方に週足・月足の信号が残ることを確認して行います。

## Context and Orientation

主対象は `app/backend/services/toredex/` 配下です。

`toredex_snapshot_service.py` は MeeMee のランキング結果から TOREDEX 用の snapshot を組み立てます。ここで週足・月足の値を落とさずに持ち上げるのが第1の変更点です。

`toredex_policy.py` は snapshot を受けて action を決めます。ここで週足・月足の信号を参照可能にし、判断結果にその文脈を残します。

`toredex_runner.py` は snapshot / decision / narrative を保存します。ここでは表示文面を少しだけ調整して、週足・月足が見えるようにします。

## Milestones

### Milestone 1: Evidence

既存の snapshot 構造と decision ロジックを確認し、週足・月足が現在どこで失われているかを固定します。必要なら最小の再現テストを先に書きます。

実行コマンド（作業ディレクトリ `C:\work\meemee-screener`）:

    python -m pytest tests/test_toredex_phase1.py -q

受け入れシグナル:

- 既存の TOREDEX テストが通る。
- 週足・月足が snapshot の raw rankings には存在するが、現状の decision 文脈には十分に残っていない場所を説明できる。

### Milestone 2: Execute

`toredex_snapshot_service.py` で週足・月足の信号を明示的に snapshot item に載せ、`toredex_policy.py` でそれを decision にも残します。必要最小限のナラティブ更新を加えて、人が見ても判断材料が分かる形にします。

受け入れシグナル:

- snapshot の buy / sell item に weekly / monthly の信号が入る。
- decision にも同じ信号の要約が残る。
- 既存の行動選択が壊れない。

### Milestone 3: Verify

TOREDEX の単体テストを通し、追加した情報が replay と日次生成の両方で残ることを確認します。

実行コマンド（作業ディレクトリ `C:\work\meemee-screener`）:

    python -m pytest tests/test_toredex_phase1.py -q

受け入れシグナル:

- 週足・月足付き snapshot を使った decision が deterministic に一致する。
- narrative または decision JSON に週足・月足の文脈が残る。

## Progress

- [x] (2026-03-18 00:00 JST) 既存コードとテストの場所を特定した。
- [x] (2026-03-18 00:00 JST) 変更方針を snapshot / policy / runner に絞った。
- [ ] (2026-03-18 00:00 JST) 週足・月足の情報を snapshot と decision に明示化する。
- [ ] (2026-03-18 00:00 JST) TOREDEX 単体テストを追加して検証する。
- [ ] (2026-03-18 00:00 JST) `pytest` で回帰がないことを確認する。

## Surprises & Discoveries

- Observation: `rankings_cache` 側には `weeklyBreakoutUpProb` / `monthlyBreakoutUpProb` / `weeklyBreakoutDownProb` / `monthlyBreakoutDownProb` が既にある。
  Evidence: `app/backend/services/ml/rankings_cache.py` の `apply_*_ml_mode` と `toredex_snapshot_service.py` の入力を確認した。
- Observation: 現在の TOREDEX snapshot は、週足・月足の値を raw ranking から受け取っていても、最終的な item には明示的に残していない。
  Evidence: `_map_rank_item()` は `ev` / `upProb` / `revRisk` / `regime` / `gate` へ正規化している。

## Decision Log

- Decision: 週足・月足は新しい別ロジックを作らず、既存の daily snapshot / decision フローに「追加文脈」として載せる。
  Rationale: 既存の実行経路を壊さず、再現性と保守性を保つため。
  Date/Author: 2026-03-18 / Codex
- Decision: 変更は `toredex_snapshot_service.py`, `toredex_policy.py`, `toredex_runner.py` の最小セットに限定する。
  Rationale: 1 fix = 1 symptom を守り、判断材料の拡張だけに焦点を当てるため。
  Date/Author: 2026-03-18 / Codex

## Outcomes & Retrospective

TBD. 変更と検証が終わったら、何を改善し、何が残ったかをここに記録する。

