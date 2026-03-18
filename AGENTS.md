# AGENTS.md

## 目的
- Codex の恒久ルールをここに集約し、反復ワークフローは repo 直下の `skills/` に分離する。
- クレジット消費と暴走を抑えつつ、最小変更で確実に前進する。

## 最重要
- 変更点・対応報告は日本語で行い、必要なら英訳を添える。
- 必ず `Plan -> Evidence -> Execute -> Verify` の順で進める。
- `1 fix = 1 symptom`。無関係な整形、リネーム、大量置換を混ぜない。
- 依頼が曖昧でも、まず repo を検索して現状の証拠を集め、勝手に仕様を変えない。

## コンテキスト節約
- 同時に開くタブやファイルは最大 5 つまでに抑える。
- 巨大ファイルは開かず、必要箇所だけ `rg` で抜く。
- 不要になった調査対象はすぐ閉じる。

## 実行ポリシー
- コード変更を伴う各タスクでは、完了前に影響範囲の最小単位で lint/test/build を実行する。
- ルート全体の lint/test/build は、変更範囲が複数領域にまたがる時だけ行う。
- 実行できない、または不要な場合は理由を明記する。
- 同一条件、同一引数、コード未変更の失敗コマンドは 2 回以上繰り返さない。
- 同じ失敗が 2 回続いたら停止し、根因候補を 1 つに絞って「証拠 + 次の 1 手」を提示する。

## 停止条件
- 3 回連続で進捗がない。
- 環境依存で再現不能。
- 影響範囲が想定以上に広い。
- 局所修正より分離設計見直しが先だと判明した。

## 読む順番
- MeeMee 固有のプロダクト原則は `docs/MEEMEE_PRINCIPLES.md` を正本として最優先で参照する。
- 詳細仕様や Runbook は必要時だけ `docs/CODEX.md` を開く。
- 領域別ルールは各ディレクトリの `AGENTS.md` を優先する。
  - `app/backend/AGENTS.md`
  - `app/frontend/AGENTS.md`
- 反復ワークフローは repo 直下の `skills/` を使う。

## 代表コマンド
以下は repo 実在ファイルに基づく代表例だけを常設する。詳細手順は `README.md` と各領域の `AGENTS.md` を参照する。

- 起動
  - `.\run.ps1`
  - `.\run_debug.ps1`
  - `python -m app.desktop.launcher`
- Frontend 検証
  - `cd app\frontend && npm run lint`
  - `cd app\frontend && npm run test`
  - `cd app\frontend && npm run build`
- Backend 検証
  - `python -m pytest`
  - `python -c "import app.backend.main"`
- Release 確認
  - `tools\build_release.cmd`

根拠:
- `README.md`
- `app/frontend/package.json`
- `app/backend/AGENTS.md`

## Skills 運用
- `skills/` は MeeMee 固有の再利用ワークフローを置く repo ローカル資産として扱う。
- 恒久ルールを `AGENTS.md` に増やしすぎず、反復手順は `SKILL.md` に寄せる。
- 各 `SKILL.md` は front matter に最低限 `name` と `description` を持たせる。

## ExecPlans
- 複雑な機能追加または大規模リファクタでは、設計から実装まで ExecPlan を必須とする。
- ExecPlan は `.agent/PLANS.md` の要件に従って作成、更新する。
- Rule (EN): When writing complex features or significant refactors, use an ExecPlan (as described in `.agent/PLANS.md`) from design to implementation.
