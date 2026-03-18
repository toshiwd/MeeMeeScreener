# app/backend AGENTS.md

このファイルは `app/backend` 配下の作業にだけ適用する。共通ルールは必ずルートの `AGENTS.md` を優先する。
MeeMee 固有の判断軸は `docs/MEEMEE_PRINCIPLES.md` を優先する。

## 方針

- 1 fix = 1 symptom を守り、API、DB、バッチ、解析ロジックの無関係な変更を混ぜない。
- 依頼が曖昧でも、既存 API 契約、DB スキーマ、ジョブ入出力の証拠を先に確認し、勝手に仕様を変えない。
- I/O を伴う処理と純粋ロジックは分けて考える。局所修正で済むなら責務の大移動をしない。
- 実データ、`stocks.duckdb`、`favorites.sqlite`、`practice.sqlite`、`update_state.json` は、依頼と証拠が揃っている時だけ触る。
- ランキングは候補抽出専用として扱い、売買トリガーと混同しない。
- 原則は終値確定ベースで扱い、場中補助ロジックは主役にしない。
- 建玉・AI出力・API返却の表記は `売-買` に正規化して一貫させる。
- 生データ、正規化データ、画面表示用の派生データやキャッシュを分けて管理する。

## 検証

- `lint`: 専用コマンドが未整備なら、その旨を明記して省略する。
- `test`: まず `python -m pytest` を実行する。
- `build` 相当: `python -c "import app.backend.main"` を実行し、import 崩れを確認する。
- API や起動導線を触った時は、必要最小限で `GET /api/health` など影響 endpoint の疎通も確認する。

## 調査の優先順

- ルータ変更: `app/backend/api`
- ドメインロジック変更: `app/backend/domain`
- 永続化/外部I/O変更: `app/backend/infra`, `app/backend/jobs`, `app/backend/services`
