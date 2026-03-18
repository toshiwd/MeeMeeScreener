# app/frontend AGENTS.md

このファイルは `app/frontend` 配下の作業にだけ適用する。共通ルールは必ずルートの `AGENTS.md` を優先する。
MeeMee 固有の判断軸は `docs/MEEMEE_PRINCIPLES.md` を優先する。

## 方針

- 1 fix = 1 symptom を守り、UI調整と状態管理変更と API 契約変更を同時に混ぜない。
- 依頼が曖昧でも、まず該当 route / component / hook を検索し、現状 UI と状態遷移の証拠を取ってから直す。
- `dist`、`node_modules`、巨大ログは読まない。必要な情報は `src` と `rg` から取る。
- 見た目だけでなく、宣言順、依存配列、state 初期化、非同期 fetch の競合を優先して疑う。
- チャートを主役として扱い、通常画面では表示面積と可読性を削る変更を避ける。
- 通常画面と検証画面の責務を混ぜない。重い比較、事後統計、AI補助は検証側へ寄せる。
- 建玉表記は UI・文言ともに必ず `売-買` で統一する。
- モバイルでは PC の縮小コピーを避け、候補確認 → チャート確認 → 建玉確認 → 今日の方針の順に絞る。

## 検証

- `lint`: `npm run lint`
- `test`: `npm run test`
- `build`: `npm run build`
- 画面遷移、チャート、更新導線を触った時は、必要に応じて `SMOKE_TEST.md` の該当手順だけ追加で確認する。

## 調査の優先順

- 画面単位: `src/routes`
- 再利用 UI: `src/components`
- 状態/副作用: `src/hooks`, `src/store`
- API 契約の影響確認: `src/api.ts` と呼び出し元
