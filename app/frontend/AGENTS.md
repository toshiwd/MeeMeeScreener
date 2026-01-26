# app/frontend/AGENTS.md (Frontend Rules)

- Framework: Next.js (App Router)
- Styling: Tailwind CSS（原則: custom CSSファイルを増やさない）
- UI: shadcn/ui (Radix)
- Animation: framer-motion（インタラクションに限定して適用）
- Icons: lucide-react

## Static Export
- Static export 必須。
- export/build の根拠は package.json scripts / next.config.js を一次情報として判断する。
- サーバ専用機能を前提にしない。

## Scope Control
- バグ修正中はUI/UX改変を混ぜない（根因がUIで証明されている場合のみ）。
- 1 PR = 1 symptom を優先。
