---
name: perf-db-audit
description: Use this when MeeMee feels slow, startup is heavy, DB size is growing, analysis is expensive, or boundaries between viewer and research logic may be blurred. Trigger for performance/DB audits and architecture boundary checks. Do not use for a single local bug with a clear stack trace.
---

# MeeMee Performance + DB Audit

目的は、MeeMeeの起動の重さ、DB肥大化、解析コスト、責務分離の問題を監査し、最小変更で効く改善タスクを決めること。

## 前提
MeeMee本体は軽量 viewer。
重い研究処理は本体から分離する。

## 優先して読むファイル
### 起動経路
- `run.ps1`
- `run_debug.ps1`
- `app/desktop/launcher.py`

### Backend 起点
- `app/main.py`
- `app/backend/api/routers/jobs.py`
- `app/backend/core/txt_update_job.py`

### Frontend 起点
- `app/frontend/src/main.tsx`
- `app/frontend/src/App.tsx`
- `app/frontend/src/store.ts`

### DB / 状態
- `app/db/session.py`
- `app/db/schema.py`
- `app/backend/update_state.json`

### Release 観点
- `build_release.cmd`
- `README.md`

## 必須手順
1. 問題を層別する
   - フロント起動
   - エラー落ち
   - DB肥大化
   - 解析重さ
   - 責務分離
   - EXE配布効率
2. 起動時に走る処理を棚卸しする
3. 不要な全件読込 / 再計算 / 再フェッチを探す
4. DBの大きいテーブル / 列 / キャッシュ / 中間生成物を特定する
5. 「本体に残すべき処理」と「分離すべき処理」を分ける
6. 影響度 × 修正コスト × 本体への悪影響で優先順位付けする
7. 今回着手すべき1件を選ぶ

## 重点観点
### フロント起動
- 初期表示時に不要なDBアクセスや全件読込があるか
- startup hook / useEffect / store 初期化が多重実行していないか
- 起動直後にAPI連打や再フェッチがないか
- 一覧の仮想化 / ページング / 遅延読込が不足していないか
- viewer が研究用データや巨大JSONを抱えていないか

### DB肥大化
- 中間テーブル、重複保存、派生データの多重保持
- 巨大JSON / blob / 実験成果物の混入
- 孤児データ、削除されない履歴
- retention / prune / vacuum 方針の欠如
- 再生成可能データの恒久保存

### 解析重さ
- UIで同期実行されていないか
- 閲覧のたびに再計算していないか
- batch に逃がすべき処理が viewer に残っていないか
- dirty フラグなしで再計算していないか

### 責務分離
本体に残す:
- 一覧、詳細、軽量チャート、検索、絞り込み、ユーザー状態表示

追い出す:
- 重い解析、研究ロジック、長時間バッチ、学習、巨大画像生成

## 禁止事項
- いきなり大規模リファクタリングを提案しない
- 根拠なしに新しい保存物を増やさない
- 研究用成果物の本体DB保存を肯定しない

## 出力形式
## 現在の観測
- 症状
- 問題層
- 影響範囲

## 原因分析
- 直接原因
- 構造的原因
- 根拠となるファイル / 関数 / データフロー
- 本体に残すべきか / 分離すべきか

## タスク候補
- 候補1
- 候補2
- 候補3
- 最小変更で効く順に並べる

## 今回の推奨1手
- 今回着手する1件
- 先にやる理由
- 期待効果
- 想定副作用

## 修正後に再確認すべき点
- 回帰確認
- DBサイズへの影響
- 起動速度への影響
- EXE配布への影響
- 次ループ候補
