# Tradex Daily Operations

## 目的
- Tradex の研究と判定更新を `Codex 起点` で回すための最小 runbook。
- MeeMee 側は閲覧中心とし、研究実行・承認記録・深掘りは CLI / PowerShell で行う。

## 基本思想
- `daily-research-run`
  - その日の研究を回す。
- `daily-research-history`
  - 過去の研究結果を履歴で見る。
- `daily-research-watchlist`
  - 未処理案件、継続 risk、改善中タグを優先度付きで見る。
- `daily-research-dispatch`
  - 今すぐ見るべき 1 件を取る。
- `daily-research-tag-report`
  - 特定タグを深掘りする。
- `promotion-decision-run`
  - promotion review の承認/保留/却下を記録する。

## 標準フロー

### 1. その日の研究を回す
```powershell
python -m external_analysis daily-research-run
```

または

```powershell
.\tools\run_tradex_daily_research.ps1
```

出力:
- JSON report
- text report
- ops DB への daily research artifact 保存

### 2. 今の優先課題を見る
```powershell
python -m external_analysis daily-research-watchlist
```

または

```powershell
.\tools\show_tradex_daily_research_watchlist.ps1
```

見る項目:
- `pending_promotions`
- `improving_tags`
- `persistent_risk_tags`
- `top_next_actions`

### 3. 次の 1 件を選ぶ
```powershell
python -m external_analysis daily-research-dispatch
```

または

```powershell
.\tools\show_tradex_daily_research_dispatch.ps1
```

返る項目:
- `selected_action`
- `selected_command`
- `action_summary`

### 4. 種別ごとに処理する

#### approve 系
- `next_action_kind = approve`
- まず promotion review の内容を確認し、必要なら decision を記録する。

```powershell
python -m external_analysis promotion-decision-run --decision hold --note "needs_manual_review"
```

#### avoid 系
- `next_action_kind = avoid`
- risk tag を深掘りする。

```powershell
python -m external_analysis daily-research-tag-report --strategy-tag "extension_fade" --limit 10
```

#### observe 系
- `next_action_kind = observe`
- improving tag を継続観察する。

```powershell
python -m external_analysis daily-research-tag-report --strategy-tag "box_breakout" --limit 10
```

### 5. 履歴を振り返る
```powershell
python -m external_analysis daily-research-history
```

または

```powershell
.\tools\show_tradex_daily_research_history.ps1
```

用途:
- `昨日から pending のまま残っている案件`
- `継続して risk のタグ`
- `最近の improving / risk / pending の推移`

## watchlist の意味

### `pending_promotions`
- まだ decision が記録されていない promotion 候補。
- `next_action_kind = approve`

### `improving_tags`
- 最近の履歴で強くなっているタグ。
- `next_action_kind = observe`

### `persistent_risk_tags`
- 継続して risk 側に出ているタグ。
- `next_action_kind = avoid`

### `top_next_actions`
- 上の 3 系統を priority score で束ねた上位 3 件。

## dispatch の意味
- watchlist を全部読まずに、`今やる 1 件` を返す薄い入口。
- Codex は基本的にここから入って良い。

## MeeMee との役割分担
- Tradex
  - 研究実行
  - 研究履歴保存
  - 承認記録
  - watchlist / dispatch / tag report
- MeeMee
  - 類似チャート表示
  - AI state evaluation 表示
  - research summary の閲覧
  - read-only dashboard

## 推奨の毎日ルーチン
1. `daily-research-run`
2. `daily-research-dispatch`
3. dispatch の `selected_action` に従って
   - `promotion-decision-run`
   - または `daily-research-tag-report`
4. 必要なら `daily-research-watchlist`
5. 週単位の確認で `daily-research-history`
