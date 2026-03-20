# DetailView の状態設計再整理

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

## Purpose / Big Picture

MeeMee の銘柄詳細画面は、日足チャートを主役にした「確認画面」であるべきで、描画・類似・カーソル情報・分析・財務が別々の主役として混ざると、横幅も視線も安定しません。この作業では、状態の責務を整理して、主モード、補助ツール、右レールの役割を分け直します。変更後は、詳細画面を開いたときに右側が状態ごとに伸び縮みしないこと、描画や類似のために mode を切り替えなくてよいこと、財務が右レールの閲覧情報としてまとまって見えることを、実機で確認できます。

## Progress

- [ ] 既存の `DetailView.tsx` と関連パネルの状態・表示条件を確認し、現状の boolean 群とモード依存を証拠として整理する。
- [ ] `DetailView.tsx` の状態を `headerMode` と `rightRailKind` の単一責務に寄せ、draw モードと similar のモード化をなくす。
- [ ] `DailyMemoPanel.tsx` を親制御前提に直し、カーソル ON/OFF と空状態の制御を親へ戻す。
- [ ] `DetailFinancialPanel.tsx` を右レール向けに縮退し、大型グラフを初期表示から外して折りたたみに移す。
- [ ] `styles.css` を更新し、詳細画面を `minmax(0, 1fr) clamp(340px, 24vw, 420px)` の 2 カラム固定寄りにする。
- [ ] `PracticeView.tsx` の上部バーを詳細と揃え、描画とカーソルの操作ルールを同系統にする。
- [ ] targeted test と build を実行し、Playwright で detail / practice の実機確認を行う。

## Surprises & Discoveries

- Observation: `DetailView.tsx` には `headerMode` とは別に `showMemoPanel` / `showAnalysisPanel` / `showFinancialPanel` / `showRightPanel` などの複数 boolean があり、右側の表示が足し算になっていた。
  Evidence: `rg -n "showMemoPanel|showDrawPanel|showDrawInfoPanel|showAnalysisPanel|showFinancialPanel|showRightPanel|headerMode|rightRailKind|cursorMode" app/frontend/src/routes/DetailView.tsx`

- Observation: `DetailFinancialPanel.tsx` には推移グラフが初期表示に含まれており、右レール向けに縮退が必要だった。
  Evidence: `rg -n "detail-financial-collapsible|推移グラフ|ScreenPanel|detail-analysis-section-title|detail-financial-panel" app/frontend/src/routes/detail/DetailFinancialPanel.tsx`

- Observation: `DailyMemoPanel.tsx` はカーソル ON/OFF の制御を自分で持っていたため、親の右レール制御と責務が重なっていた。
  Evidence: `rg -n "onToggleCursorMode|カーソル|cursorMode|memo-panel-empty|日足情報" app/frontend/src/components/DailyMemoPanel.tsx`

## Decision Log

- Decision: `draw` は mode から外し、常設ツールバー側へ寄せる。
  Rationale: 描画の有無で画面の見方を切り替える必要はなく、ツールとして常時使える方が日常操作に合うため。
  Date/Author: 2026-03-20 / Codex

- Decision: `similar` は mode にしない。
  Rationale: 類似比較は補助操作であり、画面の主モードを増やすと責務が混ざるため。
  Date/Author: 2026-03-20 / Codex

- Decision: 右側表示は `rightRailKind = none | cursor | analysis | financial` の単一状態で管理する。
  Rationale: 複数 boolean の足し算では、幅や表示タイミングが状態ごとに揺れるため。
  Date/Author: 2026-03-20 / Codex

- Decision: `DetailFinancialPanel` の大型グラフは初期表示から外し、折りたたみへ移す。
  Rationale: 財務は閲覧モードとして compact に見せる方が、日足チャートの主役感を損ねないため。
  Date/Author: 2026-03-20 / Codex

## Outcomes & Retrospective

未着手。実装後に、何を単一状態にまとめたか、何を親制御に戻したか、右レール幅が固定できたかを記録する。

## Context and Orientation

対象は `app/frontend/src/routes/DetailView.tsx`、`app/frontend/src/components/DailyMemoPanel.tsx`、`app/frontend/src/routes/detail/DetailFinancialPanel.tsx`、`app/frontend/src/routes/PracticeView.tsx`、`app/frontend/src/styles.css` である。詳細画面は左に日足主チャート、右に固定幅レールを置く。`chart` モードでは `cursorMode` が有効なときだけ右レールを開き、それ以外では閉じる。`analysis` と `financial` は右レールの内容だけを切り替える。

## Plan of Work

まず `DetailView.tsx` の状態を整理し、右側の表示条件を 1 本化する。次に `DailyMemoPanel.tsx` から親制御に移すべき条件を外し、右レールの表示責務を親に集約する。続けて `DetailFinancialPanel.tsx` を compact 化し、最後に CSS で 2 カラム固定寄りの幅を強制する。必要なら `PracticeView.tsx` も同じ上段バー構造へ寄せるが、状態設計の変更を壊さない範囲で行う。

## Concrete Steps

作業ディレクトリは `C:\work\meemee-screener\app\frontend` とする。

1. まず現状を確認する。
    `rg -n "showMemoPanel|showDrawPanel|showDrawInfoPanel|showAnalysisPanel|showFinancialPanel|showRightPanel|headerMode|rightRailKind|cursorMode" src/routes/DetailView.tsx`

2. `DetailView.tsx` を整理する。
    `headerMode` から `draw` と `similar` のモード依存を外し、`rightRailKind` のみで右側を制御する。旧 boolean 群は削除し、`DailyMemoPanel` は親から表示制御する。

3. `DailyMemoPanel.tsx` を親制御前提に直す。
    カーソル ON/OFF のボタンや空状態メッセージの制御を外し、`cursorMode` が true のときに詳細を出すだけの表示コンポーネントに寄せる。

4. `DetailFinancialPanel.tsx` を縮退する。
    大型の推移グラフを初期表示から外し、`details` / accordion の中に移す。右レール用の見出しは `タイトル / 要約 / 詳細 / 補助操作` の順で揃える。

5. `styles.css` を更新する。
    `detail-content` を `minmax(0, 1fr) clamp(340px, 24vw, 420px)` の 2 カラムにし、右レール内のカードが幅を押し広げないよう `min-width: 0` と `width: 100%` を徹底する。

6. `PracticeView.tsx` を必要最小限で合わせる。
    描画は常設ツール、類似は補助ボタン、カーソルは右端トグルというルールを揃え、上段バーの構成が詳細と大きくズレないようにする。

7. 検証する。
    `npm run test -- src/components/DailyMemoPanel.test.tsx src/routes/detail/DetailAnalysisPanel.test.tsx src/routes/detail/DetailFinancialPanel.test.tsx src/routes/detail/components/DetailPositionLedgerSheet.test.tsx`

    `npm run build`

    その後、Playwright で detail と practice を開き、右レール幅がモード切替で変わらないこと、財務の大型グラフが初期表示に出ないこと、cursor OFF で右レールが閉じることを確認する。

## Validation and Acceptance

`npm run build` が成功し、`DetailView.tsx` から旧 boolean 群が消えていることを grep で確認できること。`DetailFinancialPanel.tsx` では、大型推移グラフが `details` の外に出ていないことを確認できること。Playwright では、detail 画面の chart / analysis / financial で右レールの横幅が同じ値になり、cursor OFF で右レールが閉じることを確認できること。

## Idempotence and Recovery

途中で JSX が崩れても、`git diff` と `rg -n` で壊れた文字列やタグを特定し、該当コンポーネントだけを戻す。右レール幅の CSS は 1 箇所に集め、個別パネルで width を上書きしない。もし `PracticeView.tsx` の寄せ込みが大きくなり過ぎる場合は、DetailView の責務整理を優先して PracticeView は後回しにする。

## Artifacts and Notes

実装後は、detail 画面の実機スクリーンショットと、分析・財務・カーソル OFF の比較を残す。財務の初期表示が compact になったことと、右レール幅が固定されたことが見える証拠を保存する。

## Interfaces and Dependencies

この変更は backend contract を変えない。依存するのは React の状態、既存の `ScreenPanel`、既存の財務・日足メモ・練習画面コンポーネントだけである。追加の API やデータ構造は導入しない。
