# 詳細系UIの骨格統一

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

## Purpose / Big Picture

MeeMee の詳細、分析、財務、描画、練習画面は、いまのままだと同じプロダクト内なのに別物に見える。ユーザーは画面を切り替えるたびにボタン配置や右パネルの密度を読み直す必要があり、日々の確認作業が重くなる。

この作業の目的は、各画面を「上段ヘッダ」「セカンドバー」「本文」の3層に揃え、ボタン種別、余白、右パネルの見出し構造を共通化することだ。完了後は、詳細/分析/財務/描画/練習を切り替えても、ユーザーが視線を置く場所とボタンの意味がほぼ同じに見える。

## Progress

- [ ] (2026-03-20) 共通化対象の現状を把握し、DetailView / PracticeView / OffscreenDetailView / 右パネル群の差分を確認する。
- [ ] (2026-03-20) 共通ヘッダと共通アクションボタンの最小コンポーネントを追加し、DetailView と PracticeView へ適用する。
- [ ] (2026-03-20) 右パネルのタイトル・要約・詳細・補助操作の見出し構造を共通 CSS に寄せる。
- [ ] (2026-03-20) `styles.css` と `theme/tokens.css` を整理し、色・余白・文字サイズの役割を分ける。
- [ ] (2026-03-20) targeted test と build を通し、DetailView / PracticeView / analysis / financial / draw の実機表示を確認する。

## Surprises & Discoveries

- Observation: `app/frontend/src/theme/tokens.css` は既に存在し、背景/文字/ボタン系のトークンを一部持っている。
  Evidence: `app/frontend/src/theme/tokens.css` 先頭に `--theme-bg-primary` や `--btn-*` が定義されている。

- Observation: `DetailView` はヘッダやトップバーの整理が進んでいる一方、描画モードだけ右パネルの性格が強く、共通骨格から少し外れている。
  Evidence: `app/frontend/src/routes/DetailView.tsx` に `drawPanel` と `showRightPanel` の分岐がある。

## Decision Log

- Decision: 共通化は大きな再設計ではなく、共通ヘッダ・共通パネル・共通トークンの3層で進める。
  Rationale: 既存の画面ロジックを壊さずに、見た目の一貫性を先に確保できるため。
  Date/Author: 2026-03-20 / Codex

- Decision: `PracticeView` は個別のセッション管理を残しつつ、ヘッダとボタンの見た目だけを共通骨格に寄せる。
  Rationale: 練習画面の機能は重いので、ロジックまで統一すると変更範囲が広がりすぎるため。
  Date/Author: 2026-03-20 / Codex

## Outcomes & Retrospective

未完了。完了後に、どの画面が共通骨格へ寄ったか、どの差分を残したか、今後の保守で何を共通ルールとして維持すべきかを記録する。

## Context and Orientation

対象は `app/frontend/src/routes/DetailView.tsx`、`app/frontend/src/routes/PracticeView.tsx`、`app/frontend/src/components/OffscreenDetailView.tsx`、`app/frontend/src/routes/detail/DetailAnalysisPanel.tsx`、`app/frontend/src/routes/detail/DetailFinancialPanel.tsx`、`app/frontend/src/styles.css`、`app/frontend/src/theme/tokens.css` である。

ここでいう「上段ヘッダ」は、戻る・銘柄コード/銘柄名・期間切替・お気に入り・前後銘柄をまとめる領域を指す。「セカンドバー」は、モードタブと最低限のアクションを置く 2 段目の帯を指す。「右パネル」は、分析・財務・描画・メモ・建玉などの補助情報を縦に並べる領域を指す。

## Plan of Work

まず、共通表示の入口を作る。`app/frontend/src/components/` 配下に、ヘッダの 2 行とアクションボタン列を受け取る小さな共通コンポーネントを置き、`IconButton` と segmented ボタンの見た目を統一する。`DetailView` と `PracticeView` はこの共通コンポーネントを使って、既存の状態管理やイベントハンドラはそのまま維持する。

次に、右パネルの見出しと本文の余白をそろえる。`DetailAnalysisPanel`、`DetailFinancialPanel`、`DailyMemoPanel`、`PracticeView` の右側カードに同じ見出し階層と同じパディングを当てる。`memo-panel-header` をそのまま使う部分は残してよいが、ラッパーの意味を共通化して見た目を揃える。

最後に、`styles.css` と `theme/tokens.css` を整理する。色は background / border / accent / success / danger / muted の役割を明確にし、余白は 8 / 12 / 16 / 24 の刻みを中心に寄せる。`page title`、`section title`、`body`、`caption` の4段階に収まるよう、既存の局所フォントサイズを共通クラスへ吸収する。

## Concrete Steps

作業ディレクトリは `C:\work\meemee-screener\app\frontend` とする。

1. 既存差分の確認を行う。

    `rg -n "detail-header|practice-header|memo-panel-header|detail-analysis-panel|detail-financial-panel|segmented" src`

2. 共通コンポーネントを追加する。

    `src/components/ScreenChrome.tsx` に、ヘッダ行・セカンドバー・右パネルの見出しで再利用できる軽量な部品を定義する。

3. `DetailView.tsx` と `PracticeView.tsx` で共通コンポーネントを使う。

    既存の state や handler はそのまま流し、JSX の外枠だけを共通化する。

4. 右パネルの見出しを揃える。

    `DetailAnalysisPanel.tsx`、`DetailFinancialPanel.tsx`、`DailyMemoPanel.tsx`、`PracticeView.tsx` の右パネルを、同じ title / summary / details / actions の並びに寄せる。

5. CSS トークンと共通クラスを整理する。

    `styles.css` と `theme/tokens.css` に、色・余白・文字サイズの共通規則を集約する。

6. 検証する。

    `npm run test -- src/routes/detail/DetailAnalysisPanel.test.tsx src/routes/detail/DetailFinancialPanel.test.tsx src/routes/detail/components/DetailPositionLedgerSheet.test.tsx`

    `npm run build`

    その後、詳細、分析、財務、描画、練習を実機で切り替え、ヘッダと右パネルの骨格が揃って見えることを確認する。

## Validation and Acceptance

`npm run build` が成功し、詳細画面と練習画面を開いたときに、上段ヘッダの並び方とボタンの密度が大きく変わらないことを確認する。分析と財務の右パネルでは、タイトル、要約、詳細、補助操作の順が同じに見えることを確認する。描画モードでは、操作が右パネル側に寄っても同じボタンサイズと余白ルールで見えることを確認する。

## Idempotence and Recovery

コンポーネントの導入は追加的に行う。既存の画面ロジックを壊した場合は、共通コンポーネントの利用を外して元の JSX に戻せるよう、表示ロジックとレイアウトロジックを分けて編集する。CSS の変更はクラス単位で戻せるよう、画面固有のクラスと共通クラスを分けて追加する。

## Artifacts and Notes

実装後に、主要画面のスクリーンショットを残す。特に詳細の chart / analysis / financial / draw / practice を比較し、タイトル行・セカンドバー・右パネルの見え方が揃っていることを確認材料にする。

## Interfaces and Dependencies

最終的に、少なくとも次の共通部品または共通クラスが存在することを目標にする。

    src/components/ScreenChrome.tsx
    src/components/ScreenPanel.tsx
    src/components/ScreenActionButton.tsx

これらは、詳細と練習の両方で再利用できるよう、見た目の責務だけを持ち、各画面のデータ取得や状態変更は引き続き各画面側で行う。

