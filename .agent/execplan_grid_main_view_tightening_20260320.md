# 一覧画面の細かい仕様修正

この ExecPlan は living document です。`Progress`、`Surprises & Discoveries`、`Decision Log`、`Outcomes & Retrospective` を作業中に更新します。

## Purpose / Big Picture

一覧画面の main grid を、仕様書どおりの「俯瞰と選別」に寄せます。ユーザーは 1x1 から 5x5 の square preset だけを選び、表示本数が自動で切り替わり、並び替えも基本項目を中心に軽く使えます。操作パネルの空白を減らし、イベント・業種・フィルター・タイルの情報は読めるまま、研究画面のような重さを減らします。変更後は、一覧画面を開いて preset 切替、sort、業種絞り込み、フィルター drawer を触ると、仕様に沿った密度と操作感になっていることを目で確認できます。

## Progress

- [x] (2026-03-20 11:15JST) 正本 `docs/pages/meemee-grid.md`、`GridView`、`storeHelpers`、`TechnicalFilterDrawer`、関連 CSS の現状を確認した。
- [x] (2026-03-20 11:15JST) 既存の grid density / sort / sector / drawer の UI が `GridView` に集中していることを確認した。
- [ ] main grid の density を 1x1 〜 5x5 の square preset に固定し、表示本数を自動連動させる。
- [ ] sort menu を「基本」と「詳細」に分け、main grid では仕様中心の項目を前面化する。
- [ ] sector dropdown と technical filter drawer の余白を圧縮し、視認性を上げる。
- [ ] grid header の補助情報と tile 上部の優先順位を微調整する。
- [ ] storeHelpers / gridHelpers / menu まわりの targeted tests を更新する。
- [ ] frontend build と一覧画面の目視確認を完了する。

## Surprises & Discoveries

- Observation: `GridView` は main grid 用の独自ヘッダを持っており、`UnifiedListHeader` ではない。
  Evidence: `app/frontend/src/routes/GridView.tsx` の header 行に sort/display/sector/technical filter が集約されている。
- Observation: main grid の表示本数は `resolveGridRangeBars(rows, columns, fallback)` で square preset にだけ連動している。
  Evidence: `app/frontend/src/routes/grid/gridHelpers.ts` に `1x1=180`, `2x2=90`, `3x3=60`, `4x4=45`, `5x5=30` の対応がある。
- Observation: 現在の表示メニューは rows と columns を別々に選べて、`3x3に戻す` が残っている。
  Evidence: `app/frontend/src/routes/GridView.tsx` の display popover。

## Decision Log

- Decision: main grid の density は square preset のみを UI に残し、rows/columns の分離操作は廃止する。
  Rationale: 仕様が 1x1 〜 5x5 の square preset に固定されており、2x4 のような非仕様レイアウトは一覧の意図に反するため。
  Date/Author: 2026-03-20 / Codex
- Decision: sort menu は内部寄りの候補を残しつつ、main grid では基本ソートを先頭に出し、詳細は折りたたむ。
  Rationale: 完全削除よりも既存ロジックを壊しにくく、仕様中心の見え方に寄せられるため。
  Date/Author: 2026-03-20 / Codex
- Decision: 既存の保存値は壊しすぎないが、新規の保存/復元は square preset を単位に寄せる。
  Rationale: 既存利用者の保存状態を急に失わせず、今後の UI 仕様に合わせるため。
  Date/Author: 2026-03-20 / Codex

## Outcomes & Retrospective

未完了。実装後に、どの UI が削れ、どこが軽くなったかを結果ベースでまとめる。

## Context and Orientation

main grid の画面は `app/frontend/src/routes/GridView.tsx` にある。そこで `sortOpen`、`displayOpen`、`sectorSortOpen`、`TechFilterDrawer` をまとめて制御している。grid のサイズと本数の連動は `app/frontend/src/routes/grid/gridHelpers.ts` と `app/frontend/src/storeHelpers.ts` が source of truth で、保存値は `app/frontend/src/store.ts` の setter が localStorage に書いている。

`Square preset` とは、列数と行数が同じ 1x1, 2x2, 3x3, 4x4, 5x5 のみを指す。`basic sort` とは、コード順、騰落順、出来高急増順のような一覧向けの軽い並び替えを指す。`detailed sort` とは、買い候補や ML など内部寄りで、必要なら折りたたんで使う項目を指す。

## Plan of Work

まず `app/frontend/src/storeHelpers.ts` と `app/frontend/src/store.ts` で、main grid の density 保存を square preset 単位に寄せる。保存キーを一本化し、初期化時は未設定なら 3x3、既存の legacy 値が square ならそれを復元する。次に `app/frontend/src/routes/grid/gridHelpers.ts` と `app/frontend/src/routes/GridView.tsx` を更新し、表示メニューを preset ボタン列に置き換え、表示本数は preset から自動決定する。

その後 `GridView` の sort menu を再編する。基本セクションにはコード昇順/降順、騰落順昇順/降順、出来高急増順を置く。詳細セクションには既存の内部寄り項目をまとめ、主役にはしない。並び替えの実装は `GridView` の compare ロジックに残し、見せ方を整理するだけにする。

並行して `app/frontend/src/styles.css` と `app/frontend/src/components/TechnicalFilterDrawer.tsx` を調整し、業種 dropdown を内容に沿った幅に収め、drawer の左ナビと条件見出し周辺の余白を詰める。最後に `GridView` の上部補助情報と `StockTile.tsx` の右上アクションの視線優先度を少しだけ下げ、一覧の主情報が先に入るようにする。

## Concrete Steps

作業ディレクトリは `C:\work\meemee-screener`。変更後は frontend で次を順に実行する。

    cd app\frontend
    npm run test -- src/storeHelpers.test.ts src/routes/grid/gridHelpers.test.ts
    npm run build

テストの期待値は、未設定時の `3x3 / 60本`、保存済み preset の復元、`code asc` のデフォルト、そして square preset 以外を UI から選べないことを確認できること。build は失敗なく通ること。

## Validation and Acceptance

ユーザーが一覧画面を開いたとき、表示メニューには `1x1 / 2x2 / 3x3 / 4x4 / 5x5` だけが並び、列数と行数を別々に組み合わせる UI は見えない。preset を変えると表示本数が自動で 180 / 90 / 60 / 45 / 30 に切り替わる。sort メニューの main section にはコードと騰落と出来高急増が見え、内部寄りの候補は詳細側に退く。業種 dropdown と technical filter drawer は余白が減り、必要な情報だけが読める。

## Idempotence and Recovery

この変更は localStorage の読み書きを伴うが、初期化時の未設定判定で何度再実行しても壊れないようにする。もし途中で保存値が不整合になった場合は、`gridPreset` を再保存して square preset に戻せるようにする。旧キーは読み取り互換を残し、書き込みは新キー中心にする。

## Artifacts and Notes

作業中に必要になったら、`GridView` の sort section 定義、display menu の preset ボタン、`storeHelpers` の保存キーの差分を短い抜粋で残す。スクリーンショットは一覧画面のヘッダと 1 枚の grid tile が同時に見える状態を優先して残す。

## Interfaces and Dependencies

`app/frontend/src/storeHelpers.ts` には `getInitialGridPreset()` 相当の初期化ロジックと、preset 保存用の helper を置く。`app/frontend/src/routes/grid/gridHelpers.ts` には square preset から表示本数を返す関数と、preset の候補配列を置く。`app/frontend/src/routes/GridView.tsx` はその helper を使って density menu と sort menu を描画する。`app/frontend/src/components/TechnicalFilterDrawer.tsx` と `app/frontend/src/styles.css` は余白整理にとどめる。

Plan updated on 2026-03-20 after initial evidence gathering. The next revision must record any divergence from the square-preset-first approach or any storage migration decision.
