# TRADEX List Summary Read Path

## Purpose

MeeMee の list / screener surface で、visible rows / selected rows / favorites scope に対して read-only の TRADEX summary を見せる。
detail page と同じ TRADEX analysis semantics を使い、universe-wide synchronous analysis は行わない。

## Scope

この batch では、backend の additive summary path、frontend の list hook、list card の小さな annotation を追加する。

`AnalysisOutputContract` は変更しない。`/ops/publish` には触らない。write-back、override、background job system は追加しない。

## Orientation

既存の detail analysis path は `app/backend/services/tradex_analysis_service.py` にあり、`GET /api/ticker/tradex/analysis` から読める。
list summary path はその semantics を再利用し、短命 cache も可能な範囲で共有する。

GridView は visible rows、RankingView は selected rows、FavoritesView は favorites scope を batching 対象にする。

## Milestone 1: Backend summary service and endpoint

`app/backend/services/tradex_list_summary_service.py` に bounded な `{code, asof}` の batch を受ける thin service を置く。
返す summary は read-only で、list UI に必要な最小情報だけを含める。

- dominant tone
- confidence
- publish readiness
- top reasons
- unavailable reason

cache は 30-60 秒 TTL の短命 cache を使う。whole-universe の同期分析は行わない。

`POST /api/ticker/tradex/summary` を `app/backend/api/routers/ticker.py` に追加する。
items / scope / optional asof metadata を受け取り、analysis unavailable の場合は stable fallback を返す。

Validation:

```bash
python -m pytest tests/test_tradex_list_summary_service.py tests/test_ticker_tradex_summary_api.py -q
```

## Milestone 2: Frontend list hook and badge component

軽量な frontend hook を追加する。要件は次のとおり。

- request-id guard と AbortController を使う
- symbol + asof の short-lived cache を持つ
- fresh な item は再要求しない
- feature flag disabled 時は stable unavailable を返す

小さな summary badge component を追加し、list card の annotation row を再利用する。
必要なら StockTile へ最小限の prop extension を入れる。

Validation:

```bash
cd app/frontend
npm run test
npm run build
```

## Milestone 3: Wire the list surfaces

次の list surfaces に scope-specific batching をつなぐ。

- GridView: visible rows
- RankingView: selected rows
- FavoritesView: favorites scope

list scroll / selection が重くならないことを優先する。request storms は cache と request dedupe で抑える。

Validation:

```bash
cd app/frontend
npx playwright test --grep "TRADEX summary" --reporter=line
```

## Progress

- [x] Backend batch summary service added
- [x] Summary endpoint added
- [x] Frontend summary hook added
- [x] List card summary badge added
- [x] GridView wired to visible rows
- [x] RankingView wired to selected rows
- [ ] FavoritesView wired to favorites scope
- [ ] Tests added and passing

## Surprises & Discoveries

- None yet. Update when cache behavior, scope behavior, or data shape mismatches are discovered.

## Decision Log

- Decision: Keep the batch summary path additive and read-only.
  Reason: The detail analysis contract already represents the core TRADEX semantics; the list path should reuse those semantics and only trim the visible UI surface.
- Decision: Use scope-specific batching rather than universe-wide refresh.
  Reason: List scrolling and selection should not trigger expensive global work.

## Outcomes & Retrospective

- Pending implementation.
