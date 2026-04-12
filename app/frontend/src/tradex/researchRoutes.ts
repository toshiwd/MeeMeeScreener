export const TRADEX_RESEARCH_ROUTE_PREFIX = "/tradex/research";

export const TRADEX_RESEARCH_ENDPOINTS = {
  stateEvalTags: "/state-eval-tags",
  stateEvalTagsSummary: "/state-eval-tags/summary",
  stateEvalCandlesSummary: "/state-eval-candles/summary",
  stateEvalCandleCombosSummary: "/state-eval-candle-combos/summary",
  stateEvalDailySummary: "/state-eval-daily-summary",
  stateEvalDailySummaryHistory: "/state-eval-daily-summary/history",
  stateEvalTrends: "/state-eval-trends",
  stateEvalCandleComboTrends: "/state-eval-candle-combo-trends",
  stateEvalActionQueue: "/state-eval-action-queue",
  replayProgress: "/replay-progress",
  forecastSurfaceProjection: "/forecast-surface-projection",
  stateEvalPromotionReview: "/state-eval-promotion-review",
  stateEvalPromotionDecision: "/state-eval-promotion-decision",
  stateEvalTagsCsv: "/state-eval-tags.csv",
  stateEvalDailySummaryCsv: "/state-eval-daily-summary.csv"
} as const;

export const tradexResearchRoute = (path: string) =>
  `${TRADEX_RESEARCH_ROUTE_PREFIX}${path.startsWith("/") ? path : `/${path}`}`;

export const buildTradexResearchCsvHref = (
  path: string,
  params: URLSearchParams,
  apiBase = window.MEEMEE_API_BASE || "/api"
) => `${apiBase}${tradexResearchRoute(path)}?${params.toString()}`;
