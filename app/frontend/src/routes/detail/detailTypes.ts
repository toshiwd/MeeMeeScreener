export type Timeframe = "daily" | "weekly" | "monthly";
export type FocusPanel = Timeframe | null;

export type Candle = {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
};

export type VolumePoint = {
  time: number;
  value: number;
};

export type ParseStats = {
  total: number;
  parsed: number;
  invalidRow: number;
  invalidTime: number;
  invalidValue: number;
};

export type FetchState = {
  status: "idle" | "loading" | "success" | "error";
  responseCount: number;
  errorMessage: string | null;
};

export type JobStatusPayload = {
  id?: string;
  type?: string;
  status?: string;
  progress?: number | null;
  message?: string | null;
  error?: string | null;
};

export type ApiWarnings = {
  items: string[];
  info?: string[];
  unrecognized_labels?: { count: number; samples: string[] };
};

export type BarsMeta = {
  hasProvisional?: boolean;
  panDelayed?: boolean;
  latestPanDate?: number | null;
  latestYahooDate?: number | null;
  latestResolvedDate?: number | null;
  pendingYahooDate?: number | null;
  delayedPendingDate?: number | null;
  message?: string | null;
};

export type BarsResponse = {
  data?: number[][];
  errors?: string[];
  meta?: BarsMeta | null;
};

export type CompareListItem = {
  ticker: string;
  asof: string | null;
};

export type CompareListPayload = {
  queryTicker: string;
  mainAsOf: string | null;
  items: CompareListItem[];
};

export type AnalysisHorizonKey = 5 | 10 | 20;

export type AnalysisHorizonEntry = {
  horizon: AnalysisHorizonKey;
  pUp: number | null;
  pDown: number | null;
  evNet: number | null;
  pTurnDown: number | null;
  pTurnUp: number | null;
  pUpProjected: boolean;
  evProjected: boolean;
  turnProjected: boolean;
};

export type AnalysisHorizonPayload = {
  defaultHorizon: AnalysisHorizonKey;
  turnBaseHorizon: number | null;
  projectionMethod: string | null;
  items: Partial<Record<`${AnalysisHorizonKey}`, AnalysisHorizonEntry>>;
};

export type RankRiskMode = "defensive" | "balanced" | "aggressive";

export type AnalysisAdditiveSignals = {
  trendUpStrict: boolean;
  mtfStrongAligned: boolean;
  boxBottomAligned: boolean;
  shootingStarLike: boolean;
  threeWhiteSoldiers: boolean;
  bullEngulfing: boolean;
  reclaim60: boolean;
  v60Core: boolean;
  v60Strong: boolean;
  v60StrongPenalty: boolean;
  candlestickPatternBonus: number | null;
  bonusEstimate: number | null;
  weeklyBreakoutUpProb: number | null;
  monthlyBreakoutUpProb: number | null;
  monthlyRangeProb: number | null;
  monthlyRangePos: number | null;
};

export type AnalysisEntryPolicySide = {
  setupType: string | null;
  recommendedHoldDays: number | null;
  recommendedHoldMinDays: number | null;
  recommendedHoldMaxDays: number | null;
  recommendedHoldReason: string | null;
  invalidationTrigger: string | null;
  invalidationConservativeAction: string | null;
  invalidationAggressiveAction: string | null;
  invalidationRecommendedAction: string | null;
  invalidationDotenRecommended: boolean;
  invalidationOppositeHoldDays: number | null;
  invalidationExpectedDeltaMean: number | null;
  invalidationPolicyNote: string | null;
  playbookScoreBonus: number | null;
};

export type AnalysisEntryPolicy = {
  riskMode: RankRiskMode;
  up: AnalysisEntryPolicySide | null;
  down: AnalysisEntryPolicySide | null;
};

export type BuyStagePrecisionEntry = {
  precision: number | null;
  samples: number;
  wins: number;
};

export type BuyStrategyBacktest = {
  precision: number | null;
  samples: number;
  wins: number;
  probeShares: number | null;
  addShares: number | null;
  coreShares: number | null;
  topupShares: number | null;
  targetShares: number | null;
  takeProfitPct: number | null;
};

export type BuyStagePrecisionPayload = {
  horizon: number | null;
  lookbackBars: number | null;
  probe: BuyStagePrecisionEntry;
  add: BuyStagePrecisionEntry;
  core: BuyStagePrecisionEntry;
  strategy: BuyStrategyBacktest | null;
};

export type SellAnalysisFallback = {
  dt: number | string | null;
  close: number | null;
  dayChangePct: number | null;
  pDown: number | null;
  pTurnDown: number | null;
  ev20Net: number | null;
  rankDown20: number | null;
  predDt: number | string | null;
  pUp5: number | null;
  pUp10: number | null;
  pUp20: number | null;
  shortScore: number | null;
  aScore: number | null;
  bScore: number | null;
  ma20: number | null;
  ma60: number | null;
  ma20Slope: number | null;
  ma60Slope: number | null;
  distMa20Signed: number | null;
  distMa60Signed: number | null;
  trendDown: boolean | null;
  trendDownStrict: boolean | null;
  fwdClose5: number | null;
  fwdClose10: number | null;
  fwdClose20: number | null;
  shortRet5: number | null;
  shortRet10: number | null;
  shortRet20: number | null;
  shortWin5: boolean | null;
  shortWin10: boolean | null;
  shortWin20: boolean | null;
};

export type PhaseFallback = {
  dt: number | null;
  earlyScore: number | null;
  lateScore: number | null;
  bodyScore: number | null;
  n: number | null;
  reasons: string[];
};

export type AnalysisResearchPriorSide = {
  aligned: boolean;
  rank: number | null;
  universe: number | null;
  bonus: number | null;
  asOf: string | null;
};

export type AnalysisResearchPrior = {
  runId: string | null;
  up: AnalysisResearchPriorSide | null;
  down: AnalysisResearchPriorSide | null;
};

export type AnalysisEdinetSummary = {
  status: string | null;
  mapped: boolean | null;
  freshnessDays: number | null;
  metricCount: number | null;
  qualityScore: number | null;
  dataScore: number | null;
  scoreBonus: number | null;
  featureFlagApplied: boolean | null;
  ebitdaMetric: number | null;
  roe: number | null;
  equityRatio: number | null;
  debtRatio: number | null;
  operatingCfMargin: number | null;
  revenueGrowthYoy: number | null;
};

export type EdinetFinancialSummary = {
  latestFiscalYear: number | null;
  equityRatio: number | null;
  eps: number | null;
  bps: number | null;
  dividendPerShare: number | null;
  netInterestBearingDebt: number | null;
};

export type EdinetFinancialPoint = {
  fiscalYear: number | null;
  label: string;
  revenue: number | null;
  grossProfit: number | null;
  operatingIncome: number | null;
  netIncome: number | null;
  grossMargin: number | null;
  operatingMargin: number | null;
  netMargin: number | null;
  roe: number | null;
  roa: number | null;
  eps: number | null;
  bps: number | null;
  dividendPerShare: number | null;
  equityRatio: number | null;
  netInterestBearingDebt: number | null;
};

export type EdinetFinancialPanel = {
  status: string | null;
  mapped: boolean | null;
  fetchedAt: string | null;
  summary: EdinetFinancialSummary | null;
  series: EdinetFinancialPoint[];
};

export type TdnetDisclosureItem = {
  disclosureId: string | null;
  title: string | null;
  publishedAt: string | null;
  fetchedAt: string | null;
  tdnetUrl: string | null;
  pdfUrl: string | null;
  xbrlUrl: string | null;
  summaryText: string | null;
  eventType: string | null;
  sentiment: string | null;
  importanceScore: number | null;
  tags: string[];
};

export type TaisyakuBalanceItem = {
  applicationDate: number | null;
  settlementDate: number | null;
  issueName: string | null;
  marketName: string | null;
  reportType: string | null;
  financeBalanceShares: number | null;
  stockBalanceShares: number | null;
  netBalanceShares: number | null;
  loanRatio: number | null;
  fetchedAt: string | null;
};

export type TaisyakuFeeItem = {
  applicationDate: number | null;
  settlementDate: number | null;
  issueName: string | null;
  marketName: string | null;
  reasonType: string | null;
  reasonValue: string | null;
  priceYen: number | null;
  stockExcessShares: number | null;
  maxFeeYen: number | null;
  currentFeeYen: number | null;
  feeDays: number | null;
  priorFeeYen: number | null;
  fetchedAt: string | null;
};

export type TaisyakuRestrictionItem = {
  issueName: string | null;
  announcementKind: string | null;
  measureType: string | null;
  measureDetail: string | null;
  noticeDate: number | null;
  afternoonStop: string | null;
  fetchedAt: string | null;
};

export type TaisyakuIssueItem = {
  applicationDate: number | null;
  issueName: string | null;
  tseFlag: number | null;
  jnxFlag: number | null;
  odxFlag: number | null;
  jaxFlag: number | null;
  nseFlag: number | null;
  fseFlag: number | null;
  sseFlag: number | null;
  fetchedAt: string | null;
};

export type TaisyakuSnapshot = {
  code: string | null;
  issue: TaisyakuIssueItem | null;
  latestBalance: TaisyakuBalanceItem | null;
  balanceHistory: TaisyakuBalanceItem[];
  latestFee: TaisyakuFeeItem | null;
  restrictions: TaisyakuRestrictionItem[];
  fetchedAt: string | null;
};

export type TdnetReactionPoint = {
  bars: number;
  label: string;
  targetDate: string | null;
  returnRatio: number | null;
};

export type TdnetReactionSummary = {
  baseDate: string | null;
  baseClose: number | null;
  volumeRatio: number | null;
  reactions: TdnetReactionPoint[];
};

export type AnalysisSwingPlanSide = "long" | "short";

export type AnalysisSwingPlan = {
  code: string | null;
  side: AnalysisSwingPlanSide;
  score: number | null;
  horizonDays: number | null;
  entry: number | null;
  stop: number | null;
  tp1: number | null;
  tp2: number | null;
  timeStopDays: number | null;
  reasons: string[];
};

export type AnalysisSwingSetupExpectancy = {
  asOfYmd: number | null;
  side: "long" | "short" | null;
  setupType: string | null;
  horizonDays: number | null;
  samples: number | null;
  winRate: number | null;
  meanRet: number | null;
  shrunkMeanRet: number | null;
  p25Ret: number | null;
  p10Ret: number | null;
  maxAdverse: number | null;
  sideMeanRet: number | null;
};

export type AnalysisSwingDiagnostics = {
  edge: number | null;
  risk: number | null;
  setupExpectancy: AnalysisSwingSetupExpectancy | null;
  regimeFit: number | null;
  atrPct: number | null;
  liquidity20d: number | null;
};

export type AnalysisFallback = {
  dt: number | string | null;
  pUp: number | null;
  pDown: number | null;
  pTurnUp: number | null;
  pTurnDown: number | null;
  pTurnDownHorizon: number | null;
  retPred20: number | null;
  ev20: number | null;
  ev20Net: number | null;
  horizonAnalysis: AnalysisHorizonPayload | null;
  additiveSignals: AnalysisAdditiveSignals | null;
  entryPolicy: AnalysisEntryPolicy | null;
  riskMode: RankRiskMode | null;
  buyStagePrecision: BuyStagePrecisionPayload | null;
  researchPrior: AnalysisResearchPrior | null;
  edinetSummary: AnalysisEdinetSummary | null;
  modelVersion: string | null;
  decision: AnalysisDecision | null;
  swingPlan: AnalysisSwingPlan | null;
  swingDiagnostics: AnalysisSwingDiagnostics | null;
};

export type AnalysisDecisionTone = "up" | "down" | "neutral";
export type AnalysisDecisionScenario = {
  key: "up" | "down" | "range";
  label: string;
  tone: AnalysisDecisionTone;
  score: number;
};
export type AnalysisDecision = {
  tone: AnalysisDecisionTone;
  sideLabel: string | null;
  patternLabel: string | null;
  environmentLabel: string | null;
  confidence: number | null;
  buyProb: number | null;
  sellProb: number | null;
  neutralProb: number | null;
  version: string | null;
  scenarios: AnalysisDecisionScenario[];
};

export type EnvironmentTone = "up" | "down" | "neutral";
export type EnvironmentScenario = {
  key: "up" | "down" | "range";
  label: string;
  tone: EnvironmentTone;
  score: number;
  reasons: string[];
};
export type EnvironmentSummary = {
  environmentLabel: string;
  environmentTone: EnvironmentTone;
  markerTone: "up" | "down" | null;
  markerIsSetup: boolean;
  scenarios: EnvironmentScenario[];
};

export type EnvironmentComputationInput = {
  analysisPUp: number | null;
  analysisPDown: number | null;
  analysisPTurnUp: number | null;
  analysisPTurnDown: number | null;
  analysisEvNet: number | null;
  playbookUpScoreBonus: number | null;
  playbookDownScoreBonus: number | null;
  additiveSignals: AnalysisAdditiveSignals | null;
  sellAnalysis: Pick<
    SellAnalysisFallback,
    "pDown" | "pTurnDown" | "shortScore" | "distMa20Signed" | "ma20Slope" | "ma60Slope" | "trendDown" | "trendDownStrict"
  > | null;
  includeReasons?: boolean;
};


export type TradexAnalysisSideRatios = {
  buy: number;
  neutral: number;
  sell: number;
};

export type TradexAnalysisCandidateComparison = {
  candidateKey: string;
  baselineKey: string | null;
  comparisonScope: string;
  score: number | null;
  scoreDelta: number | null;
  rank: number | null;
  reasons: string[];
  publishReady: boolean | null;
};

export type TradexAnalysisPublishReadiness = {
  ready: boolean;
  status: string;
  reasons: string[];
  candidateKey: string | null;
  approved: boolean | null;
};

export type TradexAnalysisOverrideState = {
  present: boolean;
  source: string | null;
  logicKey: string | null;
  logicVersion: string | null;
  reason: string | null;
};

export type TradexAnalysisOutput = {
  symbol: string;
  asof: string;
  sideRatios: TradexAnalysisSideRatios;
  confidence: number | null;
  reasons: string[];
  candidateComparisons: TradexAnalysisCandidateComparison[];
  publishReadiness: TradexAnalysisPublishReadiness;
  overrideState: TradexAnalysisOverrideState;
};

export type TradexAnalysisReadResult = {
  available: boolean;
  reason: string | null;
  analysis: TradexAnalysisOutput | null;
};
