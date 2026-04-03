export type BatchBarsRequestTimeframe = "daily" | "weekly" | "monthly";

export type BatchBarsRequestPayload = {
  codes: string[];
  timeframes: BatchBarsRequestTimeframe[];
  limit: number;
  timeframeLimits?: Partial<Record<BatchBarsRequestTimeframe, number>>;
  includeProvisional: boolean;
  includeBoxes?: boolean;
  asof?: string;
};

type Params = {
  code: string;
  timeframe: BatchBarsRequestTimeframe;
  limit: number;
  includeBoxes?: boolean;
  asof?: string | null;
};

export const buildSingleBatchBarsRequestPayload = ({
  code,
  timeframe,
  limit,
  includeBoxes,
  asof,
}: Params): BatchBarsRequestPayload => {
  const payload: BatchBarsRequestPayload = {
    codes: [code],
    timeframes: [timeframe],
    limit,
    includeProvisional: true,
    includeBoxes: typeof includeBoxes === "boolean" ? includeBoxes : false,
  };
  if (typeof asof === "string" && asof.trim()) {
    payload.asof = asof.trim();
  }
  return payload;
};

type DetailParams = {
  code: string;
  dailyLimit: number;
  weeklyLimit: number;
  monthlyLimit: number;
  asof?: string | null;
};

export const buildDetailBatchBarsRequestPayload = ({
  code,
  dailyLimit,
  weeklyLimit,
  monthlyLimit,
  asof,
}: DetailParams): BatchBarsRequestPayload => {
  const payload: BatchBarsRequestPayload = {
    codes: [code],
    timeframes: ["daily", "weekly", "monthly"],
    limit: Math.max(dailyLimit, weeklyLimit, monthlyLimit),
    timeframeLimits: {
      daily: dailyLimit,
      weekly: weeklyLimit,
      monthly: monthlyLimit,
    },
    includeProvisional: true,
    includeBoxes: true,
  };
  if (typeof asof === "string" && asof.trim()) {
    payload.asof = asof.trim();
  }
  return payload;
};

type DetailPrefetchParams = DetailParams & {
  includeBoxes?: boolean;
};

export const buildDetailPrefetchBatchBarsRequestPayload = ({
  code,
  dailyLimit,
  weeklyLimit,
  monthlyLimit,
  asof,
  includeBoxes,
}: DetailPrefetchParams): BatchBarsRequestPayload => {
  const payload = buildDetailBatchBarsRequestPayload({
    code,
    dailyLimit,
    weeklyLimit,
    monthlyLimit,
    asof,
  });
  payload.includeBoxes = typeof includeBoxes === "boolean" ? includeBoxes : false;
  return payload;
};
