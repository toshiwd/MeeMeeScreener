export type BatchBarsRequestTimeframe = "daily" | "weekly" | "monthly";

export type BatchBarsRequestPayload = {
  codes: string[];
  timeframes: BatchBarsRequestTimeframe[];
  limit: number;
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
  };
  if (typeof includeBoxes === "boolean") {
    payload.includeBoxes = includeBoxes;
  }
  if (typeof asof === "string" && asof.trim()) {
    payload.asof = asof.trim();
  }
  return payload;
};
