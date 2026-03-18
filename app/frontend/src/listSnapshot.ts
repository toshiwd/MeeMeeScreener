import type { ListSnapshotMeta, Ticker } from "./storeTypes";

export type ScreenerListResponse =
  | Ticker[]
  | {
    items?: Ticker[];
    stale?: boolean;
    asOf?: string | null;
    updatedAt?: string | null;
    generation?: string | null;
    lastError?: string | null;
  };

export const normalizeScreenerListResponse = (
  payload: ScreenerListResponse | null | undefined
): { items: Ticker[]; meta: ListSnapshotMeta | null } => {
  if (Array.isArray(payload)) {
    return {
      items: payload,
      meta: {
        stale: false,
        asOf: null,
        updatedAt: null,
        generation: null,
        lastError: null
      }
    };
  }
  const items = Array.isArray(payload?.items) ? payload.items : [];
  return {
    items,
    meta: payload
      ? {
        stale: Boolean(payload.stale),
        asOf: payload.asOf ?? null,
        updatedAt: payload.updatedAt ?? null,
        generation: payload.generation ?? null,
        lastError: payload.lastError ?? null
      }
      : null
  };
};
