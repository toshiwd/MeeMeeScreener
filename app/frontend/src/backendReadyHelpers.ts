export type HealthReadyResponse = {
  ok?: boolean;
  status?: string;
  ready?: boolean;
  phase?: string;
  message?: string;
  error_code?: string | null;
  errors?: string[];
  retryAfterMs?: number;
  txt_count?: number;
  last_updated?: string | null;
  code_txt_missing?: boolean;
};

export const KEEPALIVE_FAIL_THRESHOLD = 3;
export const KEEPALIVE_RECONNECT_GRACE_MS = 25000;

export const isAliveHealthResponse = (
  status: number,
  data: HealthReadyResponse | null | undefined
): boolean => status >= 200 && status < 300 && data?.ready === true;

export const shouldReconnectAfterKeepaliveFailure = ({
  failCount,
  firstFailureAtMs,
  nowMs,
  threshold = KEEPALIVE_FAIL_THRESHOLD,
  graceMs = KEEPALIVE_RECONNECT_GRACE_MS
}: {
  failCount: number;
  firstFailureAtMs: number | null;
  nowMs: number;
  threshold?: number;
  graceMs?: number;
}): boolean => {
  if (failCount < threshold) return false;
  if (firstFailureAtMs == null) return false;
  return nowMs - firstFailureAtMs >= graceMs;
};
