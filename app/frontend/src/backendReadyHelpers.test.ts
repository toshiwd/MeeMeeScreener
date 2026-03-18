import { describe, expect, it } from "vitest";
import {
  isAliveHealthResponse,
  KEEPALIVE_FAIL_THRESHOLD,
  KEEPALIVE_RECONNECT_GRACE_MS,
  shouldReconnectAfterKeepaliveFailure
} from "./backendReadyHelpers";

describe("isAliveHealthResponse", () => {
  it("treats degraded ready responses as alive", () => {
    expect(
      isAliveHealthResponse(200, {
        ready: true,
        status: "degraded",
        errors: ["db_unavailable"]
      })
    ).toBe(true);
  });

  it("returns false when the backend is unreachable", () => {
    expect(isAliveHealthResponse(503, { ready: false, status: "starting" })).toBe(false);
  });
});

describe("shouldReconnectAfterKeepaliveFailure", () => {
  it("does not reconnect before the threshold window passes", () => {
    expect(
      shouldReconnectAfterKeepaliveFailure({
        failCount: KEEPALIVE_FAIL_THRESHOLD,
        firstFailureAtMs: 10_000,
        nowMs: 10_000 + KEEPALIVE_RECONNECT_GRACE_MS - 1
      })
    ).toBe(false);
  });

  it("reconnects only after sustained failures", () => {
    expect(
      shouldReconnectAfterKeepaliveFailure({
        failCount: KEEPALIVE_FAIL_THRESHOLD,
        firstFailureAtMs: 10_000,
        nowMs: 10_000 + KEEPALIVE_RECONNECT_GRACE_MS
      })
    ).toBe(true);
  });
});
