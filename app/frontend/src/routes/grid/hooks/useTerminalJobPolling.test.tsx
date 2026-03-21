// @vitest-environment jsdom
import { act } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, describe, expect, it, vi } from "vitest";
import { api } from "../../../api";
import { useTerminalJobPolling } from "./useTerminalJobPolling";

Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });

const originalGet = api.get;

function Probe({ onTerminalJob }: { onTerminalJob: (item: { id?: string; status?: string }) => void }) {
  useTerminalJobPolling({
    enabled: true,
    onTerminalJob,
  });
  return <div>probe</div>;
}

afterEach(() => {
  api.get = originalGet;
  vi.restoreAllMocks();
  vi.useRealTimers();
});

describe("useTerminalJobPolling", () => {
  it("treats skipped jobs as terminal notifications", async () => {
    vi.useFakeTimers();
    const onTerminalJob = vi.fn();
    const getMock = vi
      .fn()
      .mockResolvedValueOnce({
        data: [
          {
            id: "job-old",
            status: "skipped",
            message: "TDNET import skipped",
          },
        ],
      })
      .mockResolvedValueOnce({
        data: [
          {
            id: "job-new",
            status: "skipped",
            message: "TDNET import skipped",
          },
        ],
      });
    api.get = getMock as typeof api.get;

    const container = document.createElement("div");
    document.body.appendChild(container);
    const root = createRoot(container);

    await act(async () => {
      root.render(<Probe onTerminalJob={onTerminalJob} />);
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });

    expect(getMock).toHaveBeenCalledTimes(1);
    expect(onTerminalJob).not.toHaveBeenCalled();

    await act(async () => {
      await vi.advanceTimersByTimeAsync(15000);
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });

    expect(getMock).toHaveBeenCalledTimes(2);
    expect(onTerminalJob).toHaveBeenCalledTimes(1);
    expect(onTerminalJob).toHaveBeenCalledWith(
      expect.objectContaining({
        id: "job-new",
        status: "skipped",
      })
    );

    act(() => {
      root.unmount();
    });
    container.remove();
  });
});
