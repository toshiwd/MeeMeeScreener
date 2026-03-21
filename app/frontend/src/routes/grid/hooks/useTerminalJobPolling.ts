import { useEffect, useRef } from "react";
import { api } from "../../../api";

type JobHistoryItem = {
  id?: string;
  type?: string;
  status?: string;
  message?: string | null;
};

type UseTerminalJobPollingParams = {
  enabled: boolean;
  onTerminalJob: (item: JobHistoryItem) => void | Promise<void>;
};

const TERMINAL_JOB_STATUS = new Set(["success", "failed", "canceled", "skipped"]);
const ACTIVE_JOB_STATUS = new Set(["queued", "running", "cancel_requested"]);

export function useTerminalJobPolling({ enabled, onTerminalJob }: UseTerminalJobPollingParams) {
  const seenTerminalJobsRef = useRef<Set<string>>(new Set());
  const initializedRef = useRef(false);

  useEffect(() => {
    if (!enabled) return;
    let disposed = false;
    let timer: number | null = null;

    const scheduleNext = (delayMs: number) => {
      if (disposed) return;
      if (timer !== null) {
        window.clearTimeout(timer);
      }
      timer = window.setTimeout(() => {
        void pollTerminalJobs();
      }, delayMs);
    };

    const pollTerminalJobs = async () => {
      let nextDelayMs = 15000;
      try {
        const res = await api.get("/jobs/history", { params: { limit: 20 } });
        if (disposed) return;
        const list = Array.isArray(res.data) ? (res.data as JobHistoryItem[]) : [];
        const hasActiveJobs = list.some((item) => ACTIVE_JOB_STATUS.has(String(item?.status ?? "")));
        nextDelayMs = hasActiveJobs ? 4000 : 15000;
        const terminalItems = list.filter((item) =>
          TERMINAL_JOB_STATUS.has(String(item?.status ?? ""))
        );

        if (!initializedRef.current) {
          for (const item of terminalItems) {
            if (typeof item.id === "string" && item.id) {
              seenTerminalJobsRef.current.add(item.id);
            }
          }
          initializedRef.current = true;
          scheduleNext(nextDelayMs);
          return;
        }

        for (const item of [...terminalItems].reverse()) {
          const id = typeof item.id === "string" ? item.id : "";
          if (!id) continue;
          if (seenTerminalJobsRef.current.has(id)) continue;
          seenTerminalJobsRef.current.add(id);
          void onTerminalJob(item);
        }
      } catch {
        // Keep silent; polling failures are transient.
      }
      scheduleNext(nextDelayMs);
    };

    void pollTerminalJobs();
    return () => {
      disposed = true;
      if (timer !== null) {
        window.clearTimeout(timer);
      }
    };
  }, [enabled, onTerminalJob]);
}
