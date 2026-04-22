import { useEffect, useMemo, useState } from "react";
import { useLocation } from "react-router-dom";
import { api } from "../api";
import { useBackendReadyState } from "../backendReady";
import { formatTxtUpdateStatusLabel } from "../utils/txtUpdate";

type JobStatusPayload = {
  id?: string;
  type?: string;
  status?: string;
  progress?: number | null;
  message?: string;
  error?: string | null;
};

const TERMINAL_JOB_STATUS = new Set(["success", "failed", "canceled"]);

const formatStageLabel = (message?: string | null, fallback?: string | null) => {
  const text = message?.toLowerCase() ?? "";
  if (!text) return fallback ?? "待機中";
  if (text.includes("tracking_refresh")) return "追跡更新";
  if (text.includes("pan import") || text.includes("launching pan")) return "PAN取込";
  if (text.includes("pan rolling export") || text.includes("vbs export") || text.includes("export completed")) {
    return "TXT出力";
  }
  if (text.includes("ingesting")) return "TXT取込";
  if (text.includes("phase")) return "Phase更新";
  if (text.includes("ml")) return "ML更新";
  if (text.includes("score")) return "スコア更新";
  if (text.includes("cache")) return "キャッシュ更新";
  if (text.includes("walkforward")) return "検証";
  if (text.includes("final")) return "仕上げ";
  if (text.includes("queue")) return "待機";
  return fallback ?? "更新中";
};

const getTone = (status?: string | null) => {
  if (!status) return "is-idle";
  if (status === "success") return "is-done";
  if (status === "failed" || status === "canceled") return "is-error";
  return "is-running";
};

export default function GlobalTxtUpdateStatus() {
  const { backendReady } = useBackendReadyState();
  const location = useLocation();
  const [job, setJob] = useState<JobStatusPayload | null>(null);
  const [polling, setPolling] = useState(false);

  const statusLabel = useMemo(() => {
    if (!job) return null;
    return formatTxtUpdateStatusLabel(job.status);
  }, [job]);

  const tone = useMemo(() => getTone(job?.status), [job?.status]);

  const progressValue = useMemo(() => {
    const raw = job?.progress;
    if (typeof raw !== "number" || !Number.isFinite(raw)) return null;
    return Math.max(0, Math.min(100, Math.round(raw)));
  }, [job?.progress]);

  const stageLabel = useMemo(
    () => formatStageLabel(job?.message, statusLabel ?? "更新中"),
    [job?.message, statusLabel]
  );

  const shortDetail = useMemo(() => {
    const message = job?.message?.trim();
    if (!message) return null;
    return message.length > 72 ? `${message.slice(0, 72)}...` : message;
  }, [job?.message]);

  useEffect(() => {
    if (!backendReady || polling || job?.id) return;
    let disposed = false;

    const loadCurrentJob = async () => {
      try {
        const res = await api.get("/jobs/current");
        if (disposed) return;
        const payload = (res.data ?? null) as JobStatusPayload | null;
        if (!payload || payload.type !== "txt_update") {
          setJob(null);
          setPolling(false);
          return;
        }
        setJob(payload);
        setPolling(!TERMINAL_JOB_STATUS.has(payload.status ?? ""));
      } catch {
        // ignore temporary fetch errors
      }
    };

    void loadCurrentJob();
    const timer = window.setInterval(() => {
      void loadCurrentJob();
    }, 2000);
    return () => {
      disposed = true;
      window.clearInterval(timer);
    };
  }, [backendReady, polling, job?.id]);

  useEffect(() => {
    if (!backendReady || !polling || !job?.id) return;
    let disposed = false;
    const poll = async () => {
      try {
        const res = await api.get(`/jobs/${job.id}`);
        if (disposed) return;
        const payload = (res.data ?? null) as JobStatusPayload | null;
        if (!payload || payload.type !== "txt_update") {
          setJob(null);
          setPolling(false);
          return;
        }
        setJob(payload);
        setPolling(!TERMINAL_JOB_STATUS.has(payload.status ?? ""));
      } catch (error) {
        const status =
          typeof error === "object" && error && "response" in error
            ? (error as { response?: { status?: number } }).response?.status
            : undefined;
        if (status === 404) {
          setJob(null);
          setPolling(false);
        }
      }
    };

    void poll();
    const timer = window.setInterval(() => {
      void poll();
    }, 2000);
    return () => {
      disposed = true;
      window.clearInterval(timer);
    };
  }, [backendReady, polling, job?.id]);

  if (location.pathname === "/" || !job || !job.id || TERMINAL_JOB_STATUS.has(job.status ?? "")) {
    return null;
  }

  return (
    <div
      style={{
        position: "fixed",
        top: 72,
        right: 20,
        zIndex: 1400,
        width: "min(360px, calc(100vw - 32px))",
        pointerEvents: "none"
      }}
      aria-live="polite"
    >
      <div className="txt-update-meta" title={job.message ?? undefined} style={{ pointerEvents: "auto" }}>
        <div className={`txt-update-status ${tone}`}>
          <span className="txt-update-dot" />
          <span>{statusLabel ?? "TXT更新: 実行中"}</span>
          {progressValue != null && <span className="txt-update-percent">{progressValue}%</span>}
        </div>
        <div className="txt-update-detail">{stageLabel}</div>
        {shortDetail && <div className="txt-update-last">{shortDetail}</div>}
        <div
          className={`txt-update-progress ${tone} ${progressValue == null ? "is-indeterminate" : ""}`}
        >
          <div className="txt-update-progress-bar" style={{ width: `${progressValue ?? 42}%` }} />
        </div>
      </div>
    </div>
  );
}
