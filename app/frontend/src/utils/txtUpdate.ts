export type TxtUpdateStartPayload = {
  ok?: boolean;
  status?: string;
  error?: string;
  message?: string;
  job_id?: string;
  jobId?: string;
};

export const extractTxtUpdateJobId = (payload?: TxtUpdateStartPayload | null): string | null => {
  if (!payload || typeof payload !== "object") return null;
  if (typeof payload.job_id === "string" && payload.job_id.trim()) return payload.job_id;
  if (typeof payload.jobId === "string" && payload.jobId.trim()) return payload.jobId;
  return null;
};

export const isTxtUpdateConflictError = (error?: string): boolean => {
  return error === "update_in_progress" || error === "Job already running";
};

export const formatTxtUpdateStatusLabel = (status?: string | null): string | null => {
  if (!status) return null;
  if (status === "queued") return "TXT更新: 待機中";
  if (status === "running") return "TXT更新: 実行中";
  if (status === "cancel_requested") return "TXT更新: 停止要求中";
  if (status === "success") return "TXT更新: 完了";
  if (status === "failed") return "TXT更新: 失敗";
  if (status === "canceled") return "TXT更新: キャンセル";
  return `TXT更新: ${status}`;
};
