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
  if (status === "queued") return "日次更新: 待機中";
  if (status === "running") return "日次更新: 実行中";
  if (status === "cancel_requested") return "日次更新: 停止中";
  if (status === "success") return "日次更新: 完了";
  if (status === "failed") return "日次更新: 失敗";
  if (status === "canceled") return "日次更新: 停止済み";
  return "日次更新: 状態確認中";
};

export const formatTxtUpdateStageLabel = (
  message?: string | null,
  fallback?: string | null
): string => {
  const text = message?.toLowerCase() ?? "";
  if (!text) return fallback ?? "待機中";
  if (text.includes("tracking_refresh") || text.includes("tracking refresh")) return "履歴を更新中";
  if (text.includes("pan import") || text.includes("launching pan")) return "外部データを取り込み中";
  if (text.includes("pan rolling export") || text.includes("vbs export") || text.includes("export completed")) {
    return "日次データを作成中";
  }
  if (text.includes("ingesting")) return "日次データを取り込み中";
  if (text.includes("phase")) return "銘柄状態を更新中";
  if (text.includes("ml")) return "補助データを更新中";
  if (text.includes("score")) return "ランキングを更新中";
  if (text.includes("cache")) return "表示用データを更新中";
  if (text.includes("walkforward")) return "検証データを確認中";
  if (text.includes("final")) return "仕上げ中";
  if (text.includes("queue") || text.includes("waiting")) return "待機中";
  return fallback ?? "更新中";
};

export const formatTxtUpdatePublicDetail = (
  message?: string | null,
  fallback?: string | null
): string | null => {
  if (!message?.trim()) return null;
  return formatTxtUpdateStageLabel(message, fallback ?? "更新中");
};

export const formatTxtUpdateFailureMessage = (error?: string | null, message?: string | null): string => {
  const text = `${error ?? ""} ${message ?? ""}`.toLowerCase();
  if (text.includes("db_unavailable") || text.includes("database")) {
    return "日次更新が失敗しました。データベースが一時的に使用中です。少し待ってから再試行してください。";
  }
  if (text.includes("code_txt_missing")) {
    return "日次更新が失敗しました。必要な銘柄リストが見つかりません。設定を確認してください。";
  }
  if (text.includes("vbs_not_found")) {
    return "日次更新が失敗しました。必要な更新処理が見つかりません。設定を確認してください。";
  }
  if (text.includes("cancel")) {
    return "日次更新は停止されました。";
  }
  return "日次更新が失敗しました。設定とデータ保存先を確認してください。";
};
