const INTERNAL_ERROR_TOKEN = /(^|[\s:([{"'])(([a-z]+_){1,}[a-z0-9]+|[A-Z]{2,}[A-Z0-9_]*)(?=$|[\s:)\]},."'])/;
const PROGRAMMING_ERROR_HINT =
  /\b(error|exception|traceback|timeout|failed|failure|network|abort|undefined|null|promise|fetch|http|api|json|database|sqlite|duckdb)\b/i;

export const formatUserFacingOperationIssue = (value?: string | null): string | null => {
  const raw = String(value ?? "").trim();
  if (!raw) return null;
  const normalized = raw.toLowerCase();

  if (
    normalized.includes("refresh_timeout") ||
    normalized.includes("timed out") ||
    normalized.includes("timeout") ||
    normalized.includes("etimedout") ||
    normalized.includes("econnaborted")
  ) {
    return "更新確認に時間がかかっています";
  }

  if (
    normalized.includes("refresh_job_missing") ||
    normalized.includes("job not found") ||
    normalized.includes("missing job")
  ) {
    return "更新状況を確認できませんでした";
  }

  if (
    normalized.includes("database is locked") ||
    normalized.includes("db_unavailable") ||
    normalized.includes("db_busy") ||
    normalized.includes("duckdb")
  ) {
    return "データベースが混み合っています";
  }

  if (normalized.includes("code_txt_missing")) {
    return "code.txt が見つかりません";
  }

  if (normalized.includes("vbs_not_found")) {
    return "日次更新スクリプトが見つかりません";
  }

  if (
    normalized.includes("refresh_failed") ||
    normalized.includes("request failed") ||
    normalized.includes("failed to fetch") ||
    normalized.includes("network")
  ) {
    return "更新を確認できませんでした";
  }

  if (INTERNAL_ERROR_TOKEN.test(raw) || PROGRAMMING_ERROR_HINT.test(raw)) {
    return "更新を確認できませんでした";
  }

  return raw;
};
