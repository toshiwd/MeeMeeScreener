const fallback = (value: string | null | undefined, defaultLabel = "--") => {
  const text = typeof value === "string" ? value.trim() : "";
  return text || defaultLabel;
};

export function tradexFreshnessLabel(value: string | null | undefined) {
  switch (value) {
    case "fresh": return "新鮮";
    case "warning": return "注意";
    case "hard": return "要確認";
    case "degraded": return "劣化";
    case "stale": return "古い";
    default: return fallback(value);
  }
}

export function tradexReplayLabel(value: string | null | undefined) {
  const normalized = (value || "").split("/")[0]?.trim() ?? "";
  switch (normalized) {
    case "running": return "実行中";
    case "success": return "完了";
    case "pending": return "待機中";
    case "error":
    case "failed": return "異常";
    case "idle": return "待機";
    default: return fallback(normalized || value);
  }
}

export function tradexDecisionDirectionLabel(value: string | null | undefined) {
  switch (value) {
    case "up": return "上昇";
    case "neutral": return "中立";
    case "down": return "下落";
    default: return fallback(value);
  }
}

export function tradexDecisionActionLabel(value: string | null | undefined) {
  switch (value) {
    case "adopt": return "採用";
    case "hold": return "保留";
    case "retest": return "再検証";
    default: return fallback(value);
  }
}

export function tradexCandidateStatusLabel(value: string | null | undefined) {
  switch (value) {
    case "approved": return "承認済み";
    case "promoted": return "反映済み";
    case "pending": return "待機中";
    case "running": return "実行中";
    case "complete": return "完了";
    case "error": return "異常";
    default: return fallback(value);
  }
}

export function tradexValidationStatusLabel(value: string | null | undefined) {
  switch (value) {
    case "ok":
    case "healthy": return "正常";
    case "ready": return "採用可";
    case "pending": return "待機中";
    case "error":
    case "failed": return "異常";
    default: return fallback(value);
  }
}
