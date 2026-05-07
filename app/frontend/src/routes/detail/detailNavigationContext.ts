const DETAIL_LIST_BACK_FALLBACK = "/";

const DETAIL_LIST_BACK_ALLOWED_PATHS = new Set([
  "/",
  "/ranking",
  "/ranking/tracking",
  "/favorites",
  "/positions",
  "/candidates",
  "/market",
  "/tradex/legacy/tags",
  "/tradex/verify",
]);

const isSafePathSuffix = (value: string, basePath: string) => {
  if (value === basePath) return true;
  if (!value.startsWith(basePath)) return false;
  const nextChar = value.charAt(basePath.length);
  return nextChar === "?" || nextChar === "#";
};

export const normalizeDetailBackPath = (value: unknown): string => {
  if (typeof value !== "string") return DETAIL_LIST_BACK_FALLBACK;
  const trimmed = value.trim();
  if (!trimmed || !trimmed.startsWith("/") || trimmed.startsWith("//")) {
    return DETAIL_LIST_BACK_FALLBACK;
  }
  for (const allowedPath of DETAIL_LIST_BACK_ALLOWED_PATHS) {
    if (isSafePathSuffix(trimmed, allowedPath)) return trimmed;
  }
  return DETAIL_LIST_BACK_FALLBACK;
};

export const normalizeDetailListCodes = (value: unknown): string[] => {
  if (!Array.isArray(value)) return [];
  const seen = new Set<string>();
  const codes: string[] = [];
  for (const item of value) {
    if (typeof item !== "string") continue;
    const code = item.trim();
    if (!code || seen.has(code)) continue;
    seen.add(code);
    codes.push(code);
  }
  return codes;
};

export const saveDetailListContext = (backPath: string, listCodes: string[]) => {
  try {
    sessionStorage.setItem("detailListBack", normalizeDetailBackPath(backPath));
    sessionStorage.setItem("detailListCodes", JSON.stringify(normalizeDetailListCodes(listCodes)));
  } catch {
    // ignore storage failures
  }
};

export const readDetailListBackPath = (
  routeState: { from?: string } | null | undefined
): string => {
  let stored: string | null = null;
  if (typeof window !== "undefined") {
    try {
      stored = window.sessionStorage.getItem("detailListBack");
    } catch {
      stored = null;
    }
  }
  return normalizeDetailBackPath(routeState?.from ?? stored);
};

export const readDetailListCodes = (): string[] => {
  if (typeof window === "undefined") return [];
  try {
    const stored = window.sessionStorage.getItem("detailListCodes");
    if (!stored) return [];
    return normalizeDetailListCodes(JSON.parse(stored));
  } catch {
    return [];
  }
};
