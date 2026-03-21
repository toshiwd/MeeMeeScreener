const PREFIX = "tradex";

const makeKey = (parts: string[]) => `${PREFIX}:${parts.filter(Boolean).join(":")}`;

export const tradexStorageKeys = {
  compareCandidateId: makeKey(["compare", "candidateId"]),
  adoptCandidateId: makeKey(["adopt", "candidateId"]),
  detailCandidateId: makeKey(["detail", "candidateId"]),
  homeFocus: makeKey(["home", "focus"]),
  familyId: makeKey(["family", "familyId"]),
  runId: makeKey(["run", "runId"]),
  detailCode: makeKey(["detail", "code"])
} as const;

const readJson = <T,>(storage: Storage, key: string, fallback: T): T => {
  try {
    const raw = storage.getItem(key);
    if (!raw) return fallback;
    return JSON.parse(raw) as T;
  } catch {
    return fallback;
  }
};

const writeJson = (storage: Storage, key: string, value: unknown) => {
  try {
    storage.setItem(key, JSON.stringify(value));
  } catch {
    // 保存不能は UI を止めない。次回起動時に再構成する。
  }
};

export const readTradexSession = <T,>(key: string, fallback: T): T => {
  if (typeof window === "undefined") return fallback;
  return readJson(window.sessionStorage, key, fallback);
};

export const writeTradexSession = (key: string, value: unknown) => {
  if (typeof window === "undefined") return;
  writeJson(window.sessionStorage, key, value);
};

export const readTradexLocal = <T,>(key: string, fallback: T): T => {
  if (typeof window === "undefined") return fallback;
  return readJson(window.localStorage, key, fallback);
};

export const writeTradexLocal = (key: string, value: unknown) => {
  if (typeof window === "undefined") return;
  writeJson(window.localStorage, key, value);
};
