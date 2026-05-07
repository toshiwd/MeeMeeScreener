const truthyValues = new Set(["1", "true", "yes", "on"]);
const falsyValues = new Set(["0", "false", "no", "off"]);

const parseDetailV2Query = (search: string): boolean | null => {
  const normalized = search.startsWith("?") ? search : `?${search}`;
  try {
    const params = new URLSearchParams(normalized);
    const value = params.get("detailV2") ?? params.get("detail_v2");
    if (value == null) return null;
    const normalizedValue = value.trim().toLowerCase();
    if (truthyValues.has(normalizedValue)) return true;
    if (falsyValues.has(normalizedValue)) return false;
    return null;
  } catch {
    return null;
  }
};

export const shouldUseDetailV2Navigation = (
  search: string,
  flag = import.meta.env.VITE_ENABLE_DETAIL_V2_NAV
) => {
  const queryDecision = parseDetailV2Query(search);
  if (queryDecision != null) return queryDecision;
  const normalizedFlag = String(flag ?? "").trim().toLowerCase();
  if (falsyValues.has(normalizedFlag)) return false;
  if (truthyValues.has(normalizedFlag)) return true;
  return true;
};
