export const formatEventBadgeDate = (value: string | null | undefined) => {
  if (!value) return null;
  const parts = value.split("T")[0]?.split("-") ?? [];
  if (parts.length < 3) return null;
  const month = Number(parts[1]);
  const day = Number(parts[2]);
  if (!Number.isFinite(month) || !Number.isFinite(day)) return null;
  return `${month}/${day}`;
};

export const formatEventDateYmd = (value: string | null | undefined) => {
  if (!value) return null;
  const parts = value.split("T")[0]?.split("-") ?? [];
  if (parts.length < 3) return null;
  const [year, month, day] = parts;
  if (!year || !month || !day) return null;
  return `${year}/${month.padStart(2, "0")}/${day.padStart(2, "0")}`;
};

export const parseEventDateMs = (value: string | null | undefined) => {
  if (!value) return null;
  const ms = Date.parse(value);
  return Number.isNaN(ms) ? null : ms;
};
