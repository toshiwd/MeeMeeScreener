export type DensityPreset = 1 | 2 | 3 | 4;

export type DensityOption = {
  value: DensityPreset;
  label: string;
  bars: number;
};

export const DENSITY_PRESET_OPTIONS = [
  { value: 1, label: "1x1", bars: 180 },
  { value: 2, label: "2x2", bars: 90 },
  { value: 3, label: "3x3", bars: 60 },
  { value: 4, label: "4x4", bars: 45 }
] as const satisfies readonly DensityOption[];

export const DEFAULT_DENSITY_PRESET: DensityPreset = 3;

export const normalizeDensityPreset = (value: unknown): DensityPreset => {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return DEFAULT_DENSITY_PRESET;
  const preset = Math.trunc(numeric);
  if (preset <= 1) return 1;
  if (preset >= 4) return 4;
  return preset as DensityPreset;
};

export const densityPresetToBars = (preset: DensityPreset) =>
  DENSITY_PRESET_OPTIONS.find((item) => item.value === preset)?.bars ?? 60;

