type RangePreset = {
  label: string;
  months: number;
};

type DetailTimeframeSwitcherProps = {
  presets: RangePreset[];
  rangeMonths: number;
  onChange: (value: number) => void;
};

export default function DetailTimeframeSwitcher({
  presets,
  rangeMonths,
  onChange
}: DetailTimeframeSwitcherProps) {
  return (
    <div className="detail-summary-center">
      <div className="segmented detail-range">
        {presets.map((preset) => (
          <button
            key={preset.label}
            className={rangeMonths === preset.months ? "active" : ""}
            onClick={() => onChange(preset.months)}
          >
            {preset.label}
          </button>
        ))}
      </div>
    </div>
  );
}
