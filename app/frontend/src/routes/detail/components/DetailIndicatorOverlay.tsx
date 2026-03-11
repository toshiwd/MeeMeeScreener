import type { MaSetting } from "../../../store";

type Timeframe = "daily" | "weekly" | "monthly";
type EditMode = "main" | "compare";

type MaSettingsByFrame = {
  daily: MaSetting[];
  weekly: MaSetting[];
  monthly: MaSetting[];
};

type Props = {
  isOpen: boolean;
  compareCode: string | null;
  maEditMode: EditMode;
  activeMaSettings: MaSettingsByFrame;
  onSetMaEditMode: (mode: EditMode) => void;
  onUpdateSetting: (timeframe: Timeframe, index: number, patch: Partial<MaSetting>) => void;
  onResetSettings: (timeframe: Timeframe) => void;
  onClose: () => void;
};

export default function DetailIndicatorOverlay({
  isOpen,
  compareCode,
  maEditMode,
  activeMaSettings,
  onSetMaEditMode,
  onUpdateSetting,
  onResetSettings,
  onClose,
}: Props) {
  if (!isOpen) return null;

  return (
    <div className="indicator-overlay" onClick={onClose}>
      <div className="indicator-panel" onClick={(event) => event.stopPropagation()}>
        <div className="indicator-header">
          <div className="indicator-title">Indicators</div>
          {compareCode && (
            <div className="ma-toggle">
              <button
                type="button"
                className={`indicator-button${maEditMode === "main" ? " active" : ""}`}
                onClick={() => onSetMaEditMode("main")}
              >
                {"\u682a\u5f0f"}
              </button>
              <button
                type="button"
                className={`indicator-button${maEditMode === "compare" ? " active" : ""}`}
                onClick={() => onSetMaEditMode("compare")}
              >
                {"\u6bd4\u8f03"}
              </button>
            </div>
          )}
          <button className="indicator-close" onClick={onClose}>
            Close
          </button>
        </div>
        {(["daily", "weekly", "monthly"] as Timeframe[]).map((frame) => (
          <div className="indicator-section" key={frame}>
            <div className="indicator-subtitle">Moving Averages ({frame})</div>
            <div className="indicator-rows">
              {activeMaSettings[frame].map((setting, index) => (
                <div className="indicator-row" key={setting.key}>
                  <input
                    type="checkbox"
                    checked={setting.visible}
                    onChange={() => onUpdateSetting(frame, index, { visible: !setting.visible })}
                  />
                  <div className="indicator-label">{setting.label}</div>
                  <input
                    className="indicator-input"
                    type="number"
                    min={1}
                    value={setting.period}
                    onChange={(event) =>
                      onUpdateSetting(frame, index, { period: Number(event.target.value) || 1 })
                    }
                  />
                  <input
                    className="indicator-input indicator-width"
                    type="number"
                    min={1}
                    max={6}
                    value={setting.lineWidth}
                    onChange={(event) =>
                      onUpdateSetting(frame, index, { lineWidth: Number(event.target.value) })
                    }
                  />
                  <input
                    className="indicator-color-input"
                    type="color"
                    value={setting.color}
                    onChange={(event) => onUpdateSetting(frame, index, { color: event.target.value })}
                  />
                </div>
              ))}
            </div>
            <button className="indicator-reset" onClick={() => onResetSettings(frame)}>
              Reset {frame}
            </button>
          </div>
        ))}
      </div>
    </div>
  );
}
