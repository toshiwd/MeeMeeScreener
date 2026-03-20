import {
  IconChartArrows,
  IconBox,
  IconMinus,
  IconTrash
} from "@tabler/icons-react";
import IconButton from "./IconButton";
import type { DrawTool } from "./DetailChart";

type DetailDrawToolbarProps = {
  activeTool: DrawTool | null;
  activeDrawColor: string;
  activeLineOpacity: number;
  activeLineWidth: number;
  onSelectTool: (tool: DrawTool | null) => void;
  onResetAll: () => void;
  onCycleColor: () => void;
  onLineOpacityChange: (value: number) => void;
  onLineWidthChange: (value: number) => void;
};

export default function DetailDrawToolbar({
  activeTool,
  activeDrawColor,
  activeLineOpacity,
  activeLineWidth,
  onSelectTool,
  onResetAll,
  onCycleColor,
  onLineOpacityChange,
  onLineWidthChange
}: DetailDrawToolbarProps) {
  return (
    <div className="detail-draw-toolbar">
      <div className="detail-analysis-actions detail-draw-tools">
        <IconButton
          icon={<IconChartArrows size={18} />}
          tooltip="時間ゾーン描画"
          ariaLabel="時間ゾーン描画"
          className="draw-tool-button"
          selected={activeTool === "timeZone"}
          onClick={() => onSelectTool("timeZone")}
        />
        <IconButton
          icon={<IconBox size={18} />}
          tooltip="四角描画"
          ariaLabel="四角描画"
          className="draw-tool-button"
          selected={activeTool === "drawBox"}
          onClick={() => onSelectTool("drawBox")}
        />
        <IconButton
          icon={<span style={{ fontSize: 18, lineHeight: 1 }}>▭</span>}
          tooltip="価格帯描画"
          ariaLabel="価格帯描画"
          className="draw-tool-button"
          selected={activeTool === "priceBand"}
          onClick={() => onSelectTool("priceBand")}
        />
        <IconButton
          icon={<IconMinus size={18} />}
          tooltip="水平線描画"
          ariaLabel="水平線描画"
          className="draw-tool-button"
          selected={activeTool === "horizontalLine"}
          onClick={() => onSelectTool("horizontalLine")}
        />
        <IconButton
          icon={<IconTrash size={18} />}
          tooltip="描画をリセット"
          ariaLabel="描画をリセット"
          onClick={onResetAll}
        />
      </div>
      {activeTool !== null && (
        <div className="detail-analysis-actions detail-draw-adjustments">
          <IconButton
            icon={
              <span
                style={{
                  width: 14,
                  height: 14,
                  borderRadius: 999,
                  background: activeDrawColor,
                  display: "inline-block",
                  border: "1px solid rgba(0,0,0,0.2)"
                }}
              />
            }
            tooltip="描画色を変更"
            ariaLabel="描画色を変更"
            onClick={onCycleColor}
          />
          <input
            type="range"
            min={0.1}
            max={1}
            step={0.05}
            value={activeLineOpacity}
            title="不透明度"
            style={{ width: 60 }}
            onChange={(event) => onLineOpacityChange(Number(event.target.value))}
          />
          <input
            type="range"
            min={1}
            max={6}
            step={0.5}
            value={activeLineWidth}
            title="太さ"
            style={{ width: 60 }}
            onChange={(event) => onLineWidthChange(Number(event.target.value))}
          />
        </div>
      )}
    </div>
  );
}
