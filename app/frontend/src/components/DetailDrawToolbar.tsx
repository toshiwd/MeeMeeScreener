import {
  IconBox,
  IconChartArrows,
  IconMinus,
  IconPointer,
  IconRepeat,
  IconTrash
} from "@tabler/icons-react";
import IconButton from "./IconButton";
import type { DrawTool } from "./DetailChart";

type DetailDrawToolbarProps = {
  activeTool: DrawTool | null;
  activeDrawColor: string;
  activeLineOpacity: number;
  activeLineWidth: number;
  continuousDraw: boolean;
  onSelectTool: (tool: DrawTool | null) => void;
  onResetAll: () => void;
  onToggleContinuous: () => void;
  onCycleColor: () => void;
  onLineOpacityChange: (value: number) => void;
  onLineWidthChange: (value: number) => void;
};

export default function DetailDrawToolbar({
  activeTool,
  activeDrawColor,
  activeLineOpacity,
  activeLineWidth,
  continuousDraw,
  onSelectTool,
  onResetAll,
  onToggleContinuous,
  onCycleColor,
  onLineOpacityChange,
  onLineWidthChange
}: DetailDrawToolbarProps) {
  return (
    <div className="detail-draw-toolbar" data-testid="detail-draw-toolbar">
      <div className="detail-draw-tool-group" aria-label="描画ツール">
        <IconButton
          icon={<IconPointer size={18} />}
          tooltip="選択 / 描画編集"
          ariaLabel="選択 / 描画編集"
          className="draw-tool-button"
          selected={activeTool === null}
          onClick={() => onSelectTool(null)}
        />
        <IconButton
          icon={<IconChartArrows size={18} />}
          tooltip="ローソク足カウント"
          ariaLabel="ローソク足カウント"
          className="draw-tool-button"
          selected={activeTool === "timeZone"}
          onClick={() => onSelectTool("timeZone")}
        />
        <IconButton
          icon={<IconBox size={18} />}
          tooltip="BOX"
          ariaLabel="BOX"
          className="draw-tool-button"
          selected={activeTool === "drawBox"}
          onClick={() => onSelectTool("drawBox")}
        />
        <IconButton
          icon={<span style={{ fontSize: 18, lineHeight: 1 }}>▭</span>}
          tooltip="価格帯"
          ariaLabel="価格帯"
          className="draw-tool-button"
          selected={activeTool === "priceBand"}
          onClick={() => onSelectTool("priceBand")}
        />
        <IconButton
          icon={<IconMinus size={18} />}
          tooltip="水平ライン"
          ariaLabel="水平ライン"
          className="draw-tool-button"
          selected={activeTool === "horizontalLine"}
          onClick={() => onSelectTool("horizontalLine")}
        />
      </div>
      <div className="detail-draw-tool-group detail-draw-options" aria-label="描画オプション">
        <IconButton
          icon={<IconRepeat size={18} />}
          tooltip={continuousDraw ? "連続描画 ON" : "連続描画 OFF"}
          ariaLabel={continuousDraw ? "連続描画 ON" : "連続描画 OFF"}
          selected={continuousDraw}
          onClick={onToggleContinuous}
        />
        <IconButton
          icon={<IconTrash size={18} />}
          tooltip="描画を全削除"
          ariaLabel="描画を全削除"
          className="detail-draw-delete-all"
          onClick={onResetAll}
        />
      </div>
      {activeTool !== null && (
        <div className="detail-draw-tool-group detail-draw-adjustments" aria-label="描画スタイル">
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
            title="透明度"
            aria-label="透明度"
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
            aria-label="太さ"
            style={{ width: 60 }}
            onChange={(event) => onLineWidthChange(Number(event.target.value))}
          />
        </div>
      )}
    </div>
  );
}
