import { IconBox, IconChartBar, IconEyeOff, IconLine, IconMessageCircle, IconPointer } from "@tabler/icons-react";
import IconButton from "../../components/IconButton";
import type { AnnotationFilter, AnnotationTool } from "./annotations";

type Props = {
  enabled: boolean;
  activeTool: AnnotationTool;
  filter: AnnotationFilter;
  onToggleEnabled: () => void;
  onSelectTool: (tool: AnnotationTool) => void;
  onFilterChange: (filter: AnnotationFilter) => void;
};

export default function AnnotationToolbar({
  enabled,
  activeTool,
  filter,
  onToggleEnabled,
  onSelectTool,
  onFilterChange,
}: Props) {
  return (
    <div className="annotation-toolbar" data-testid="annotation-toolbar">
      <IconButton
        icon={<IconPointer size={18} />}
        label={enabled ? "注釈編集 ON" : "注釈編集"}
        variant="iconLabel"
        selected={enabled}
        tooltip="注釈編集モード"
        ariaLabel="注釈編集モード"
        onClick={onToggleEnabled}
      />
      {enabled && (
        <>
          <div className="annotation-tool-group">
            <IconButton
              icon={<IconPointer size={18} />}
              tooltip="対象を選択"
              ariaLabel="注釈 対象選択"
              selected={activeTool === "select"}
              onClick={() => onSelectTool("select")}
            />
            <IconButton
              icon={<IconChartBar size={18} />}
              tooltip="ローソク足"
              ariaLabel="注釈 ローソク足"
              selected={activeTool === "bar"}
              onClick={() => onSelectTool("bar")}
            />
            <IconButton
              icon={<IconBox size={18} />}
              tooltip="ボックス/範囲"
              ariaLabel="注釈 ボックス範囲"
              selected={activeTool === "region"}
              onClick={() => onSelectTool("region")}
            />
            <IconButton
              icon={<IconLine size={18} />}
              tooltip="水平ライン"
              ariaLabel="注釈 水平ライン"
              selected={activeTool === "line"}
              onClick={() => onSelectTool("line")}
            />
            <IconButton
              icon={<IconMessageCircle size={18} />}
              tooltip="引き出しコメント"
              ariaLabel="注釈 引き出しコメント"
              selected={activeTool === "callout"}
              onClick={() => onSelectTool("callout")}
            />
          </div>
          <select
            className="annotation-filter"
            aria-label="注釈表示フィルター"
            value={filter}
            onChange={(event) => onFilterChange(event.target.value as AnnotationFilter)}
          >
            <option value="all">すべて</option>
            <option value="bar">ローソク足</option>
            <option value="region">ボックス</option>
            <option value="line">ライン</option>
            <option value="callout">引き出し</option>
            <option value="context">環境認識</option>
            <option value="hidden">非表示</option>
          </select>
          {filter === "hidden" && <IconEyeOff size={16} aria-hidden="true" />}
        </>
      )}
    </div>
  );
}
