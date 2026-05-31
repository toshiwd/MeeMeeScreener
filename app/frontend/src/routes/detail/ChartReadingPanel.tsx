import type { SelectedDrawingInfo } from "../../components/DetailChart";
import AnnotationPanel from "./AnnotationPanel";
import type { ChartAnnotation } from "./annotations";

export type ReadingTimeframe = "daily" | "weekly" | "monthly" | "environment";
export type ReadingTargetType =
  | "bar"
  | "region"
  | "line"
  | "indicator"
  | "chart_context"
  | "scenario"
  | "position_action";
export type ReadingCommentType =
  | "bar"
  | "region"
  | "line"
  | "chart_context"
  | "scenario"
  | "position_action"
  | "review";

type SelectedBarSummary = {
  date: string;
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume?: number | null;
};

type Props = {
  loading?: boolean;
  timeframe: ReadingTimeframe;
  targetType: ReadingTargetType;
  commentType: ReadingCommentType;
  noteText: string;
  tagsText: string;
  saving?: boolean;
  selectedDrawing: SelectedDrawingInfo | null;
  selectedAnnotation: ChartAnnotation | null;
  selectedBar: SelectedBarSummary | null;
  noteDate: string | null;
  onTimeframeChange: (value: ReadingTimeframe) => void;
  onTargetTypeChange: (value: ReadingTargetType) => void;
  onCommentTypeChange: (value: ReadingCommentType) => void;
  onNoteTextChange: (value: string) => void;
  onTagsTextChange: (value: string) => void;
  onClearNote: () => void;
  onAnnotateDrawing: () => void;
  onAnnotationChange: (annotation: ChartAnnotation) => void;
  onAnnotationDelete: (annotation: ChartAnnotation) => void;
};

const drawingLabel = (drawing: SelectedDrawingInfo | null) => {
  if (!drawing) return "描画対象は選択されていません";
  if (drawing.kind === "drawBox") return "選択中: ボックス/範囲";
  if (drawing.kind === "horizontalLine") return "選択中: 水平ライン";
  return `選択中: ${drawing.kind}`;
};

const timeframeLabels: Record<ReadingTimeframe, string> = {
  daily: "日足",
  weekly: "週足",
  monthly: "月足",
  environment: "環境認識",
};

const targetLabels: Record<ReadingTargetType, string> = {
  bar: "ローソク足",
  region: "ボックス/範囲",
  line: "ライン",
  indicator: "移動平均線",
  chart_context: "チャート全体",
  scenario: "シナリオ",
  position_action: "建玉・売買判断",
};

const commentTypeLabels: Record<ReadingCommentType, string> = {
  bar: "ローソク足",
  region: "ボックス/範囲",
  line: "ライン",
  chart_context: "環境認識",
  scenario: "シナリオ",
  position_action: "建玉・売買判断",
  review: "振り返り",
};

const formatNumber = (value: number | null | undefined) =>
  Number.isFinite(Number(value)) ? Math.round(Number(value)).toLocaleString("ja-JP") : "--";

export default function ChartReadingPanel({
  loading = false,
  timeframe,
  targetType,
  commentType,
  noteText,
  tagsText,
  saving = false,
  selectedDrawing,
  selectedAnnotation,
  selectedBar,
  noteDate,
  onTimeframeChange,
  onTargetTypeChange,
  onCommentTypeChange,
  onNoteTextChange,
  onTagsTextChange,
  onClearNote,
  onAnnotateDrawing,
  onAnnotationChange,
  onAnnotationDelete,
}: Props) {
  const canAnnotateDrawing = selectedDrawing?.kind === "drawBox" || selectedDrawing?.kind === "horizontalLine";

  return (
    <div className="chart-reading-panel" data-testid="chart-reading-panel">
      {loading && <div className="annotation-panel-meta">注釈を読み込み中...</div>}
      <section className="annotation-panel chart-reading-form">
        <div className="annotation-panel-title">チャート読解メモ</div>
        {noteDate && (
          <div className="annotation-panel-meta" data-testid="chart-reading-note-date">
            保存先日付: {noteDate}
          </div>
        )}
        <label>
          時間軸
          <select
            data-testid="chart-reading-timeframe"
            value={timeframe}
            onChange={(event) => onTimeframeChange(event.target.value as ReadingTimeframe)}
          >
            {Object.entries(timeframeLabels).map(([value, label]) => (
              <option value={value} key={value}>
                {label}
              </option>
            ))}
          </select>
        </label>
        <label>
          対象
          <select
            data-testid="chart-reading-target-type"
            value={targetType}
            onChange={(event) => onTargetTypeChange(event.target.value as ReadingTargetType)}
          >
            {Object.entries(targetLabels).map(([value, label]) => (
              <option value={value} key={value}>
                {label}
              </option>
            ))}
          </select>
        </label>
        {targetType === "bar" && (
          <div className="chart-reading-selected-target" data-testid="chart-reading-selected-bar">
            <div className="annotation-panel-subtitle">選択中のローソク足</div>
            {selectedBar ? (
              <div className="chart-reading-target-card">
                <strong>{selectedBar.date}</strong>
                <span>O {formatNumber(selectedBar.open)}</span>
                <span>H {formatNumber(selectedBar.high)}</span>
                <span>L {formatNumber(selectedBar.low)}</span>
                <span>C {formatNumber(selectedBar.close)}</span>
              </div>
            ) : (
              <div className="annotation-panel-meta">
                チャート上の日足ローソク足を選択すると、このメモに日付が紐づきます。
              </div>
            )}
          </div>
        )}
        <label>
          コメント種別
          <select
            data-testid="chart-reading-comment-type"
            value={commentType}
            onChange={(event) => onCommentTypeChange(event.target.value as ReadingCommentType)}
          >
            {Object.entries(commentTypeLabels).map(([value, label]) => (
              <option value={value} key={value}>
                {label}
              </option>
            ))}
          </select>
        </label>
        <label>
          メモ
          <textarea
            data-testid="chart-reading-note-text"
            value={noteText}
            onChange={(event) => onNoteTextChange(event.target.value)}
          />
        </label>
        <label>
          タグ
          <input
            data-testid="chart-reading-tags"
            value={tagsText}
            onChange={(event) => onTagsTextChange(event.target.value)}
          />
        </label>
        <div className="chart-reading-actions">
          <button
            type="button"
            className="annotation-save"
            data-testid="chart-reading-clear"
            disabled={saving}
            onClick={onClearNote}
          >
            クリア
          </button>
          <span className="annotation-panel-meta" data-testid="chart-reading-autosave-status">
            {saving ? "自動保存中..." : "自動保存"}
          </span>
        </div>
      </section>

      <section className="annotation-panel chart-reading-drawing">
        <div className="annotation-panel-title">描画への注釈</div>
        <div className="annotation-panel-meta" data-testid="chart-reading-selected-drawing">
          {drawingLabel(selectedDrawing)}
        </div>
        <button
          type="button"
          className="annotation-save"
          data-testid="chart-reading-annotate-drawing"
          disabled={!canAnnotateDrawing}
          onClick={onAnnotateDrawing}
        >
          注釈を付ける
        </button>
      </section>

      <AnnotationPanel annotation={selectedAnnotation} onChange={onAnnotationChange} onDelete={onAnnotationDelete} />
    </div>
  );
}
