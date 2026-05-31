import type { ChartAnnotation } from "./annotations";

export type ChartNoteTimeframe = "daily" | "weekly" | "monthly" | "environment" | "mixed";

export type ChartNoteParagraphCommentType =
  | "monthly_context"
  | "weekly_context"
  | "daily_bar_reading"
  | "region_reading"
  | "line_reading"
  | "indicator_reading"
  | "callout_reading"
  | "position_action"
  | "scenario_update"
  | "risk_note"
  | "review";

export type ChartNoteLinkedObject = {
  object_type: string;
  annotation_id?: string;
  anchor_type?: string;
  anchor_target?: string;
  resolution: string;
  provisional?: boolean;
};

export type ChartNoteParagraph = {
  paragraph_id: string;
  order: number;
  text: string;
  comment_type: ChartNoteParagraphCommentType;
  linked_objects: ChartNoteLinkedObject[];
  reason_tags: string[];
  action_label?: string | null;
  no_lookahead: boolean;
};

type Props = {
  title: string;
  timeframe: ChartNoteTimeframe;
  paragraphs: ChartNoteParagraph[];
  selectedAnnotation: ChartAnnotation | null;
  saving?: boolean;
  onTitleChange: (value: string) => void;
  onTimeframeChange: (value: ChartNoteTimeframe) => void;
  onAddParagraph: () => void;
  onParagraphChange: (paragraph: ChartNoteParagraph) => void;
  onLinkSelectedAnnotation: (paragraphId: string) => void;
  onLinkMa20: (paragraphId: string) => void;
  onSave: () => void;
};

const commentTypes: ChartNoteParagraphCommentType[] = [
  "monthly_context",
  "weekly_context",
  "daily_bar_reading",
  "region_reading",
  "line_reading",
  "indicator_reading",
  "callout_reading",
  "position_action",
  "scenario_update",
  "risk_note",
  "review",
];

const timeframeLabels: Record<ChartNoteTimeframe, string> = {
  mixed: "複合",
  daily: "日足",
  weekly: "週足",
  monthly: "月足",
  environment: "環境認識",
};

const commentTypeLabels: Record<ChartNoteParagraphCommentType, string> = {
  monthly_context: "月足環境",
  weekly_context: "週足環境",
  daily_bar_reading: "日足ローソク足",
  region_reading: "ボックス/範囲",
  line_reading: "ライン",
  indicator_reading: "移動平均線",
  callout_reading: "引き出しコメント",
  position_action: "建玉/売買判断",
  scenario_update: "シナリオ更新",
  risk_note: "リスクメモ",
  review: "振り返り",
};

const linkLabel = (link: ChartNoteLinkedObject) => {
  if (link.annotation_id) return `${objectTypeLabel(link.object_type)}:${link.annotation_id.slice(0, 8)}`;
  if (link.anchor_type === "indicator" || link.object_type === "indicator") return `移動平均:${link.anchor_target ?? ""}`;
  return `${link.object_type}:${link.resolution}`;
};

const objectTypeLabel = (type: string) => {
  if (type === "bar") return "ローソク足";
  if (type === "region") return "ボックス";
  if (type === "line") return "ライン";
  if (type === "callout") return "引き出し";
  if (type === "indicator") return "移動平均";
  return type;
};

export default function ChartNotePanel({
  title,
  timeframe,
  paragraphs,
  selectedAnnotation,
  saving = false,
  onTitleChange,
  onTimeframeChange,
  onAddParagraph,
  onParagraphChange,
  onLinkSelectedAnnotation,
  onLinkMa20,
  onSave,
}: Props) {
  return (
    <div className="chart-note-panel" data-testid="chart-note-panel">
      <section className="annotation-panel chart-note-form">
        <div className="annotation-panel-title">チャートノート</div>
        <label>
          タイトル
          <input data-testid="chart-note-title" value={title} onChange={(event) => onTitleChange(event.target.value)} />
        </label>
        <label>
          時間軸
          <select
            data-testid="chart-note-timeframe"
            value={timeframe}
            onChange={(event) => onTimeframeChange(event.target.value as ChartNoteTimeframe)}
          >
            {Object.entries(timeframeLabels).map(([value, label]) => (
              <option value={value} key={value}>
                {label}
              </option>
            ))}
          </select>
        </label>
        <div className="chart-reading-actions">
          <button type="button" className="annotation-save" data-testid="chart-note-add-paragraph" onClick={onAddParagraph}>
            段落を追加
          </button>
          <button type="button" className="annotation-save" data-testid="chart-note-save" disabled={saving} onClick={onSave}>
            ノートを保存
          </button>
        </div>
      </section>

      {paragraphs.map((paragraph, index) => (
        <section className="annotation-panel chart-note-paragraph" data-testid="chart-note-paragraph" key={paragraph.paragraph_id}>
          <div className="annotation-panel-header">
            <div>
              <div className="annotation-panel-title">段落 {index + 1}</div>
              <div className="annotation-panel-meta">{paragraph.paragraph_id}</div>
            </div>
          </div>
          <label>
            本文
            <textarea
              data-testid="chart-note-paragraph-text"
              value={paragraph.text}
              onChange={(event) => onParagraphChange({ ...paragraph, text: event.target.value })}
            />
          </label>
          <label>
            コメント種別
            <select
              data-testid="chart-note-paragraph-comment-type"
              value={paragraph.comment_type}
              onChange={(event) =>
                onParagraphChange({
                  ...paragraph,
                  comment_type: event.target.value as ChartNoteParagraphCommentType,
                })
              }
            >
              {commentTypes.map((type) => (
                <option value={type} key={type}>
                  {commentTypeLabels[type]}
                </option>
              ))}
            </select>
          </label>
          <label>
            理由タグ
            <input
              data-testid="chart-note-paragraph-tags"
              value={paragraph.reason_tags.join(", ")}
              onChange={(event) =>
                onParagraphChange({
                  ...paragraph,
                  reason_tags: event.target.value
                    .split(",")
                    .map((item) => item.trim())
                    .filter(Boolean),
                })
              }
            />
          </label>
          <div className="chart-reading-actions">
            <button
              type="button"
              className="annotation-save"
              data-testid="chart-note-link-selected"
              disabled={!selectedAnnotation}
              onClick={() => onLinkSelectedAnnotation(paragraph.paragraph_id)}
            >
              選択中の対象をリンク
            </button>
            <button
              type="button"
              className="annotation-save"
              data-testid="chart-note-link-ma20"
              onClick={() => onLinkMa20(paragraph.paragraph_id)}
            >
              20MAをリンク
            </button>
          </div>
          <div className="annotation-panel-meta" data-testid="chart-note-linked-objects">
            {paragraph.linked_objects.length
              ? paragraph.linked_objects.map((link) => (
                  <span className="chart-note-link-chip" key={`${linkLabel(link)}-${link.resolution}`}>
                    {linkLabel(link)}
                  </span>
                ))
              : "リンク対象なし"}
          </div>
        </section>
      ))}
    </div>
  );
}
