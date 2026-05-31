import type { ChartAnnotation, ChartAnnotationType } from "./annotations";
import { parseTagsInput } from "./annotations";

type Props = {
  annotation: ChartAnnotation | null;
  onChange: (annotation: ChartAnnotation) => void;
  onDelete: (annotation: ChartAnnotation) => void;
};

const updatePayload = (
  annotation: ChartAnnotation,
  key: string,
  value: string | number | boolean | string[]
): ChartAnnotation => ({
  ...annotation,
  payload: {
    ...(annotation.payload ?? {}),
    [key]: value,
  },
});

const typeLabel = (type: ChartAnnotationType) =>
  type === "bar"
    ? "ローソク足注釈"
    : type === "region"
      ? "ボックス/範囲注釈"
      : type === "line"
        ? "ライン注釈"
        : type === "callout"
          ? "引き出しコメント"
          : "環境認識";

export default function AnnotationPanel({ annotation, onChange, onDelete }: Props) {
  if (!annotation) {
    return (
      <section className="annotation-panel" data-testid="annotation-panel">
        <div className="annotation-panel-title">注釈</div>
        <div className="annotation-panel-empty">チャート上の対象を選択するか、新しい注釈を作成してください。</div>
      </section>
    );
  }

  const payload = annotation.payload ?? {};
  const tagsText = (annotation.tags ?? []).join(", ");
  return (
    <section className="annotation-panel" data-testid="annotation-panel">
      <div className="annotation-panel-header">
        <div>
          <div className="annotation-panel-title">{typeLabel(annotation.object_type)}</div>
          <div className="annotation-panel-meta">
            {annotation.code} / {annotation.as_of_date} / {annotation.timeframe}
          </div>
        </div>
        <button type="button" className="annotation-delete" onClick={() => onDelete(annotation)}>
          削除
        </button>
      </div>

      <label>
        コメント
        <textarea
          data-testid="annotation-free-text"
          value={payload.free_text ?? ""}
          onChange={(event) => onChange(updatePayload(annotation, "free_text", event.target.value))}
        />
      </label>
      <label>
        タグ
        <input
          data-testid="annotation-tags"
          value={tagsText}
          onChange={(event) =>
            onChange({
              ...annotation,
              tags: parseTagsInput(event.target.value),
              payload: { ...payload, tags: parseTagsInput(event.target.value) },
            })
          }
        />
      </label>
      <label>
        シナリオ
        <input
          value={payload.scenario_label ?? ""}
          onChange={(event) => onChange(updatePayload(annotation, "scenario_label", event.target.value))}
        />
      </label>
      <label>
        行動ラベル
        <input
          data-testid="annotation-action-label"
          value={payload.action_label ?? ""}
          onChange={(event) => onChange(updatePayload(annotation, "action_label", event.target.value))}
        />
      </label>
      <label>
        重要度
        <input
          data-testid="annotation-importance"
          type="number"
          min={1}
          max={5}
          value={payload.importance ?? 3}
          onChange={(event) => onChange(updatePayload(annotation, "importance", Number(event.target.value)))}
        />
      </label>
      <label className="annotation-checkbox">
        <input
          type="checkbox"
          checked={payload.no_lookahead !== false && annotation.no_lookahead !== false}
          onChange={(event) =>
            onChange({
              ...annotation,
              no_lookahead: event.target.checked,
              payload: { ...payload, no_lookahead: event.target.checked },
            })
          }
        />
        未来情報を使わない
      </label>

      {annotation.object_type === "bar" && (
        <label>
          ローソク足の役割
          <input
            data-testid="annotation-bar-role"
            value={payload.bar_role ?? ""}
            onChange={(event) => onChange(updatePayload(annotation, "bar_role", event.target.value))}
          />
        </label>
      )}
      {annotation.object_type === "region" && (
        <>
          <label>
            範囲の種類
            <input
              data-testid="annotation-region-type"
              value={payload.region_type ?? ""}
              onChange={(event) => onChange(updatePayload(annotation, "region_type", event.target.value))}
            />
          </label>
          <label>
            有効条件
            <input
              data-testid="annotation-valid-while"
              value={payload.valid_while ?? ""}
              onChange={(event) => onChange(updatePayload(annotation, "valid_while", event.target.value))}
            />
          </label>
          <label>
            無効条件
            <input
              data-testid="annotation-invalid-if"
              value={payload.invalid_if ?? ""}
              onChange={(event) => onChange(updatePayload(annotation, "invalid_if", event.target.value))}
            />
          </label>
        </>
      )}
      {annotation.object_type === "line" && (
        <>
          <label>
            ラインの種類
            <input
              data-testid="annotation-line-type"
              value={payload.line_type ?? ""}
              onChange={(event) => onChange(updatePayload(annotation, "line_type", event.target.value))}
            />
          </label>
          <label>
            ブレイク条件
            <input
              data-testid="annotation-break-rule"
              value={payload.break_rule ?? ""}
              onChange={(event) => onChange(updatePayload(annotation, "break_rule", event.target.value))}
            />
          </label>
          <label>
            ブレイク時の行動
            <input
              data-testid="annotation-action-if-broken"
              value={payload.action_if_broken ?? ""}
              onChange={(event) => onChange(updatePayload(annotation, "action_if_broken", event.target.value))}
            />
          </label>
        </>
      )}
      {annotation.object_type === "callout" && (
        <>
          <label>
            指している対象
            <select
              data-testid="annotation-anchor-type"
              value={payload.anchor_type ?? "bar"}
              onChange={(event) => onChange(updatePayload(annotation, "anchor_type", event.target.value))}
            >
              <option value="bar">ローソク足</option>
              <option value="indicator">移動平均線</option>
              <option value="region">ボックス/範囲</option>
              <option value="line">ライン</option>
            </select>
          </label>
          <label>
            対象の詳細
            <input
              data-testid="annotation-anchor-target"
              value={payload.anchor_target ?? ""}
              onChange={(event) => onChange(updatePayload(annotation, "anchor_target", event.target.value))}
            />
          </label>
          <label>
            コメント種別
            <input
              data-testid="annotation-comment-type"
              value={payload.comment_type ?? ""}
              onChange={(event) => onChange(updatePayload(annotation, "comment_type", event.target.value))}
            />
          </label>
          <label className="annotation-checkbox">
            <input
              type="checkbox"
              checked={payload.leader_line !== false}
              onChange={(event) => onChange(updatePayload(annotation, "leader_line", event.target.checked))}
            />
            引き出し線を表示
          </label>
        </>
      )}
    </section>
  );
}
