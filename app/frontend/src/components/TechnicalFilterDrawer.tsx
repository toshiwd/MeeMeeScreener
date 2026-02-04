import type {
  ConditionOperator,
  Operand,
  TechnicalCondition,
  TechnicalFilterState,
  Timeframe
} from "../utils/technicalFilter";
import { buildDefaultCondition } from "../utils/technicalFilter";

type TechnicalFilterDrawerProps = {
  open: boolean;
  timeframe: Timeframe;
  anchorLabel: string | null;
  matchCount: number | null;
  value: TechnicalFilterState;
  onChange: (next: TechnicalFilterState) => void;
  onApply: () => void;
  onCancel: () => void;
  onReset: () => void;
  onTimeframeChange: (next: Timeframe) => void;
};

const OPERATOR_OPTIONS: { value: ConditionOperator; label: string }[] = [
  { value: "above", label: "以上" },
  { value: "below", label: "以下" },
  { value: "cross_up", label: "上抜け" },
  { value: "cross_down", label: "下抜け" }
];

const LEFT_OPERAND_OPTIONS = [
  { value: "price", label: "価格" },
  { value: "ma", label: "MA" }
];

const RIGHT_OPERAND_OPTIONS = [
  { value: "ma", label: "MA" },
  { value: "price", label: "価格" }
];

const updateCondition = (
  conditions: TechnicalCondition[],
  id: string,
  patch: Partial<TechnicalCondition>
) => conditions.map((item) => (item.id === id ? { ...item, ...patch } : item));

const normalizePeriod = (value: string, fallback = 20) => {
  const next = Number(value);
  if (!Number.isFinite(next) || next <= 0) return fallback;
  return Math.floor(next);
};

const resolveLeftKey = (operand: Operand) => {
  if (operand.type === "ma") return "ma";
  return "price";
};

const resolveRightKey = (operand: Operand) => {
  if (operand.type === "number") return "price";
  return "ma";
};

const normalizeNumber = (value: string, fallback = 0) => {
  const next = Number(value);
  if (!Number.isFinite(next)) return fallback;
  return next;
};

const formatTimeframeLabel = (timeframe: Timeframe) =>
  timeframe === "daily" ? "日足" : timeframe === "weekly" ? "週足" : "月足";

export default function TechnicalFilterDrawer({
  open,
  timeframe,
  anchorLabel,
  matchCount,
  value,
  onChange,
  onApply,
  onCancel,
  onReset,
  onTimeframeChange
}: TechnicalFilterDrawerProps) {
  const timeframeLabel = formatTimeframeLabel(timeframe);
  const boxThisMonth = value.boxThisMonth;

  const handleAddCondition = () => {
    if (value.conditions.length >= 10) return;
    onChange({
      ...value,
      conditions: [...value.conditions, buildDefaultCondition(timeframe)]
    });
  };

  const handleRemoveCondition = (id: string) => {
    onChange({
      ...value,
      conditions: value.conditions.filter((item) => item.id !== id)
    });
  };

  const matchLabel =
    matchCount == null ? "--" : `${matchCount.toLocaleString("ja-JP")}件`;

  const drawerClass = `tech-filter-drawer ${open ? "is-open" : ""}`;
  return (
    <div className={`tech-filter-shell ${open ? "is-visible" : "is-hidden"}`}>
      <div className="tech-filter-backdrop" onClick={onCancel} />
      <div className={drawerClass} role="dialog" aria-modal="true">
        <div className="tech-filter-header">
          <div className="tech-filter-header-top">
            <div className="tech-filter-header-title">
              <span className="tech-filter-header-icon">MA</span>
              条件検索
            </div>
            <button
              type="button"
              className="tech-filter-header-close"
              onClick={onCancel}
              aria-label="閉じる"
            >
              ×
            </button>
          </div>
        </div>
        <div className="tech-filter-body">
          <div className="tech-filter-sidebar">
            <div className="tech-filter-sidebar-title">テクニカル</div>
            <div className="tech-filter-sidebar-item active">MA</div>
          </div>
          <div className="tech-filter-panel">
            <div className="tech-filter-section">
              <div className="tech-filter-section-title">足種</div>
              <div className="tech-filter-pill-row">
                {(["daily", "weekly", "monthly"] as const).map((frame) => (
                  <button
                    key={frame}
                    type="button"
                    className={`tech-filter-pill ${timeframe === frame ? "active" : ""}`}
                    onClick={() => onTimeframeChange(frame)}
                  >
                    {formatTimeframeLabel(frame)}
                  </button>
                ))}
              </div>
              <div className="tech-filter-section-meta">
                判定: {timeframeLabel} / 基準日: {anchorLabel ?? "---"}
              </div>
            </div>
            <div className="tech-filter-section">
              <div className="tech-filter-section-title">ボックス</div>
              <div className="tech-filter-pill-row">
                <button
                  type="button"
                  className={`tech-filter-pill ${boxThisMonth ? "active" : ""}`}
                  onClick={() =>
                    onChange({
                      ...value,
                      boxThisMonth: !boxThisMonth
                    })
                  }
                >
                  今月ボックス
                </button>
              </div>
            </div>
            <div className="tech-filter-section tech-filter-conditions">
              <div className="tech-filter-section-header">
                <div className="tech-filter-section-title">条件（最大10）</div>
                <button type="button" className="tech-filter-add-link" onClick={handleAddCondition}>
                  追加
                </button>
              </div>
              {matchCount === 0 && (
                <div className="tech-filter-hint">
                  条件に一致する銘柄がありません。データ不足や asof の可能性があります。
                </div>
              )}
              <div className="tech-filter-conditions-list">
                {value.conditions.map((condition) => {
                  const leftKey = resolveLeftKey(condition.left);
                  const rightKey = resolveRightKey(condition.right);
                  const leftPeriod =
                    condition.left.type === "ma" ? condition.left.period : 20;
                  const rightPeriod =
                    condition.right.type === "ma" ? condition.right.period : 20;
                  const rightValue =
                    condition.right.type === "number" ? condition.right.value : 0;
                  return (
                    <div className="tech-filter-condition" key={condition.id}>
                      <div className="tech-filter-row-fields">
                        <select
                          className="tech-filter-timeframe-select"
                          value={condition.timeframe}
                          onChange={(event) => {
                            onChange({
                              ...value,
                              conditions: updateCondition(value.conditions, condition.id, {
                                timeframe: event.target.value as Timeframe
                              })
                            });
                          }}
                        >
                          {(["daily", "weekly", "monthly"] as const).map((frame) => (
                            <option key={frame} value={frame}>
                              {formatTimeframeLabel(frame)}
                            </option>
                          ))}
                        </select>
                        <div className="tech-filter-select-group">
                          <select
                            value={leftKey}
                            onChange={(event) => {
                              const next = event.target.value;
                              if (next === "ma") {
                                onChange({
                                  ...value,
                                  conditions: updateCondition(value.conditions, condition.id, {
                                    left: { type: "ma", period: leftPeriod }
                                  })
                                });
                                return;
                              }
                              onChange({
                                ...value,
                                conditions: updateCondition(value.conditions, condition.id, {
                                  left: { type: "field", field: "C" }
                                })
                              });
                            }}
                          >
                            {LEFT_OPERAND_OPTIONS.map((item) => (
                              <option key={item.value} value={item.value}>
                                {item.label}
                              </option>
                            ))}
                          </select>
                          {leftKey === "ma" && (
                            <input
                              type="number"
                              min={1}
                              value={leftPeriod}
                              onChange={(event) => {
                                const nextPeriod = normalizePeriod(event.target.value, leftPeriod);
                                onChange({
                                  ...value,
                                  conditions: updateCondition(value.conditions, condition.id, {
                                    left: { type: "ma", period: nextPeriod }
                                  })
                                });
                              }}
                            />
                          )}
                        </div>
                        <div className="tech-filter-operator">
                          <select
                            value={condition.operator}
                            onChange={(event) => {
                              onChange({
                                ...value,
                                conditions: updateCondition(value.conditions, condition.id, {
                                  operator: event.target.value as ConditionOperator
                                })
                              });
                            }}
                          >
                            {OPERATOR_OPTIONS.map((item) => (
                              <option key={item.value} value={item.value}>
                                {item.label}
                              </option>
                            ))}
                          </select>
                        </div>
                        <div className="tech-filter-select-group">
                          <select
                            value={rightKey}
                            onChange={(event) => {
                              const next = event.target.value;
                              if (next === "price") {
                                onChange({
                                  ...value,
                                  conditions: updateCondition(value.conditions, condition.id, {
                                    right: { type: "number", value: rightValue }
                                  })
                                });
                                return;
                              }
                              onChange({
                                ...value,
                                conditions: updateCondition(value.conditions, condition.id, {
                                  right: { type: "ma", period: rightPeriod }
                                })
                              });
                            }}
                          >
                            {RIGHT_OPERAND_OPTIONS.map((item) => (
                              <option key={item.value} value={item.value}>
                                {item.label}
                              </option>
                            ))}
                          </select>
                          {rightKey === "ma" ? (
                            <input
                              type="number"
                              min={1}
                              value={rightPeriod}
                              onChange={(event) => {
                                const nextPeriod = normalizePeriod(event.target.value, rightPeriod);
                                onChange({
                                  ...value,
                                  conditions: updateCondition(value.conditions, condition.id, {
                                    right: { type: "ma", period: nextPeriod }
                                  })
                                });
                              }}
                            />
                          ) : (
                            <input
                              type="number"
                              value={rightValue}
                              onChange={(event) => {
                                const nextValue = normalizeNumber(
                                  event.target.value,
                                  rightValue
                                );
                                onChange({
                                  ...value,
                                  conditions: updateCondition(value.conditions, condition.id, {
                                    right: { type: "number", value: nextValue }
                                  })
                                });
                              }}
                            />
                          )}
                        </div>
                        <button
                          type="button"
                          className="tech-filter-row-delete"
                          onClick={() => handleRemoveCondition(condition.id)}
                        >
                          削除
                        </button>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          </div>
        </div>
        <div className="tech-filter-footer">
          <button type="button" className="tech-filter-footer-reset" onClick={onReset}>
            リセット
          </button>
          <div className="tech-filter-footer-meta">
            このフィルター条件に合致する銘柄は合計 {matchLabel}
          </div>
          <div className="tech-filter-footer-actions">
            <button type="button" className="primary" onClick={onApply}>
              確認
            </button>
            <button type="button" onClick={onCancel}>
              キャンセル
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
