import type { ChartCallout, DrawBox, HorizontalLine } from "../../components/DetailChart";

export type ChartAnnotationType = "bar" | "region" | "line" | "callout" | "chart_context";
export type AnnotationTool = "select" | "bar" | "region" | "line" | "callout";
export type AnnotationFilter = "all" | "bar" | "region" | "line" | "callout" | "context" | "hidden";

export type ChartAnnotation = {
  id: string;
  code: string;
  as_of_date: string;
  timeframe: string;
  object_type: ChartAnnotationType;
  payload: Record<string, any>;
  tags: string[];
  no_lookahead: boolean;
  created_at?: string | null;
  updated_at?: string | null;
};

export const emptyAnnotationPayload = (type: ChartAnnotationType) => {
  if (type === "bar") {
    return {
      bar_role: "observed",
      action_label: "watch",
      reason_tags: [],
      free_text: "",
      importance: 3,
      no_lookahead: true,
    };
  }
  if (type === "region") {
    return {
      region_type: "support_zone",
      meaning_tags: [],
      free_text: "",
      valid_while: "",
      invalid_if: "",
      importance: 3,
      no_lookahead: true,
    };
  }
  if (type === "callout") {
    return {
      anchor_type: "bar",
      anchor_target: "candle",
      label_position: {},
      leader_line: true,
      comment_type: "review",
      free_text: "",
      tags: [],
      importance: 3,
      no_lookahead: true,
    };
  }
  if (type === "chart_context") {
    return {
      context_type: "environment",
      market_phase: "",
      support_resistance_summary: "",
      comment_type: "chart_context",
      free_text: "",
      tags: [],
      importance: 3,
      no_lookahead: true,
    };
  }
  return {
    line_type: "support",
    break_rule: "",
    action_if_broken: "watch",
    free_text: "",
    importance: 3,
    no_lookahead: true,
  };
};

export const parseTagsInput = (value: string): string[] =>
  value
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);

const dateLikeToChartTime = (value: unknown): number | null => {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  const iso = trimmed.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (iso) return Number(`${iso[1]}${iso[2]}${iso[3]}`);
  const compact = trimmed.match(/^(\d{8})$/);
  return compact ? Number(compact[1]) : null;
};

export const annotationToDrawBox = (annotation: ChartAnnotation): DrawBox | null => {
  if (annotation.object_type !== "region") return null;
  const payload = annotation.payload ?? {};
  const startTime = Number(payload.start_time);
  const endTime = Number(payload.end_time);
  const topPrice = Number(payload.price_high);
  const bottomPrice = Number(payload.price_low);
  if (![startTime, endTime, topPrice, bottomPrice].every(Number.isFinite)) return null;
  return {
    annotationId: annotation.id,
    startTime,
    endTime,
    topPrice,
    bottomPrice,
    color: "#2563eb",
    opacity: 0.14,
    lineWidth: 2,
  };
};

export const annotationToHorizontalLine = (annotation: ChartAnnotation): HorizontalLine | null => {
  if (annotation.object_type !== "line") return null;
  const price = Number(annotation.payload?.price);
  if (!Number.isFinite(price)) return null;
  return {
    annotationId: annotation.id,
    price,
    color: "#0f766e",
    opacity: 0.8,
    lineWidth: 1.5,
  };
};

export const annotationToEventMarker = (annotation: ChartAnnotation) => {
  if (annotation.object_type !== "bar" && annotation.object_type !== "callout") return null;
  const time = Number(annotation.payload?.bar_time ?? annotation.payload?.anchor_time);
  if (!Number.isFinite(time)) return null;
  return {
    time,
    label: annotation.object_type === "callout" ? annotation.payload?.comment_type || "callout" : annotation.payload?.action_label || "note",
    kind: "tdnet-neutral" as const,
  };
};

export const annotationToCallout = (annotation: ChartAnnotation, selected = false): ChartCallout | null => {
  if (annotation.object_type !== "callout") return null;
  const payload = annotation.payload ?? {};
  const anchorTime = dateLikeToChartTime(payload.anchor_time ?? payload.anchor_date);
  const labelTime = dateLikeToChartTime(payload.label_position?.time ?? payload.label_position?.date);
  const anchorPrice = Number(payload.anchor_price);
  const labelPrice = Number(payload.label_position?.price);
  if (![anchorTime, labelTime, anchorPrice, labelPrice].every((item) => item != null && Number.isFinite(item))) {
    return null;
  }
  return {
    id: annotation.id,
    anchorTime,
    anchorPrice,
    labelTime,
    labelPrice,
    text: payload.free_text ?? "",
    tags: annotation.tags ?? payload.tags ?? [],
    selected,
  };
};
