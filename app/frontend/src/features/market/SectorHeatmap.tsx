import { useCallback, useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { ResponsiveContainer, Treemap, Tooltip } from "recharts";
import { api } from "../../api";

type PeriodKey = "1d" | "1w" | "1m";
const HEATMAP_VIEW_STATE_KEY = "heatmapViewState";

type SectorItem = {
  name: string;
  sector33_code?: string;
  weight?: number;
  value?: number;
  tickerCount?: number;
  flow?: number;
  size?: number;
  color?: number;
  count?: number;
  industryName?: string;
  sector33Name?: string;
};

type HeatmapFrame = {
  asof: number;
  label: string;
  items: SectorItem[];
};

const FALLBACK_SECTORS: Array<{ sector33_code: string; name: string }> = Array.from({ length: 33 }, (_, idx) => {
  const code = String(idx + 1).padStart(2, "0");
  return { sector33_code: code, name: `セクター${code}` };
});

const buildFallbackItems = (): SectorItem[] =>
  FALLBACK_SECTORS.map((item) => ({
    sector33_code: item.sector33_code,
    name: item.name,
    weight: 0,
    value: 0,
    tickerCount: 0,
    flow: 0
  }));

const normalizeHeatmapItems = (items: SectorItem[]): SectorItem[] =>
  items.map((item) => {
    const normalizedWeight = Number(item.weight ?? item.size ?? 0);
    const normalizedValue = Number(item.value ?? item.color ?? 0);
    const normalizedCount = Number(item.tickerCount ?? item.count ?? 0);
    const normalizedFlow = Number(item.flow ?? 0);
    return {
      ...item,
      weight: Number.isFinite(normalizedWeight) ? normalizedWeight : 0,
      value: Number.isFinite(normalizedValue) ? normalizedValue : 0,
      tickerCount: Number.isFinite(normalizedCount) ? normalizedCount : 0,
      flow: Number.isFinite(normalizedFlow) ? normalizedFlow : 0
    };
  });

const PERIOD_OPTIONS: { key: PeriodKey; label: string }[] = [
  { key: "1m", label: "1ヶ月" },
  { key: "1w", label: "1週" },
  { key: "1d", label: "1日" }
];

const getColorScale = (value: number) => {
  // Positive (Red) - Darker means higher rise
  if (value >= 7) return "#7f1d1d"; // Very dark red
  if (value >= 5) return "#991b1b"; // Dark red
  if (value >= 3) return "#b91c1c"; // Red
  if (value >= 2) return "#dc2626"; // Bright red
  if (value >= 1) return "#ef4444"; // Light red
  if (value > 0) return "#f87171";  // Pale red

  // Zero
  if (value === 0) return "#6b7280"; // Neutral gray

  // Negative (Green) - Darker means deeper fall
  if (value <= -7) return "#064e3b"; // Very dark green
  if (value <= -5) return "#14532d"; // Dark green
  if (value <= -3) return "#15803d"; // Green
  if (value <= -2) return "#16a34a"; // Bright green
  if (value <= -1) return "#22c55e"; // Light green
  return "#4ade80";                  // Pale green
};

const resolveTileData = (payload: any) => {
  let current = payload;
  for (let depth = 0; depth < 4; depth += 1) {
    if (!current || typeof current !== "object") return {};
    if (current.tile && typeof current.tile === "object") return current.tile;
    if (
      "sector33_code" in current ||
      "industryName" in current ||
      "sector33Name" in current ||
      ("name" in current && current.name !== undefined && current.name !== "業界")
    ) {
      return current;
    }
    // Recharts Treemap passes data in 'root' property for custom content
    if (current.root && typeof current.root === "object") {
      current = current.root;
      continue;
    }
    if (current.payload && typeof current.payload === "object") {
      current = current.payload;
      continue;
    }
    if (current.data && typeof current.data === "object") {
      current = current.data;
      continue;
    }
    return current;
  }
  return current ?? {};
};

const getTextColor = (hex: string) => {
  const raw = hex.replace("#", "");
  if (raw.length !== 6) return "#000";
  const r = parseInt(raw.slice(0, 2), 16) / 255;
  const g = parseInt(raw.slice(2, 4), 16) / 255;
  const b = parseInt(raw.slice(4, 6), 16) / 255;
  const luminance = 0.2126 * r + 0.7152 * g + 0.0722 * b;
  return luminance < 0.5 ? "#fff" : "#000";
};

const formatRate = (value: number) => {
  if (!Number.isFinite(value)) return "--";
  const rounded = Math.round(value * 10) / 10;
  if (rounded > 0) return `+${rounded.toFixed(1)}%`;
  if (rounded < 0) return `${rounded.toFixed(1)}%`;
  return "0.0%";
};

const formatValue = (value: number) => {
  if (!Number.isFinite(value)) return "--";
  if (value >= 100_000_000) {
    return `${(value / 100_000_000).toFixed(1)}億円`;
  }
  if (value >= 1_000_000) {
    return `${(value / 1_000_000).toFixed(1)}百万円`;
  }
  return `${Math.round(value).toLocaleString("ja-JP")}円`;
};

const formatFlow = (value: number) => {
  if (!Number.isFinite(value)) return "--";
  const absValue = Math.abs(value);
  const base = formatValue(absValue);
  if (base === "--") return base;
  if (value > 0) return `+${base}`;
  if (value < 0) return `-${base}`;
  return base;
};

type RenderSectorTileProps = {
  onClick?: (item: SectorItem) => void;
  colorScale: (value: number) => string;
};

const RenderSectorTile = (props: any & RenderSectorTileProps) => {
  const { x, y, width, height, onClick, colorScale } = props;

  // Recharts Treemap spreads the original data item into props.
  // We explicitly set 'tile' property in treemapData with all the data we need.
  // Priority order:
  // 1. props.tile (our explicit tile property set in treemapData)
  // 2. props directly (Recharts spreads the data)
  // 3. props.root.tile or props.root (Recharts internal structure)
  // 4. payload (legacy support)

  let data: any = {};

  // 1. Check props.tile first (we explicitly set this in treemapData)
  if (props.tile && typeof props.tile === "object" && props.tile.name && props.tile.name !== "業界") {
    data = props.tile;
  }
  // 2. Check if data is directly spread into props
  else if (props.name && props.name !== "業界" && (props.sector33_code || props.industryName)) {
    data = {
      name: props.name,
      industryName: props.industryName ?? props.name,
      sector33_code: props.sector33_code,
      value: props.value ?? props.color ?? 0,
      weight: props.weight ?? props.size ?? 0,
      tickerCount: props.tickerCount ?? props.count ?? 0,
      flow: props.flow ?? 0,
    };
  }
  // 3. Check props.root (Recharts 2.x internal structure)
  else if (props.root && typeof props.root === "object") {
    const root = props.root;
    if (root.tile && typeof root.tile === "object" && root.tile.name && root.tile.name !== "業界") {
      data = root.tile;
    } else if (root.name && root.name !== "業界") {
      data = root;
    }
  }
  // 4. Fallback to payload (legacy)
  else if (props.payload) {
    const resolved = resolveTileData(props.payload);
    if (resolved && Object.keys(resolved).length > 0) {
      data = resolved;
    }
  }

  // Debug log for first tile only in development
  if (typeof import.meta !== "undefined" && (import.meta as any).env?.MODE === "development" && x === 0 && y === 0) {
    console.log("RenderSectorTile FIRST TILE props keys:", Object.keys(props));
    console.log("RenderSectorTile FIRST TILE props.tile:", props.tile);
    console.log("RenderSectorTile FIRST TILE props.name:", props.name);
    console.log("RenderSectorTile FIRST TILE resolved data:", data);
  }


  const rate = Number(data?.value ?? data?.color ?? 0);
  const count = Number(data?.tickerCount ?? data?.count ?? 0);
  const weight = Number(data?.weight ?? data?.size ?? 0);
  const flow = Number(data?.flow ?? 0);
  const hasData =
    (Number.isFinite(weight) && weight > 0) || (Number.isFinite(count) && count > 0);
  const bgColor = hasData ? colorScale(rate) : "#374151";
  const textColor = getTextColor(bgColor);
  const minWidth = 56;
  const minHeight = 34;
  const showText = width >= minWidth && height >= minHeight;
  const fontSize = width >= 140 && height >= 80 ? 14 : width >= 90 ? 12 : 10;
  const padding = 4;
  const industryLabel = data?.industryName ?? data?.name ?? data?.sector33Name ?? data?.sector33_code ?? "業界";
  const detailValue = hasData ? formatRate(rate) : "--";
  const flowLabel = hasData ? formatFlow(flow) : "--";
  const valueLabel = hasData ? formatValue(weight) : "--";


  return (
    <g onClick={() => onClick?.(data)}>
      <rect
        x={x}
        y={y}
        width={width}
        height={height}
        fill={bgColor}
        stroke="#fff"
        strokeWidth={1}
        rx={6}
        ry={6}
      />
      {showText && (
        <foreignObject x={x + padding} y={y + padding} width={width - padding * 2} height={height - padding * 2}>
          <div
            style={{
              width: "100%",
              height: "100%",
              display: "flex",
              flexDirection: "column",
              alignItems: "center",
              justifyContent: "center",
              color: textColor,
              fontWeight: 600,
              fontSize,
              textAlign: "center",
              lineHeight: 1.2,
              overflow: "hidden"
            }}
          >
            <div
              style={{
                maxWidth: "100%",
                overflow: "hidden",
                textOverflow: "ellipsis",
                whiteSpace: "nowrap",
                fontSize,
                fontWeight: 600
              }}
            >
              {industryLabel}
            </div>
            <div style={{ fontSize: Math.max(10, fontSize - 2), opacity: 0.88, marginTop: 4 }}>
              {detailValue} / {flowLabel} / {valueLabel}
            </div>
          </div>
        </foreignObject>
      )}
    </g>
  );
};

const CustomTooltip = ({ active, payload }: any) => {
  if (!active || !payload?.length) return null;
  const item = payload[0]?.payload as SectorItem | undefined;
  if (!item) return null;
  const rate = Number(item.value ?? item.color ?? 0);
  const weight = Number(item.weight ?? item.size ?? 0);
  const flow = Number(item.flow ?? 0);
  const count = Number(item.tickerCount ?? item.count ?? 0);
  return (
    <div
      style={{
        background: "rgba(15, 23, 42, 0.92)",
        color: "#fff",
        padding: "10px 12px",
        borderRadius: 8,
        fontSize: 12,
        boxShadow: "0 8px 24px rgba(0,0,0,0.2)",
        minWidth: 180
      }}
    >
      <div style={{ fontWeight: 700, marginBottom: 6 }}>{item.name}</div>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
        <span>騰落率</span>
        <span>{formatRate(rate)}</span>
      </div>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
        <span>資金移動</span>
        <span>{formatFlow(flow)}</span>
      </div>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
        <span>売買代金</span>
        <span>{formatValue(weight)}</span>
      </div>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
        <span>構成銘柄数</span>
        <span>{Number(count).toLocaleString("ja-JP")}</span>
      </div>
    </div>
  );
};

export default function SectorHeatmap() {
  const navigate = useNavigate();
  const sectorNameMap = useMemo(() => {
    const map = new Map<string, string>();
    FALLBACK_SECTORS.forEach((item) => map.set(item.sector33_code, item.name));
    return map;
  }, []);
  const [period, setPeriod] = useState<PeriodKey>("1d");
  const [frames, setFrames] = useState<HeatmapFrame[]>([]);
  const [cursorIndex, setCursorIndex] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (typeof window === "undefined") return;
    try {
      const stored = window.sessionStorage.getItem(HEATMAP_VIEW_STATE_KEY);
      if (!stored) return;
      const parsed = JSON.parse(stored) as { period?: PeriodKey };
      if (parsed.period === "1d" || parsed.period === "1w" || parsed.period === "1m") {
        setPeriod(parsed.period);
      }
    } catch {
      // ignore storage failures
    }
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") return;
    try {
      window.sessionStorage.setItem(HEATMAP_VIEW_STATE_KEY, JSON.stringify({ period }));
    } catch {
      // ignore storage failures
    }
  }, [period]);

  useEffect(() => {
    let canceled = false;
    const timelineLimit = 180;
    const applyFrames = (nextFrames: HeatmapFrame[]) => {
      if (canceled) return;
      setFrames(nextFrames);
      setCursorIndex(nextFrames.length > 0 ? nextFrames.length - 1 : 0);
    };
    const buildFallbackFrame = (items: SectorItem[]) => {
      const now = new Date();
      const label = period === "1m" ? now.toISOString().slice(0, 7) : now.toISOString().slice(0, 10);
      return {
        asof: Math.floor(now.getTime() / 1000),
        label,
        items
      };
    };

    setLoading(true);
    setError(null);
    const loadFallback = (message: string | null) => {
      api
        .get("/market/heatmap", { params: { period } })
        .then((res) => {
          if (canceled) return;
          const data = res.data?.items;
          const normalized =
            Array.isArray(data) && data.length > 0 ? normalizeHeatmapItems(data) : [];
          const next = normalized.length > 0 ? normalized : buildFallbackItems();
          applyFrames([buildFallbackFrame(next)]);
          setError(message);
        })
        .catch(() => {
          if (canceled) return;
          setError(message || "ヒートマップの取得に失敗しました");
          applyFrames([buildFallbackFrame(buildFallbackItems())]);
        });
    };

    api
      .get("/market/heatmap/timeline", { params: { period, limit: timelineLimit } })
      .then((res) => {
        if (canceled) return;
        const data = res.data?.frames;
        const hasFrames = Array.isArray(data) && data.length > 0;
        const hasItems =
          hasFrames &&
          data.some((frame: HeatmapFrame) => Array.isArray(frame.items) && frame.items.length > 0);
        if (hasFrames && hasItems) {
          applyFrames(data);
          return;
        }
        loadFallback(null);
      })
      .catch((err) => {
        if (canceled) return;
        const message = err?.message || "ヒートマップの取得に失敗しました";
        loadFallback(message);
      })
      .finally(() => {
        if (canceled) return;
        setLoading(false);
      });
    return () => {
      canceled = true;
    };
  }, [period]);

  const handleSectorClick = useCallback(
    (item: SectorItem) => {
      if (!item?.sector33_code) return;
      navigate(`/?sector=${encodeURIComponent(item.sector33_code)}`);
    },
    [navigate]
  );

  const clampedIndex = useMemo(() => {
    if (!frames.length) return 0;
    return Math.min(Math.max(cursorIndex, 0), frames.length - 1);
  }, [cursorIndex, frames.length]);

  const activeFrame = useMemo(() => {
    if (!frames.length) return null;
    return frames[clampedIndex] ?? null;
  }, [frames, clampedIndex]);

  const activeItems = useMemo(() => {
    if (!activeFrame) return [];
    return normalizeHeatmapItems(activeFrame.items ?? []);
  }, [activeFrame]);

  const valueRange = useMemo(() => {
    const values = activeItems
      .map((item) => Number(item.value ?? 0))
      .filter((value) => Number.isFinite(value));
    if (!values.length) {
      return { min: 0, max: 0, maxAbs: 1 };
    }
    const min = Math.min(...values);
    const max = Math.max(...values);
    const maxAbs = Math.max(Math.abs(min), Math.abs(max)) || 1;
    return { min, max, maxAbs };
  }, [activeItems]);

  const colorScale = useCallback(
    (value: number) => {
      if (!Number.isFinite(value)) return "#6b7280";
      const maxAbs = valueRange.maxAbs || 1;
      const normalized = Math.max(-maxAbs, Math.min(maxAbs, value)) / maxAbs;
      if (normalized === 0) return "#6b7280";
      const hue = normalized > 0 ? 0 : 135;
      const intensity = Math.abs(normalized);
      const lightness = 75 - intensity * 40;
      return `hsl(${hue} 75% ${lightness}%)`;
    },
    [valueRange.maxAbs]
  );

  const treemapData = useMemo(() => {
    const sorted = [...activeItems].sort((a, b) => {
      const aCode = a.sector33_code ?? "";
      const bCode = b.sector33_code ?? "";
      if (aCode === bCode) return 0;
      if (!aCode) return 1;
      if (!bCode) return -1;
      return aCode.localeCompare(bCode, "ja");
    });
    return sorted.map((item) => {
      const weight = Number(item.weight ?? 0);
      const flow = Number(item.flow ?? 0);
      const resolvedName =
        item.name ??
        item.industryName ??
        item.sector33Name ??
        (item.sector33_code ? sectorNameMap.get(item.sector33_code) : null) ??
        "業界";
      return {
        ...item,
        name: resolvedName,
        industryName: resolvedName,
        tile: {
          ...item,
          name: resolvedName,
          industryName: resolvedName,
          weight,
          flow,
          tickerCount: Number(item.tickerCount ?? 0),
          value: Number(item.value ?? 0)
        },
        rawSize: 1,
        size: 1,
        color: Number(item.value ?? 0),
        count: Number(item.tickerCount ?? 0),
        weight,
        flow
      };
    });
  }, [activeItems]);

  const sectorSummary = useMemo(() => {
    if (!activeItems.length) return null;
    const withData = activeItems.filter(
      (item) => Number(item.tickerCount ?? 0) > 0 && Number(item.weight ?? 0) > 0
    );
    if (!withData.length) return null;
    const best = withData.reduce(
      (prev, next) => (Number(next.value ?? 0) > Number(prev.value ?? 0) ? next : prev),
      withData[0]
    );
    const worst = withData.reduce(
      (prev, next) => (Number(next.value ?? 0) < Number(prev.value ?? 0) ? next : prev),
      withData[0]
    );
    const totalWeight = withData.reduce((sum, item) => sum + Number(item.weight ?? 0), 0);
    const totalFlow = withData.reduce((sum, item) => sum + Number(item.flow ?? 0), 0);
    const avgValue = withData.reduce((sum, item) => sum + Number(item.value ?? 0), 0) / withData.length;
    return {
      best,
      worst,
      totalWeight,
      totalFlow,
      avgValue
    };
  }, [activeItems]);

  const rendered = !loading && activeItems.length > 0;
  const showEmpty = !loading && (!frames.length || activeItems.length === 0);
  const renderState = error ? "error" : rendered ? "ready" : loading ? "loading" : "empty";
  const timelineLabel = activeFrame?.label ?? "";
  const frameCount = frames.length;

  if (import.meta.env.MODE === "development") {
    // console.log("render state", { ... });
  }


  return (
    <div className="market-heatmap">
      <div className="heatmap-toolbar">
        {PERIOD_OPTIONS.map((option) => (
          <button
            key={option.key}
            type="button"
            onClick={() => setPeriod(option.key)}
            style={{
              border: "1px solid var(--theme-border)",
              background: period === option.key ? "var(--theme-accent)" : "var(--theme-bg-secondary)",
              color: period === option.key ? "#fff" : "var(--theme-text-primary)",
              padding: "6px 12px",
              borderRadius: 999,
              fontWeight: 600,
              cursor: "pointer"
            }}
          >
            {option.label}
          </button>
        ))}
        <span style={{ fontSize: 12, color: "var(--theme-text-muted)" }}>{loading ? "読み込み中..." : ""}</span>
      </div>
      {frameCount > 0 && (
        <div className="heatmap-timeline">
          <span className="heatmap-timeline-label">{timelineLabel}</span>
          <input
            className="heatmap-timeline-range"
            type="range"
            min={0}
            max={Math.max(frameCount - 1, 0)}
            value={clampedIndex}
            onChange={(event) => setCursorIndex(Number(event.target.value))}
          />
          <span className="heatmap-timeline-meta">
            {clampedIndex + 1}/{frameCount}
          </span>
        </div>
      )}
      <div className="heatmap-stage">
        {error && (
          <div className="heatmap-empty">
            <div className="heatmap-empty-card">
              <div className="heatmap-empty-title">ヒートマップの取得に失敗しました</div>
              <div className="heatmap-empty-sub">{error}</div>
            </div>
          </div>
        )}
        {showEmpty && (
          <div className="heatmap-empty">
            <div className="heatmap-empty-card">
              <div className="heatmap-empty-title">ヒートマップのデータがありません</div>
              <div className="heatmap-empty-sub">
                データ更新後に再読み込みしてください。
              </div>
            </div>
          </div>
        )}
        <div
          className="heatmap-canvas"
          data-heatmap-rendered={rendered ? "1" : "0"}
          data-heatmap-state={renderState}
        >
          <div style={{ width: "100%", height: 460 }}>
            <ResponsiveContainer>
              <Treemap
                data={treemapData}
                dataKey="size"
                stroke="#fff"
                animationDuration={240}
                content={<RenderSectorTile onClick={handleSectorClick} colorScale={colorScale} />}
              >
                <Tooltip content={<CustomTooltip />} />
              </Treemap>
            </ResponsiveContainer>
          </div>
        </div>
        {sectorSummary && (
          <div className="sector-summary">
            <div className="sector-summary-row">
              <strong>トップ騰落セクター</strong>
              <span>{sectorSummary.best.name ?? sectorSummary.best.industryName}</span>
              <span>{formatRate(Number(sectorSummary.best.value ?? 0))}</span>
            </div>
            <div className="sector-summary-row">
              <strong>ボトム騰落セクター</strong>
              <span>{sectorSummary.worst.name ?? sectorSummary.worst.industryName}</span>
              <span>{formatRate(Number(sectorSummary.worst.value ?? 0))}</span>
            </div>
            <div className="sector-summary-row">
              <strong>平均騰落率</strong>
              <span>{formatRate(sectorSummary.avgValue)}</span>
            </div>
            <div className="sector-summary-row">
              <strong>合計売買代金</strong>
              <span>{formatValue(sectorSummary.totalWeight)}</span>
            </div>
            <div className="sector-summary-row">
              <strong>資金移動合計</strong>
              <span>{formatFlow(sectorSummary.totalFlow)}</span>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
