import { useCallback, useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { ResponsiveContainer, Treemap, Tooltip } from "recharts";
import { api } from "../../api";

type PeriodKey = "1d" | "1w" | "1m";
const HEATMAP_VIEW_STATE_KEY = "heatmapViewState";

type SectorItem = {
  name: string;
  size: number;
  color: number;
  count?: number;
  sector33_code?: string;
  weight?: number;
  value?: number;
  tickerCount?: number;
  industryName?: string;
  sector33Name?: string;
};

const FALLBACK_SECTORS: Array<{ sector33_code: string; name: string }> = Array.from({ length: 33 }, (_, idx) => {
  const code = String(idx + 1).padStart(2, "0");
  return { sector33_code: code, name: `セクター${code}` };
});

const buildFallbackItems = (): SectorItem[] =>
  FALLBACK_SECTORS.map((item) => ({
    sector33_code: item.sector33_code,
    name: item.name,
    size: 0,
    color: 0,
    count: 0
  }));

const normalizeHeatmapItems = (items: SectorItem[]): SectorItem[] =>
  items.map((item) => {
    const normalizedSize = Number(item.size ?? item.weight ?? 0);
    const normalizedColor = Number(item.color ?? item.value ?? 0);
    const normalizedCount = Number(item.count ?? item.tickerCount ?? 0);
    return {
      ...item,
      size: Number.isFinite(normalizedSize) ? normalizedSize : 0,
      color: Number.isFinite(normalizedColor) ? normalizedColor : 0,
      count: Number.isFinite(normalizedCount) ? normalizedCount : 0
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
  if (!payload) return {};
  if (payload.payload && typeof payload.payload === "object") return payload.payload;
  if (payload.data && typeof payload.data === "object") return payload.data;
  return payload;
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

type RenderSectorTileProps = {
  onClick?: (item: SectorItem) => void;
};

const RenderSectorTile = (props: any & RenderSectorTileProps) => {
  const { x, y, width, height, payload, onClick } = props;

  // Debug: check what props we receive
  if (!payload && import.meta.env.MODE === "development") {
    console.warn("RenderSectorTile: payload is missing. Props:", props);
  }

  // Fallback to props if payload is missing/empty
  const resolved = resolveTileData(payload);
  const data = (resolved && Object.keys(resolved).length > 0) ? resolved : props;

  if (import.meta.env.MODE === "development" && (!data || !data.name)) {
    console.log("RenderSectorTile resolved data:", data, "from payload:", payload, "props:", props);
  }

  const rate = Number(data?.color ?? 0);
  const count = Number(data?.count ?? 0);
  const rawSize = Number(data?.rawSize ?? data?.size ?? 0);
  const hasData = Number.isFinite(count) && Number.isFinite(rawSize) && count > 0 && rawSize > 0;
  const bgColor = hasData ? getColorScale(rate) : "#374151";
  const textColor = getTextColor(bgColor);
  const minWidth = 56;
  const minHeight = 34;
  const showText = width >= minWidth && height >= minHeight;
  const fontSize = width >= 140 && height >= 80 ? 14 : width >= 90 ? 12 : 10;
  const padding = 4;
  const rateLabel = hasData ? formatRate(rate) : "--";

  const industryLabel = data?.industryName ?? data?.name ?? data?.sector33Name ?? data?.sector33_code ?? "業界";
  const detailValue = hasData ? formatRate(rate) : "--";
  const valueLabel = hasData ? formatValue(Number(data?.size ?? data?.weight ?? rawSize)) : "--";
  const tickerLabel = hasData ? Number(count).toLocaleString("ja-JP") : "--";


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
              {detailValue} / {valueLabel} / {tickerLabel}
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
        <span>{formatRate(Number(item.color))}</span>
      </div>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
        <span>売買代金</span>
        <span>{formatValue(Number(item.size))}</span>
      </div>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
        <span>構成銘柄数</span>
        <span>{Number(item.count ?? 0).toLocaleString("ja-JP")}</span>
      </div>
    </div>
  );
};

export default function SectorHeatmap() {
  const navigate = useNavigate();
  const [period, setPeriod] = useState<PeriodKey>("1d");
  const [items, setItems] = useState<SectorItem[]>([]);
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
    setLoading(true);
    setError(null);
    api
      .get("/market/heatmap", { params: { period } })
      .then((res) => {
        if (canceled) return;
        const data = res.data?.items;
        const normalized =
          Array.isArray(data) && data.length > 0 ? normalizeHeatmapItems(data) : [];
        const next = normalized.length > 0 ? normalized : buildFallbackItems();
        setItems(next);
      })
      .catch((err) => {
        if (canceled) return;
        const message = err?.message || "ヒートマップの取得に失敗しました";
        setError(message);
        setItems(buildFallbackItems());
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

  const treemapData = useMemo(() => {
    return items.map((item) => ({
      ...item,
      industryName: item.name ?? item.sector33Name ?? item.sector33_code ?? "業界",
      rawSize: Number(item.size ?? 0),
      size: Number(item.size ?? 0) > 0 ? Number(item.size ?? 0) : 1,
      color: Number(item.color ?? 0)
    }));
  }, [items]);

  const sectorSummary = useMemo(() => {
    if (!items.length) return null;
    const withData = items.filter((item) => Number(item.count ?? 0) > 0 && Number(item.size ?? 0) > 0);
    if (!withData.length) return null;
    const best = withData.reduce((prev, next) => (Number(next.color ?? 0) > Number(prev.color ?? 0) ? next : prev), withData[0]);
    const worst = withData.reduce((prev, next) => (Number(next.color ?? 0) < Number(prev.color ?? 0) ? next : prev), withData[0]);
    const totalWeight = withData.reduce((sum, item) => sum + Number(item.size ?? 0), 0);
    const avgValue = withData.reduce((sum, item) => sum + Number(item.color ?? 0), 0) / withData.length;
    return {
      best,
      worst,
      totalWeight,
      avgValue
    };
  }, [items]);

  const rendered = !loading && items.length > 0;
  const showEmpty = !loading && items.length === 0;
  const renderState = error ? "error" : rendered ? "ready" : loading ? "loading" : "empty";

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
                content={<RenderSectorTile onClick={handleSectorClick} />}
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
              <span>{formatRate(Number(sectorSummary.best.color ?? 0))}</span>
            </div>
            <div className="sector-summary-row">
              <strong>ボトム騰落セクター</strong>
              <span>{sectorSummary.worst.name ?? sectorSummary.worst.industryName}</span>
              <span>{formatRate(Number(sectorSummary.worst.color ?? 0))}</span>
            </div>
            <div className="sector-summary-row">
              <strong>平均騰落率</strong>
              <span>{formatRate(sectorSummary.avgValue)}</span>
            </div>
            <div className="sector-summary-row">
              <strong>合計値（size）</strong>
              <span>{formatValue(sectorSummary.totalWeight)}</span>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
