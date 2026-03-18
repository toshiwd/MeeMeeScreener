import { useMemo, useCallback, useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import {
  ResponsiveContainer,
  ScatterChart,
  Scatter,
  XAxis,
  YAxis,
  ZAxis,
  CartesianGrid,
  Tooltip,
  Cell,
  ReferenceLine
} from "recharts";
import { api } from "../../api";

type PeriodKey = "1d" | "1w" | "1m";
type Momentum = "heating" | "cooling" | "neutral";
const BUBBLE_VIEW_STATE_KEY = "heatmapViewState"; // Reuse period state from heatmap

type SectorItem = {
  name: string;
  industryName?: string;
  sector33Name?: string;
  sector33_code?: string;
  weight?: number;
  value?: number;
  tickerCount?: number;
  flow?: number;
};

type HeatmapFrame = {
  asof: number;
  label: string;
  items: SectorItem[];
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

const normalizeHeatmapItems = (items: SectorItem[]): SectorItem[] =>
  items.map((item) => ({
    ...item,
    weight: Number.isFinite(Number(item.weight)) ? Number(item.weight) : 0,
    value: Number.isFinite(Number(item.value)) ? Number(item.value) : 0,
    tickerCount: Number.isFinite(Number(item.tickerCount)) ? Number(item.tickerCount) : 0,
    flow: Number.isFinite(Number(item.flow)) ? Number(item.flow) : 0
  }));

const CustomTooltip = ({ active, payload }: any) => {
  if (!active || !payload?.length) return null;
  const item = payload[0]?.payload;
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
        minWidth: 180,
        zIndex: 100
      }}
    >
      <div style={{ fontWeight: 700, marginBottom: 6 }}>{item.name}</div>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
        <span>騰落率</span>
        <span style={{ color: item.value > 0 ? "#ef4444" : "#3b82f6" }}>{formatRate(item.value)}</span>
      </div>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
        <span>資金移動</span>
        <span style={{ color: item.flow > 0 ? "#ef4444" : "#3b82f6" }}>{formatFlow(item.flow)}</span>
      </div>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
        <span>売買代金</span>
        <span>{formatValue(item.weight)}</span>
      </div>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
        <span>構成銘柄数</span>
        <span>{Number(item.tickerCount).toLocaleString("ja-JP")}</span>
      </div>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 8, marginTop: 4 }}>
        <span style={{ color: "var(--theme-text-muted)" }}>モメンタム (前回比)</span>
        <span>
          {item.momentum === "heating" ? <span style={{ color: "#ef4444" }}>▲ 資金流入加速</span> : 
           item.momentum === "cooling" ? <span style={{ color: "#3b82f6" }}>▼ 資金流出加速</span> : "横ばい"}
        </span>
      </div>
    </div>
  );
};

export default function SectorBubbleChart() {
  const navigate = useNavigate();
  const [period, setPeriod] = useState<PeriodKey>("1d");
  const [frames, setFrames] = useState<HeatmapFrame[]>([]);
  const [cursorIndex, setCursorIndex] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (typeof window === "undefined") return;
    try {
      const stored = window.sessionStorage.getItem(BUBBLE_VIEW_STATE_KEY);
      if (!stored) return;
      const parsed = JSON.parse(stored) as { period?: PeriodKey };
      if (parsed.period === "1d" || parsed.period === "1w" || parsed.period === "1m") {
        setPeriod(parsed.period);
      }
    } catch {
      // ignore
    }
  }, []);

  useEffect(() => {
    let canceled = false;
    const timelineLimit = 180;
    
    setError(null); // Added as per instruction
    setLoading(true);
    
    api
      .get("/market/heatmap/timeline", { params: { period, limit: timelineLimit } })
      .then((res) => {
        if (canceled) return;
        const data = res.data?.frames;
        if (Array.isArray(data) && data.length > 0) {
          setFrames(data);
          setCursorIndex(data.length - 1);
          setError(null);
        } else {
          setError("バブルチャートのデータがありません");
        }
      })
      .catch((err) => {
        if (canceled) return;
        setError(err?.message || "データの取得に失敗しました");
      })
      .finally(() => {
        if (canceled) return;
        setLoading(false);
      });
      
    return () => { canceled = true; };
  }, [period]);

  const clampedIndex = useMemo(() => {
    if (!frames.length) return 0;
    return Math.min(Math.max(cursorIndex, 0), frames.length - 1);
  }, [cursorIndex, frames.length]);

  const activeFrame = useMemo(() => frames[clampedIndex] ?? null, [frames, clampedIndex]);
  const activeItems = useMemo(() => activeFrame ? normalizeHeatmapItems(activeFrame.items) : [], [activeFrame]);
  
  const prevFrame = useMemo(() => {
    if (!frames.length || clampedIndex === 0) return null;
    return frames[clampedIndex - 1] ?? null;
  }, [frames, clampedIndex]);

  const prevItemsMap = useMemo(() => {
    const map = new Map<string, SectorItem>();
    if (!prevFrame) return map;
    normalizeHeatmapItems(prevFrame.items ?? []).forEach((item) => {
      if (item.sector33_code) map.set(item.sector33_code, item);
    });
    return map;
  }, [prevFrame]);

  const chartData = useMemo(() => {
    return activeItems.map(item => {
      const flow = Number(item.flow ?? 0);
      const prevItem = prevItemsMap.get(item.sector33_code ?? "");
      const prevFlow = Number(prevItem?.flow ?? flow);
      
      let momentum: Momentum = "neutral";
      if (flow < prevFlow) momentum = "cooling";
      else if (flow > prevFlow) momentum = "heating";

      const name = item.name ?? item.industryName ?? item.sector33Name ?? "業界";
      const shortName = name.replace(/業$/, "").substring(0, 6);

      return {
        ...item,
        name,
        shortName,
        x: Number(item.value ?? 0), // 騰落率
        y: flow,               // 資金フロー (金額)
        z: Math.max(Number(item.weight ?? 0), 1),
        momentum,
      };
    }).filter(item => item.sector33_code && item.sector33_code !== "00" && (item.tickerCount ?? 0) > 0);
  }, [activeItems, prevItemsMap]);

  const { minX, maxX, minY, maxY } = useMemo(() => {
    if (!chartData.length) return { minX: 0, maxX: 0, minY: 0, maxY: 0 };
    const xs = chartData.map(d => d.x);
    const ys = chartData.map(d => d.y);
    const mX = Math.max(Math.abs(Math.min(...xs)), Math.abs(Math.max(...xs))) * 1.1 || 1;
    const mY = Math.max(Math.abs(Math.min(...ys)), Math.abs(Math.max(...ys))) * 1.1 || 1;
    return { minX: -mX, maxX: mX, minY: -mY, maxY: mY };
  }, [chartData]);

  const handleSectorClick = useCallback((data: any) => {
    if (data?.sector33_code) {
      navigate(`/?sector=${encodeURIComponent(data.sector33_code)}`);
    }
  }, [navigate]);

  const getBubbleColor = (momentum: Momentum) => {
    if (momentum === "heating") return "rgba(239, 68, 68, 0.75)"; // red
    if (momentum === "cooling") return "rgba(59, 130, 246, 0.75)"; // blue
    return "rgba(107, 114, 128, 0.75)"; // gray
  };

  const frameCount = frames.length;
  const timelineLabel = activeFrame?.label ?? "";

  const renderCustomLabel = (props: any) => {
    const { x, y, value, payload } = props;
    const name = payload?.shortName ?? "";
    const momentum = payload?.momentum;
    const renderArrow = () => {
      if (momentum === "heating") return <tspan fill="#ef4444" fontSize={14}> ▲</tspan>;
      if (momentum === "cooling") return <tspan fill="#3b82f6" fontSize={14}> ▼</tspan>;
      return null;
    };
    return (
      <text
        x={x}
        y={y}
        dy={4}
        textAnchor="middle"
        fill="#e2e8f0"
        fontSize={11}
        fontWeight={600}
        pointerEvents="none"
        style={{ textShadow: "0px 1px 3px rgba(0,0,0,0.9)" }}
      >
        {name}{renderArrow()}
      </text>
    );
  };

  return (
    <div className="market-heatmap bubble-chart-view">
      <div className="heatmap-header">
        <h2 className="heatmap-title">業種別バブルチャート</h2>
        <div className="heatmap-controls">
          <div className="heatmap-period-selector" style={{ display: "flex", gap: "4px", background: "rgba(30, 41, 59, 0.5)", padding: "2px", borderRadius: "6px" }}>
            {(["1d", "1w", "1m"] as PeriodKey[]).map((p) => (
              <button
                key={p}
                onClick={() => setPeriod(p)}
                style={{
                  padding: "4px 12px",
                  fontSize: "12px",
                  borderRadius: "4px",
                  border: "none",
                  background: period === p ? "rgba(59, 130, 246, 0.2)" : "transparent",
                  color: period === p ? "#60a5fa" : "rgba(255,255,255,0.5)",
                  cursor: "pointer",
                  fontWeight: period === p ? 600 : 400,
                  transition: "all 0.2s"
                }}
              >
                {p === "1d" ? "1日" : p === "1w" ? "1週" : "1ヶ月"}
              </button>
            ))}
          </div>
        </div>
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
      <div className="heatmap-stage" style={{ minHeight: "520px" }}>
        {error && (
          <div className="heatmap-empty">
            <div className="heatmap-empty-card">
              <div className="heatmap-empty-title">データエラー</div>
              <div className="heatmap-empty-sub">{error}</div>
            </div>
          </div>
        )}
        <div style={{ position: "absolute", inset: 0, pointerEvents: "none", zIndex: 0 }}>
          <div style={{ position: "absolute", top: "16px", right: "24px", color: "rgba(239, 68, 68, 0.3)", fontSize: "20px", fontWeight: "bold" }}>強いセクター (上昇+資金流入)</div>
          <div style={{ position: "absolute", bottom: "32px", left: "24px", color: "rgba(59, 130, 246, 0.3)", fontSize: "20px", fontWeight: "bold" }}>弱いセクター (下落+資金流出)</div>
          <div style={{ position: "absolute", bottom: "32px", right: "24px", color: "rgba(234, 179, 8, 0.3)", fontSize: "16px", fontWeight: "bold" }}>ピークアウト警戒 (上昇だが流出)</div>
          <div style={{ position: "absolute", top: "16px", left: "24px", color: "rgba(234, 179, 8, 0.3)", fontSize: "16px", fontWeight: "bold" }}>反転候補 (下落だが流入)</div>
        </div>
        {!loading && (
          <div className="heatmap-canvas" style={{ minHeight: "520px" }}>

            <ResponsiveContainer width="100%" height="100%">
              <ScatterChart margin={{ top: 20, right: 20, bottom: 20, left: 20 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.05)" />
                <XAxis 
                  type="number" 
                  dataKey="x" 
                  name="騰落率" 
                  unit="%" 
                  domain={[minX, maxX]} 
                  stroke="rgba(255,255,255,0.2)"
                  tick={{ fill: "rgba(255,255,255,0.5)", fontSize: 12 }} 
                  tickFormatter={(val) => `${val.toFixed(1)}%`}
                />
                <YAxis 
                  type="number" 
                  dataKey="y" 
                  name="資金フロー" 
                  domain={[minY, maxY]} 
                  stroke="rgba(255,255,255,0.2)"
                  tickFormatter={(val) => {
                    const scaled = val / 100_000_000;
                    return scaled >= 1 || scaled <= -1 ? `${scaled.toFixed(0)}億` : `${val.toExponential(1)}`;
                  }}
                  tick={{ fill: "rgba(255,255,255,0.5)", fontSize: 12 }} 
                />
                <ZAxis type="number" dataKey="z" range={[200, 3500]} name="売買代金" />
                <Tooltip 
                  content={<CustomTooltip />} 
                  cursor={{ strokeDasharray: '3 3', stroke: 'rgba(255,255,255,0.2)' }}
                />
                <ReferenceLine x={0} stroke="rgba(255,255,255,0.4)" strokeWidth={1} />
                <ReferenceLine y={0} stroke="rgba(255,255,255,0.4)" strokeWidth={1} />
                <Scatter 
                  name="Sectors" 
                  data={chartData} 
                  onClick={handleSectorClick}
                  style={{ cursor: "pointer" }}
                  label={renderCustomLabel}
                >
                  {chartData.map((entry, index) => (
                    <Cell 
                      key={`cell-${index}`} 
                      fill={getBubbleColor(entry.momentum)} 
                      stroke={entry.momentum === "heating" ? "#f87171" : entry.momentum === "cooling" ? "#60a5fa" : "#9ca3af"}
                      strokeWidth={1}
                    />
                  ))}
                </Scatter>
              </ScatterChart>
            </ResponsiveContainer>
          </div>
        )}
      </div>
      <div className="sector-summary bubble-summary" style={{ justifyContent: "center", gap: "24px", paddingTop: "12px", borderTop: "1px solid var(--theme-border)", marginTop: "12px" }}>
        <div style={{ display: "flex", alignItems: "center", gap: "6px", fontSize: "12px", color: "var(--theme-text-secondary)" }}>
          <span style={{ display: "inline-block", width: "12px", height: "12px", borderRadius: "50%", background: "rgba(239, 68, 68, 0.75)" }} />
          ▲ 資金流入が加速（加熱）
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: "6px", fontSize: "12px", color: "var(--theme-text-secondary)" }}>
          <span style={{ display: "inline-block", width: "12px", height: "12px", borderRadius: "50%", background: "rgba(59, 130, 246, 0.75)" }} />
          ▼ 資金流出が加速（冷却/ピークアウト）
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: "6px", fontSize: "12px", color: "var(--theme-text-secondary)" }}>
          <span style={{ display: "inline-block", width: "12px", height: "12px", borderRadius: "50%", background: "rgba(107, 114, 128, 0.75)" }} />
          横ばい
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: "6px", fontSize: "12px", color: "var(--theme-text-secondary)", marginLeft: "16px" }}>
          ※円の大きさ = 売買代金（関心度）
        </div>
      </div>
    </div>
  );
}
