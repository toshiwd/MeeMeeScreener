import { useCallback, useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { ResponsiveContainer, Treemap, Tooltip } from "recharts";
import { api } from "../../api";

type PeriodKey = "1d" | "1w" | "1m";

type SectorItem = {
  name: string;
  size: number;
  color: number;
  count?: number;
  sector33_code?: string;
};

const PERIOD_OPTIONS: { key: PeriodKey; label: string }[] = [
  { key: "1d", label: "1日" },
  { key: "1w", label: "1週" },
  { key: "1m", label: "1ヶ月" }
];

const getColorScale = (value: number) => {
  if (value >= 2) return "#d32f2f";
  if (value >= 0.5) return "#f44336";
  if (value > -0.5 && value < 0.5) return "#424242";
  if (value > -2) return "#4caf50";
  return "#388e3c";
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

const RenderSectorTile = (props: any) => {
  const { x, y, width, height, payload } = props;
  const data = resolveTileData(payload);
  const name = data?.name ?? "";
  const rate = Number(data?.color ?? 0);
  const bgColor = getColorScale(rate);
  const textColor = getTextColor(bgColor);
  const minWidth = 56;
  const minHeight = 34;
  const showText = width >= minWidth && height >= minHeight;
  const fontSize = width >= 140 && height >= 80 ? 14 : width >= 90 ? 12 : 10;
  const padding = 4;

  return (
    <g>
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
            <div style={{ maxWidth: "100%", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
              {name}
            </div>
            <div style={{ fontSize: Math.max(10, fontSize - 1), opacity: 0.9 }}>{formatRate(rate)}</div>
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

  useEffect(() => {
    let canceled = false;
    setLoading(true);
    api
      .get("/market/heatmap", { params: { period } })
      .then((res) => {
        if (canceled) return;
        const data = res.data?.items;
        setItems(Array.isArray(data) ? data : []);
      })
      .catch(() => {
        if (canceled) return;
        setItems([]);
      })
      .finally(() => {
        if (canceled) return;
        setLoading(false);
      });
    return () => {
      canceled = true;
    };
  }, [period]);

  const handleSelectSector = useCallback(
    (item: SectorItem) => {
      if (item.sector33_code) {
        navigate(`/watchlist?sector=${encodeURIComponent(item.sector33_code)}`);
      } else {
        console.log("sector selected", item);
      }
    },
    [navigate]
  );

  const treemapData = useMemo(() => {
    return items.map((item) => ({
      ...item,
      size: Number(item.size ?? 0),
      color: Number(item.color ?? 0)
    }));
  }, [items]);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
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
      <div style={{ width: "100%", height: 520 }}>
        <ResponsiveContainer>
          <Treemap
            data={treemapData}
            dataKey="size"
            stroke="#fff"
            animationDuration={240}
            content={<RenderSectorTile />}
            onClick={(data) => handleSelectSector(data?.payload)}
          >
            <Tooltip content={<CustomTooltip />} />
          </Treemap>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
