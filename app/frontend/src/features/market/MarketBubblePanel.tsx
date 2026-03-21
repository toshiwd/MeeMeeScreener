import { useMemo } from "react";
import {
  CartesianGrid,
  Cell,
  ReferenceLine,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
  ZAxis
} from "recharts";
import {
  formatMarketFlow,
  formatMarketRate,
  formatMarketValue,
  getMarketDirectionColor,
  type MarketMetricKey,
  type MarketSectorViewItem
} from "./marketHelpers";

type Props = {
  loading: boolean;
  error: string | null;
  items: MarketSectorViewItem[];
  metric: MarketMetricKey;
  selectedSector: string | null;
  onSectorSelect: (item: MarketSectorViewItem) => void;
  onSectorHover: (item: MarketSectorViewItem | null) => void;
};

const BubbleTooltip = ({ active, payload }: any) => {
  if (!active || !payload?.length) return null;
  const item = payload[0]?.payload as MarketSectorViewItem | undefined;
  if (!item) return null;
  return (
    <div className="market-tooltip">
      <div className="market-tooltip-title">{item.label}</div>
      <div className="market-tooltip-sub">{item.sector33_code}</div>
      <div className="market-tooltip-row">
        <span>騰落率</span>
        <span>{formatMarketRate(item.rate)}</span>
      </div>
      <div className="market-tooltip-row">
        <span>資金フロー</span>
        <span>{formatMarketFlow(item.flow)}</span>
      </div>
      <div className="market-tooltip-row">
        <span>売買代金</span>
        <span>{formatMarketValue(item.weight)}</span>
      </div>
      <div className="market-tooltip-row">
        <span>監視銘柄</span>
        <span>{`${item.watchlistCount}件`}</span>
      </div>
      <div className="market-tooltip-row">
        <span>代表銘柄</span>
        <span>
          {item.representatives.length
            ? item.representatives.map((entry) => `${entry.code} ${entry.name}`).join(" / ")
            : "--"}
        </span>
      </div>
    </div>
  );
};

export default function MarketBubblePanel({
  loading,
  error,
  items,
  metric,
  selectedSector,
  onSectorSelect,
  onSectorHover
}: Props) {
  const chartData = useMemo(() => {
    if (!items.length) return [];
    const rateAbs = Math.max(...items.map((item) => Math.abs(item.rate)), 1);
    const flowAbs = Math.max(...items.map((item) => Math.abs(item.flow)), 1);
    return items.map((item) => {
        const combined = metric === "flow"
          ? item.flow / flowAbs
          : metric === "both"
            ? ((item.rate / rateAbs) + (item.flow / flowAbs)) / 2
            : item.rate / rateAbs;
        return {
          ...item,
          x: item.rate,
          y: item.flow,
          z: Math.max(1, item.weight),
          bubbleColor: getMarketDirectionColor(combined, 1)
        };
      });
  }, [items, metric]);

  const bounds = useMemo(() => {
    if (!chartData.length) return { xAbs: 1, yAbs: 1, zMax: 1 };
    const xAbs = Math.max(...chartData.map((item) => Math.abs(item.x)), 1);
    const yAbs = Math.max(...chartData.map((item) => Math.abs(item.y)), 1);
    const zMax = Math.max(...chartData.map((item) => item.z), 1);
    return { xAbs, yAbs, zMax };
  }, [chartData]);

  if (loading && !items.length) {
    return (
      <div className="market-empty-state">
        <div className="heatmap-empty-card">
          <div className="heatmap-empty-title">データ取得中...</div>
          <div className="heatmap-empty-sub">市場データを読み込んでいます。</div>
        </div>
      </div>
    );
  }

  if (error && !items.length) {
    return (
      <div className="market-empty-state">
        <div className="heatmap-empty-card">
          <div className="heatmap-empty-title">回転図の取得に失敗しました</div>
          <div className="heatmap-empty-sub">{error}</div>
        </div>
      </div>
    );
  }

  if (!items.length) {
    return (
      <div className="market-empty-state">
        <div className="heatmap-empty-card">
          <div className="heatmap-empty-title">表示対象がありません</div>
          <div className="heatmap-empty-sub">scope を切り替えてください。</div>
        </div>
      </div>
    );
  }

  return (
    <div className="market-bubble-panel">
      <div className="market-chart-legend">
        <span className="market-legend-item"><i className="is-up" />上昇</span>
        <span className="market-legend-item"><i className="is-neutral" />中立</span>
        <span className="market-legend-item"><i className="is-down" />下落</span>
        <span className="market-legend-item"><i className="is-active" />{metric === "flow" ? "資金フロー" : metric === "both" ? "両方" : "騰落率"}</span>
      </div>
      <div className="market-chart-surface market-bubble-surface">
        <ResponsiveContainer width="100%" height="100%">
          <ScatterChart margin={{ top: 20, right: 20, bottom: 24, left: 20 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.08)" />
            <XAxis
              type="number"
              dataKey="x"
              name="騰落率"
              unit="%"
              domain={[-bounds.xAbs, bounds.xAbs]}
              stroke="rgba(255,255,255,0.3)"
              tick={{ fill: "rgba(255,255,255,0.65)", fontSize: 11 }}
              tickFormatter={(val) => `${Number(val).toFixed(1)}%`}
            />
            <YAxis
              type="number"
              dataKey="y"
              name="資金フロー"
              domain={[-bounds.yAbs, bounds.yAbs]}
              stroke="rgba(255,255,255,0.3)"
              tick={{ fill: "rgba(255,255,255,0.65)", fontSize: 11 }}
              tickFormatter={(val) => formatMarketFlow(Number(val))}
            />
            <ZAxis type="number" dataKey="z" range={[120, 3200]} name="売買代金" />
            <Tooltip content={<BubbleTooltip />} />
            <ReferenceLine x={0} stroke="rgba(255,255,255,0.45)" strokeWidth={1} />
            <ReferenceLine y={0} stroke="rgba(255,255,255,0.45)" strokeWidth={1} />
            <Scatter
              data={chartData}
              style={{ cursor: "pointer" }}
              onClick={(payload: any) => {
                const item = payload?.payload as MarketSectorViewItem | undefined;
                if (item) onSectorSelect(item);
              }}
              onMouseEnter={(payload: any) => {
                const item = payload?.payload as MarketSectorViewItem | undefined;
                onSectorHover(item ?? null);
              }}
              onMouseLeave={() => onSectorHover(null)}
              shape={(props: any) => {
                const item = props.payload as MarketSectorViewItem | undefined;
                const isSelected = item?.sector33_code && item.sector33_code === selectedSector;
                const fill = item?.bubbleColor ?? "var(--theme-text-muted)";
                return (
                  <circle
                    cx={props.cx}
                    cy={props.cy}
                    r={Math.max(8, Math.min(26, (props.size ?? 1) / 160))}
                    fill={fill}
                    fillOpacity={0.8}
                    stroke={isSelected ? "rgba(59, 130, 246, 0.95)" : "rgba(255,255,255,0.35)"}
                    strokeWidth={isSelected ? 2 : 1}
                  />
                );
              }}
            >
              {chartData.map((entry, index) => (
                <Cell
                  key={`bubble-${entry.sector33_code}-${index}`}
                  fill={entry.bubbleColor}
                  stroke={entry.sector33_code === selectedSector ? "rgba(59, 130, 246, 0.95)" : "rgba(255,255,255,0.35)"}
                />
              ))}
            </Scatter>
          </ScatterChart>
        </ResponsiveContainer>
      </div>
      <div className="market-axis-note">
        <span>横軸: 騰落率</span>
        <span>縦軸: 資金フロー</span>
        <span>大きさ: 売買代金</span>
      </div>
    </div>
  );
}
