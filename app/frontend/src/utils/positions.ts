export type TradeEvent = {
  date: string;
  code: string;
  name: string;
  side: "buy" | "sell";
  action: "open" | "close";
  units: number;
  price?: number;
  kind?: string;
  broker?: string;
  tradeDate?: string;
  settleDate?: string | null;
  market?: string;
  account?: string;
  txnType?: string;
  qty?: number;
  qtyShares?: number;
  fee?: number | null;
  tax?: number | null;
  realizedPnlGross?: number | null;
  realizedPnlNet?: number | null;
  memo?: string;
  raw?: Record<string, unknown>;
};

export type PositionLedgerRow = {
  date: string;
  kindLabel: string;
  qtyShares: number;
  price: number | null;
  buyShares: number;
  sellShares: number;
  realizedPnL: number | null;
  totalPnL: number;
  brokerKey?: string;
  brokerLabel?: string;
  account?: string;
  brokerGroupKey?: string;
};

export type DailyPosition = {
  time: number;
  date: string;
  shortLots: number;
  longLots: number;
  posText: string;
  avgLongPrice: number;
  avgShortPrice: number;
  realizedPnL: number;
  unrealizedPnL: number;
  totalPnL: number;
  close: number;
  brokerKey?: string;
  brokerLabel?: string;
  account?: string;
  brokerGroupKey?: string;
};

export type TradeMarker = {
  time: number;
  date: string;
  buyLots: number;
  sellLots: number;
  trades: TradeEvent[];
  brokerKey?: string;
  brokerLabel?: string;
  brokerGroupKey?: string;
};

export type CurrentPosition = {
  brokerKey: string;
  brokerLabel: string;
  account?: string;
  longLots: number;
  shortLots: number;
  avgLongPrice: number;
  avgShortPrice: number;
  realizedPnL: number;
};

export type PositionSource = {
  getTrades: (code: string) => Promise<TradeEvent[]>;
};

type Candle = {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
};

const formatDate = (time: number) => {
  const date = new Date(time * 1000);
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
};

const formatLots = (value: number) => {
  if (Number.isNaN(value)) return "0";
  return Number.isInteger(value) ? `${value}` : value.toFixed(1);
};

const clampLots = (value: number) => Math.max(0, Number.isFinite(value) ? value : 0);

const resolveQtyShares = (trade: TradeEvent, lots: number) => {
  const qty = Number(trade.qtyShares);
  if (Number.isFinite(qty) && qty > 0) return qty;
  return lots * 100;
};

const resolveBrokerMeta = (trade: TradeEvent) => {
  const raw = (trade.broker ?? "").toString().trim();
  const lower = raw.toLowerCase();
  const account = trade.account ? String(trade.account).trim() : "";
  if (lower.includes("sbi")) {
    return { key: "sbi", label: "SBI", account };
  }
  if (lower.includes("rakuten")) {
    return { key: "rakuten", label: "RAKUTEN", account };
  }
  if (raw) {
    return { key: lower, label: raw.toUpperCase(), account };
  }
  return { key: "unknown", label: "N/A", account };
};

const resolveKindLabel = (trade: TradeEvent) => {
  if (trade.memo) return trade.memo;
  if (trade.raw && typeof trade.raw === "object") {
    const raw = trade.raw as { trade?: string; type?: string };
    if (raw.trade) return raw.trade;
    if (raw.type) return raw.type;
  }
  return trade.kind ?? "N/A";
};

const brokerOrder = (key: string) => {
  if (key === "rakuten") return 0;
  if (key === "sbi") return 1;
  if (key === "unknown") return 2;
  return 3;
};

const compareBrokerKey = (a?: string, b?: string) => {
  const keyA = a ?? "unknown";
  const keyB = b ?? "unknown";
  const rankA = brokerOrder(keyA);
  const rankB = brokerOrder(keyB);
  if (rankA !== rankB) return rankA - rankB;
  return keyA.localeCompare(keyB);
};

export const buildCurrentPositions = (trades: TradeEvent[]) => {
  const groups = new Map<string, { meta: { key: string; label: string; account: string }; items: TradeEvent[] }>();
  trades.forEach((trade) => {
    const meta = resolveBrokerMeta(trade);
    const groupKey = `${meta.key}|${meta.account}`;
    const existing = groups.get(groupKey);
    if (existing) {
      existing.items.push(trade);
    } else {
      groups.set(groupKey, { meta, items: [trade] });
    }
  });

  const results: CurrentPosition[] = [];
  groups.forEach((group) => {
    const ordered = group.items
      .map((trade, index) => ({ trade, index }))
      .sort((a, b) => {
        if (a.trade.date !== b.trade.date) return a.trade.date.localeCompare(b.trade.date);
        const aOrder = a.trade.action === "open" ? 0 : a.trade.action === "close" ? 1 : 2;
        const bOrder = b.trade.action === "open" ? 0 : b.trade.action === "close" ? 1 : 2;
        if (aOrder !== bOrder) return aOrder - bOrder;
        return a.index - b.index;
      });

    let longLots = 0;
    let shortLots = 0;
    let avgLongPrice = 0;
    let avgShortPrice = 0;
    let realizedPnL = 0;

    ordered.forEach(({ trade }) => {
      const lots = clampLots(trade.units);
      const price = Number(trade.price ?? 0);
      const action = trade.action ?? "open";
      const kind = trade.kind ?? "";

      if (kind === "DELIVERY") {
        longLots = Math.max(0, longLots - lots);
        shortLots = Math.max(0, shortLots - lots);
        return;
      }
      if (kind === "TAKE_DELIVERY") {
        return;
      }
      if (kind === "INBOUND") {
        if (longLots > 0 && lots > 0) {
          const totalCost = avgLongPrice * longLots;
          longLots += lots;
          avgLongPrice = totalCost / longLots;
        } else if (lots > 0) {
          longLots += lots;
          avgLongPrice = price || avgLongPrice;
        }
        return;
      }
      if (kind === "OUTBOUND") {
        longLots = Math.max(0, longLots - lots);
        if (longLots === 0) avgLongPrice = 0;
        return;
      }

      if (trade.side === "buy" && action === "open") {
        const nextLots = longLots + lots;
        avgLongPrice = nextLots > 0 ? (avgLongPrice * longLots + price * lots) / nextLots : 0;
        longLots = nextLots;
      } else if (trade.side === "sell" && action === "close") {
        const closeLots = Math.min(longLots, lots);
        if (Number.isFinite(trade.realizedPnlNet)) {
          realizedPnL += Number(trade.realizedPnlNet);
        } else {
          realizedPnL += (price - avgLongPrice) * closeLots * 100;
        }
        longLots = Math.max(0, longLots - lots);
        if (longLots === 0) avgLongPrice = 0;
      } else if (trade.side === "sell" && action === "open") {
        const nextLots = shortLots + lots;
        avgShortPrice =
          nextLots > 0 ? (avgShortPrice * shortLots + price * lots) / nextLots : 0;
        shortLots = nextLots;
      } else if (trade.side === "buy" && action === "close") {
        const closeLots = Math.min(shortLots, lots);
        if (Number.isFinite(trade.realizedPnlNet)) {
          realizedPnL += Number(trade.realizedPnlNet);
        } else {
          realizedPnL += (avgShortPrice - price) * closeLots * 100;
        }
        shortLots = Math.max(0, shortLots - lots);
        if (shortLots === 0) avgShortPrice = 0;
      }
    });

    results.push({
      brokerKey: group.meta.key,
      brokerLabel: group.meta.label,
      account: group.meta.account,
      longLots,
      shortLots,
      avgLongPrice,
      avgShortPrice,
      realizedPnL
    });
  });

  return results.sort((a, b) => compareBrokerKey(a.brokerKey, b.brokerKey));
};

const buildPositionsForGroup = (
  bars: Candle[],
  trades: TradeEvent[],
  brokerMeta: { key: string; label: string; account: string }
) => {
  const brokerGroupKey = `${brokerMeta.key}|${brokerMeta.account}`;
  const tradeMap = new Map<string, TradeEvent[]>();
  trades.forEach((trade) => {
    if (!trade.date) return;
    const list = tradeMap.get(trade.date) ?? [];
    list.push(trade);
    tradeMap.set(trade.date, list);
  });

  const dateSet = new Set(bars.map((bar) => formatDate(bar.time)));
  const skipped = new Set<string>();
  tradeMap.forEach((items, date) => {
    if (!dateSet.has(date)) {
      skipped.add(date);
    }
  });
  if (skipped.size) {
    console.warn("[trades] skipped dates not in bars", [...skipped]);
  }

  let longLots = 0;
  let shortLots = 0;
  let avgLongPrice = 0;
  let avgShortPrice = 0;
  let realizedPnL = 0;

  const dailyPositions: DailyPosition[] = [];
  const tradeMarkers: TradeMarker[] = [];

  bars.forEach((bar) => {
    const date = formatDate(bar.time);
    const dayTrades = tradeMap.get(date) ?? [];
    let buyLots = 0;
    let sellLots = 0;
    const markerTrades: TradeEvent[] = [];

    dayTrades.forEach((trade) => {
      const lots = clampLots(trade.units);
      const price = Number(trade.price ?? 0);
      const action =
        trade.action ??
        (trade.kind?.includes("open")
          ? "open"
          : trade.kind?.includes("close")
          ? "close"
          : "open");
      const kind = trade.kind ?? "";
      const isMarker =
        kind !== "DELIVERY" && kind !== "TAKE_DELIVERY" && kind !== "INBOUND" && kind !== "OUTBOUND";
      if (isMarker) {
        if (trade.side === "buy") {
          buyLots += lots;
        } else {
          sellLots += lots;
        }
        markerTrades.push(trade);
      }

      if (kind === "DELIVERY") {
        longLots = Math.max(0, longLots - lots);
        shortLots = Math.max(0, shortLots - lots);
        return;
      }
      if (kind === "TAKE_DELIVERY") {
        return;
      }
      if (kind === "INBOUND") {
        if (longLots > 0 && lots > 0) {
          const totalCost = avgLongPrice * longLots;
          longLots += lots;
          avgLongPrice = totalCost / longLots;
        }
        return;
      }
      if (kind === "OUTBOUND") {
        if (longLots > 0 && lots > 0) {
          const totalCost = avgLongPrice * longLots;
          longLots = Math.max(0, longLots - lots);
          avgLongPrice = longLots > 0 ? totalCost / longLots : 0;
        }
        return;
      }

      const isOpen = action === "open";
      if (trade.side === "buy" && isOpen) {
        const nextLots = longLots + lots;
        avgLongPrice =
          nextLots > 0 ? (avgLongPrice * longLots + price * lots) / nextLots : 0;
        longLots = nextLots;
      } else if (trade.side === "sell" && !isOpen) {
        const closeLots = Math.min(longLots, lots);
        if (Number.isFinite(trade.realizedPnlNet)) {
          realizedPnL += Number(trade.realizedPnlNet);
        } else {
          realizedPnL += (price - avgLongPrice) * closeLots * 100;
        }
        longLots = Math.max(0, longLots - lots);
        if (longLots === 0) avgLongPrice = 0;
      } else if (trade.side === "sell" && isOpen) {
        const nextLots = shortLots + lots;
        avgShortPrice =
          nextLots > 0 ? (avgShortPrice * shortLots + price * lots) / nextLots : 0;
        shortLots = nextLots;
      } else if (trade.side === "buy" && !isOpen) {
        const closeLots = Math.min(shortLots, lots);
        if (Number.isFinite(trade.realizedPnlNet)) {
          realizedPnL += Number(trade.realizedPnlNet);
        } else {
          realizedPnL += (avgShortPrice - price) * closeLots * 100;
        }
        shortLots = Math.max(0, shortLots - lots);
        if (shortLots === 0) avgShortPrice = 0;
      }
    });

    const unrealizedLong = longLots > 0 ? (bar.close - avgLongPrice) * longLots * 100 : 0;
    const unrealizedShort = shortLots > 0 ? (avgShortPrice - bar.close) * shortLots * 100 : 0;
    const unrealizedPnL = unrealizedLong + unrealizedShort;
    const totalPnL = realizedPnL + unrealizedPnL;
    const posText = `${formatLots(shortLots)}-${formatLots(longLots)}`;

    dailyPositions.push({
      time: bar.time,
      date,
      shortLots,
      longLots,
      posText,
      avgLongPrice,
      avgShortPrice,
      realizedPnL,
      unrealizedPnL,
      totalPnL,
      close: bar.close,
      brokerKey: brokerMeta.key,
      brokerLabel: brokerMeta.label,
      account: brokerMeta.account,
      brokerGroupKey
    });

    if (markerTrades.length) {
      tradeMarkers.push({
        time: bar.time,
        date,
        buyLots,
        sellLots,
        trades: markerTrades,
        brokerKey: brokerMeta.key,
        brokerLabel: brokerMeta.label,
        brokerGroupKey
      });
    }
  });

  return { dailyPositions, tradeMarkers };
};

export const buildDailyPositions = (bars: Candle[], trades: TradeEvent[]) => {
  const groups = new Map<string, { meta: { key: string; label: string; account: string }; items: TradeEvent[] }>();
  trades.forEach((trade) => {
    const meta = resolveBrokerMeta(trade);
    const groupKey = `${meta.key}|${meta.account}`;
    const existing = groups.get(groupKey);
    if (existing) {
      existing.items.push(trade);
    } else {
      groups.set(groupKey, { meta, items: [trade] });
    }
  });

  const dailyPositions: DailyPosition[] = [];
  const tradeMarkers: TradeMarker[] = [];

  if (groups.size === 0) {
    return buildPositionsForGroup(bars, trades, { key: "unknown", label: "N/A", account: "" });
  }

  groups.forEach((group) => {
    const result = buildPositionsForGroup(bars, group.items, group.meta);
    dailyPositions.push(...result.dailyPositions);
    tradeMarkers.push(...result.tradeMarkers);
  });

  dailyPositions.sort((a, b) => {
    if (a.time !== b.time) return a.time - b.time;
    return (a.brokerKey ?? "").localeCompare(b.brokerKey ?? "");
  });
  tradeMarkers.sort((a, b) => {
    if (a.time !== b.time) return a.time - b.time;
    return (a.brokerKey ?? "").localeCompare(b.brokerKey ?? "");
  });

  return { dailyPositions, tradeMarkers };
};

export const buildPositionLedger = (trades: TradeEvent[]) => {
  const rows: PositionLedgerRow[] = [];
  const groups = new Map<string, { meta: { key: string; label: string; account: string }; items: TradeEvent[] }>();
  trades.forEach((trade) => {
    const meta = resolveBrokerMeta(trade);
    const groupKey = `${meta.key}|${meta.account}`;
    const existing = groups.get(groupKey);
    if (existing) {
      existing.items.push(trade);
    } else {
      groups.set(groupKey, { meta, items: [trade] });
    }
  });

  const buildGroupRows = (groupTrades: TradeEvent[], meta: { key: string; label: string; account: string }) => {
    const ordered = groupTrades
      .map((trade, index) => ({ trade, index }))
      .sort((a, b) => {
        if (a.trade.date !== b.trade.date) return a.trade.date.localeCompare(b.trade.date);
        const aOrder = a.trade.action === "open" ? 0 : a.trade.action === "close" ? 1 : 2;
        const bOrder = b.trade.action === "open" ? 0 : b.trade.action === "close" ? 1 : 2;
        if (aOrder !== bOrder) return aOrder - bOrder;
        return a.index - b.index;
      });

    let longLots = 0;
    let shortLots = 0;
    let avgLongPrice = 0;
    let avgShortPrice = 0;
    let realizedPnL = 0;

    ordered.forEach(({ trade }) => {
      const lots = clampLots(trade.units);
      const price = Number(trade.price ?? 0);
      const action = trade.action ?? "open";
      const kind = trade.kind ?? "";
      let dayPnL: number | null = null;

      if (kind === "DELIVERY") {
        longLots = Math.max(0, longLots - lots);
        shortLots = Math.max(0, shortLots - lots);
      } else if (kind === "TAKE_DELIVERY") {
        // no-op
      } else if (kind === "INBOUND") {
        if (longLots > 0 && lots > 0) {
          const totalCost = avgLongPrice * longLots;
          longLots += lots;
          avgLongPrice = totalCost / longLots;
        }
      } else if (kind === "OUTBOUND") {
        if (longLots > 0 && lots > 0) {
          const totalCost = avgLongPrice * longLots;
          longLots = Math.max(0, longLots - lots);
          avgLongPrice = longLots > 0 ? totalCost / longLots : 0;
        }
      } else if (trade.side === "buy" && action === "open") {
        const nextLots = longLots + lots;
        avgLongPrice = nextLots > 0 ? (avgLongPrice * longLots + price * lots) / nextLots : 0;
        longLots = nextLots;
      } else if (trade.side === "sell" && action === "close") {
        const closeLots = Math.min(longLots, lots);
        if (Number.isFinite(trade.realizedPnlNet)) {
          dayPnL = Number(trade.realizedPnlNet);
        } else {
          dayPnL = (price - avgLongPrice) * closeLots * 100;
        }
        realizedPnL += dayPnL;
        longLots = Math.max(0, longLots - lots);
        if (longLots === 0) avgLongPrice = 0;
      } else if (trade.side === "sell" && action === "open") {
        const nextLots = shortLots + lots;
        avgShortPrice =
          nextLots > 0 ? (avgShortPrice * shortLots + price * lots) / nextLots : 0;
        shortLots = nextLots;
      } else if (trade.side === "buy" && action === "close") {
        const closeLots = Math.min(shortLots, lots);
        if (Number.isFinite(trade.realizedPnlNet)) {
          dayPnL = Number(trade.realizedPnlNet);
        } else {
          dayPnL = (avgShortPrice - price) * closeLots * 100;
        }
        realizedPnL += dayPnL;
        shortLots = Math.max(0, shortLots - lots);
        if (shortLots === 0) avgShortPrice = 0;
      }

      const qtyShares = resolveQtyShares(trade, lots);
      rows.push({
        date: trade.date,
        kindLabel: resolveKindLabel(trade),
        qtyShares,
        price: Number.isFinite(price) && price > 0 ? price : null,
        buyShares: Math.round(longLots * 100),
        sellShares: Math.round(shortLots * 100),
        realizedPnL: dayPnL,
        totalPnL: realizedPnL,
        brokerKey: meta.key,
        brokerLabel: meta.label,
        account: meta.account,
        brokerGroupKey: `${meta.key}|${meta.account}`
      });
    });
  };

  if (groups.size === 0) {
    buildGroupRows(trades, { key: "unknown", label: "N/A", account: "" });
  } else {
    groups.forEach((group) => buildGroupRows(group.items, group.meta));
  }

  return rows;
};
