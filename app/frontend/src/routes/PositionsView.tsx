import { useEffect, useMemo, useState, useCallback, type CSSProperties } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { useBackendReadyState } from "../backendReady";
import { api } from "../api";
import UnifiedListHeader from "../components/UnifiedListHeader";
import ChartListCard from "../components/ChartListCard";
import { useStore } from "../store";

type HeldItem = {
  symbol: string;
  name: string;
  sell_buy_text: string;
  opened_at: string | null;
  has_issue: boolean;
  issue_note?: string | null;
  buy_qty: number;
  sell_qty: number;
};

type HistoryItem = {
  round_id: string;
  symbol: string;
  name: string;
  opened_at: string | null;
  closed_at: string | null;
  round_no: number;
  has_issue: boolean;
  issue_note?: string | null;
};

type CurrentPositionsResponse = {
  holding_codes?: string[];
  all_traded_codes?: string[];
  current_positions_by_code?: Record<
    string,
    {
      buyShares?: number;
      sellShares?: number;
      opened_at?: string | null;
      has_issue?: boolean;
      issue_note?: string | null;
    }
  >;
};

type RoundEvent = {
  broker: string;
  exec_dt: string | null;
  action: string;
  qty: number;
  price: number | null;
};

const formatDate = (value: string | null | undefined) => {
  if (!value) return "--";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "--";
  const yyyy = date.getFullYear();
  const mm = String(date.getMonth() + 1).padStart(2, "0");
  const dd = String(date.getDate()).padStart(2, "0");
  return `${yyyy}/${mm}/${dd}`;
};

export default function PositionsView() {
  const { ready: backendReady } = useBackendReadyState();
  const navigate = useNavigate();
  const location = useLocation();

  // Store access
  const ensureBarsForVisible = useStore((state) => state.ensureBarsForVisible);
  const barsCache = useStore((state) => state.barsCache);
  const barsStatus = useStore((state) => state.barsStatus);
  const maSettings = useStore((state) => state.maSettings);
  const tickers = useStore((state) => state.tickers);
  const loadList = useStore((state) => state.loadList);

  // Settings
  const listTimeframe = useStore((state) => state.settings.listTimeframe);
  const listRangeMonths = useStore((state) => state.settings.listRangeMonths);
  const listColumns = useStore((state) => state.settings.listColumns);
  const listRows = useStore((state) => state.settings.listRows);
  const setListTimeframe = useStore((state) => state.setListTimeframe);
  const setListRangeMonths = useStore((state) => state.setListRangeMonths);
  const setListColumns = useStore((state) => state.setListColumns);
  const setListRows = useStore((state) => state.setListRows);

  const [tab, setTab] = useState<"held" | "history">("held");
  const [heldItems, setHeldItems] = useState<HeldItem[]>([]);
  const [historyItems, setHistoryItems] = useState<HistoryItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [selectedRound, setSelectedRound] = useState<HistoryItem | null>(null);
  const [roundEvents, setRoundEvents] = useState<RoundEvent[]>([]);
  const [eventsLoading, setEventsLoading] = useState(false);

  useEffect(() => {
    if (!backendReady) return;
    if (tickers.length) return;
    loadList().catch(() => {});
  }, [backendReady, loadList, tickers.length]);

  const tickerMap = useMemo(() => {
    return new Map(tickers.map((ticker) => [ticker.code, ticker]));
  }, [tickers]);

  // Load positions
  useEffect(() => {
    if (!backendReady) return;
    setLoading(true);
    const load = async () => {
      try {
        const currentRes = await api.get("/positions/current");
        const currentPayload = (currentRes.data || {}) as CurrentPositionsResponse;
        const holdingCodes = Array.isArray(currentPayload.holding_codes)
          ? currentPayload.holding_codes
          : [];
        const allTradedCodes = Array.isArray(currentPayload.all_traded_codes)
          ? currentPayload.all_traded_codes
          : [];
        const positionsByCode = currentPayload.current_positions_by_code ?? {};

        if (tab === "held") {
          const items: HeldItem[] = holdingCodes.map((code) => {
            const position = positionsByCode[code] ?? {};
            const buy = Number(position.buyShares ?? 0);
            const sell = Number(position.sellShares ?? 0);
            const buyLabel = Number.isInteger(buy) ? `${buy}` : `${buy}`;
            const sellLabel = Number.isInteger(sell) ? `${sell}` : `${sell}`;
            const ticker = tickerMap.get(code);
            return {
              symbol: code,
              name: ticker?.name ?? code,
              buy_qty: buy,
              sell_qty: sell,
              sell_buy_text: `${sellLabel}-${buyLabel}`,
              opened_at: position.opened_at ?? null,
              has_issue: Boolean(position.has_issue),
              issue_note: position.issue_note ?? null
            };
          });
          setHeldItems(items);
          setHistoryItems([]);
        } else {
          const holdingSet = new Set(holdingCodes);
          const historyCodes = allTradedCodes.filter((code) => !holdingSet.has(code));
          const historyCodeSet = new Set(historyCodes);
          const res = await api.get("/positions/history");
          const rawItems = (res.data?.items || []) as HistoryItem[];
          const filtered = rawItems
            .filter((item) => historyCodeSet.has(item.symbol))
            .map((item, index) => ({ ...item, round_no: index + 1 }));
          setHistoryItems(filtered);
          setHeldItems([]);
        }
      } catch (e) {
        console.error(e);
        if (tab === "held") setHeldItems([]);
        else setHistoryItems([]);
      } finally {
        setLoading(false);
      }
    };
    load();
  }, [backendReady, tab, tickerMap]);

  // Determine active items
  const activeItems = useMemo(() => {
    return tab === "held" ? heldItems : historyItems;
  }, [tab, heldItems, historyItems]);

  // Load detail for selected round
  useEffect(() => {
    if (!selectedRound) return;
    setEventsLoading(true);
    api
      .get("/positions/history/events", { params: { round_id: selectedRound.round_id } })
      .then((res) => {
        setRoundEvents((res.data?.events || []) as RoundEvent[]);
      })
      .catch(() => setRoundEvents([]))
      .finally(() => setEventsLoading(false));
  }, [selectedRound]);

  // Ensure bars are loaded for visible items
  useEffect(() => {
    if (!backendReady || activeItems.length === 0) return;
    const codes = activeItems.map((item) => item.symbol);
    // Unique list
    const uniqueCodes = [...new Set(codes)];
    ensureBarsForVisible(listTimeframe, uniqueCodes, "positions");
  }, [backendReady, activeItems, ensureBarsForVisible, listTimeframe]);

  const handleImport = async (e: React.ChangeEvent<HTMLInputElement>) => {
    if (!e.target.files?.length) return;
    const file = e.target.files[0];
    const formData = new FormData();
    formData.append("file", file);
    formData.append("broker", "rakuten");

    try {
      setLoading(true);
      await api.post("/imports/trade-history", formData);
      alert("インポートが完了しました");

      // Reload
      if (tab === "held") {
        const res = await api.get("/positions/held");
        setHeldItems((res.data?.items || []) as HeldItem[]);
      } else {
        const res = await api.get("/positions/history");
        setHistoryItems((res.data?.items || []) as HistoryItem[]);
      }
    } catch (err: any) {
      console.error(err);
      const msg = err.response?.data?.error || err.message || "Unknown error";
      const warnings = err.response?.data?.warnings || [];
      const warnMsg = warnings.length ? "\n" + warnings.join("\n") : "";
      alert(`インポートに失敗しました: ${msg}${warnMsg}`);
    } finally {
      e.target.value = "";
    }
  };

  const handleOpenDetail = useCallback(
    (code: string) => {
      navigate(`/detail/${code}`, { state: { from: location.pathname } });
    },
    [navigate, location.pathname]
  );

  const densityKey = `${listColumns}x${listRows}`;
  const listStyles = useMemo(
    () =>
    ({
      "--list-cols": listColumns,
      "--list-rows": listRows
    } as CSSProperties),
    [listColumns, listRows]
  );

  const isSingleDensity = listColumns === 1 && listRows === 1;
  const emptyLabel = tab === "held" ? "保有銘柄はありません" : "履歴はまだありません";

  const renderItem = (item: HeldItem | HistoryItem) => {
    if ("buy_qty" in item && !(item.buy_qty > 0 || item.sell_qty > 0)) {
      return null;
    }
    const code = item.symbol;
    const payload = barsCache[listTimeframe][code] ?? null;
    const status = barsStatus[listTimeframe][code];
    const signals = [] as any[];
    const ticker = tickerMap.get(code);

    let displayName = item.name;
    let extraInfo = "";

    if ("sell_buy_text" in item) {
      extraInfo = ` ${item.sell_buy_text}`;
      if (item.has_issue) extraInfo += " ⚠️";
    } else {
      extraInfo = ` Round${item.round_no}`;
      if (item.has_issue) extraInfo += " ⚠️";
    }

    const uniqueKey = "round_id" in item ? item.round_id : code;

    return (
      <ChartListCard
        key={uniqueKey}
        code={code}
        name={`${displayName}${extraInfo}`}
        payload={payload}
        status={status}
        maSettings={maSettings[listTimeframe]}
        rangeMonths={listRangeMonths}
        eventEarningsDate={ticker?.eventEarningsDate ?? null}
        eventRightsDate={ticker?.eventRightsDate ?? null}
        densityKey={densityKey}
        signals={signals}
        onOpenDetail={handleOpenDetail}
        action={"round_id" in item ? {
          label: "履歴",
          ariaLabel: "取引履歴",
          className: "favorite-toggle",
          onClick: (e) => {
            e.stopPropagation();
            setSelectedRound(item as HistoryItem);
          }
        } : undefined}
      />
    );
  };

  return (
    <div className="app-shell list-view">
      <UnifiedListHeader
        timeframe={listTimeframe}
        onTimeframeChange={setListTimeframe}
        rangeMonths={listRangeMonths}
        onRangeChange={setListRangeMonths}
        search=""
        onSearchChange={() => { }}
        sortValue=""
        sortOptions={[]}
        onSortChange={() => { }}
        columns={listColumns}
        rows={listRows}
        onColumnsChange={setListColumns}
        onRowsChange={setListRows}
        filterItems={[]}
      />

      <div style={{
        padding: "8px 16px",
        display: "flex",
        gap: "12px",
        alignItems: "center",
        borderBottom: "1px solid var(--theme-border)",
        background: "var(--theme-bg-secondary)"
      }}>
        <div className="positions-tabs" style={{ display: "flex", gap: "8px" }}>
          <button
            type="button"
            className={tab === "held" ? "active" : ""}
            onClick={() => { setSelectedRound(null); setTab("held"); }}
            style={{
              padding: "6px 12px",
              borderRadius: "999px",
              border: "none",
              background: tab === "held" ? "var(--theme-accent)" : "transparent",
              color: tab === "held" ? "#fff" : "var(--theme-text-secondary)",
              cursor: "pointer",
              fontWeight: 600
            }}
          >
            保有
          </button>
          <button
            type="button"
            className={tab === "history" ? "active" : ""}
            onClick={() => setTab("history")}
            style={{
              padding: "6px 12px",
              borderRadius: "999px",
              border: "none",
              background: tab === "history" ? "var(--theme-accent)" : "transparent",
              color: tab === "history" ? "#fff" : "var(--theme-text-secondary)",
              cursor: "pointer",
              fontWeight: 600
            }}
          >
            履歴
          </button>
        </div>

        <div style={{ flex: 1 }}></div>

        <label className="positions-import-btn" style={{
          display: "inline-flex",
          alignItems: "center",
          gap: "6px",
          padding: "6px 12px",
          background: "var(--theme-bg-tertiary)",
          border: "1px solid var(--theme-border)",
          borderRadius: "6px",
          cursor: "pointer",
          fontSize: "13px"
        }}>
          📂 インポート
          <input type="file" accept=".csv" onChange={handleImport} hidden />
        </label>
      </div>

      <div
        className={`rank-shell list-shell${isSingleDensity ? " is-single" : ""}`}
        style={listStyles}
      >
        {loading && <div className="rank-status">読み込み中...</div>}
        {!loading && activeItems.length === 0 && (
          <div className="positions-empty" style={{
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            justifyContent: "center",
            height: "100%",
            gap: "16px",
            color: "var(--theme-text-muted)"
          }}>
            <div>{emptyLabel}</div>
            <button
              type="button"
              onClick={async () => {
                try {
                  const res = await api.get("/debug/trade-sync");
                  alert(JSON.stringify(res.data, null, 2));
                } catch (e) {
                  alert("Debug fetch failed");
                }
              }}
              style={{
                fontSize: "0.8rem",
                opacity: 0.7,
                background: "transparent",
                border: "1px solid currentColor",
                borderRadius: 4,
                padding: "4px 8px",
                color: "inherit",
                cursor: "pointer"
              }}
            >
              同期ステータス詳細
            </button>
          </div>
        )}

        <div className="rank-grid">
          {activeItems.map((item) => renderItem(item))}
        </div>
      </div>

      {selectedRound && (
        <div className="positions-detail" style={{
          position: "fixed",
          inset: 0,
          background: "rgba(0,0,0,0.5)",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          zIndex: 1000
        }}>
          <div style={{
            background: "var(--theme-bg-secondary)",
            width: "min(600px, 90vw)",
            borderRadius: "12px",
            overflow: "hidden",
            boxShadow: "0 20px 25px -5px rgba(0, 0, 0, 0.1), 0 10px 10px -5px rgba(0, 0, 0, 0.04)",
            color: "var(--theme-text-primary)"
          }}>
            <div style={{
              padding: "16px",
              borderBottom: "1px solid var(--theme-border)",
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center"
            }}>
              <div style={{ fontWeight: 600, fontSize: "16px" }}>
                {selectedRound.symbol} Round {selectedRound.round_no}
              </div>
              <button onClick={() => setSelectedRound(null)} style={{
                background: "transparent", border: "none", cursor: "pointer", fontSize: "20px", color: "var(--theme-text-secondary)"
              }}>×</button>
            </div>

            <div style={{ padding: "16px", maxHeight: "60vh", overflowY: "auto" }}>
              {eventsLoading ? (
                <div>読み込み中...</div>
              ) : roundEvents.length ? (
                <div style={{ display: "flex", flexDirection: "column", gap: "8px" }}>
                  {roundEvents.map((event, index) => (
                    <div key={`${event.exec_dt}-${index}`} style={{
                      display: "grid",
                      gridTemplateColumns: "100px 80px 80px 1fr",
                      gap: "12px",
                      padding: "8px",
                      borderBottom: "1px solid var(--theme-border-subtle)",
                      fontSize: "13px"
                    }}>
                      <span>{formatDate(event.exec_dt)}</span>
                      <span style={{ fontWeight: 600 }}>{event.action}</span>
                      <span>{event.qty}</span>
                      <span style={{ textAlign: "right" }}>
                        {event.price != null ? event.price.toLocaleString() : "--"}
                      </span>
                    </div>
                  ))}
                </div>
              ) : (
                <div>イベントがありません</div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
