import { useEffect, useMemo, useState } from "react";
import { useBackendReadyState } from "../backendReady";
import { api } from "../api";
import TopNav from "../components/TopNav";

type HeldItem = {
  symbol: string;
  name: string;
  sell_buy_text: string;
  opened_at: string | null;
  has_issue: boolean;
  issue_note?: string | null;
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
  const [tab, setTab] = useState<"held" | "history">("held");
  const [heldItems, setHeldItems] = useState<HeldItem[]>([]);
  const [historyItems, setHistoryItems] = useState<HistoryItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [selectedRound, setSelectedRound] = useState<HistoryItem | null>(null);
  const [roundEvents, setRoundEvents] = useState<RoundEvent[]>([]);
  const [eventsLoading, setEventsLoading] = useState(false);

  useEffect(() => {
    if (!backendReady) return;
    setLoading(true);
    const load = async () => {
      if (tab === "held") {
        const res = await api.get("/positions/held");
        setHeldItems((res.data?.items || []) as HeldItem[]);
      } else {
        const res = await api.get("/positions/history");
        setHistoryItems((res.data?.items || []) as HistoryItem[]);
      }
    };
    load()
      .catch(() => {
        if (tab === "held") setHeldItems([]);
        else setHistoryItems([]);
      })
      .finally(() => setLoading(false));
  }, [backendReady, tab]);

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

  const emptyLabel = tab === "held" ? "保有銘柄はありません" : "履歴はまだありません";

  const roundTitle = useMemo(() => {
    if (!selectedRound) return "";
    return `${selectedRound.symbol} Round ${selectedRound.round_no}`;
  }, [selectedRound]);

  return (
    <div className="positions-shell">
      <TopNav />
      <div className="positions-header">
        <div className="positions-title">保有 / 履歴</div>
        <div className="positions-tabs">
          <button
            type="button"
            className={tab === "held" ? "active" : ""}
            onClick={() => {
              setSelectedRound(null);
              setTab("held");
            }}
          >
            保有
          </button>
          <button
            type="button"
            className={tab === "history" ? "active" : ""}
            onClick={() => setTab("history")}
          >
            履歴
          </button>
        </div>
      </div>

      {loading ? (
        <div className="positions-empty">読み込み中...</div>
      ) : tab === "held" ? (
        heldItems.length ? (
          <div className="positions-list">
            {heldItems.map((item) => (
              <div className="positions-row" key={item.symbol}>
                <div className="positions-main">
                  <div className="positions-code">{item.symbol}</div>
                  <div className="positions-name">{item.name}</div>
                </div>
                <div className="positions-meta">
                  <span className="positions-qty">{item.sell_buy_text}</span>
                  <span className="positions-date">{formatDate(item.opened_at)}</span>
                  {item.has_issue && (
                    <span className="positions-issue" title={item.issue_note ?? undefined}>
                      要確認
                    </span>
                  )}
                </div>
              </div>
            ))}
          </div>
        ) : (
          <div className="positions-empty">{emptyLabel}</div>
        )
      ) : historyItems.length ? (
        <div className="positions-list">
          {historyItems.map((item) => (
            <button
              type="button"
              className="positions-row positions-row-action"
              key={item.round_id}
              onClick={() => setSelectedRound(item)}
            >
              <div className="positions-main">
                <div className="positions-code">{item.symbol}</div>
                <div className="positions-name">{item.name}</div>
              </div>
              <div className="positions-meta">
                <span className="positions-range">
                  {formatDate(item.opened_at)}〜{formatDate(item.closed_at)}
                </span>
                <span className="positions-round">Round {item.round_no}</span>
                {item.has_issue && (
                  <span className="positions-issue" title={item.issue_note ?? undefined}>
                    要確認
                  </span>
                )}
              </div>
            </button>
          ))}
        </div>
      ) : (
        <div className="positions-empty">{emptyLabel}</div>
      )}

      {selectedRound && (
        <div className="positions-detail">
          <div className="positions-detail-header">
            <div className="positions-detail-title">{roundTitle}</div>
            <button type="button" onClick={() => setSelectedRound(null)}>
              閉じる
            </button>
          </div>
          {eventsLoading ? (
            <div className="positions-detail-body">読み込み中...</div>
          ) : roundEvents.length ? (
            <div className="positions-detail-body">
              {roundEvents.map((event, index) => (
                <div className="positions-event" key={`${event.exec_dt}-${index}`}>
                  <span className="positions-event-time">{formatDate(event.exec_dt)}</span>
                  <span className="positions-event-action">{event.action}</span>
                  <span className="positions-event-qty">{event.qty}</span>
                  <span className="positions-event-price">
                    {event.price != null ? event.price.toLocaleString() : "--"}
                  </span>
                </div>
              ))}
            </div>
          ) : (
            <div className="positions-detail-body">イベントがありません</div>
          )}
        </div>
      )}
    </div>
  );
}
