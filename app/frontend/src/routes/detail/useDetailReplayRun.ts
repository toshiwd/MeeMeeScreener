// @ts-nocheck
import { useEffect, useMemo, useState } from "react";
import { api } from "../../api";
import type { TradeEvent } from "../../utils/positions";

type ReplayLedgerRow = {
  date: string;
  symbol: string;
  state_from: string;
  state_to: string;
  position_notation_before: string;
  position_notation_after: string;
  action_taken: string;
  trigger_reason_code: string;
  trigger_reason_text: string;
  units_changed: number;
  close_price: number;
  avg_entry_price_after: number;
  realized_pnl_delta: number;
  unrealized_pnl_after: number;
  cash_after: number;
  equity_after: number;
  position_id: string;
  feature_state_id: string | null;
  holding_days_open: number;
};

type ReplayTimelineRow = {
  date: string;
  symbol: string;
  position_notation: string;
  long_units: number;
  short_units: number;
  avg_entry_price: number | null;
  close_price: number | null;
  market_value: number;
  unrealized_pnl: number;
  realized_pnl_cum: number;
  holding_days_open: number;
  cash_after: number;
  equity_after: number;
  action_taken: string;
  trigger_reason_code: string;
  trigger_reason_text: string;
  feature_state_id: string | null;
};

type ReplaySummary = {
  run_id?: string | null;
  policy_id?: string | null;
  policy_version?: string | null;
  window_start_date?: string | null;
  window_end_date?: string | null;
  portfolio_return_3m?: number | null;
  excess_vs_universe?: number | null;
  final_score?: number | null;
  weekly_activity_pass_rate?: number | null;
  weeks_with_no_trade?: number | null;
  weekly_trade_count_map?: Record<string, number> | null;
};

type ReplayRunPayload = {
  run_id?: string | null;
  window_summary?: ReplaySummary | null;
  trade_ledger?: ReplayLedgerRow[];
  positions_timeline?: ReplayTimelineRow[];
};

type Props = {
  runId: string | null;
  symbol: string | null;
  selectedDate: string | null;
};

type Result = {
  loading: boolean;
  error: string | null;
  summary: ReplaySummary | null;
  tradeEvents: TradeEvent[];
  ledgerRows: ReplayLedgerRow[];
  timelineRows: ReplayTimelineRow[];
  selectedTimelineRow: ReplayTimelineRow | null;
};

const parseNotation = (notation: string) => {
  const [shortText, longText] = String(notation || "0-0").split("-", 2);
  const shortUnits = Number(shortText);
  const longUnits = Number(longText);
  return {
    shortUnits: Number.isFinite(shortUnits) ? shortUnits : 0,
    longUnits: Number.isFinite(longUnits) ? longUnits : 0,
  };
};

const inferTradeSide = (beforeNotation: string, afterNotation: string, action: string) => {
  const before = parseNotation(beforeNotation);
  const after = parseNotation(afterNotation);
  const side =
    after.longUnits > before.longUnits || action === "enter_long"
      ? "buy"
      : after.shortUnits > before.shortUnits || action === "enter_short"
        ? "sell"
        : before.longUnits > after.longUnits
          ? "sell"
          : "buy";
  const isOpen = action === "enter_long" || action === "enter_short" || action === "add_on" || action === "forced_entry";
  return {
    side,
    action: isOpen ? "open" : "close",
  } as const;
};

const toTradeEvent = (row: ReplayLedgerRow, runId: string): TradeEvent => {
  const inferred = inferTradeSide(row.position_notation_before, row.position_notation_after, row.action_taken);
  return {
    date: row.date,
    code: row.symbol,
    name: row.symbol,
    side: inferred.side,
    action: inferred.action,
    units: Number(row.units_changed) || 0,
    price: Number(row.close_price) || 0,
    kind: row.action_taken,
    memo: row.trigger_reason_text,
    account: runId,
    broker: "replay",
    qtyShares: (Number(row.units_changed) || 0) * 100,
    realizedPnlNet: Number(row.realized_pnl_delta) || 0,
    raw: row as unknown as Record<string, unknown>,
  };
};

export function useDetailReplayRun({ runId, symbol, selectedDate }: Props): Result {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [summary, setSummary] = useState<ReplaySummary | null>(null);
  const [tradeEvents, setTradeEvents] = useState<TradeEvent[]>([]);
  const [ledgerRows, setLedgerRows] = useState<ReplayLedgerRow[]>([]);
  const [timelineRows, setTimelineRows] = useState<ReplayTimelineRow[]>([]);

  useEffect(() => {
    let active = true;
    if (!runId) {
      setLoading(false);
      setError(null);
      setSummary(null);
      setTradeEvents([]);
      setLedgerRows([]);
      setTimelineRows([]);
      return () => {
        active = false;
      };
    }

    setLoading(true);
    setError(null);
    void api
      .get(`/tradex/replay/runs/${encodeURIComponent(runId)}`)
      .then((response) => {
        if (!active) return;
        const payload = (response.data ?? null) as ReplayRunPayload | null;
        const ledger = Array.isArray(payload?.trade_ledger) ? payload.trade_ledger : [];
        const timeline = Array.isArray(payload?.positions_timeline) ? payload.positions_timeline : [];
        const filteredLedger = symbol ? ledger.filter((row) => row.symbol === symbol) : ledger;
        const filteredTimeline = symbol ? timeline.filter((row) => row.symbol === symbol) : timeline;
        setSummary((payload?.window_summary ?? null) as ReplaySummary | null);
        setLedgerRows(filteredLedger);
        setTimelineRows(filteredTimeline);
        setTradeEvents(filteredLedger.map((row) => toTradeEvent(row, runId)));
      })
      .catch((err) => {
        if (!active) return;
        setError(err instanceof Error ? err.message : "failed to load replay");
        setSummary(null);
        setLedgerRows([]);
        setTimelineRows([]);
        setTradeEvents([]);
      })
      .finally(() => {
        if (!active) return;
        setLoading(false);
      });

    return () => {
      active = false;
    };
  }, [runId, symbol]);

  const selectedTimelineRow = useMemo(() => {
    if (!timelineRows.length) return null;
    if (selectedDate) {
      const matched = timelineRows.find((row) => row.date === selectedDate);
      if (matched) return matched;
    }
    return timelineRows[timelineRows.length - 1] ?? null;
  }, [selectedDate, timelineRows]);

  return {
    loading,
    error,
    summary,
    tradeEvents,
    ledgerRows,
    timelineRows,
    selectedTimelineRow,
  };
}

