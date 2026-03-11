export type Timeframe = "daily" | "weekly" | "monthly";

export type Candle = {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  isPartial?: boolean;
};

export type VolumePoint = {
  time: number;
  value: number;
};

export type DailyBar = Candle & {
  volume: number;
};

export type BarsResponse = {
  data?: number[][];
  errors?: string[];
};

export type PracticeTrade = {
  id: string;
  time: number;
  side: "buy" | "sell";
  action: "open" | "close";
  book: "long" | "short";
  quantity: number;
  price: number;
  lotSize?: number;
  kind?: "DAY_CONFIRM";
  note?: string;
};

export type PracticeLedgerEntry = {
  trade: PracticeTrade;
  kind: "TRADE" | "DAY_CONFIRM";
  longLots: number;
  shortLots: number;
  avgLongPrice: number;
  avgShortPrice: number;
  realizedPnL: number;
  realizedDelta: number;
  positionText: string;
};

export type OverlayTradeEvent = {
  date: string;
  code: string;
  name: string;
  side: "buy" | "sell";
  action: "open" | "close";
  units: number;
  price?: number;
  memo?: string;
};

export type OverlayTradeMarker = {
  time: number;
  date: string;
  buyLots: number;
  sellLots: number;
  trades: OverlayTradeEvent[];
};

export type OverlayPosition = {
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
};

export type PracticeSession = {
  session_id: string;
  code: string;
  start_date?: string | null;
  end_date?: string | null;
  cursor_time?: number | null;
  max_unlocked_time?: number | null;
  lot_size?: number | null;
  range_months?: number | null;
  trades?: PracticeTrade[];
  notes?: string | null;
  ui_state?: Record<string, unknown> | null;
  created_at?: string | null;
  updated_at?: string | null;
};

export type PracticeUiState = {
  panelCollapsed?: boolean;
  notesCollapsed?: boolean;
  tradeLogCollapsed?: boolean;
};
