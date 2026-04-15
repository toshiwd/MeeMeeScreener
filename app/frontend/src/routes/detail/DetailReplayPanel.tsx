// @ts-nocheck
import ScreenPanel from "../../components/ScreenPanel";
import { formatLedgerDateLabel } from "../../utils/dateLabels";

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

type ReplayLedgerRow = {
  date: string;
  symbol: string;
  position_notation_before: string;
  position_notation_after: string;
  action_taken: string;
  trigger_reason_code: string;
  trigger_reason_text: string;
  units_changed: number;
  close_price: number;
  realized_pnl_delta: number;
  holding_days_open: number;
};

type ReplayTimelineRow = {
  date: string;
  position_notation: string;
  unrealized_pnl: number;
  realized_pnl_cum: number;
  cash_after: number;
  equity_after: number;
  close_price: number | null;
  action_taken: string;
  trigger_reason_code: string;
  trigger_reason_text: string;
};

type Props = {
  runId: string | null;
  loading: boolean;
  error: string | null;
  summary: ReplaySummary | null;
  selectedDate: string | null;
  selectedRow: ReplayTimelineRow | null;
  ledgerRows: ReplayLedgerRow[];
  formatNumber: (value: number | null | undefined, digits?: number) => string;
  formatSignedNumber: (value: number | null | undefined, digits?: number) => string;
};

export default function DetailReplayPanel({
  runId,
  loading,
  error,
  summary,
  selectedDate,
  selectedRow,
  ledgerRows,
  formatNumber,
  formatSignedNumber,
}: Props) {
  return (
    <ScreenPanel
      title="Replay"
      summary={summary?.run_id || runId ? `run ${summary?.run_id ?? runId}` : undefined}
      details={summary?.window_start_date && summary?.window_end_date ? `${summary.window_start_date} -> ${summary.window_end_date}` : undefined}
      className="detail-analysis-panel detail-replay-panel"
    >
      <div className="detail-analysis-body detail-replay-body">
        {loading ? (
          <div className="detail-analysis-empty">replay loading...</div>
        ) : error ? (
          <div className="detail-analysis-empty">{error}</div>
        ) : !summary ? (
          <div className="detail-analysis-empty">replay data unavailable</div>
        ) : (
          <>
            <div className="detail-analysis-section-title">selected date</div>
            <div className="detail-replay-selected">
              <span>{selectedDate ? formatLedgerDateLabel(selectedDate) : "--"}</span>
              <span>{selectedRow?.position_notation ?? "0-0"}</span>
              <span>realized {formatSignedNumber(selectedRow?.realized_pnl_cum ?? 0, 0)}</span>
              <span>unrealized {formatSignedNumber(selectedRow?.unrealized_pnl ?? 0, 0)}</span>
            </div>
            <div className="detail-replay-metrics">
              <div>score {formatNumber(summary.final_score ?? 0, 4)}</div>
              <div>excess vs universe {formatSignedNumber(summary.excess_vs_universe ?? 0, 4)}</div>
              <div>portfolio return {formatSignedNumber(summary.portfolio_return_3m ?? 0, 4)}</div>
              <div>weekly activity {formatNumber(summary.weekly_activity_pass_rate ?? 0, 2)}</div>
              <div>weeks with no trade {formatNumber(summary.weeks_with_no_trade ?? 0, 0)}</div>
            </div>
            <div className="detail-analysis-section-title">ledger</div>
            <div className="detail-replay-ledger">
              {ledgerRows.length === 0 ? (
                <div className="detail-analysis-empty">no replay trades</div>
              ) : (
                ledgerRows.slice(-12).map((row) => (
                  <div key={`${row.date}-${row.symbol}-${row.action_taken}-${row.position_notation_after}`} className="detail-replay-ledger-row">
                    <span>{formatLedgerDateLabel(row.date)}</span>
                    <span>{row.symbol}</span>
                    <span>{row.position_notation_before} → {row.position_notation_after}</span>
                    <span>{row.action_taken}</span>
                    <span>{row.trigger_reason_code}</span>
                    <span>{formatSignedNumber(row.realized_pnl_delta, 0)}</span>
                  </div>
                ))
              )}
            </div>
          </>
        )}
      </div>
    </ScreenPanel>
  );
}
