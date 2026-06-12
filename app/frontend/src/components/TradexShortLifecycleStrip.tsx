import { useEffect, useMemo, useState } from "react";
import { api } from "../api";

type LifecycleCandidate = {
  code: string;
  name?: string | null;
  signal_ymd?: string | number | null;
  lifecycle_rank?: number | null;
  lifecycle_state?: string | null;
  expected_downside_pct?: number | null;
  risk_reward_to_sl8?: number | null;
};

type LifecycleBoard = {
  available?: boolean;
  created_at?: string | null;
  candidates?: LifecycleCandidate[];
};

const stateLabel: Record<string, string> = {
  Probe: "打診",
  Watch: "監視",
  AddWatch: "追加監視",
  HoldReview: "保有確認",
  TakeProfit: "利確",
  Exit: "撤退",
};

const stateClass = (state: string) => {
  if (state === "Probe" || state === "AddWatch" || state === "TakeProfit") return "is-ok";
  if (state === "Exit") return "is-warn";
  return "is-neutral";
};

const formatPct = (value?: number | null) =>
  Number.isFinite(value ?? NaN) ? `${(Number(value) * 100).toFixed(1)}%` : "--";

export default function TradexShortLifecycleStrip({ enabled }: { enabled: boolean }) {
  const [board, setBoard] = useState<LifecycleBoard | null>(null);

  useEffect(() => {
    if (!enabled) {
      setBoard(null);
      return;
    }
    let cancelled = false;
    void api
      .get<LifecycleBoard>("/tradex/research/short-lifecycle-board", { params: { limit: 30 }, timeout: 15000 })
      .then((response) => {
        if (!cancelled) setBoard(response.data);
      })
      .catch(() => {
        if (!cancelled) setBoard(null);
      });
    return () => {
      cancelled = true;
    };
  }, [enabled]);

  const candidates = useMemo(
    () =>
      (board?.candidates ?? [])
        .filter((candidate) => candidate.lifecycle_state && candidate.lifecycle_state !== "Avoid")
        .slice(0, 8),
    [board]
  );

  if (!enabled || !board?.available || candidates.length === 0) return null;

  return (
    <section className="rank-overview-panel tradex-short-lifecycle-strip" aria-label="TRADEX売り監視">
      <div className="tradex-short-lifecycle-strip-head">
        <strong>TRADEX売り監視</strong>
        <span>review-only / ランキング順・スコアには未反映</span>
      </div>
      <div className="tradex-short-lifecycle-strip-items">
        {candidates.map((candidate, index) => {
          const state = candidate.lifecycle_state ?? "Unknown";
          return (
            <div className="tradex-short-lifecycle-strip-item" key={`${candidate.code}-${candidate.signal_ymd ?? index}`}>
              <span className={`rank-score-badge rank-qualification ${stateClass(state)}`}>
                {stateLabel[state] ?? state}
              </span>
              <strong>{candidate.code}</strong>
              <span>期待下げ {formatPct(candidate.expected_downside_pct)}</span>
              <span>
                RR {Number.isFinite(candidate.risk_reward_to_sl8 ?? NaN) ? Number(candidate.risk_reward_to_sl8).toFixed(2) : "--"}
              </span>
              <span>signal {candidate.signal_ymd ?? "--"}</span>
            </div>
          );
        })}
      </div>
    </section>
  );
}
