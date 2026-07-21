import { useEffect, useState } from "react";
import { api } from "../api";

type Candidate = {
  code: string;
  intraday_rank?: number | null;
  name?: string | null;
  price?: number | null;
  signal_low?: number | null;
  volume_vs20?: number | null;
  state?: string;
};

type Preview = {
  status?: string;
  confirmed_ymd?: number | null;
  provisional_ymd?: number | null;
  intraday_available?: boolean;
  market_breadth_below_ma20?: number | null;
  market_gate_pass?: boolean;
  candidate_count?: number;
  candidates?: Candidate[];
};

const pct = (value?: number | null) =>
  Number.isFinite(value ?? NaN) ? `${(Number(value) * 100).toFixed(1)}%` : "--";

export default function TradexIntradayShortPreview({ enabled }: { enabled: boolean }) {
  const [preview, setPreview] = useState<Preview | null>(null);
  const [failed, setFailed] = useState(false);

  useEffect(() => {
    if (!enabled) return;
    let cancelled = false;
    const load = () => {
      void api
        .get<Preview>("/tradex/research/intraday-short-preview", { params: { limit: 20 }, timeout: 20000 })
        .then((response) => {
          if (!cancelled) {
            setPreview(response.data);
            setFailed(false);
          }
        })
        .catch(() => {
          if (!cancelled) setFailed(true);
        });
    };
    load();
    const timer = window.setInterval(load, 15_000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [enabled]);

  if (!enabled) return null;
  const candidates = preview?.candidates ?? [];
  return (
    <section className="rank-overview-panel tradex-short-lifecycle-strip" aria-label="TRADEX引け前売り候補">
      <div className="tradex-short-lifecycle-strip-head">
        <strong>引け前・売り候補</strong>
        <span>暫定表示 / 引け確定後、翌日の安値割れでのみエントリー</span>
      </div>
      <div className="tradex-short-lifecycle-strip-items">
        {failed ? (
          <span>場中候補を取得できません。</span>
        ) : !preview?.intraday_available ? (
          <span>新しい場中データなし（確定 {preview?.confirmed_ymd ?? "--"}）</span>
        ) : (
          <>
            <span className={`rank-score-badge rank-qualification ${preview.market_gate_pass ? "is-ok" : "is-neutral"}`}>
              市場弱気幅 {pct(preview.market_breadth_below_ma20)}
            </span>
            <span>暫定 {preview.provisional_ymd ?? "--"}</span>
            <strong>{preview.candidate_count ?? 0}件</strong>
            {candidates.length === 0 ? <span>条件一致なし</span> : null}
            {candidates.slice(0, 8).map((candidate) => (
              <div className="tradex-short-lifecycle-strip-item" key={candidate.code}>
                <span className="rank-score-badge rank-qualification is-warn">
                  暫定{candidate.intraday_rank ?? "--"}位
                </span>
                <strong>{candidate.code}</strong>
                <span>{candidate.name ?? ""}</span>
                <span>現在 {candidate.price ?? "--"}</span>
                <span>安値 {candidate.signal_low ?? "--"}</span>
                <span>出来高比 {Number(candidate.volume_vs20 ?? 0).toFixed(1)}x</span>
              </div>
            ))}
          </>
        )}
      </div>
    </section>
  );
}
