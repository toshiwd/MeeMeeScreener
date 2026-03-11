import type { ApiErrorInfo } from "../apiErrors";
import { formatApiErrorText } from "../apiErrors";

type StartupOverlayProps = {
  visible: boolean;
  subtitle: string;
  error: string | null;
  errorDetails?: string | null;
  lastRequest?: ApiErrorInfo | null;
  attemptCount: number;
  elapsedMs: number;
  onRetry: () => void;
};

export default function StartupOverlay({
  visible,
  subtitle,
  error,
  errorDetails,
  lastRequest,
  attemptCount,
  elapsedMs,
  onRetry
}: StartupOverlayProps) {
  const isError = Boolean(error);
  const elapsedSeconds = Math.max(0, Math.floor(elapsedMs / 1000));
  const progress = Math.min(0.9, elapsedMs / 60000);
  const progressPct = `${Math.round(progress * 100)}%`;

  const handleCopy = () => {
    const detailText = lastRequest ? formatApiErrorText(lastRequest) : errorDetails ?? error ?? "";
    if (!detailText) return;
    if (navigator.clipboard?.writeText) {
      navigator.clipboard.writeText(detailText).catch(() => undefined);
      return;
    }
    const textarea = document.createElement("textarea");
    textarea.value = detailText;
    textarea.style.position = "fixed";
    textarea.style.opacity = "0";
    document.body.appendChild(textarea);
    textarea.select();
    try {
      document.execCommand("copy");
    } catch {
      // noop
    }
    document.body.removeChild(textarea);
  };

  return (
    <div className={`startup-overlay ${visible ? "is-visible" : "is-hidden"}`}>
      {/* Animated background particles */}
      <div className="startup-bg-particles" aria-hidden="true">
        <div className="startup-particle startup-particle-1" />
        <div className="startup-particle startup-particle-2" />
        <div className="startup-particle startup-particle-3" />
      </div>

      <div className="startup-card" role="status" aria-live="polite">
        {/* Logo / brand area */}
        <div className="startup-brand">
          <div className="startup-logo-ring" aria-hidden="true">
            <div className="startup-logo-orbit" />
            <div className="startup-logo-core">
              <span className="startup-logo-text">M</span>
            </div>
          </div>
          <div className="startup-brand-name">MeeMee<span className="startup-brand-accent"> Screener</span></div>
        </div>

        {/* Status area */}
        <div className="startup-status">
          <div className="startup-status-text">
            {isError ? "起動エラー" : subtitle}
          </div>
          {!isError && (
            <>
              <div className="startup-progress-track">
                <div className="startup-progress-fill" style={{ width: progressPct }} />
                <div className="startup-progress-glow" style={{ left: progressPct }} />
              </div>
              <div className="startup-meta">
                試行 {attemptCount} 回 · 経過 {elapsedSeconds}s
              </div>
            </>
          )}
        </div>

        {/* Loading indicator */}
        {!isError && (
          <div className="startup-loading-dots" aria-hidden="true">
            <span className="startup-dot" />
            <span className="startup-dot" />
            <span className="startup-dot" />
          </div>
        )}

        {/* Error state */}
        {isError && (
          <div className="startup-error">
            <div className="startup-error-message">{error}</div>
            <div className="startup-error-actions">
              <button type="button" className="startup-retry" onClick={onRetry}>
                再試行
              </button>
              <button type="button" className="startup-copy" onClick={handleCopy}>
                詳細をコピー
              </button>
            </div>
            {lastRequest && (
              <div className="startup-error-grid">
                <div className="startup-error-label">Endpoint</div>
                <div className="startup-error-value">{lastRequest.url}</div>
                <div className="startup-error-label">Method</div>
                <div className="startup-error-value">{lastRequest.method}</div>
                <div className="startup-error-label">Status</div>
                <div className="startup-error-value">{lastRequest.status ?? "--"}</div>
                <div className="startup-error-label">Request ID</div>
                <div className="startup-error-value">{lastRequest.requestId ?? "--"}</div>
                <div className="startup-error-label">Response</div>
                <pre className="startup-error-response">{lastRequest.response ?? "--"}</pre>
              </div>
            )}
            {errorDetails && !lastRequest && (
              <details className="startup-details">
                <summary>詳細</summary>
                <pre>{errorDetails}</pre>
              </details>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
