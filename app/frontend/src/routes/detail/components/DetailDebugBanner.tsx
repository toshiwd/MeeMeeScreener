import { IconCopy } from "@tabler/icons-react";

type Props = {
  hasIssues: boolean;
  bannerTone: string;
  bannerTitle: string;
  debugSummary: string[];
  debugOpen: boolean;
  showInfoDetails: boolean;
  debugLines: string[];
  copyFallbackText: string | null;
  onToggleOpen: () => void;
  onCopy: () => void;
  onToggleInfoDetails: () => void;
  onClose: () => void;
};

export default function DetailDebugBanner({
  hasIssues,
  bannerTone,
  bannerTitle,
  debugSummary,
  debugOpen,
  showInfoDetails,
  debugLines,
  copyFallbackText,
  onToggleOpen,
  onCopy,
  onToggleInfoDetails,
  onClose,
}: Props) {
  if (!hasIssues) return null;

  return (
    <div className={`detail-debug-banner ${bannerTone}`}>
      <button
        type="button"
        className="detail-debug-toggle"
        onClick={onToggleOpen}
      >
        {`${bannerTitle}${debugSummary.length ? ` (${debugSummary.join(", ")})` : ""}`}
      </button>
      {debugOpen && (
        <div className="detail-debug-panel">
          <div className="detail-debug-header">
            <div className="detail-debug-title">Debug Details</div>
            <div className="detail-debug-actions">
              <button
                type="button"
                className="detail-debug-copy"
                onClick={onCopy}
                title="Copy"
                aria-label="Copy"
              >
                <IconCopy size={16} />
              </button>
              <button
                type="button"
                className="detail-debug-info-toggle"
                onClick={onToggleInfoDetails}
              >
                {showInfoDetails ? "Info: ON" : "Info: OFF"}
              </button>
              <button
                type="button"
                className="detail-debug-close"
                onClick={onClose}
              >
                Close
              </button>
            </div>
          </div>
          <div className="detail-debug-lines">
            {debugLines.map((line, index) => (
              <div key={`${line}-${index}`}>{line}</div>
            ))}
          </div>
          {copyFallbackText && (
            <div className="detail-debug-fallback">
              <div className="detail-debug-fallback-title">Copy failed</div>
              <textarea readOnly value={copyFallbackText} />
            </div>
          )}
        </div>
      )}
    </div>
  );
}
