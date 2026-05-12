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
  inline?: boolean;
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
  inline = false,
  onToggleOpen,
  onCopy,
  onToggleInfoDetails,
  onClose,
}: Props) {
  if (!hasIssues) return null;

  return (
    <div className={`detail-debug-banner ${bannerTone} ${inline ? "is-inline" : ""}`.trim()}>
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
            <div className="detail-debug-title">確認詳細</div>
            <div className="detail-debug-actions">
              <button
                type="button"
                className="detail-debug-copy"
                onClick={onCopy}
                title="コピー"
                aria-label="コピー"
              >
                <IconCopy size={16} />
              </button>
              <button
                type="button"
                className="detail-debug-info-toggle"
                onClick={onToggleInfoDetails}
              >
                {showInfoDetails ? "補足: 表示" : "補足: 非表示"}
              </button>
              <button
                type="button"
                className="detail-debug-close"
                onClick={onClose}
              >
                閉じる
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
              <div className="detail-debug-fallback-title">コピーできませんでした</div>
              <textarea readOnly value={copyFallbackText} />
            </div>
          )}
        </div>
      )}
    </div>
  );
}
