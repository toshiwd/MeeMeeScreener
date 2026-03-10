import type { Dispatch, SetStateAction } from "react";

import type { TdnetDisclosureItem, TdnetReactionSummary } from "./detailTypes";

type FormatNumber = (value: number | null | undefined, digits?: number) => string;
type FormatSignedPercentLabel = (value: number | null | undefined, digits?: number) => string;

export type Props = {
  activeTdnetDisclosure: TdnetDisclosureItem;
  activeTdnetReaction: TdnetReactionSummary | null;
  selectedTdnetDisclosures: TdnetDisclosureItem[];
  selectedTdnetDisclosureIndex: number;
  setSelectedTdnetDisclosures: Dispatch<SetStateAction<TdnetDisclosureItem[]>>;
  setSelectedTdnetDisclosureIndex: Dispatch<SetStateAction<number>>;
  formatNumber: FormatNumber;
  formatSignedPercentLabel: FormatSignedPercentLabel;
};

export function DetailTdnetCard(props: Props) {
  const {
    activeTdnetDisclosure,
    activeTdnetReaction,
    selectedTdnetDisclosures,
    selectedTdnetDisclosureIndex,
    setSelectedTdnetDisclosures,
    setSelectedTdnetDisclosureIndex,
    formatNumber,
    formatSignedPercentLabel,
  } = props;

  return (
    <div className="detail-tdnet-card">
      <div className="detail-tdnet-card-header">
        <div className="detail-tdnet-card-title">TDNET開示</div>
        <div className="detail-tdnet-card-actions">
          {selectedTdnetDisclosures.length > 1 && (
            <div className="detail-tdnet-card-pager">
              <button
                type="button"
                className="detail-tdnet-card-nav"
                onClick={() => setSelectedTdnetDisclosureIndex((prev) => Math.max(0, prev - 1))}
                disabled={selectedTdnetDisclosureIndex <= 0}
              >
                前
              </button>
              <span className="detail-tdnet-card-page">
                {selectedTdnetDisclosureIndex + 1}/{selectedTdnetDisclosures.length}
              </span>
              <button
                type="button"
                className="detail-tdnet-card-nav"
                onClick={() =>
                  setSelectedTdnetDisclosureIndex((prev) => Math.min(selectedTdnetDisclosures.length - 1, prev + 1))
                }
                disabled={selectedTdnetDisclosureIndex >= selectedTdnetDisclosures.length - 1}
              >
                次
              </button>
            </div>
          )}
          <button
            type="button"
            className="detail-tdnet-card-close"
            onClick={() => {
              setSelectedTdnetDisclosures([]);
              setSelectedTdnetDisclosureIndex(0);
            }}
          >
            閉じる
          </button>
        </div>
      </div>
      <div className="detail-tdnet-card-body">
        <div className="detail-tdnet-card-headline">{activeTdnetDisclosure.title ?? "--"}</div>
        <div className="detail-analysis-meta">
          {[
            activeTdnetDisclosure.eventType ? `種別 ${activeTdnetDisclosure.eventType}` : null,
            activeTdnetDisclosure.sentiment ? `方向 ${activeTdnetDisclosure.sentiment}` : null,
            activeTdnetDisclosure.publishedAt
              ? `開示 ${new Date(activeTdnetDisclosure.publishedAt).toLocaleString("ja-JP")}`
              : null,
          ].filter(Boolean).join(" / ")}
        </div>
        {activeTdnetDisclosure.summaryText && (
          <div className="detail-tdnet-card-summary">{activeTdnetDisclosure.summaryText}</div>
        )}
        {activeTdnetReaction && (
          <div className="detail-tdnet-reaction">
            <div className="detail-tdnet-reaction-header">株価反応</div>
            <div className="detail-tdnet-reaction-meta">
              {[
                activeTdnetReaction.baseDate ? `基準日 ${activeTdnetReaction.baseDate}` : null,
                activeTdnetReaction.baseClose != null ? `終値 ${formatNumber(activeTdnetReaction.baseClose, 2)}` : null,
                activeTdnetReaction.volumeRatio != null
                  ? `出来高 ${formatNumber(activeTdnetReaction.volumeRatio, 2)}x`
                  : null,
              ].filter(Boolean).join(" / ")}
            </div>
            <div className="detail-tdnet-reaction-grid">
              {activeTdnetReaction.reactions.map((reaction) => (
                <div key={reaction.bars} className="detail-tdnet-reaction-item">
                  <div className="detail-tdnet-reaction-label">{reaction.label}</div>
                  <div className="detail-tdnet-reaction-value">
                    {formatSignedPercentLabel(reaction.returnRatio, 1)}
                  </div>
                  <div className="detail-tdnet-reaction-date">{reaction.targetDate ?? "--"}</div>
                </div>
              ))}
            </div>
          </div>
        )}
        {activeTdnetDisclosure.tags.length > 0 && (
          <div className="detail-tdnet-card-tags">
            {activeTdnetDisclosure.tags.map((tag) => (
              <span key={tag} className="detail-tdnet-card-tag">{tag}</span>
            ))}
          </div>
        )}
        <div className="detail-tdnet-card-links">
          {activeTdnetDisclosure.tdnetUrl && (
            <a href={activeTdnetDisclosure.tdnetUrl} target="_blank" rel="noreferrer">TDNET</a>
          )}
          {activeTdnetDisclosure.pdfUrl && (
            <a href={activeTdnetDisclosure.pdfUrl} target="_blank" rel="noreferrer">PDF</a>
          )}
          {activeTdnetDisclosure.xbrlUrl && (
            <a href={activeTdnetDisclosure.xbrlUrl} target="_blank" rel="noreferrer">XBRL</a>
          )}
        </div>
      </div>
    </div>
  );
}
