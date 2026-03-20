import type { ReactNode } from "react";

type DetailHeaderChromeProps = {
  summaryBack: ReactNode;
  summaryMain: ReactNode;
  summaryStatus?: ReactNode;
  summaryCenter?: ReactNode;
  summaryActions?: ReactNode;
  modeControls: ReactNode;
  topbarActions: ReactNode;
  topbarRight?: ReactNode;
  belowTopbar?: ReactNode;
  className?: string;
};

export default function DetailHeaderChrome({
  summaryBack,
  summaryMain,
  summaryStatus,
  summaryCenter,
  summaryActions,
  modeControls,
  topbarActions,
  topbarRight,
  belowTopbar,
  className
}: DetailHeaderChromeProps) {
  return (
    <div className={["detail-header", className].filter(Boolean).join(" ")}>
      <div className="detail-summary-row">
        <div className="detail-summary-back">{summaryBack}</div>
        <div className="detail-summary-main">
          {summaryMain}
          {summaryStatus ? <div className="detail-summary-status">{summaryStatus}</div> : null}
        </div>
        {summaryCenter}
        <div className="detail-summary-actions">{summaryActions}</div>
      </div>
      <div className="detail-topbar-row">
        <div className="detail-topbar-main">
          {modeControls}
          <div className="detail-topbar-divider" aria-hidden="true" />
          <div className="detail-topbar-actions">{topbarActions}</div>
        </div>
        {topbarRight ? (
          <>
            <div className="detail-topbar-divider" aria-hidden="true" />
            <div className="detail-topbar-right">{topbarRight}</div>
          </>
        ) : null}
      </div>
      {belowTopbar}
    </div>
  );
}
