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
      <div className="detail-header-row">
        <div className="detail-header-left">
          <div className="detail-summary-back">{summaryBack}</div>
          <div className="detail-summary-main">
            {summaryMain}
            {summaryStatus ? <div className="detail-summary-status">{summaryStatus}</div> : null}
          </div>
          <div className="detail-summary-mode">{modeControls}</div>
        </div>
        <div className="detail-header-center">{topbarActions}</div>
        <div className="detail-header-right">
          <div className="detail-summary-actions">{summaryActions}</div>
          {summaryCenter ? <div className="detail-summary-center">{summaryCenter}</div> : null}
          {topbarRight ? <div className="detail-header-practice">{topbarRight}</div> : null}
        </div>
      </div>
      {belowTopbar}
    </div>
  );
}
