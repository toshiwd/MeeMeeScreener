import { forwardRef, type ReactNode } from "react";

type ScreenPanelProps = {
  title: string;
  summary?: ReactNode;
  details?: ReactNode;
  actions?: ReactNode;
  className?: string;
  children: ReactNode;
};

const ScreenPanel = forwardRef<HTMLDivElement, ScreenPanelProps>(function ScreenPanel(
  { title, summary, details, actions, className, children },
  ref
) {
  return (
    <div ref={ref} className={["screen-panel", className].filter(Boolean).join(" ")}>
      <div className="screen-panel-header">
        <div className="screen-panel-header-main">
          <div className="screen-panel-title">{title}</div>
          {summary ? <div className="screen-panel-summary">{summary}</div> : null}
          {details ? <div className="screen-panel-details">{details}</div> : null}
        </div>
        {actions ? <div className="screen-panel-actions">{actions}</div> : null}
      </div>
      <div className="screen-panel-body">{children}</div>
    </div>
  );
});

export default ScreenPanel;
