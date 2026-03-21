import type { ReactNode } from "react";

export function TradexLegacyFrame({
  title,
  description,
  children
}: {
  title: string;
  description: string;
  children: ReactNode;
}) {
  return (
    <section className="tradex-panel tradex-legacy-panel">
      <div className="tradex-panel-head">
        <div>
          <div className="tradex-panel-title">{title}</div>
          <div className="tradex-panel-caption">{description}</div>
        </div>
      </div>
      {children}
    </section>
  );
}
