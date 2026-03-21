import type { ReactNode } from "react";

type Props = {
  title: string;
  description: string;
  children: ReactNode;
};

export default function LegacyViewFrame({ title, description, children }: Props) {
  return (
    <section className="tradex-legacy-frame">
      <div className="tradex-legacy-frame-head">
        <div>
          <div className="tradex-legacy-frame-title">{title}</div>
          <div className="tradex-legacy-frame-description">{description}</div>
        </div>
        <span className="tradex-pill is-muted">移行中</span>
      </div>
      <div className="tradex-legacy-frame-body">{children}</div>
    </section>
  );
}
