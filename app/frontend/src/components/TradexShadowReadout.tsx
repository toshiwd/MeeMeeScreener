import { useTradexShadowIntegration } from "../hooks/useTradexShadowIntegration";

type TradexShadowReadoutProps = {
  className?: string;
  variant?: "ranking" | "detail";
};

const badgeToneClass = (tone: "ok" | "warn" | "neutral") => {
  if (tone === "ok") return "is-ok";
  if (tone === "warn") return "is-warn";
  return "is-neutral";
};

function Badge({
  tone,
  children,
}: {
  tone: "ok" | "warn" | "neutral";
  children: string;
}) {
  return <span className={`rank-score-badge rank-qualification ${badgeToneClass(tone)}`.trim()}>{children}</span>;
}

export default function TradexShadowReadout({ className, variant = "ranking" }: TradexShadowReadoutProps) {
  const shadow = useTradexShadowIntegration();

  if (!shadow.loaded) return null;

  const rootClassName = [
    "tradex-shadow-readout",
    variant === "detail" ? "is-detail" : "is-ranking",
    className,
  ]
    .filter(Boolean)
    .join(" ");

  if (shadow.unavailable) return null;

  if (!shadow.isReady) return null;

  const readinessLabel =
    variant === "detail"
      ? null
      : shadow.adoptionReadiness === "ready"
        ? "確認済み"
        : shadow.adoptionReadiness
          ? "採用準備を確認中"
          : null;

  return (
    <div className={rootClassName} data-testid="tradex-shadow-readout" aria-label="検証表示が有効">
      <Badge tone="ok">検証表示: 有効</Badge>
      {readinessLabel && <Badge tone="neutral">状態: {readinessLabel}</Badge>}
      {shadow.outsideTop20Locked === false && <Badge tone="warn">表示範囲を確認</Badge>}
    </div>
  );
}
