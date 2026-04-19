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

  if (shadow.unavailable) {
    return (
      <div className={rootClassName} data-testid="tradex-shadow-readout" aria-label="R2 shadow unavailable">
        <Badge tone="warn">shadow unavailable</Badge>
      </div>
    );
  }

  if (!shadow.isReady) return null;

  const acceptanceLabel = shadow.adoptionReadiness?.replaceAll("_", "-") ?? "shadow-only";
  const compareMethodLabel = shadow.compareMethod ?? "boundary_local_rerank";

  return (
    <div className={rootClassName} data-testid="tradex-shadow-readout" aria-label="R2 shadow active">
      <Badge tone="ok">R2 shadow: ON</Badge>
      <Badge tone="neutral">acceptance: {acceptanceLabel}</Badge>
      <Badge tone="neutral">method: {compareMethodLabel}</Badge>
      <Badge tone={shadow.outsideTop20Locked === true ? "ok" : "warn"}>
        outside_top20_locked: {shadow.outsideTop20Locked === true ? "true" : "false"}
      </Badge>
    </div>
  );
}
