import { useMemo } from "react";
import {
  buildTradexDetailAnalysisWarmRequest,
  shouldShowTradexDetailAnalysis,
} from "./tradexAnalysis";
import { useTradexDetailAnalysis } from "./useTradexDetailAnalysis";
import { TradexAnalysisPanel } from "./TradexAnalysisPanel";

type FormatNumber = (value: number | null | undefined, digits?: number) => string;
type FormatPercentLabel = (value: number | null | undefined, digits?: number) => string;
type FormatSignedPercentLabel = (value: number | null | undefined, digits?: number) => string;

type Props = {
  backendReady: boolean;
  readyToFetch: boolean;
  analysisFetchEnabled: boolean;
  code: string | null;
  asof: number | null;
  formatPercentLabel: FormatPercentLabel;
  formatSignedPercentLabel: FormatSignedPercentLabel;
  formatNumber: FormatNumber;
};

export function TradexAnalysisMount({
  backendReady,
  readyToFetch,
  analysisFetchEnabled,
  code,
  asof,
  formatPercentLabel,
  formatSignedPercentLabel,
  formatNumber,
}: Props) {
  const tradexEnabled = shouldShowTradexDetailAnalysis();
  const warmRequest = useMemo(
    () => buildTradexDetailAnalysisWarmRequest(code, asof),
    [asof, code]
  );
  const enabled = analysisFetchEnabled && tradexEnabled && warmRequest != null;
  const state = useTradexDetailAnalysis({
    backendReady,
    readyToFetch,
    enabled,
    code: warmRequest?.code ?? null,
    asof: warmRequest?.asof ?? null,
  });
  if (!enabled) return null;
  return (
    <TradexAnalysisPanel
      state={state}
      formatPercentLabel={formatPercentLabel}
      formatSignedPercentLabel={formatSignedPercentLabel}
      formatNumber={formatNumber}
    />
  );
}

export default TradexAnalysisMount;
