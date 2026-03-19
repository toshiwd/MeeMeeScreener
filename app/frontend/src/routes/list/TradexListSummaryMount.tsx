import type { ReactNode } from "react";
import { useMemo } from "react";
import { useTradexListSummary } from "../../hooks/useTradexListSummary";
import {
  buildTradexListSummaryWarmItems,
  shouldShowTradexListSummary,
  TRADEX_LIST_SUMMARY_WARM_CAPS,
  type TradexListSummaryRequestItem,
} from "./tradexSummary";

type TradexListSummaryState = ReturnType<typeof useTradexListSummary>;

type Props = {
  backendReady: boolean;
  enabled?: boolean;
  scope: string;
  items: TradexListSummaryRequestItem[];
  warmCap?: number;
  children: (state: TradexListSummaryState) => ReactNode;
};

export function TradexListSummaryMount({
  backendReady,
  enabled,
  scope,
  items,
  warmCap,
  children,
}: Props) {
  const tradexEnabled = shouldShowTradexListSummary() && (enabled ?? true);
  const cap = warmCap ?? TRADEX_LIST_SUMMARY_WARM_CAPS.visible;
  const warmItems = useMemo(
    () => buildTradexListSummaryWarmItems(items, scope, cap),
    [cap, items, scope]
  );
  const state = useTradexListSummary({
    backendReady,
    enabled: tradexEnabled,
    scope,
    items: warmItems,
  });
  return <>{children(state)}</>;
}

export default TradexListSummaryMount;
