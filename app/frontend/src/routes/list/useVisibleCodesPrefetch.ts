import { useCallback, useEffect, useRef, useState } from "react";
import type { GridTimeframe } from "../../storeTypes";

type CodeItem = {
  code: string;
};

type Params<T extends CodeItem> = {
  backendReady: boolean;
  timeframe: GridTimeframe;
  reason: string;
  ensureBarsForVisible: (timeframe: GridTimeframe, codes: string[], reason?: string) => Promise<void>;
};

export function useVisibleCodesPrefetch<T extends CodeItem>({
  backendReady,
  timeframe,
  reason,
  ensureBarsForVisible,
}: Params<T>) {
  const [visibleCodes, setVisibleCodes] = useState<string[]>([]);
  const visibleKeyRef = useRef("");

  const handleVisibleItemsChange = useCallback((items: T[]) => {
    const nextCodes = items.map((item) => item.code).filter((code) => code);
    const nextKey = nextCodes.join(",");
    if (visibleKeyRef.current === nextKey) return;
    visibleKeyRef.current = nextKey;
    setVisibleCodes(nextCodes);
  }, []);

  useEffect(() => {
    if (!backendReady || visibleCodes.length === 0) return;
    void ensureBarsForVisible(timeframe, visibleCodes, reason);
  }, [backendReady, ensureBarsForVisible, reason, timeframe, visibleCodes]);

  return { handleVisibleItemsChange };
}
