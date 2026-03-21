import { useCallback, useEffect, useMemo, useState, type ReactNode } from "react";
import { useBackendReadyState } from "../backendReady";
import { loadTradexBootstrap } from "./data";
import { TradexBootstrapContext } from "./tradexBootstrapState";
import type { TradexBootstrapData } from "./contracts";

export function TradexBootstrapProvider({ children }: { children: ReactNode }) {
  const { ready } = useBackendReadyState();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<TradexBootstrapData | null>(null);

  const refresh = useCallback(async () => {
    if (!ready) return;
    setLoading(true);
    setError(null);
    try {
      const next = await loadTradexBootstrap();
      setData(next);
    } catch (err) {
      setError(err instanceof Error ? err.message : "TRADEX の初期データを取得できませんでした。");
    } finally {
      setLoading(false);
    }
  }, [ready]);

  useEffect(() => {
    if (!ready) {
      setLoading(true);
      return;
    }
    void refresh();
  }, [ready, refresh]);

  const value = useMemo(
    () => ({ loading, error, data, refresh }),
    [data, error, loading, refresh]
  );

  return <TradexBootstrapContext.Provider value={value}>{children}</TradexBootstrapContext.Provider>;
}
