/* eslint-disable react-refresh/only-export-components */
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { useBackendReadyState } from "../../backendReady";
import {
  defaultAiExplainSettings,
  loadAiExplainSettings,
  saveAiExplainSettings,
  type AiExplainSettings,
  type AiExplainSettingsDraft,
  type AiExplainSettingsState,
} from "./aiExplainApi";

type AiExplainContextValue = {
  loading: boolean;
  error: string | null;
  state: AiExplainSettingsState | null;
  settings: AiExplainSettings;
  refresh: () => Promise<void>;
  save: (settings: AiExplainSettingsDraft) => Promise<AiExplainSettingsState>;
  canShowUi: boolean;
  canUse: boolean;
};

const EMPTY_STATE: AiExplainSettingsState = {
  settings: defaultAiExplainSettings(),
  providerReady: false,
  credentialConfigured: false,
  canShowUi: false,
  canUse: false,
};

const AiExplainContext = createContext<AiExplainContextValue | null>(null);

export function AiExplainProvider({ children }: { children: ReactNode }) {
  const { ready: backendReady } = useBackendReadyState();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [state, setState] = useState<AiExplainSettingsState | null>(null);

  const refresh = useCallback(async () => {
    if (!backendReady) return;
    setLoading(true);
    setError(null);
    try {
      const next = await loadAiExplainSettings();
      setState(next);
    } catch (err) {
      setState(EMPTY_STATE);
      setError(err instanceof Error ? err.message : "ai_explain_settings_load_failed");
    } finally {
      setLoading(false);
    }
  }, [backendReady]);

  useEffect(() => {
    if (!backendReady) return;
    void refresh();
  }, [backendReady, refresh]);

  const save = useCallback(async (settings: AiExplainSettingsDraft) => {
    setLoading(true);
    setError(null);
    try {
      const next = await saveAiExplainSettings(settings);
      setState(next);
      return next;
    } catch (err) {
      setError(err instanceof Error ? err.message : "ai_explain_settings_save_failed");
      throw err;
    } finally {
      setLoading(false);
    }
  }, []);

  const value = useMemo<AiExplainContextValue>(() => {
    const resolved = state ?? EMPTY_STATE;
    return {
      loading,
      error,
      state,
      settings: resolved.settings,
      refresh,
      save,
      canShowUi: Boolean(resolved.canShowUi),
      canUse: Boolean(resolved.canUse),
    };
  }, [error, loading, refresh, save, state]);

  return <AiExplainContext.Provider value={value}>{children}</AiExplainContext.Provider>;
}

export function useAiExplain() {
  const context = useContext(AiExplainContext);
  if (!context) {
    return {
      loading: false,
      error: null as string | null,
      state: null as AiExplainSettingsState | null,
      settings: defaultAiExplainSettings(),
      refresh: async () => {},
      save: async (settings: AiExplainSettingsDraft) => ({
        settings: settings as AiExplainSettings,
        providerReady: false,
        credentialConfigured: false,
        canShowUi: false,
        canUse: false,
      }),
      canShowUi: false,
      canUse: false,
    };
  }
  return context;
}
