import { createContext } from "react";
import type { TradexBootstrapData } from "./contracts";

export type TradexBootstrapContextValue = {
  loading: boolean;
  error: string | null;
  data: TradexBootstrapData | null;
  refresh: () => Promise<void>;
};

export const TradexBootstrapContext = createContext<TradexBootstrapContextValue | null>(null);
