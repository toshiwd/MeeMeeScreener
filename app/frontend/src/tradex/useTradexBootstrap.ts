import { useContext } from "react";
import { TradexBootstrapContext } from "./tradexBootstrapState";

export function useTradexBootstrap() {
  const context = useContext(TradexBootstrapContext);
  if (!context) {
    throw new Error("useTradexBootstrap must be used within TradexBootstrapProvider");
  }
  return context;
}
