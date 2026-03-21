import { Suspense, lazy, useLayoutEffect } from "react";
import { Navigate, Route, Routes } from "react-router-dom";
import { BackendReadyProvider } from "../backendReady";
import { applyTheme, getStoredTheme } from "../utils/theme";
import { TradexBootstrapProvider } from "./TradexBootstrapContext";
import TradexShell from "./TradexShell";
import TradexHomePage from "./pages/TradexHomePage";
import TradexVerifyPage from "./pages/TradexVerifyPage";
import TradexComparePage from "./pages/TradexComparePage";
import TradexAdoptPage from "./pages/TradexAdoptPage";
import TradexCandidateDetailPage from "./pages/TradexCandidateDetailPage";
import { TradexLegacyPublishPage, TradexLegacySimPage, TradexLegacyTagsPage } from "./pages/TradexLegacyPages";

const NotFound = lazy(async () => ({
  default: function TradexNotFound() {
    return (
      <div className="tradex-page">
        <section className="tradex-panel">
          <div className="tradex-panel-title">ページが見つかりません</div>
          <div className="tradex-inline-note">TRADEX の正規ルートから開いてください。</div>
        </section>
      </div>
    );
  }
}));

export default function TradexApp() {
  useLayoutEffect(() => {
    const theme = getStoredTheme();
    applyTheme(theme);
  }, []);

  return (
    <BackendReadyProvider>
      <TradexBootstrapProvider>
        <Suspense fallback={null}>
          <Routes>
            <Route element={<TradexShell />}>
              <Route index element={<TradexHomePage />} />
              <Route path="/verify" element={<TradexVerifyPage />} />
              <Route path="/compare" element={<TradexComparePage />} />
              <Route path="/adopt" element={<TradexAdoptPage />} />
              <Route path="/detail/:runId" element={<TradexCandidateDetailPage />} />
              <Route path="/legacy/tags" element={<TradexLegacyTagsPage />} />
              <Route path="/legacy/publish" element={<TradexLegacyPublishPage />} />
              <Route path="/legacy/sim" element={<TradexLegacySimPage />} />
              <Route path="*" element={<Navigate to="/" replace />} />
            </Route>
            <Route path="*" element={<NotFound />} />
          </Routes>
        </Suspense>
      </TradexBootstrapProvider>
    </BackendReadyProvider>
  );
}
