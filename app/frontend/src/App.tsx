import { Suspense, lazy, useLayoutEffect } from "react";
import { Route, Routes } from "react-router-dom";
import { BackendReadyProvider } from "./backendReady";
import { applyTheme, getStoredTheme } from "./utils/theme";

const GridView = lazy(() => import("./routes/GridView"));
const RankingView = lazy(() => import("./routes/RankingView"));
const FavoritesView = lazy(() => import("./routes/FavoritesView"));
const CandidatesView = lazy(() => import("./routes/CandidatesView"));
const PositionsView = lazy(() => import("./routes/PositionsView"));
const MarketView = lazy(() => import("./routes/MarketView"));
const ToredexSimulationView = lazy(() => import("./routes/ToredexSimulationView"));
const TradexTagValidationView = lazy(() => import("./routes/TradexTagValidationView"));
const DetailView = lazy(() => import("./routes/DetailView"));
const PracticeView = lazy(() => import("./routes/PracticeView"));

export default function App() {
  // Initialize theme on app mount
  useLayoutEffect(() => {
    const theme = getStoredTheme();
    applyTheme(theme);
  }, []);

  return (
    <BackendReadyProvider>
      <Suspense fallback={null}>
        <Routes>
          <Route path="/" element={<GridView />} />
          <Route path="/ranking" element={<RankingView />} />
          <Route path="/favorites" element={<FavoritesView />} />
          <Route path="/candidates" element={<CandidatesView />} />
          <Route path="/positions" element={<PositionsView />} />
          <Route path="/market" element={<MarketView />} />
          <Route path="/toredex-sim" element={<ToredexSimulationView />} />
          <Route path="/tradex-tags" element={<TradexTagValidationView />} />
          <Route path="/detail/:code" element={<DetailView />} />
          <Route path="/practice/:code" element={<PracticeView />} />
        </Routes>
      </Suspense>
    </BackendReadyProvider>
  );
}
