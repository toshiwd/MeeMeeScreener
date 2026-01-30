import { useLayoutEffect } from "react";
import { Route, Routes } from "react-router-dom";
import { BackendReadyProvider } from "./backendReady";
import CandidatesView from "./routes/CandidatesView";
import DetailView from "./routes/DetailView";
import FavoritesView from "./routes/FavoritesView";
import GridView from "./routes/GridView";
import MarketView from "./routes/MarketView";
import PracticeView from "./routes/PracticeView";
import RankingView from "./routes/RankingView";
import PositionsView from "./routes/PositionsView";
import { applyTheme, getStoredTheme } from "./utils/theme";

export default function App() {
  // Initialize theme on app mount
  useLayoutEffect(() => {
    const theme = getStoredTheme();
    applyTheme(theme);
  }, []);

  return (
    <BackendReadyProvider>
      <Routes>
        <Route path="/" element={<GridView />} />
        <Route path="/ranking" element={<RankingView />} />
        <Route path="/favorites" element={<FavoritesView />} />
        <Route path="/candidates" element={<CandidatesView />} />
        <Route path="/positions" element={<PositionsView />} />
        <Route path="/market" element={<MarketView />} />
        <Route path="/detail/:code" element={<DetailView />} />
        <Route path="/practice/:code" element={<PracticeView />} />
      </Routes>
    </BackendReadyProvider>
  );
}
