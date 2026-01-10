import { useEffect } from "react";
import { Route, Routes } from "react-router-dom";
import { BackendReadyProvider } from "./backendReady";
import CandidatesView from "./routes/CandidatesView";
import DetailView from "./routes/DetailView";
import FavoritesView from "./routes/FavoritesView";
import GridView from "./routes/GridView";
import PracticeView from "./routes/PracticeView";
import RankingView from "./routes/RankingView";
import { applyTheme, getStoredTheme } from "./utils/theme";

export default function App() {
  // Initialize theme on app mount
  useEffect(() => {
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
        <Route path="/detail/:code" element={<DetailView />} />
        <Route path="/practice/:code" element={<PracticeView />} />
      </Routes>
    </BackendReadyProvider>
  );
}
