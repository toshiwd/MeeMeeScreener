type DetailMode = "chart" | "analysis" | "financial" | "practice" | "positions";

type DetailModeTabsProps = {
  activeMode: DetailMode;
  onChart: () => void;
  onAnalysis: () => void;
  onFinancial: () => void;
  onPractice: () => void;
  onPositions: () => void;
};

export default function DetailModeTabs({
  activeMode,
  onChart,
  onAnalysis,
  onFinancial,
  onPractice,
  onPositions
}: DetailModeTabsProps) {
  return (
    <div className="detail-mode-bar">
      <div className="segmented detail-mode">
        <button className={activeMode === "chart" ? "active" : ""} onClick={onChart}>
          チャート
        </button>
        <button className={activeMode === "analysis" ? "active" : ""} onClick={onAnalysis}>
          分析
        </button>
        <button className={activeMode === "financial" ? "active" : ""} onClick={onFinancial}>
          財務
        </button>
        <button className={activeMode === "practice" ? "active" : ""} onClick={onPractice}>
          練習
        </button>
        <button className={activeMode === "positions" ? "active" : ""} onClick={onPositions}>
          建玉
        </button>
      </div>
    </div>
  );
}
