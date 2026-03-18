import {
    IconArrowLeft,
    IconArrowBackUp,
    IconArrowRight,
    IconHeart,
    IconBox,
    IconListDetails,
    IconLink,
    IconPointer,
    IconAdjustments,
    IconCamera,
    IconSparkles
} from "@tabler/icons-react";
import DetailChart from "./DetailChart";
import IconButton from "./IconButton";
import DailyMemoPanel from "./DailyMemoPanel";
import { useDetailInfo } from "../hooks/useDetailInfo";
import { Box as BoxType } from "../store";
import type { DailyPosition, TradeMarker } from "../utils/positions";

// Dummy functions for UI consistency
const noop = () => { };

type OffscreenDetailViewProps = {
    code: string;
    tickerName: string;
    dailyCandles: any[];
    weeklyCandles: any[];
    monthlyCandles: any[];
    dailyVolume: any[];
    weeklyVolume: any[];
    monthlyVolume: any[];
    dailyMaLines: any[];
    weeklyMaLines: any[];
    monthlyMaLines: any[];
    boxes: BoxType[];
    showBoxes: boolean;
    dailyPositions: DailyPosition[];
    tradeMarkers: TradeMarker[];
};

export const OffscreenDetailView = ({
    code,
    tickerName,
    dailyCandles,
    weeklyCandles: _weeklyCandles,
    monthlyCandles,
    dailyVolume,
    weeklyVolume: _weeklyVolume,
    monthlyVolume,
    dailyMaLines,
    weeklyMaLines: _weeklyMaLines,
    monthlyMaLines,
    boxes,
    showBoxes,
    dailyPositions,
    tradeMarkers
}: OffscreenDetailViewProps) => {

    // Derived Data
    const selectedBarIndex = dailyCandles.length - 1;
    const selectedBarData = dailyCandles[selectedBarIndex] || null;
    const selectedDate = selectedBarData ? new Date(selectedBarData.time * 1000).toISOString().split('T')[0] : null;

    const memoPanelData = useDetailInfo(
        selectedBarData,
        selectedBarIndex,
        dailyCandles,
        dailyPositions,
        dailyMaLines
    );

    // Hardcoded layout constants
    const DAILY_ROW_RATIO = 0.60;
    return (
        <div className="detail-shell" style={{ width: 1400, height: 900, background: '#0f172a' }}>
            <div className="detail-header">
                <div className="detail-header-nav">
                    <button className="back nav-button nav-primary">
                        <span className="nav-icon"><IconArrowLeft size={16} /></span>
                        <span className="nav-label">一覧に戻る</span>
                    </button>
                    <button className="back nav-button">
                        <span className="nav-icon"><IconArrowBackUp size={16} /></span>
                        <span className="nav-label">前の画面</span>
                    </button>
                    <button className="back nav-button" disabled>
                        <span className="nav-icon"><IconArrowRight size={16} /></span>
                        <span className="nav-label">次の銘柄</span>
                    </button>
                </div>
                <div className="detail-title">
                    <div className="detail-title-text">
                        <div className="detail-title-top">
                            <div className="detail-title-code">{code}</div>
                            <div className="detail-title-name">{tickerName}</div>
                        </div>
                    </div>
                    <div className="detail-title-actions">
                        <button className="favorite-toggle"><IconHeart size={18} /></button>
                    </div>
                </div>
                <div className="detail-controls">
                    <div className="detail-controls-group">
                        <button className="indicator-button is-primary">練習</button>
                        <div className="segmented detail-range">
                            <button className="active">20年</button>
                        </div>
                    </div>
                    <div className="detail-controls-group">
                        <IconButton icon={<IconBox size={18} />} label="Boxes" variant="iconLabel" selected={showBoxes} onClick={noop} />
                        <IconButton icon={<IconListDetails size={18} />} label="建玉推移" variant="iconLabel" onClick={noop} />
                        <IconButton icon={<IconLink size={18} />} label="連動 ON" variant="iconLabel" selected={true} onClick={noop} />
                        <IconButton icon={<IconPointer size={18} />} label="カーソル ON" variant="iconLabel" selected={true} onClick={noop} />
                    </div>
                    <div className="detail-controls-group detail-controls-icons">
                        <IconButton label="Indicators" icon={<IconAdjustments size={18} />} onClick={noop} />
                        <IconButton label="スクショ" icon={<IconCamera size={18} />} onClick={noop} />
                        <IconButton label="AI出力" icon={<IconSparkles size={18} />} onClick={noop} />
                    </div>
                </div>
            </div>

            <div className="detail-content">
                <div className="detail-row detail-row-top" style={{ flex: `${DAILY_ROW_RATIO} 1 0%` }}>
                    <div className="detail-pane-header">Daily</div>
                    <div className="detail-chart">
                        <DetailChart
                            candles={dailyCandles}
                            volume={dailyVolume}
                            maLines={dailyMaLines}
                            showVolume={true}
                            boxes={boxes}
                            showBoxes={showBoxes}
                            positionOverlay={{
                                dailyPositions,
                                tradeMarkers,
                                showOverlay: true,
                                showMarkers: true,
                                showPnL: false,
                                hoverTime: null,
                            }}
                        />
                        {/* DailyMemoPanel as Left/Overlay Box */}
                        <div style={{ position: 'absolute', left: 0, top: 0, bottom: 0, zIndex: 20 }}>
                            <DailyMemoPanel
                                code={code}
                                selectedDate={selectedDate}
                                selectedBarData={selectedBarData}
                                {...(memoPanelData || {})}
                                cursorMode={true}
                                onToggleCursorMode={noop}
                                onPrevDay={noop}
                                onNextDay={noop}
                                onCopyForConsult={noop}
                            />
                        </div>
                    </div>
                </div>
                <div className="detail-row detail-row-bottom" style={{ flex: `${1 - DAILY_ROW_RATIO} 1 0%` }}>
                    <div className="detail-pane" style={{ flex: '1 1 0%' }}>
                        <div className="detail-pane-header">Monthly</div>
                        <div className="detail-chart">
                            <DetailChart
                                candles={monthlyCandles}
                                volume={monthlyVolume}
                                maLines={monthlyMaLines}
                                showVolume={false}
                                boxes={boxes}
                                showBoxes={showBoxes}
                            />
                        </div>
                    </div>
                </div>
            </div>
            {/* Footer with stats */}
            <div className="detail-footer">
                <div className="detail-footer-left"></div>
                <div className="detail-hint">
                    Daily {dailyCandles.length} bars | Monthly {monthlyCandles.length} bars
                </div>
            </div>
        </div>
    );
};
