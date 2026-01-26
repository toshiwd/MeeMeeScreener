import { useMemo } from "react";
import type { DailyPosition } from "../utils/positions";

type DailyCandle = {
    time: number;
    open: number;
    high: number;
    low: number;
    close: number;
};

type MaLine = {
    key: string;
    period?: number;
    data: { time: number; value: number }[];
};

export const useDetailInfo = (
    selectedBarData: DailyCandle | null,
    selectedBarIndex: number,
    dailyCandles: DailyCandle[],
    dailyPositions: DailyPosition[],
    dailyMaLines: MaLine[]
) => {
    return useMemo(() => {
        if (!selectedBarData || selectedBarIndex < 0) return null;

        // 1. Previous Day Data
        let prevDayData = undefined;
        if (selectedBarIndex > 0) {
            const prevBar = dailyCandles[selectedBarIndex - 1];
            const change = selectedBarData.close - (prevBar.close || 0);
            const changePercent = prevBar.close ? (change / prevBar.close) * 100 : 0;
            prevDayData = { close: prevBar.close, change, changePercent };
        }

        // 2. Position Data
        const posList = dailyPositions.filter((p) => p.time === selectedBarData.time);
        const position = {
            buy: posList.reduce((acc, p) => acc + p.longLots, 0),
            sell: posList.reduce((acc, p) => acc + p.shortLots, 0),
        };

        // 3. MA Values
        const maValues: any = {};

        // 4. MA Trends
        const maTrends: any = {};

        const countTrend = (lineData: { time: number; value: number }[]) => {
            const valueMap = new Map(lineData.map((d) => [d.time, d.value]));
            const currentMa = valueMap.get(selectedBarData.time);
            if (currentMa == null) return null;

            const isUp = selectedBarData.close >= currentMa;
            let count = 0;

            for (let i = selectedBarIndex; i >= 0; i--) {
                const bar = dailyCandles[i];
                const ma = valueMap.get(bar.time);
                if (ma == null) break;

                const barIsUp = bar.close >= ma;
                if (barIsUp === isUp) {
                    count++;
                } else {
                    break;
                }
            }

            const upStr = isUp ? count : 0;
            const downStr = isUp ? 0 : count;
            return `上${upStr} / 下${downStr}`;
        };

        dailyMaLines.forEach((line) => {
            // Value
            const point = line.data.find((d) => d.time === selectedBarData.time);
            if (point) {
                if (line.period === 5 || line.period === 7) maValues.ma7 = point.value;
                else if (line.period === 20 || line.period === 25) maValues.ma20 = point.value;
                else if (line.period === 60 || line.period === 75) maValues.ma60 = point.value;
                else if (line.period === 100) maValues.ma100 = point.value;
                else if (line.period === 200) maValues.ma200 = point.value;
            }

            // Trend
            const trend = countTrend(line.data);
            if (trend) {
                if (line.period === 5 || line.period === 7) maTrends.ma7 = trend;
                else if (line.period === 20 || line.period === 25) maTrends.ma20 = trend;
                else if (line.period === 60 || line.period === 75) maTrends.ma60 = trend;
                else if (line.period === 100) maTrends.ma100 = trend;
                else if (line.period === 200) maTrends.ma200 = trend;
            }
        });

        return { prevDayData, position, maValues, maTrends };
    }, [selectedBarData, selectedBarIndex, dailyCandles, dailyPositions, dailyMaLines]);
};
