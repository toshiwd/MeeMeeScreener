# DetailView 実装ガイド - 残りのステップ

## 現在の状態
✅ 状態変数追加完了
✅ インポート追加完了
✅ DailyMemoPanelコンポーネント作成完了
✅ consultCopyユーティリティ作成完了

## 残りの実装ステップ

### ステップ1: カーソルモード関連の関数を追加

DetailView関数内に以下の関数を追加する位置: `showShortToast` 関数の後あたり (行920付近)

```typescript
// Cursor mode functions
const toggleCursorMode = () => {
  setCursorMode(prev => !prev);
  if (!cursorMode && dailyCandles.length > 0) {
    // Initialize with last bar when turning on
    updateSelectedBar(dailyCandles.length - 1);
  }
};

const updateSelectedBar = (index: number) => {
  if (index < 0 || index >= dailyCandles.length) return;
  
  const bar = dailyCandles[index];
  setSelectedBarIndex(index);
  setSelectedBarData(bar);
  
  // Convert time to date string (YYYY-MM-DD)
  const date = new Date(bar.time * 1000);
  const dateStr = date.toISOString().split('T')[0];
  setSelectedDate(dateStr);
  
  // Auto-pan if needed
  autoPanToBar(bar.time);
};

const autoPanToBar = (time: number) => {
  if (!dailyChartRef.current) return;
  
  // Get current visible range
  const visibleRange = dailyVisibleRange;
  if (!visibleRange) return;
  
  const { from, to } = visibleRange;
  const rangeSize = to - from;
  const margin = rangeSize * 0.1; // 10% margin
  
  // Check if time is outside visible range
  if (time < from + margin || time > to - margin) {
    // Pan to center the selected bar
    const newFrom = time - rangeSize / 2;
    const newTo = time + rangeSize / 2;
    dailyChartRef.current.setVisibleRange({ from: newFrom, to: newTo });
  }
};

const moveToPrevDay = () => {
  if (selectedBarIndex === null || selectedBarIndex <= 0) return;
  updateSelectedBar(selectedBarIndex - 1);
};

const moveToNextDay = () => {
  if (selectedBarIndex === null || selectedBarIndex >= dailyCandles.length - 1) return;
  updateSelectedBar(selectedBarIndex + 1);
};

const handleCopyForConsult = async () => {
  if (!selectedDate || !selectedBarData || !code) return;
  
  // Get current memo from DailyMemoPanel (we'll need to pass it as prop or fetch it)
  let memo = "";
  try {
    const response = await api.get("/memo", {
      params: { symbol: code, date: selectedDate, timeframe: "D" },
    });
    memo = response.data.memo || "";
  } catch (error) {
    console.error("Failed to fetch memo:", error);
  }
  
  const consultData = {
    symbol: code,
    name: tickerName || code,
    date: selectedDate,
    ohlc: {
      open: selectedBarData.open,
      high: selectedBarData.high,
      low: selectedBarData.low,
      close: selectedBarData.close,
    },
    volume: dailyVolume.find(v => v.time === selectedBarData.time)?.value,
    // TODO: Add position, MA, signals data if available
    memo,
  };
  
  const text = buildConsultCopyText(consultData);
  const success = await copyConsultToClipboard(text);
  
  if (success) {
    setToastMessage("相談用データをコピーしました");
  } else {
    setToastMessage("コピーに失敗しました");
  }
};
```

### ステップ2: キーボードイベントハンドラを追加

useEffect内に追加 (既存のhandleKeyDown関数の近く、行1197付近)

```typescript
// Cursor mode keyboard handler
useEffect(() => {
  if (!cursorMode) return;
  
  const handleCursorKeyDown = (e: KeyboardEvent) => {
    // Don't handle if typing in textarea
    if ((e.target as HTMLElement).tagName === 'TEXTAREA') {
      return;
    }
    
    switch (e.key) {
      case 'ArrowLeft':
        e.preventDefault();
        moveToPrevDay();
        break;
      case 'ArrowRight':
        e.preventDefault();
        moveToNextDay();
        break;
      case 'c':
      case 'C':
        e.preventDefault();
        toggleCursorMode();
        break;
      case 'Escape':
        e.preventDefault();
        setCursorMode(false);
        break;
    }
  };
  
  window.addEventListener('keydown', handleCursorKeyDown);
  return () => window.removeEventListener('keydown', handleCursorKeyDown);
}, [cursorMode, selectedBarIndex, dailyCandles]);
```

### ステップ3: チャートクリックハンドラを追加

DetailChartコンポーネントに `onChartClick` プロップを追加する必要があります。
まず、DetailChartのpropsに追加:

```typescript
// DetailChart.tsx に追加
interface DetailChartProps {
  // ... existing props
  onChartClick?: (time: number | null) => void;
}
```

DetailView内でハンドラを作成:

```typescript
const handleDailyChartClick = (time: number | null) => {
  if (!cursorMode || time === null) return;
  
  // Find nearest bar index
  const index = dailyCandles.findIndex(c => c.time >= time);
  if (index >= 0) {
    updateSelectedBar(index);
  } else if (dailyCandles.length > 0) {
    // If time is after all bars, select last bar
    updateSelectedBar(dailyCandles.length - 1);
  }
};
```

### ステップ4: レイアウト変更

DetailViewのreturn文内、チャート部分を以下のように変更:

```tsx
<div className="detail-content">
  <div className={`detail-charts ${cursorMode ? 'with-memo-panel' : ''}`}>
    {/* 既存のチャートコード */}
    <DetailChart
      ref={dailyChartRef}
      // ... existing props
      cursorTime={cursorMode && selectedBarData ? selectedBarData.time : null}
      onChartClick={handleDailyChartClick}
    />
  </div>
  
  {cursorMode && (
    <DailyMemoPanel
      code={code || ''}
      selectedDate={selectedDate}
      selectedBarData={selectedBarData}
      cursorMode={cursorMode}
      onToggleCursorMode={toggleCursorMode}
      onPrevDay={moveToPrevDay}
      onNextDay={moveToNextDay}
      onCopyForConsult={handleCopyForConsult}
    />
  )}
</div>
```

### ステップ5: CSS追加

`app/frontend/src/index.css` に以下を追加:

```css
.detail-content {
  display: flex;
  gap: 0;
  height: 100%;
}

.detail-charts {
  flex: 1;
  min-width: 0;
  transition: all 0.3s ease;
}

.detail-charts.with-memo-panel {
  flex: 1;
}

.daily-memo-panel {
  width: 400px;
  flex-shrink: 0;
  background: var(--bg-secondary);
  border-left: 1px solid var(--border-color);
  padding: 16px;
  overflow-y: auto;
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.memo-panel-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 12px;
}

.memo-panel-header h3 {
  margin: 0;
  font-size: 16px;
  font-weight: 600;
}

.cursor-mode-toggle {
  padding: 6px 12px;
  border-radius: 4px;
  border: 1px solid var(--border-color);
  background: var(--bg-primary);
  cursor: pointer;
  font-size: 13px;
  transition: all 0.2s;
}

.cursor-mode-toggle.active {
  background: var(--accent-color);
  color: white;
  border-color: var(--accent-color);
}

.memo-panel-info {
  flex: 1;
  overflow-y: auto;
}

.info-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16px;
}

.info-date {
  font-size: 15px;
  font-weight: 600;
}

.info-nav {
  display: flex;
  gap: 4px;
}

.nav-btn {
  padding: 4px 8px;
  border: 1px solid var(--border-color);
  background: var(--bg-primary);
  border-radius: 4px;
  cursor: pointer;
  font-size: 14px;
}

.nav-btn:hover {
  background: var(--bg-hover);
}

.info-section {
  margin-bottom: 12px;
  padding: 8px;
  background: var(--bg-primary);
  border-radius: 4px;
}

.info-label {
  font-size: 12px;
  color: var(--text-secondary);
  margin-bottom: 4px;
}

.info-values {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 4px;
  font-size: 13px;
}

.info-value {
  font-size: 13px;
}

.info-actions {
  margin-top: 16px;
}

.consult-copy-btn {
  width: 100%;
  padding: 10px;
  background: var(--accent-color);
  color: white;
  border: none;
  border-radius: 4px;
  cursor: pointer;
  font-size: 14px;
  font-weight: 500;
  transition: opacity 0.2s;
}

.consult-copy-btn:hover {
  opacity: 0.9;
}

.memo-panel-input {
  flex-shrink: 0;
  border-top: 1px solid var(--border-color);
  padding-top: 16px;
}

.memo-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 8px;
}

.memo-header label {
  font-size: 13px;
  font-weight: 500;
}

.memo-status {
  display: flex;
  gap: 8px;
  align-items: center;
  font-size: 11px;
}

.status-saving {
  color: var(--text-secondary);
}

.status-saved {
  color: var(--success-color, #10b981);
}

.status-error {
  color: var(--error-color, #ef4444);
}

.char-count {
  color: var(--text-secondary);
}

.char-count.warning {
  color: var(--warning-color, #f59e0b);
  font-weight: 600;
}

.memo-textarea {
  width: 100%;
  padding: 8px;
  border: 1px solid var(--border-color);
  border-radius: 4px;
  background: var(--bg-primary);
  color: var(--text-primary);
  font-size: 13px;
  font-family: inherit;
  resize: vertical;
  min-height: 60px;
}

.memo-textarea:focus {
  outline: none;
  border-color: var(--accent-color);
}

.memo-error {
  margin-top: 4px;
  font-size: 12px;
  color: var(--error-color, #ef4444);
}

.memo-panel-empty {
  padding: 32px 16px;
  text-align: center;
  color: var(--text-secondary);
  font-size: 14px;
}
```

## 実装の優先順位

1. ✅ 状態変数とインポート (完了)
2. ⏳ カーソルモード関数追加
3. ⏳ キーボードハンドラ追加
4. ⏳ レイアウト変更
5. ⏳ CSS追加

## 注意事項

- DetailChart.tsxに `onChartClick` プロップを追加する必要がある
- `dailyVisibleRange` 状態が必要 (既存のコードを確認)
- `dailyCandles` と `dailyVolume` が正しく計算されているか確認

## テスト項目

1. カーソルモードON/OFF切替
2. ←/→キーで日付移動
3. チャートクリックで日付選択
4. メモの自動保存
5. 相談用コピー機能
6. 画面遷移時のメモ保存
