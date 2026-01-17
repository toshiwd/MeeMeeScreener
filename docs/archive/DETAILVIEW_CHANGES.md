# DetailView への変更点まとめ

## 追加する状態

```typescript
// カーソルモード関連
const [cursorMode, setCursorMode] = useState(false);
const [selectedBarIndex, setSelectedBarIndex] = useState<number | null>(null);
const [selectedDate, setSelectedDate] = useState<string | null>(null);
const [selectedBarData, setSelectedBarData] = useState<Candle | null>(null);
```

## 追加する関数

### 1. カーソルモード切替
```typescript
const toggleCursorMode = () => {
  setCursorMode(prev => !prev);
};
```

### 2. 日付移動
```typescript
const moveToPrevDay = () => {
  if (selectedBarIndex === null || selectedBarIndex <= 0) return;
  const newIndex = selectedBarIndex - 1;
  updateSelectedBar(newIndex);
};

const moveToNextDay = () => {
  if (selectedBarIndex === null || selectedBarIndex >= dailyCandles.length - 1) return;
  const newIndex = selectedBarIndex + 1;
  updateSelectedBar(newIndex);
};
```

### 3. バー選択更新
```typescript
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
```

### 4. 自動パン
```typescript
const autoPanToBar = (time: number) => {
  // Check if time is outside visible range
  // If so, adjust visible range to include it
  // This will be implemented in DetailView
};
```

### 5. チャートクリックハンドラ
```typescript
const handleDailyChartClick = (time: number | null) => {
  if (!cursorMode || time === null) return;
  
  // Find nearest bar index
  const index = dailyCandles.findIndex(c => c.time >= time);
  if (index >= 0) {
    updateSelectedBar(index);
  }
};
```

### 6. キーボードハンドラ
```typescript
useEffect(() => {
  const handleKeyDown = (e: KeyboardEvent) => {
    // Only handle if cursor mode is on and not typing in input
    if (!cursorMode || (e.target as HTMLElement).tagName === 'TEXTAREA') {
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
  
  window.addEventListener('keydown', handleKeyDown);
  return () => window.removeEventListener('keydown', handleKeyDown);
}, [cursorMode, selectedBarIndex, dailyCandles]);
```

### 7. 相談用コピー
```typescript
const handleCopyForConsult = async () => {
  if (!selectedDate || !selectedBarData) return;
  
  const consultData = {
    symbol: code || '',
    name: tickerName || '',
    date: selectedDate,
    ohlc: {
      open: selectedBarData.open,
      high: selectedBarData.high,
      low: selectedBarData.low,
      close: selectedBarData.close,
    },
    volume: dailyVolume.find(v => v.time === selectedBarData.time)?.value,
    // TODO: Add position, MA, signals data
    memo: '', // Will be fetched from DailyMemoPanel
  };
  
  const text = buildConsultCopyText(consultData);
  const success = await copyToClipboard(text);
  
  if (success) {
    setToastMessage('相談用データをコピーしました');
  } else {
    setToastMessage('コピーに失敗しました');
  }
};
```

## レイアウト変更

```tsx
<div className="detail-content">
  <div className="detail-charts">
    {/* 既存のチャート */}
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

## CSS追加

```css
.detail-content {
  display: flex;
  gap: 16px;
}

.detail-charts {
  flex: 1;
  min-width: 0;
}

.daily-memo-panel {
  width: 400px;
  flex-shrink: 0;
  background: var(--bg-secondary);
  border-left: 1px solid var(--border-color);
  padding: 16px;
  overflow-y: auto;
}
```
