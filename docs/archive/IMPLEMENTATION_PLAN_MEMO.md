# 実装計画: 日足カーソル＋メモ機能

## フェーズ1: バックエンド - メモAPI実装

### 1.1 データベーススキーマ
```sql
CREATE TABLE daily_memo (
    symbol TEXT NOT NULL,
    date DATE NOT NULL,
    timeframe TEXT NOT NULL DEFAULT 'D',
    memo TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, date, timeframe)
);
```

### 1.2 API エンドポイント
- GET `/api/memo?symbol={code}&date={YYYY-MM-DD}&timeframe=D`
- PUT `/api/memo` (body: {symbol, date, timeframe, memo})
- DELETE `/api/memo?symbol={code}&date={YYYY-MM-DD}&timeframe=D`

---

## フェーズ2: フロントエンド - UI構造

### 2.1 DetailView レイアウト変更
- 左: チャート領域 (既存)
- 右: 新規ペイン (380-420px固定幅)
  - 上段: 情報パネル (スクロール可)
  - 下段: メモ入力 (固定表示)

### 2.2 状態管理追加
```typescript
const [cursorMode, setCursorMode] = useState(false);
const [selectedBarIndex, setSelectedBarIndex] = useState<number | null>(null);
const [selectedDate, setSelectedDate] = useState<string | null>(null);
const [memo, setMemo] = useState("");
const [memoStatus, setMemoStatus] = useState<"idle" | "saving" | "saved" | "error">("idle");
const [lastSavedAt, setLastSavedAt] = useState<Date | null>(null);
```

---

## フェーズ3: カーソルモード実装

### 3.1 クリックハンドラ
- チャートクリック → 最寄りバーを検索 → selectedBarIndex更新
- 縦線表示 (DetailChart に props 追加)

### 3.2 キーボード操作
- ←/→: selectedBarIndex ± 1
- C: カーソルモードトグル
- Esc: カーソルモードOFF
- フォーカス管理: チャート領域 vs メモ入力欄

### 3.3 自動パン
- selectedBarIndex が visible range 外なら range を調整

---

## フェーズ4: メモ機能実装

### 4.1 オートセーブ
- デバウンス (800ms)
- 日付移動前の保存
- 画面遷移前の保存

### 4.2 100字制限
- input/paste イベントで制限
- 残り文字数表示

### 4.3 保存状態UI
- 保存中... / 保存済み HH:MM:SS / 保存失敗

---

## フェーズ5: 相談用コピー機能

### 5.1 データ収集
- 選択日付の OHLC, 出来高
- MA状態 (7/20/60)
- シグナル
- 建玉
- メモ

### 5.2 フォーマット生成
```
【銘柄】{symbol} {name}
【日付】{YYYY-MM-DD}（日足カーソル）
【建玉】売-買={sell}-{buy}
【MA】7:{状態} 20:{状態} 60:{状態}
【シグナル】{シグナル一覧}
【メモ】{memo または（コメントなし）}
```

---

## 実装順序

1. ✅ バックエンド: daily_memo テーブル作成
2. ✅ バックエンド: メモAPI実装
3. ✅ フロントエンド: 右ペインUI追加
4. ✅ フロントエンド: カーソルモード基本実装
5. ✅ フロントエンド: メモロード/保存実装
6. ✅ フロントエンド: オートセーブ実装
7. ✅ フロントエンド: 相談用コピー実装
8. ✅ テスト & デバッグ

---

## 技術的な注意点

### DetailChart への変更
- `selectedBarTime` prop 追加 → 縦線描画
- クリックイベントから時間を取得する方法を確認

### 日付計算
- bars 配列の index ベースで移動 (日付±1は禁止)
- 休場日は自動的にスキップされる

### キーずれ防止
- 保存時に symbol + date を必ず検証
- 別の日付に移動した後の遅延保存を防ぐ

---

## ファイル構成

### 新規作成
- `app/frontend/src/components/DailyMemoPanel.tsx` - 右ペインコンポーネント
- `app/frontend/src/hooks/useDailyMemo.ts` - メモ管理フック
- `app/frontend/src/utils/consultCopy.ts` - 相談用コピーユーティリティ

### 修正
- `app/backend/db.py` - daily_memo テーブル追加
- `app/backend/main.py` - メモAPI追加
- `app/frontend/src/routes/DetailView.tsx` - レイアウト & カーソルモード
- `app/frontend/src/components/DetailChart.tsx` - 縦線表示対応
