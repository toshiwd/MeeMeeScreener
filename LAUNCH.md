# MeeMee Screener - 起動方法

## 通常モード（本番用）

```powershell
.\run.ps1
```

または

```powershell
python -m app.desktop.launcher
```

## デバッグモード（開発用）

```powershell
.\run_debug.ps1
```

### デバッグモードで使える機能

- **F5キー**: ページをリロード
- **右クリック > 検証**: 開発者ツールを開く
- **Ctrl+Shift+I**: 開発者ツールを開く
- **Ctrl+Shift+C**: 要素を選択

### デバッグモードの使い方

1. `run_debug.ps1`でアプリを起動
2. エラーが発生したら、**Ctrl+Shift+I**で開発者ツールを開く
3. **Console**タブでエラーの詳細を確認
4. コードを修正したら、**F5**でリロード（再起動不要）

### 手動でデバッグモードを有効にする

環境変数`DEBUG=1`を設定してから起動：

```powershell
$env:DEBUG = "1"
python -m app.desktop.launcher
```

## トラブルシューティング

### "Something went wrong"エラーが出た場合

1. **Ctrl+Shift+I**で開発者ツールを開く
2. **Console**タブでエラーメッセージを確認
3. **F5**でリロードを試す
4. それでも直らない場合は、アプリを再起動

### フロントエンドを再ビルド

```powershell
cd app\frontend
npm run build
```

### バックエンドのみ起動（開発用）

```powershell
python -m app.backend.main
```

フロントエンドは別ターミナルで：

```powershell
cd app\frontend
npm run dev
```

ブラウザで `http://localhost:5173` を開く
