$path = "c:\work\meemee-screener\app\frontend\src\routes\GridView.tsx"
$lines = Get-Content $path -Encoding UTF8

# Imports
$imports = @'
import IconButton from "../components/IconButton";
import {
  IconMessage,
  IconArrowsSort,
  IconLayoutGrid,
  IconFilter,
  IconRefresh,
  IconSettings,
  IconMoon,
  IconSun,
  IconUpload,
  IconDownload,
  IconFileText
} from "@tabler/icons-react";
'@

# New Header HTML
$headerHtml = @'
      <header className="unified-list-header">
        <div className="list-header-row">
          <div className="header-row-top">
            <TopNav />
            <div className="list-header-actions">
              {keepList.length > 0 && (
                <button
                  type="button"
                  className={`consult-trigger ${consultVisible ? "active" : ""}`}
                  onClick={() => setConsultVisible(!consultVisible)}
                >
                  <IconMessage size={16} />
                  <span>相談</span>
                  <span className="badge">{keepList.length}</span>
                </button>
              )}
              <div className="list-header-spacer" style={{ width: 8 }} />
              <div className="popover-anchor" ref={sortRef}>
                <IconButton
                  icon={<IconArrowsSort size={18} />}
                  label={`並び: ${sortLabel}`}
                  variant="iconLabel"
                  tooltip="並び替え"
                  ariaLabel="並び替えメニューを開く"
                  active={sortOpen}
                  onClick={() => {
                    setSortOpen(!sortOpen);
                    setDisplayOpen(false);
                    setSettingsOpen(false);
                  }}
                />
                {sortOpen && (
                  <div className="popover-panel">
                    {(isCandidateView ? candidateSortSections : sortSections).map((section) => (
                      <div className="popover-section" key={section.title}>
                        <div className="popover-title">{section.title}</div>
                        <div className="popover-grid">
                          {section.options.map((opt) => (
                            <button
                              key={opt.key}
                              type="button"
                              className={`popover-item ${sortKey === opt.key ? "active" : ""}`}
                              onClick={() => {
                                if (sortKey === opt.key) {
                                  setSortDir(sortDir === "asc" ? "desc" : "asc");
                                } else {
                                  setSortKey(opt.key);
                                  setSortDir("desc");
                                }
                                setSortOpen(false);
                              }}
                            >
                              <span className="popover-item-label">{opt.label}</span>
                              {sortKey === opt.key && (
                                <span className="popover-check">{sortDirLabel}</span>
                              )}
                            </button>
                          ))}
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
              <div className="popover-anchor" ref={displayRef}>
                <IconButton
                  icon={<IconLayoutGrid size={18} />}
                  label="表示"
                  variant="iconLabel"
                  tooltip="表示設定"
                  ariaLabel="表示設定メニューを開く"
                  active={displayOpen}
                  onClick={() => {
                    setDisplayOpen(!displayOpen);
                    setSortOpen(false);
                    setSettingsOpen(false);
                  }}
                />
                {displayOpen && (
                  <div className="popover-panel">
                    <div className="popover-section">
                      <div className="popover-title">行数</div>
                      <div className="segmented">
                        {[1, 2, 3, 4, 5, 6].map((r) => (
                          <button
                            key={r}
                            className={rows === r ? "active" : ""}
                            onClick={() => setRows(r as any)}
                          >
                            {r}
                          </button>
                        ))}
                      </div>
                    </div>
                    <div className="popover-section">
                      <div className="popover-title">列数</div>
                      <div className="segmented">
                        {[1, 2, 3, 4].map((c) => (
                          <button
                            key={c}
                            className={columns === c ? "active" : ""}
                            onClick={() => setColumns(c as any)}
                          >
                            {c}
                          </button>
                        ))}
                      </div>
                    </div>
                    <div className="popover-section">
                      <button
                        className="popover-item"
                        onClick={() => {
                          setRows(3);
                          setColumns(3);
                          setDisplayOpen(false);
                        }}
                      >
                        <span className="popover-item-label">3x3に戻す</span>
                      </button>
                    </div>
                    <div className="popover-section">
                      <div className="popover-title">表示オプション</div>
                      <button
                        className={`popover-item ${showBoxes ? "active" : ""}`}
                        onClick={() => setShowBoxes(!showBoxes)}
                      >
                        <span className="popover-item-label">ボックス枠を表示</span>
                        {showBoxes && <span className="popover-check">ON</span>}
                      </button>
                      <button
                        className={`popover-item ${showIndicators ? "active" : ""}`}
                        onClick={() => {
                          setShowIndicators(!showIndicators);
                          setDisplayOpen(false);
                        }}
                      >
                        <span className="popover-item-label">インジケーター設定</span>
                      </button>
                      <button
                        className={`popover-item ${maSettings[gridTimeframe].some(s => s.visible) ? "active" : ""}`}
                        onClick={() => {
                          if (maSettings[gridTimeframe].some(s => s.visible)) {
                            const newState = maSettings[gridTimeframe].map(s => ({ ...s, visible: false }));
                            newState.forEach((s, i) => updateMaSetting(gridTimeframe, i, { visible: false }));
                          } else {
                            resetMaSettings(gridTimeframe);
                          }
                        }}
                      >
                        <span className="popover-item-label">MA一括表示切替</span>
                        <span className="popover-status">
                          {maSettings[gridTimeframe].some(s => s.visible) ? "ON" : "OFF"}
                        </span>
                      </button>
                    </div>
                  </div>
                )}
              </div>
              <div className="popover-anchor">
                <IconButton
                  icon={<IconFilter size={18} />}
                  label="フィルタ"
                  active={techFilterActive.conditions.length > 0}
                  variant="iconLabel"
                  onClick={() => setTechFilterOpen(true)}
                />
              </div>
              <div className="txt-update-group">
                <IconButton
                  icon={<IconRefresh size={18} />}
                  label={isUpdatingTxt ? "更新中" : "TXT更新"}
                  variant="iconLabel"
                  tooltip="TXT更新"
                  ariaLabel="TXT更新"
                  className={`txt-update-button ${isUpdatingTxt ? "is-updating" : ""}`}
                  onClick={handleUpdateTxt}
                  disabled={backendReady === false || isUpdatingTxt}
                />
                {(updateProgressLabel || lastUpdatedLabel) && (
                  <div className="txt-update-meta">
                    <span>{updateProgressLabel ?? "更新待ち"}</span>
                    <span>最終更新：{lastUpdatedLabel ?? "--"}</span>
                  </div>
                )}
              </div>
              <div className="popover-anchor" ref={settingsRef}>
                <IconButton
                  icon={<IconSettings size={18} />}
                  tooltip="設定"
                  ariaLabel="設定"
                  onClick={() => {
                    setSettingsOpen(!settingsOpen);
                    setSortOpen(false);
                    setDisplayOpen(false);
                  }}
                />
                {settingsOpen && (
                  <div className="popover-panel popover-right-aligned" style={{ right: 0 }}>
                    <div className="popover-section">
                      <div className="popover-title">外観設定</div>
                      <div className="segmented">
                        <button
                          className={currentTheme === "dark" ? "active" : ""}
                          onClick={() => currentTheme !== "dark" && handleThemeToggle()}
                        >
                          <IconMoon size={16} />
                          <span>ダーク</span>
                        </button>
                        <button
                          className={currentTheme === "light" ? "active" : ""}
                          onClick={() => currentTheme !== "light" && handleThemeToggle()}
                        >
                          <IconSun size={16} />
                          <span>ライト</span>
                        </button>
                      </div>
                    </div>
                    <div className="popover-section">
                      <div className="popover-title">取引CSV</div>
                      <button
                        type="button"
                        className="popover-item"
                        onClick={handleTradeCsvPick}
                        disabled={tradeUploadInFlight}
                      >
                        <span className="popover-item-label">
                          <IconUpload size={16} />
                          <span>{tradeUploadInFlight ? "取り込み中..." : "CSV取り込み"}</span>
                        </span>
                        <span className="popover-status">手動</span>
                      </button>
                      <button
                        type="button"
                        className="popover-item"
                        onClick={handleForceTradeSync}
                        disabled={tradeSyncInFlight}
                      >
                        <span className="popover-item-label">
                          <IconRefresh size={16} />
                          <span>{tradeSyncInFlight ? "同期中..." : "強制同期"}</span>
                        </span>
                        <span className="popover-status">強制</span>
                      </button>
                      <div className="popover-hint">
                        保存先: %LOCALAPPDATA%\\MeeMeeScreener\\data\\
                      </div>
                    </div>
                    <div className="popover-section">
                      <div className="popover-title">銘柄一覧</div>
                      <button
                        type="button"
                        className="popover-item"
                        onClick={handleExportWatchlist}
                        disabled={watchlistExporting}
                      >
                        <span className="popover-item-label">
                          <IconDownload size={16} />
                          <span>{watchlistExporting ? "エクスポート中..." : "EXPORT"}</span>
                        </span>
                        <span className="popover-status">EBK</span>
                      </button>
                      <button type="button" className="popover-item" onClick={handleOpenCodeTxt}>
                        <span className="popover-item-label">
                          <IconFileText size={16} />
                          <span>code.txt</span>
                        </span>
                        <span className="popover-status">編集</span>
                      </button>
                    </div>
                    <div className="popover-section">
                      <div className="popover-title">イベント</div>
                      <button
                        type="button"
                        className="popover-item"
                        disabled={eventsMeta?.isRefreshing}
                        onClick={() => {
                          void refreshEvents();
                          setSettingsOpen(false);
                        }}
                      >
                        <span className="popover-item-label">
                          <IconRefresh size={16} />
                          <span>
                            {eventsMeta?.isRefreshing ? "更新中..." : "イベント更新"}
                          </span>
                        </span>
                        <span className="popover-status">手動</span>
                      </button>
                      <div className="popover-hint">
                        状態: {eventsMeta?.isRefreshing ? "更新中" : "待機中"}
                      </div>
                      <div className="popover-hint">
                        最終試行: {eventsAttemptLabel ?? "--"}
                      </div>
                      {eventsMeta?.lastError && (
                        <div className="popover-hint">エラー: {eventsMeta.lastError}</div>
                      )}
                    </div>
                  </div>
                )}
                <input
                  ref={tradeCsvInputRef}
                  type="file"
                  accept=".csv"
                  onChange={handleTradeCsvChange}
                  style={{ display: "none" }}
                />
              </div>
            </div>
          </div>
          <div className="header-row-bottom">
            <div className="list-timeframe">
              {(["monthly", "weekly", "daily"] as const).map((frame) => (
                <button
                  key={frame}
                  type="button"
                  className={gridTimeframe === frame ? "active" : ""}
                  onClick={() => setGridTimeframe(frame)}
                >
                  {frame === "daily"
                    ? "日足"
                    : frame === "weekly"
                    ? "週足"
                    : "月足"}
                </button>
              ))}
            </div>
            <div className="list-search">
              <input
                className="list-search-input"
                type="search"
                placeholder="コード / 銘柄名で検索"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
              />
              {canAddWatchlist && (
                <button type="button" onClick={() => addKeep(canAddWatchlist)}>
                  +
                </button>
              )}
            </div>
          </div>
        </div>
'@

# Locate Header
$headerStart = 0
$techFilterStart = 0
for ($i = 0; $i -lt $lines.Count; $i++) {
    if ($lines[$i] -match 'header className="top-bar"') {
        $headerStart = $i
    }
    if ($lines[$i] -match 'techFilterActive\.conditions\.length') {
        $techFilterStart = $i
        break
    }
}

$output = @()

# Add imports
$importsAdded = $false
for ($i = 0; $i -lt $headerStart; $i++) {
    $output += $lines[$i]
    if (-not $importsAdded -and $lines[$i] -match 'import TopNav from') {
        $output += $imports
        $importsAdded = $true
    }
}

# Add new header
$output += $headerHtml

# Add rest of file
for ($i = $techFilterStart; $i -lt $lines.Count; $i++) {
    $output += $lines[$i]
}

# Write back
$output | Set-Content $path -Encoding UTF8
