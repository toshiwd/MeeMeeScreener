import { useEffect, useMemo, useRef, useState } from "react";
import TopNav from "./TopNav";
import { useStore } from "../store";
import { formatEventDateYmd, parseEventDateMs } from "../utils/events";

type ListTimeframe = "monthly" | "weekly" | "daily";

type SortOption = {
  value: string;
  label: string;
};

type FilterItem = {
  key: string;
  label: string;
  checked: boolean;
  onToggle: () => void;
};

type UnifiedListHeaderProps = {
  timeframe: ListTimeframe;
  onTimeframeChange: (value: ListTimeframe) => void;
  rangeBars: number;
  onRangeChange: (value: number) => void;
  search: string;
  onSearchChange: (value: string) => void;
  searchPlaceholder?: string;
  sortValue: string;
  sortOptions: SortOption[];
  onSortChange: (value: string) => void;
  columns: 1 | 2 | 3 | 4;
  rows: 1 | 2 | 3 | 4 | 5 | 6;
  onColumnsChange: (value: 1 | 2 | 3 | 4) => void;
  onRowsChange: (value: 1 | 2 | 3 | 4 | 5 | 6) => void;
  filterItems?: FilterItem[];
  filterLabel?: string;
  onHelpClick?: () => void;
  helpLabel?: string;
};

const LABELS = {
  monthly: "\u6708",
  weekly: "\u9031",
  daily: "\u65e5",
  searchPlaceholder: "\u30b3\u30fc\u30c9/\u9298\u67c4\u540d\u3067\u691c\u7d22",
  clear: "\u30af\u30ea\u30a2",
  sortFallback: "\u672a\u9078\u629e",
  sort: "\u4e26\u3073\u66ff\u3048",
  displayDensity: "\u8868\u793a\u5bc6\u5ea6",
  columns: "\u5217\u6570",
  rows: "\u884c\u6570",
  resetDensity: "3x3\u306b\u623b\u3059",
  filter: "\u30d5\u30a3\u30eb\u30bf",
  help: "\u76f8\u8ac7",
  menu: "\u30e1\u30cb\u30e5\u30fc",
  selected: "\u9078\u629e\u4e2d",
  active: "\u9069\u7528\u4e2d",
  actions: "\u64cd\u4f5c",
  refreshEvents: "\u30a4\u30d9\u30f3\u30c8\u66f4\u65b0"
};

const rangeOptions = [
  { label: "60本", count: 60 },
  { label: "120本", count: 120 },
  { label: "240本", count: 240 },
  { label: "360本", count: 360 }
];

export default function UnifiedListHeader({
  timeframe,
  onTimeframeChange,
  rangeBars,
  onRangeChange,
  search,
  onSearchChange,
  searchPlaceholder,
  sortValue,
  sortOptions,
  onSortChange,
  columns,
  rows,
  onColumnsChange,
  onRowsChange,
  filterItems,
  filterLabel,
  onHelpClick,
  helpLabel
}: UnifiedListHeaderProps) {
  const [sortOpen, setSortOpen] = useState(false);
  const [densityOpen, setDensityOpen] = useState(false);
  const [filterOpen, setFilterOpen] = useState(false);
  const [moreOpen, setMoreOpen] = useState(false);
  const sortRef = useRef<HTMLDivElement | null>(null);
  const densityRef = useRef<HTMLDivElement | null>(null);
  const filterRef = useRef<HTMLDivElement | null>(null);
  const moreRef = useRef<HTMLDivElement | null>(null);
  const eventsMeta = useStore((state) => state.eventsMeta);
  const refreshEvents = useStore((state) => state.refreshEvents);

  const filterItemsSafe = filterItems ?? [];
  const hasFilters = filterItemsSafe.length > 0;
  const isFiltering = filterItemsSafe.some((item) => item.checked);

  const sortLabel = useMemo(() => {
    const match = sortOptions.find((option) => option.value === sortValue);
    return match?.label ?? LABELS.sortFallback;
  }, [sortOptions, sortValue]);

  const eventsLastSuccessLabel = useMemo(() => {
    const earningsMs = parseEventDateMs(eventsMeta?.earningsLastSuccessAt);
    const rightsMs = parseEventDateMs(eventsMeta?.rightsLastSuccessAt);
    const candidates = [
      { value: eventsMeta?.earningsLastSuccessAt ?? null, ms: earningsMs },
      { value: eventsMeta?.rightsLastSuccessAt ?? null, ms: rightsMs }
    ].filter((item) => item.value && item.ms != null) as { value: string; ms: number }[];
    if (!candidates.length) return null;
    const oldest = candidates.reduce((prev, next) => (next.ms < prev.ms ? next : prev));
    return formatEventDateYmd(oldest.value);
  }, [eventsMeta]);

  const rightsCoverageLabel = useMemo(() => {
    const rightsMaxDate = eventsMeta?.dataCoverage?.rightsMaxDate ?? null;
    const maxMs = parseEventDateMs(rightsMaxDate);
    if (!rightsMaxDate || maxMs == null) return null;
    const thresholdMs = Date.now() + 30 * 24 * 60 * 60 * 1000;
    if (maxMs >= thresholdMs) return null;
    const formatted = formatEventDateYmd(rightsMaxDate);
    return formatted ? `権利データ範囲: ～${formatted}` : null;
  }, [eventsMeta]);

  useEffect(() => {
    const handlePointer = (event: MouseEvent) => {
      const target = event.target as Node;
      if (sortRef.current?.contains(target)) return;
      if (densityRef.current?.contains(target)) return;
      if (filterRef.current?.contains(target)) return;
      if (moreRef.current?.contains(target)) return;
      setSortOpen(false);
      setDensityOpen(false);
      setFilterOpen(false);
      setMoreOpen(false);
    };
    const handleKey = (event: KeyboardEvent) => {
      if (event.key !== "Escape") return;
      setSortOpen(false);
      setDensityOpen(false);
      setFilterOpen(false);
      setMoreOpen(false);
    };
    document.addEventListener("mousedown", handlePointer);
    document.addEventListener("keydown", handleKey);
    return () => {
      document.removeEventListener("mousedown", handlePointer);
      document.removeEventListener("keydown", handleKey);
    };
  }, []);


  useEffect(() => {
    const handlePointer = (event: MouseEvent) => {
      const target = event.target as Node;
      if (sortRef.current?.contains(target)) return;
      if (densityRef.current?.contains(target)) return;
      if (filterRef.current?.contains(target)) return;
      if (moreRef.current?.contains(target)) return;
      setSortOpen(false);
      setDensityOpen(false);
      setFilterOpen(false);
      setMoreOpen(false);
    };
    const handleKey = (event: KeyboardEvent) => {
      if (event.key !== "Escape") return;
      setSortOpen(false);
      setDensityOpen(false);
      setFilterOpen(false);
      setMoreOpen(false);
    };
    document.addEventListener("mousedown", handlePointer);
    document.addEventListener("keydown", handleKey);
    return () => {
      document.removeEventListener("mousedown", handlePointer);
      document.removeEventListener("keydown", handleKey);
    };
  }, []);

  const searchPlaceholderText = searchPlaceholder ?? LABELS.searchPlaceholder;
  const filterLabelText = filterLabel ?? LABELS.filter;
  const helpLabelText = helpLabel ?? LABELS.help;

  return (
    <header className="unified-list-header">
      <div className="list-header-row">
        <div className="dynamic-header-row header-row-top">
          <div className="header-row-left">
            <TopNav />
          </div>

          <div className="list-header-actions-wrapper">
            <div className="list-header-actions">
              <div className="popover-anchor" ref={sortRef}>
                <button
                  type="button"
                  className={`sort-button ${sortOpen ? "is-sorting" : ""}`}
                  onClick={() => {
                    setSortOpen((prev) => !prev);
                    setDensityOpen(false);
                    setFilterOpen(false);
                    setMoreOpen(false);
                  }}
                >
                  {`${LABELS.sort}\uFF1A${sortLabel}`}
                  <span className="caret">{"\u25BC"}</span>
                </button>
                {sortOpen && (
                  <div className="popover-panel">
                    <div className="popover-section">
                      <div className="popover-title">{LABELS.sort}</div>
                      <div className="popover-list">
                        {sortOptions.map((option) => (
                          <button
                            type="button"
                            key={option.value}
                            className={
                              sortValue === option.value ? "popover-item active" : "popover-item"
                            }
                            onClick={() => {
                              onSortChange(option.value);
                              setSortOpen(false);
                            }}
                          >
                            <span>{option.label}</span>
                            {sortValue === option.value && (
                              <span className="popover-status">{LABELS.selected}</span>
                            )}
                          </button>
                        ))}
                      </div>
                    </div>
                  </div>
                )}
              </div>
              <div className="popover-anchor" ref={densityRef}>
                <button
                  type="button"
                  className="display-button"
                  onClick={() => {
                    setDensityOpen((prev) => !prev);
                    setSortOpen(false);
                    setFilterOpen(false);
                    setMoreOpen(false);
                  }}
                >
                  {`${LABELS.displayDensity} ${columns}x${rows}`}
                  <span className="caret">{"\u25BC"}</span>
                </button>
                {densityOpen && (
                  <div className="popover-panel">
                    <div className="popover-section">
                      <div className="popover-title">{LABELS.columns}</div>
                      <div className="segmented">
                        {[1, 2, 3, 4].map((count) => (
                          <button
                            key={count}
                            className={columns === count ? "active" : ""}
                            onClick={() => onColumnsChange(count as 1 | 2 | 3 | 4)}
                          >
                            {`${count}\u5217`}
                          </button>
                        ))}
                      </div>
                    </div>
                    <div className="popover-section">
                      <div className="popover-title">{LABELS.rows}</div>
                      <div className="segmented">
                        {[1, 2, 3, 4, 5, 6].map((count) => (
                          <button
                            key={count}
                            className={rows === count ? "active" : ""}
                            onClick={() => onRowsChange(count as 1 | 2 | 3 | 4 | 5 | 6)}
                          >
                            {`${count}\u884c`}
                          </button>
                        ))}
                      </div>
                    </div>
                    <div className="popover-section">
                      <button
                        type="button"
                        className="popover-reset"
                        onClick={() => {
                          onColumnsChange(3);
                          onRowsChange(3);
                        }}
                      >
                        {LABELS.resetDensity}
                      </button>
                    </div>
                  </div>
                )}
              </div>
              <div className="popover-anchor" ref={filterRef}>
                <button
                  type="button"
                  className={`filter-button ${isFiltering ? "is-filtering" : ""}`}
                  onClick={() => {
                    if (!hasFilters) return;
                    setFilterOpen((prev) => !prev);
                    setSortOpen(false);
                    setDensityOpen(false);
                    setMoreOpen(false);
                  }}
                  disabled={!hasFilters}
                >
                  {filterLabelText}
                </button>
                {filterOpen && hasFilters && (
                  <div className="popover-panel">
                    <div className="popover-section">
                      <div className="popover-title">{filterLabelText}</div>
                      <div className="popover-list">
                        {filterItemsSafe.map((item) => (
                          <button
                            type="button"
                            key={item.key}
                            className={item.checked ? "popover-item active" : "popover-item"}
                            onClick={item.onToggle}
                          >
                            <span>{item.label}</span>
                            {item.checked && (
                              <span className="popover-status">{LABELS.active}</span>
                            )}
                          </button>
                        ))}
                      </div>
                    </div>
                  </div>
                )}
              </div>
              <button
                type="button"
                className="help-button"
                onClick={() => onHelpClick?.()}
                disabled={!onHelpClick}
              >
                {helpLabelText}
              </button>
            </div>

            <div className="list-header-more popover-anchor" ref={moreRef}>
              <button
                type="button"
                className="more-button"
                aria-label={LABELS.menu}
                onClick={() => {
                  setMoreOpen((prev) => !prev);
                  setSortOpen(false);
                  setDensityOpen(false);
                  setFilterOpen(false);
                }}
              >
                {"\u2026"}
              </button>
              {moreOpen && (
                <div className="popover-panel list-header-menu">
                  <div className="popover-section">
                    <div className="popover-title">{LABELS.sort}</div>
                    <div className="popover-list">
                      {sortOptions.map((option) => (
                        <button
                          type="button"
                          key={`more-${option.value}`}
                          className={
                            sortValue === option.value
                              ? "popover-item active"
                              : "popover-item"
                          }
                          onClick={() => {
                            onSortChange(option.value);
                            setMoreOpen(false);
                          }}
                        >
                          <span>{option.label}</span>
                          {sortValue === option.value && (
                            <span className="popover-status">{LABELS.selected}</span>
                          )}
                        </button>
                      ))}
                    </div>
                  </div>
                  <div className="popover-section">
                    <div className="popover-title">{LABELS.displayDensity}</div>
                    <div className="popover-list">
                      <div className="popover-group">
                        <div className="popover-group-title">{LABELS.columns}</div>
                        <div className="segmented">
                          {[1, 2, 3, 4].map((count) => (
                            <button
                              key={`more-col-${count}`}
                              className={columns === count ? "active" : ""}
                              onClick={() =>
                                onColumnsChange(count as 1 | 2 | 3 | 4)
                              }
                            >
                              {`${count}\u5217`}
                            </button>
                          ))}
                        </div>
                      </div>
                      <div className="popover-group">
                        <div className="popover-group-title">{LABELS.rows}</div>
                        <div className="segmented">
                          {[1, 2, 3, 4, 5, 6].map((count) => (
                            <button
                              key={`more-row-${count}`}
                              className={rows === count ? "active" : ""}
                              onClick={() =>
                                onRowsChange(
                                  count as 1 | 2 | 3 | 4 | 5 | 6
                                )
                              }
                            >
                              {`${count}\u884c`}
                            </button>
                          ))}
                        </div>
                      </div>
                      <button
                        type="button"
                        className="popover-reset"
                        onClick={() => {
                          onColumnsChange(3);
                          onRowsChange(3);
                          setMoreOpen(false);
                        }}
                      >
                        {LABELS.resetDensity}
                      </button>
                    </div>
                  </div>
                  {hasFilters && (
                    <div className="popover-section">
                      <div className="popover-title">{filterLabelText}</div>
                      <div className="popover-list">
                        {filterItemsSafe.map((item) => (
                          <button
                            type="button"
                            key={`more-filter-${item.key}`}
                            className={
                              item.checked ? "popover-item active" : "popover-item"
                            }
                            onClick={item.onToggle}
                          >
                            <span>{item.label}</span>
                            {item.checked && (
                              <span className="popover-status">{LABELS.active}</span>
                            )}
                          </button>
                        ))}
                      </div>
                    </div>
                  )}
                  <div className="popover-section">
                    <div className="popover-title">{LABELS.actions}</div>
                    <div className="popover-list">
                      <button
                        type="button"
                        className="popover-item"
                        disabled={eventsMeta?.isRefreshing}
                        onClick={() => {
                          void refreshEvents();
                          setMoreOpen(false);
                        }}
                      >
                        <span>
                          {eventsMeta?.isRefreshing
                            ? "\u30a4\u30d9\u30f3\u30c8\u66f4\u65b0\u4e2d..."
                            : LABELS.refreshEvents}
                        </span>
                      </button>
                      <button
                        type="button"
                        className="popover-item"
                        disabled={!onHelpClick}
                        onClick={() => {
                          onHelpClick?.();
                          setMoreOpen(false);
                        }}
                      >
                        <span>{helpLabelText}</span>
                      </button>
                    </div>
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>

        <div className="header-row-bottom">
          <div className="segmented list-timeframe">
            {(["monthly", "weekly", "daily"] as const).map((value) => (
              <button
                key={value}
                className={timeframe === value ? "active" : ""}
                onClick={() => onTimeframeChange(value)}
              >
                {value === "monthly"
                  ? LABELS.monthly
                  : value === "weekly"
                    ? LABELS.weekly
                    : LABELS.daily}
              </button>
            ))}
          </div>
          <div className="segmented segmented-compact list-range">
            {rangeOptions.map((option) => (
              <button
                key={option.label}
                className={rangeBars === option.count ? "active" : ""}
                onClick={() => onRangeChange(option.count)}
              >
                {option.label}
              </button>
            ))}
          </div>
          <div className="search-field list-search">
            <input
              className="search-input"
              placeholder={searchPlaceholderText}
              value={search}
              onChange={(event) => onSearchChange(event.target.value)}
            />
            {search && (
              <button
                type="button"
                className="search-clear"
                onClick={() => onSearchChange("")}
              >
                {LABELS.clear}
              </button>
            )}
          </div>
          <div className="list-events-inline">
            <span className="event-meta-status">
              状態: {eventsMeta?.isRefreshing ? "更新中" : "待機中"}
            </span>
            {eventsMeta?.lastError && (
              <span className="event-meta-error" title={eventsMeta.lastError}>
                エラー: {eventsMeta.lastError}
              </span>
            )}
            <span className="event-meta-last">
              イベント最終更新: {eventsLastSuccessLabel ?? "--"}
            </span>
            {rightsCoverageLabel && <span className="event-meta-rights">{rightsCoverageLabel}</span>}
          </div>
        </div>
      </div>

    </header>
  );
}
