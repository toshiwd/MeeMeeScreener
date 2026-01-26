
import { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { IconX, IconTrendingUp, IconChartArrows } from "@tabler/icons-react";
import { api } from "../api";

type SimilarSearchPanelProps = {
    isOpen: boolean;
    onClose: () => void;
    queryTicker: string | null;
    queryAsOf?: string | null; // YYYY-MM-DD
};

type SearchResult = {
    ticker: string;
    asof: string;
    score_total: number;
    score60: number;
    score24: number;
    tag_id: string;
    tags: {
        ma20: string;
        ma60: string;
        dir: string;
        range: string;
        fallback: string;
    };
    vec60?: number[];
    vec24?: number[];
};

type RefreshStatus = {
    running: boolean;
    started_at?: string | null;
    finished_at?: string | null;
    error?: string | null;
    mode?: string | null;
};

export default function SimilarSearchPanel({
    isOpen,
    onClose,
    queryTicker,
    queryAsOf
}: SimilarSearchPanelProps) {
    const navigate = useNavigate();
    const [results, setResults] = useState<SearchResult[]>([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [alpha, setAlpha] = useState(0.7);
    const [refreshStatus, setRefreshStatus] = useState<RefreshStatus | null>(null);
    const [refreshMessage, setRefreshMessage] = useState<string | null>(null);
    const [refreshBusy, setRefreshBusy] = useState(false);
    const getTodayInputValue = () => {
        const today = new Date();
        const yyyy = today.getFullYear();
        const mm = String(today.getMonth() + 1).padStart(2, "0");
        const dd = String(today.getDate()).padStart(2, "0");
        return `${yyyy}-${mm}-${dd}`;
    };

    const [targetDate, setTargetDate] = useState<string>(() => {
        if (queryAsOf) return queryAsOf;
        return getTodayInputValue();
    });

    useEffect(() => {
        if (queryAsOf) {
            setTargetDate(queryAsOf);
            return;
        }
        setTargetDate(getTodayInputValue());
    }, [queryAsOf]);

    const fetchRefreshStatus = () => {
        api
            .get("/search/similar/status")
            .then((res) => {
                setRefreshStatus(res.data?.status ?? null);
            })
            .catch(() => {
                setRefreshStatus(null);
            });
    };

    useEffect(() => {
        if (!isOpen) return;
        fetchRefreshStatus();
    }, [isOpen]);

    useEffect(() => {
        if (!isOpen || !queryTicker) return;

        setLoading(true);
        setError(null);
        setResults([]);

        const params: any = {
            ticker: queryTicker,
            alpha,
            k: 30
        };
        if (targetDate) {
            params.asof = targetDate;
        }

        api
            .get("/search/similar", { params })
            .then((res) => {
                setResults(res.data);
            })
            .catch((err) => {
                const detail = err.response?.data?.detail as string | undefined;
                if (err.response && err.response.status === 404) {
                    // Custom message for not indexed
                    setError(detail || "検索対象外の銘柄です");
                } else {
                    setError(detail || err.message || "Search failed");
                }
            })
            .finally(() => {
                setLoading(false);
            });
    }, [isOpen, queryTicker, targetDate, alpha]);

    if (!isOpen) return null;

    return (
        <div className="tech-filter-shell is-visible">
            <div className="tech-filter-backdrop" onClick={onClose} />
            <div className="tech-filter-drawer is-open" style={{ width: "600px" }}>
                {/* Header */}
                <div className="tech-filter-header">
                    <div className="tech-filter-header-top">
                        <div className="tech-filter-header-title">
                            <span className="tech-filter-header-icon">
                                <IconChartArrows size={14} />
                            </span>
                            類似チャート検索
                        </div>
                        <button className="tech-filter-header-close" onClick={onClose}>
                            <IconX size={20} />
                        </button>
                    </div>
                    <div className="tech-filter-header-meta">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                            <span>対象: {queryTicker}</span>
                            <input
                                type="date"
                                value={targetDate}
                                onChange={(e) => setTargetDate(e.target.value)}
                                style={{
                                    background: 'var(--theme-bg-tertiary)',
                                    border: '1px solid var(--theme-border)',
                                    color: 'var(--theme-text-primary)',
                                    borderRadius: '4px',
                                    padding: '2px 6px',
                                    fontSize: '12px'
                                }}
                            />
                            <span style={{ fontSize: '11px', color: 'var(--theme-text-secondary)' }}>
                                {targetDate ? "" : "(未指定: 直近)"}
                            </span>
                        </div>
                    </div>

                    {/* Controls */}
                    <div className="tech-filter-row" style={{ marginTop: '8px' }}>
                        <div className="tech-filter-row-header">
                            <span className="tech-filter-hint">
                                重視: 長期(60ヶ月) {Math.round(alpha * 100)}% / 短期(12ヶ月) {Math.round((1 - alpha) * 100)}%
                            </span>
                            <input
                                type="range"
                                min="0" max="1" step="0.1"
                                value={alpha}
                                onChange={(e) => setAlpha(parseFloat(e.target.value))}
                                style={{ width: '150px' }}
                            />
                            <button
                                type="button"
                                className="tech-filter-row-remove"
                                disabled={refreshBusy || refreshStatus?.running}
                                onClick={() => {
                                    setRefreshBusy(true);
                                    setRefreshMessage(null);
                                    api
                                        .post("/search/similar/refresh", null, { params: { mode: "incremental" } })
                                        .then(() => {
                                            setRefreshMessage("類似検索データを更新開始しました");
                                            fetchRefreshStatus();
                                        })
                                        .catch((err) => {
                                            if (err.response?.status === 409) {
                                                setRefreshMessage("更新処理は既に実行中です");
                                            } else {
                                                setRefreshMessage(err.response?.data?.detail || "更新の開始に失敗しました");
                                            }
                                        })
                                        .finally(() => {
                                            setRefreshBusy(false);
                                        });
                                }}
                            >
                                更新
                            </button>
                        </div>
                        <div className="tech-filter-row-preview">
                            0に近いほど短期重視、1に近いほど長期重視になります。
                        </div>
                        {(refreshMessage || refreshStatus) && (
                            <div className="tech-filter-row-preview">
                                {refreshMessage && <div>{refreshMessage}</div>}
                                {refreshStatus && (
                                    <div>
                                        {refreshStatus.running
                                            ? "更新中..."
                                            : refreshStatus.finished_at
                                                ? `最終更新: ${refreshStatus.finished_at}`
                                                : "未更新"}
                                        {refreshStatus.mode ? ` [${refreshStatus.mode}]` : ""}
                                        {refreshStatus.error ? ` (エラー: ${refreshStatus.error})` : ""}
                                    </div>
                                )}
                            </div>
                        )}
                    </div>
                </div>

                {/* Body */}
                <div className="tech-filter-body" style={{ display: 'flex', flexDirection: 'column', overflowY: 'auto' }}>
                    {loading && (
                        <div style={{ padding: '20px', textAlign: 'center', color: 'var(--theme-text-secondary)' }}>
                            検索中...
                        </div>
                    )}

                    {error && (
                        <div style={{ padding: '20px', color: '#ef4444' }}>
                            エラー: {error}
                        </div>
                    )}

                    {!loading && !error && results.length === 0 && (
                        <div style={{ padding: '20px', textAlign: 'center', color: 'var(--theme-text-muted)' }}>
                            類似したチャートは見つかりませんでした。
                        </div>
                    )}

                    {!loading && results.map((item, idx) => (
                        <ResultItem
                            key={`${item.ticker}-${item.asof}`}
                            item={item}
                            rank={idx + 1}
                            onJump={() => navigate(`/detail/${item.ticker}`)}
                            onCompare={() => {
                                if (!queryTicker) return;
                                if (typeof window !== "undefined") {
                                    try {
                                        const payload = {
                                            queryTicker,
                                            mainAsOf: targetDate || null,
                                            items: results.map((result) => ({
                                                ticker: result.ticker,
                                                asof: result.asof ?? null
                                            }))
                                        };
                                        window.sessionStorage.setItem(
                                            "similarCompareList",
                                            JSON.stringify(payload)
                                        );
                                    } catch {
                                        // ignore storage errors
                                    }
                                }
                                const params = new URLSearchParams();
                                params.set("compare", item.ticker);
                                if (targetDate) {
                                    params.set("mainAsOf", targetDate);
                                }
                                if (item.asof) {
                                    params.set("compareAsOf", item.asof);
                                }
                                navigate(`/detail/${queryTicker}?${params.toString()}`);
                            }}
                        />
                    ))}
                </div>
            </div>
        </div>
    );
}

function ResultItem({
    item,
    rank,
    onJump,
    onCompare
}: {
    item: SearchResult;
    rank: number;
    onJump: () => void;
    onCompare: () => void;
}) {
    // Sparkline
    const sparkline = useMemo(() => {
        if (!item.vec60 || item.vec60.length === 0) return null;
        const values = item.vec60;
        const width = 200;
        const height = 40;
        const min = Math.min(...values);
        const max = Math.max(...values);
        const range = max - min || 1;

        const points = values.map((v, i) => {
            const x = (i / (values.length - 1)) * width;
            const y = height - ((v - min) / range) * height; // Invert Y
            return `${x},${y}`;
        }).join(" ");

        return (
            <svg width={width} height={height} style={{ overflow: 'visible' }}>
                <polyline
                    points={points}
                    fill="none"
                    stroke="var(--theme-accent)"
                    strokeWidth="1.5"
                />
            </svg>
        );
    }, [item.vec60]);

    return (
        <div
            className="tech-filter-row"
            style={{
                margin: '8px 16px',
                display: 'grid',
                gridTemplateColumns: '40px 100px 1fr 120px',
                alignItems: 'center',
                gap: '12px',
                cursor: 'pointer'
            }}
            onClick={onJump}
        >
            <div style={{ fontSize: '14px', fontWeight: 'bold', color: 'var(--theme-text-secondary)' }}>
                #{rank}
            </div>
            <div>
                <div style={{ fontWeight: 'bold' }}>{item.ticker}</div>
                <div style={{ fontSize: '11px', color: 'var(--theme-text-muted)' }}>{item.asof}</div>
            </div>

            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                {sparkline}
            </div>

            <div style={{ textAlign: 'right' }}>
                <div style={{ fontWeight: 'bold', color: 'var(--theme-accent)' }}>
                    {(item.score_total * 100).toFixed(1)}%
                </div>
                <div style={{ fontSize: '10px', color: 'var(--theme-text-muted)' }}>
                    {getFallbackText(item.tags.fallback)}
                </div>
                <button
                    type="button"
                    className="tech-filter-row-remove"
                    style={{ marginTop: '6px' }}
                    onClick={(event) => {
                        event.stopPropagation();
                        onCompare();
                    }}
                >
                    比較
                </button>
            </div>
        </div>
    );
}

function getFallbackText(text: string) {
    if (text === "Level 0 (Exact)") return "完全一致";
    if (text === "Level 1 (Ignore Range)") return "⚠️ レベル1 (レンジ無視)";
    if (text === "Level 2 (Ignore Dir)") return "⚠️ レベル2 (方向無視)";
    if (text === "Level 3 (MA60 Only)") return "⚠️ レベル3 (MA60のみ)";
    if (text === "Level 4 (All)") return "⚠️ レベル4 (全探索)";
    return text;
}
