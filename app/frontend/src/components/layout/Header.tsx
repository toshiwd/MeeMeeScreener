import { useLocation, useNavigate, useParams } from 'react-router-dom';
import {
    IconArrowLeft,
    IconSearch,
    IconBulb,
    IconTrash,
    IconCamera,
    IconFileText,
    IconHeart
} from '@tabler/icons-react';
import { type AxiosError } from 'axios';
import { api } from '../../api';
import { useStore } from '../../store';
import { useEffect, useMemo, useState, type ComponentType } from 'react';

// This header is now "Dynamic" based on the route.
// In a more complex app, we might use a Context or Zustand store to let pages "teleport" content here.
// For now, we'll use simple route matching logic.

export function Header({ className }: { className?: string }) {
    const location = useLocation();
    const navigate = useNavigate();
    const { code } = useParams<{ code: string }>();
    const isDetail = location.pathname.startsWith('/detail/');
    const isPractice = location.pathname.startsWith('/practice/');
    const showBack = isDetail || isPractice;

    // Store integration
    const search = useStore((state) => state.settings.search);
    const setSearch = useStore((state) => state.setSearch);
    const tickers = useStore((state) => state.tickers);
    const favorites = useStore((state) => state.favorites);
    const setFavoriteLocal = useStore((state) => state.setFavoriteLocal);
    const loadFavorites = useStore((state) => state.loadFavorites);
    const favoritesLoaded = useStore((state) => state.favoritesLoaded);

    // Derived State
    const activeTicker = isDetail && code ? tickers.find((t) => t.code === code) : null;
    const tickerName = activeTicker?.name || "";
    const detailIsFavorite = useMemo(() => {
        if (!code) return false;
        return favorites.includes(code);
    }, [code, favorites]);
    const [toggleBusy, setToggleBusy] = useState(false);

    useEffect(() => {
        if (isDetail && !favoritesLoaded) {
            void loadFavorites();
        }
    }, [isDetail, favoritesLoaded, loadFavorites]);

    const handleDetailFavorite = async () => {
        if (!code || toggleBusy) return;
        const next = !detailIsFavorite;
        setFavoriteLocal(code, next);
        setToggleBusy(true);
        try {
            if (next) {
                await api.post(`/favorites/${encodeURIComponent(code)}`);
            } else {
                await api.delete(`/favorites/${encodeURIComponent(code)}`);
            }
        } catch (error) {
            setFavoriteLocal(code, detailIsFavorite);
            const axiosError = error as AxiosError | undefined;
            const status = axiosError?.response?.status;
            const detail =
                axiosError?.response?.data?.error ??
                axiosError?.response?.data ??
                axiosError?.message ??
                'unknown';
            console.error('Favorite update failed', {
                code,
                next,
                status,
                detail,
                error,
            });
        } finally {
            setToggleBusy(false);
        }
    };

    return (
        <header className={`dynamic-header ${className}`} style={{ background: 'var(--theme-bg-secondary)', borderColor: 'var(--theme-border)' }}>
            <div className="dynamic-header-row header-row-top">
                <div className="header-title-group">
                    <div className="header-nav-title">
                        {showBack && (
                            <button
                                onClick={() => navigate(-1)}
                                className="icon-button"
                                aria-label="Back"
                            >
                                <IconArrowLeft size={18} />
                            </button>
                        )}
                        <span className="header-brand">
                            MeeMee Screener{isDetail && tickerName ? ` · ${tickerName}` : ""}
                        </span>
                    </div>
                    {!isDetail && (
                        <div className="header-search">
                            <IconSearch className="header-search-icon" size={16} />
                            <input
                                type="text"
                                placeholder="Search ticker..."
                                value={search}
                                onChange={(e) => setSearch(e.target.value)}
                            />
                        </div>
                    )}
                </div>
                <div className="header-actions-row">
                    {!isDetail && <span className="updates-label">Updates: --</span>}
                    <ActionButton icon={IconFileText} label="Log" />
                </div>
            </div>

            <div className="dynamic-header-row header-row-bottom">
                {isDetail && (
                    <div className="header-detail-actions">
                        <ActionButton icon={IconBulb} label="Similar" />
                        <ActionButton icon={IconCamera} label="Shot" />
                        <ActionButton icon={IconHeart} label="Favorite" active={detailIsFavorite} onClick={handleDetailFavorite} />
                        <div className="header-divider" />
                        <ActionButton icon={IconTrash} label="Remove" color="text-red-600 hover:bg-red-50" />
                    </div>
                )}
            </div>
        </header>
    );
}

function ActionButton({
    icon: Icon,
    label,
    color = "text-gray-600 hover:bg-gray-100",
    active = false,
    onClick
}: {
    icon: ComponentType<{ size?: number }>;
    label: string;
    color?: string;
    active?: boolean;
    onClick?: () => void;
}) {
    return (
        <button
            type="button"
            aria-pressed={active}
            className={`p-1.5 rounded flex items-center gap-1 ${color} ${active ? "header-action-active" : ""}`}
            onClick={onClick}
        >
            <Icon size={18} />
            <span className="text-xs font-medium">{label}</span>
        </button>
    );
}
