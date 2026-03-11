import { Outlet } from 'react-router-dom';
import { Sidebar } from '../components/layout/Sidebar';
import { Header } from '../components/layout/Header';

export function AppShell() {
    return (
        <div className="flex h-screen w-screen overflow-hidden" style={{ background: 'var(--theme-bg-primary)', color: 'var(--theme-text-primary)' }}>
            {/* Zone A: Fixed Sidebar */}
            <Sidebar className="w-16 flex-none" />

            <div className="flex flex-col flex-1 min-w-0">
                {/* Zone B: Dynamic Header */}
                <Header className="flex-none" />

                {/* Zone C: Main Content */}
                <main className="flex-1 overflow-hidden relative">
                    <Outlet />
                </main>
            </div>
        </div>
    );
}
