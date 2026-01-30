import { IconHome, IconChartBar, IconBooks, IconSettings, IconRefresh } from '@tabler/icons-react';
import { NavLink } from 'react-router-dom';

const navItems = [
    { icon: IconHome, label: 'Home', to: '/' },
    { icon: IconChartBar, label: 'Scan', to: '/ranking' },
    { icon: IconBooks, label: 'Practice', to: '/practice' },
    { icon: IconSettings, label: 'Config', to: '/config' },
];

export function Sidebar({ className }: { className?: string }) {
    return (
        <div className={`flex flex-col items-center py-4 bg-gray-900 text-white gap-4 border-r border-gray-800 ${className}`}>
            {/* App Logo/Icon */}
            <div className="w-8 h-8 rounded bg-blue-500 mb-4 flex items-center justify-center font-bold">
                M
            </div>

            {navItems.map((item) => (
                <NavLink
                    key={item.label}
                    to={item.to}
                    className={({ isActive }) => `
            p-2 rounded-lg transition-colors
            ${isActive ? 'bg-blue-600 text-white' : 'text-gray-400 hover:bg-gray-800 hover:text-gray-200'}
          `}
                    title={item.label}
                >
                    <item.icon size={20} stroke={1.5} />
                </NavLink>
            ))}

            <div className="flex-1" />

            {/* Bottom Actions (Update/System) */}
            <button className="p-2 text-gray-500 hover:text-white" title="Refresh Data">
                <IconRefresh size={20} stroke={1.5} />
            </button>
        </div>
    );
}
