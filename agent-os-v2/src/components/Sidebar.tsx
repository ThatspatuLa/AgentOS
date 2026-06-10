import sidebarOpen from 'lucide-react'
import { NavLink, useLocation } from 'react-router-dom'
import { useStore } from '../store'
import {
  LayoutDashboard,
  Coins,
  FolderKanban,
  Globe,
  Settings,
  ChevronLeft,
  ChevronRight,
} from 'lucide-react'

const navGroups = [
  {
    label: 'Main',
    items: [
      { to: '/dashboard', icon: LayoutDashboard, label: 'Dashboard' },
      { to: '/tokens', icon: Coins, label: 'Tokens' },
      { to: '/projects', icon: FolderKanban, label: 'Projects' },
    ],
  },
  {
    label: 'Insights',
    items: [
      { to: '/memory-world', icon: Globe, label: 'Memory World' },
    ],
  },
]

export default function Sidebar() {
  const { sidebarOpen, toggleSidebar } = useStore()
  const location = useLocation()

  return (
    <aside className={`
      fixed left-0 top-0 h-full z-50 flex flex-col
      bg-[var(--color-bg-secondary)] border-r border-[var(--color-border)]
      transition-all duration-200
      ${sidebarOpen ? 'w-[220px]' : 'w-[60px]'}
    `}>
      {/* Logo */}
      <div className={`p-5 border-b border-[var(--color-border)] ${!sidebarOpen && 'px-3'}`}>
        <h1 className="text-lg font-bold tracking-tight">
          {sidebarOpen ? 'Zen Agent OS' : 'Z'}
        </h1>
        {sidebarOpen && (
          <span className="text-[11px] text-[var(--color-text-muted)] mt-0.5 block">
            System Dashboard
          </span>
        )}
      </div>

      {/* Navigation */}
      <nav className="flex-1 py-2 overflow-y-auto">
        {navGroups.map((group) => (
          <div key={group.label} className="mb-2 px-3">
            {sidebarOpen && (
              <div className="text-[10px] font-semibold uppercase tracking-wider text-[var(--color-text-muted)] px-2 pt-3 pb-1">
                {group.label}
              </div>
            )}
            {group.items.map((item) => {
              const Icon = item.icon
              const active = location.pathname === item.to
              return (
                <NavLink
                  key={item.to}
                  to={item.to}
                  className={`
                    flex items-center gap-2.5 px-2.5 py-2 rounded-md text-[13px] mb-0.5
                    transition-colors duration-150
                    ${active
                      ? 'bg-[var(--color-bg-tertiary)] text-[var(--color-text-primary)]'
                      : 'text-[var(--color-text-secondary)] hover:bg-[var(--color-bg-tertiary)] hover:text-[var(--color-text-primary)]'
                    }
                    ${!sidebarOpen && 'justify-center px-0'}
                  `}
                  title={!sidebarOpen ? item.label : undefined}
                >
                  <Icon size={16} />
                  {sidebarOpen && <span>{item.label}</span>}
                </NavLink>
              )
            })}
          </div>
        ))}
      </nav>

      {/* Footer / Toggle */}
      <div className="px-3 py-3 border-t border-[var(--color-border)]">
        <button
          onClick={toggleSidebar}
          className="flex items-center gap-2 text-[var(--color-text-muted)] hover:text-[var(--color-text-secondary)] transition-colors text-[13px] w-full px-2.5 py-1.5 rounded-md"
        >
          {sidebarOpen ? (
            <>
              <ChevronLeft size={14} />
              <span>Collapse</span>
            </>
          ) : (
            <ChevronRight size={14} />
          )}
        </button>
      </div>
    </aside>
  )
}
