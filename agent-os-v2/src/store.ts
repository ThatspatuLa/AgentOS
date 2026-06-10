import { create } from 'zustand'
import type { MemoryWorldData, AgentInfo, TokenStats, ProjectInfo } from './types'

interface AppState {
  // Memory World graph data
  memoryWorld: MemoryWorldData | null
  setMemoryWorld: (data: MemoryWorldData) => void

  // Agents
  agents: AgentInfo[]
  setAgents: (agents: AgentInfo[]) => void

  // Tokens
  tokens: TokenStats | null
  setTokens: (stats: TokenStats) => void

  // Projects
  projects: ProjectInfo[]
  setProjects: (projects: ProjectInfo[]) => void

  // UI state
  sidebarOpen: boolean
  toggleSidebar: () => void
  currentPage: string
  setCurrentPage: (page: string) => void
}

export const useStore = create<AppState>((set) => ({
  memoryWorld: null,
  setMemoryWorld: (data) => set({ memoryWorld: data }),

  agents: [],
  setAgents: (agents) => set({ agents }),

  tokens: null,
  setTokens: (stats) => set({ tokens: stats }),

  projects: [],
  setProjects: (projects) => set({ projects }),

  sidebarOpen: true,
  toggleSidebar: () => set((s) => ({ sidebarOpen: !s.sidebarOpen })),
  currentPage: 'dashboard',
  setCurrentPage: (page) => set({ currentPage: page }),
}))
