import { Routes, Route, Navigate } from 'react-router-dom'
import { useEffect } from 'react'
import { useStore } from './store'
import Layout from './components/Layout'
import Dashboard from './pages/Dashboard'
import Tokens from './pages/Tokens'
import Projects from './pages/Projects'
import MemoryWorld from './pages/MemoryWorld'
import { MemoryWorldData, AgentInfo, TokenStats, ProjectInfo } from './types'

import agentData from './data/agent-os-data.json'
import memoryWorldData from './data/memory-world.json'

// Hardcoded agents from SOUL.md / Obsidian vault
const AGENTS: AgentInfo[] = [
  { id: 'zen-agent', label: 'Zen', domain: 'Governance, safety, system law', authority: 'root', risk: 'high', active: true, channel: 'zen-chat' },
  { id: 'agent-rin', label: 'Rin', domain: 'Tactical sparring, daily ops, project coordination', authority: 'managed', risk: 'medium', active: true, channel: 'rin-chat' },
  { id: 'agent-kiyosaki', label: 'Kiyosaki', domain: 'Trading, system strategy, automation', authority: 'managed', risk: 'high', active: true, channel: 'kiyosaki-chat' },
  { id: 'agent-toji', label: 'Toji', domain: 'Body discipline, training, recovery, nutrition', authority: 'managed', risk: 'medium', active: true, channel: 'toji-chat' },
  { id: 'agent-minato', label: 'Minato', domain: 'Websites, monetisation, client value, business execution', authority: 'managed', risk: 'medium', active: true, channel: 'minato-chat' },
  { id: 'agent-kazuki', label: 'Kazuki', domain: 'Guitar, creative skill, deliberate practice', authority: 'managed', risk: 'medium', active: true, channel: 'kazuki-chat' },
]

function parseTokens(data: any): TokenStats {
  const today = data.token_usage?.today ?? {}
  return {
    calls: today.calls ?? 0,
    cost: today.cost ?? 0,
    in_tokens: today.in ?? 0,
    out_tokens: today.out ?? 0,
    local_calls: 40, // from memory
    local_tokens: 152782,
    local_pct: 14.1,
  }
}

function parseProjects(data: any): ProjectInfo[] {
  const projects: ProjectInfo[] = []
  const projData = data.projects ?? {}
  for (const [id, info] of Object.entries(projData)) {
    const p = info as any
    projects.push({
      id,
      label: id,
      active: (p.accepted ?? 0) > 0,
      file_count: p.total ?? 0,
      path: `01_Projects/${id}`,
    })
  }
  return projects
}

export default function App() {
  const { setAgents, setTokens, setProjects, setMemoryWorld } = useStore()

  useEffect(() => {
    setAgents(AGENTS)
    setTokens(parseTokens(agentData))
    setProjects(parseProjects(agentData))
    setMemoryWorld(memoryWorldData as unknown as MemoryWorldData)
  }, [setAgents, setTokens, setProjects, setMemoryWorld])

  return (
    <Layout>
      <Routes>
        <Route path="/" element={<Navigate to="/dashboard" replace />} />
        <Route path="/dashboard" element={<Dashboard />} />
        <Route path="/tokens" element={<Tokens />} />
        <Route path="/projects" element={<Projects />} />
        <Route path="/memory-world" element={<MemoryWorld />} />
      </Routes>
    </Layout>
  )
}
