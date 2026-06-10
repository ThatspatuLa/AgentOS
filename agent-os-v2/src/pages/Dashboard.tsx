import { useStore } from '../store'
import { Users, Cpu, FolderGit2, Activity } from 'lucide-react'

function StatCard({ icon: Icon, label, value, sub, color }: {
  icon: React.ElementType
  label: string
  value: string | number
  sub?: string
  color: string
}) {
  return (
    <div className="bg-[#1c2128] border border-[#30363d] rounded-lg p-4">
      <div className="flex items-center gap-3 mb-3">
        <div className={`w-9 h-9 rounded-lg flex items-center justify-center ${color}`}>
          <Icon size={18} />
        </div>
        <span className="text-[13px] text-[#8b949e]">{label}</span>
      </div>
      <div className="text-2xl font-semibold text-[#e6edf3]">{value}</div>
      {sub && <div className="text-xs text-[#484f58] mt-1">{sub}</div>}
    </div>
  )
}

export default function Dashboard() {
  const { agents, tokens, projects, memoryWorld } = useStore()

  const activeAgents = agents.filter(a => a.active).length
  const activeProjects = projects.filter(p => p.active).length

  return (
    <div>
      <h1 className="text-xl font-semibold mb-6">Dashboard</h1>

      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4 mb-8">
        <StatCard
          icon={Users}
          label="Active Agents"
          value={`${activeAgents} / ${agents.length}`}
          color="bg-[#58a6ff]/15 text-[#58a6ff]"
        />
        <StatCard
          icon={Cpu}
          label="Cloud Calls Today"
          value={tokens?.calls ?? '—'}
          sub={tokens ? `$${tokens.cost.toFixed(4)} spent` : undefined}
          color="bg-[#bc8cff]/15 text-[#bc8cff]"
        />
        <StatCard
          icon={FolderGit2}
          label="Active Projects"
          value={activeProjects}
          color="bg-[#3fb950]/15 text-[#3fb950]"
        />
        <StatCard
          icon={Activity}
          label="Memory Graph"
          value={memoryWorld ? `${memoryWorld.stats.total_nodes} nodes` : '—'}
          sub={memoryWorld ? `${memoryWorld.stats.total_edges} edges` : undefined}
          color="bg-[#f0883e]/15 text-[#f0883e]"
        />
      </div>

      {/* Agent Roster */}
      <h2 className="text-sm font-semibold text-[#8b949e] uppercase tracking-wider mb-3">Agent Roster</h2>
      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-3">
        {agents.map((agent) => (
          <div
            key={agent.id}
            className="bg-[#1c2128] border border-[#30363d] rounded-lg p-4 hover:border-[#58a6ff]/40 transition-colors"
          >
            <div className="flex items-center justify-between mb-2">
              <span className="font-medium text-[#e6edf3]">{agent.label}</span>
              <span className={`text-[10px] font-medium uppercase px-2 py-0.5 rounded-full ${
                agent.active
                  ? 'bg-[#3fb950]/15 text-[#3fb950]'
                  : 'bg-[#30363d] text-[#8b949e]'
              }`}>
                {agent.active ? 'Active' : 'Idle'}
              </span>
            </div>
            <div className="text-xs text-[#8b949e] mb-1">{agent.domain}</div>
            <div className="flex items-center gap-2 text-[11px] text-[#484f58]">
              <span>Risk: {agent.risk}</span>
              <span>•</span>
              <span>Authority: {agent.authority}</span>
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}
