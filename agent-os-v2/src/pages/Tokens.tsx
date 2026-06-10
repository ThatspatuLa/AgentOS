import { useStore } from '../store'

function formatNumber(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`
  return n.toLocaleString()
}

function Bar({ value, max, color }: { value: number; max: number; color: string }) {
  const pct = max > 0 ? Math.min((value / max) * 100, 100) : 0
  return (
    <div className="h-2 bg-[#21262d] rounded-full overflow-hidden">
      <div
        className={`h-full rounded-full ${color}`}
        style={{ width: `${pct}%` }}
      />
    </div>
  )
}

export default function Tokens() {
  const { tokens } = useStore()

  if (!tokens) {
    return <div className="text-[#8b949e]">No token data available.</div>
  }

  const cloudPct = tokens.calls > 0 ? ((tokens.calls - tokens.local_calls) / tokens.calls * 100) : 0
  const localPct = tokens.calls > 0 ? (tokens.local_calls / tokens.calls * 100) : 0

  return (
    <div>
      <h1 className="text-xl font-semibold mb-6">Tokens</h1>

      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-8">
        <div className="bg-[#1c2128] border border-[#30363d] rounded-lg p-4">
          <div className="text-xs text-[#8b949e] mb-1">Total Calls</div>
          <div className="text-2xl font-semibold">{formatNumber(tokens.calls)}</div>
        </div>
        <div className="bg-[#1c2128] border border-[#30363d] rounded-lg p-4">
          <div className="text-xs text-[#8b949e] mb-1">Est. Cost</div>
          <div className="text-2xl font-semibold">${tokens.cost.toFixed(4)}</div>
        </div>
        <div className="bg-[#1c2128] border border-[#30363d] rounded-lg p-4">
          <div className="text-xs text-[#8b949e] mb-1">Input Tokens</div>
          <div className="text-2xl font-semibold">{formatNumber(tokens.in_tokens)}</div>
        </div>
        <div className="bg-[#1c2128] border border-[#30363d] rounded-lg p-4">
          <div className="text-xs text-[#8b949e] mb-1">Output Tokens</div>
          <div className="text-2xl font-semibold">{formatNumber(tokens.out_tokens)}</div>
        </div>
      </div>

      {/* Cloud vs Local split */}
      <div className="bg-[#1c2128] border border-[#30363d] rounded-lg p-5 mb-6">
        <h2 className="text-sm font-semibold mb-4">Cloud vs Local Worker</h2>
        <div className="flex items-center gap-4 mb-3">
          <div className="text-center">
            <div className="text-2xl font-bold text-[#bc8cff]">{cloudPct.toFixed(1)}%</div>
            <div className="text-xs text-[#8b949e] mt-1">Cloud</div>
          </div>
          <div className="flex-1">
            <div className="flex h-3 rounded-full overflow-hidden">
              <div className="bg-[#bc8cff]" style={{ width: `${cloudPct}%` }} />
              <div className="bg-[#3fb950]/70" style={{ width: `${localPct}%` }} />
            </div>
          </div>
          <div className="text-center">
            <div className="text-2xl font-bold text-[#3fb950]">{localPct.toFixed(1)}%</div>
            <div className="text-xs text-[#8b949e] mt-1">Local</div>
          </div>
        </div>
        <div className="grid grid-cols-2 gap-4 mt-4">
          <div>
            <div className="text-xs text-[#8b949e] mb-2">Cloud Calls</div>
            <Bar value={tokens.calls - tokens.local_calls} max={tokens.calls} color="bg-[#bc8cff]" />
            <div className="text-xs text-[#484f58] mt-1">{formatNumber(tokens.calls - tokens.local_calls)} calls</div>
          </div>
          <div>
            <div className="text-xs text-[#8b949e] mb-2">Local Calls</div>
            <Bar value={tokens.local_calls} max={tokens.calls} color="bg-[#3fb950]/70" />
            <div className="text-xs text-[#484f58] mt-1">{formatNumber(tokens.local_calls)} calls</div>
          </div>
        </div>
      </div>

      {/* Cost breakdown */}
      <div className="bg-[#1c2128] border border-[#30363d] rounded-lg p-5">
        <h2 className="text-sm font-semibold mb-3">Cost Efficiency</h2>
        <p className="text-sm text-[#8b949e]">
          Local worker handles <span className="text-[#3fb950] font-medium">{formatNumber(tokens.local_calls)}</span> calls
          via gpt-oss:20b at <span className="text-[#3fb950] font-medium">$0.00</span> cost.
          Cloud tokens are reserved for planning, research, and review.
        </p>
      </div>
    </div>
  )
}
