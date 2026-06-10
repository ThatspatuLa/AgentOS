import { useEffect, useRef, useState, useCallback } from 'react'
import { useStore } from '../store'
import { GraphNode } from '../types'

const NODE_COLORS: Record<string, string> = {
  agent: '#58a6ff',
  model: '#bc8cff',
  project: '#3fb950',
  resource: '#f0883e',
  rule: '#d29922',
  tool: '#56d4dd',
}

const EDGE_COLORS: Record<string, string> = {
  governs: '#58a6ff',
  routes_to: '#bc8cff',
  delegates_to: '#3fb950',
  enforces: '#d29922',
  consumes: '#f85149',
  owns: '#f0883e',
  oversees: '#8b949e',
  available_to: '#484f58',
  configures: '#56d4dd',
  launches: '#56d4dd',
  default: '#30363d',
}

function getNodeColor(type: string): string {
  return NODE_COLORS[type] ?? '#8b949e'
}

function getEdgeColor(type: string): string {
  return EDGE_COLORS[type] ?? EDGE_COLORS.default
}

interface SimNode {
  node: GraphNode
  x: number
  y: number
  vx: number
  vy: number
}

export default function MemoryWorld() {
  const canvasRef = useRef<HTMLCanvasElement>(null)
  const { memoryWorld } = useStore()
  const [hoveredNode, setHoveredNode] = useState<GraphNode | null>(null)
  const [selectedNode, setSelectedNode] = useState<GraphNode | null>(null)
  const transformRef = useRef({ x: 0, y: 0, scale: 1 })
  const draggedRef = useRef<string | null>(null)
  const panningRef = useRef<{ startX: number; startY: number; startTx: number; startTy: number } | null>(null)
  const nodesRef = useRef<SimNode[]>([])
  const animRef = useRef<number>(0)

  // Initialize/reflow nodes when data changes
  useEffect(() => {
    if (!memoryWorld) return
    const canvas = canvasRef.current
    if (!canvas) return

    const cx = 0
    const cy = 0
    const count = memoryWorld.nodes.length

    // Keep existing positions if possible
    const existing = new Map(nodesRef.current.map(n => [n.node.id, n]))

    nodesRef.current = memoryWorld.nodes.map((node: GraphNode, i: number) => {
      const prev = existing.get(node.id)
      if (prev) return prev
      const angle = (2 * Math.PI * i) / count
      const r = 180 + Math.random() * 60
      return {
        node,
        x: cx + r * Math.cos(angle),
        y: cy + r * Math.sin(angle),
        vx: 0,
        vy: 0,
      }
    })
  }, [memoryWorld])

  // Physics + render loop
  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas || !memoryWorld) return
    const ctx = canvas.getContext('2d')
    if (!ctx) return

    const nodes = nodesRef.current
    const edges = memoryWorld.edges
    let running = true

    function tick() {
      if (!running) return

      // Resize canvas to match parent every frame
      const parent = canvas!.parentElement
      if (parent) {
        const pw = parent.clientWidth
        const ph = parent.clientHeight
        if (canvas!.width !== pw) canvas!.width = pw
        if (canvas!.height !== ph) canvas!.height = ph
      }

      const W = canvas!.width
      const H = canvas!.height
      const tx = transformRef.current

      // Physics step
      const alpha = 0.3
      for (const n of nodes) {
        let fx = 0, fy = 0
        for (const m of nodes) {
          if (n === m) continue
          const dx = n.x - m.x
          const dy = n.y - m.y
          const dist = Math.sqrt(dx * dx + dy * dy) || 1
          if (dist < 300) {
            const repel = 3000 / (dist * dist)
            fx += (dx / dist) * repel * alpha
            fy += (dy / dist) * repel * alpha
          }
        }
        // Center gravity
        fx += (-n.x) * 0.001 * alpha
        fy += (-n.y) * 0.001 * alpha
        n.vx = n.vx * 0.88 + fx
        n.vy = n.vy * 0.88 + fy
      }

      // Edge spring
      const posMap = new Map(nodes.map(n => [n.node.id, n]))
      for (const edge of edges) {
        const a = posMap.get(edge.source)
        const b = posMap.get(edge.target)
        if (!a || !b) continue
        const dx = b.x - a.x
        const dy = b.y - a.y
        const dist = Math.sqrt(dx * dx + dy * dy) || 1
        const attract = (dist - 140) * 0.003 * alpha
        const fx = (dx / dist) * attract
        const fy = (dy / dist) * attract
        a.vx += fx; a.vy += fy
        b.vx -= fx; b.vy -= fy
      }

      // Integrate
      for (const n of nodes) {
        if (draggedRef.current === n.node.id) continue
        n.x += n.vx
        n.y += n.vy
      }

      // Render
      ctx!.clearRect(0, 0, W, H)
      ctx!.save()
      ctx!.translate(tx.x, tx.y)
      ctx!.scale(tx.scale, tx.scale)

      // Edges
      for (const edge of edges) {
        const a = posMap.get(edge.source)
        const b = posMap.get(edge.target)
        if (!a || !b) continue
        const dx = b.x - a.x
        const dy = b.y - a.y
        ctx!.beginPath()
        ctx!.moveTo(a.x, a.y)
        ctx!.bezierCurveTo(a.x + dx * 0.4, a.y, b.x - dx * 0.4, b.y, b.x, b.y)
        ctx!.strokeStyle = getEdgeColor(edge.type) + '50'
        ctx!.lineWidth = 1.5
        ctx!.stroke()
      }

      // Nodes
      for (const n of nodes) {
        const color = getNodeColor(n.node.type)
        const isHover = hoveredNode?.id === n.node.id
        const isSelected = selectedNode?.id === n.node.id
        const r = isSelected ? 12 : isHover ? 10 : 8

        if (isHover || isSelected) {
          ctx!.beginPath()
          ctx!.arc(n.x, n.y, r + 6, 0, Math.PI * 2)
          ctx!.fillStyle = color + '20'
          ctx!.fill()
        }

        ctx!.beginPath()
        ctx!.arc(n.x, n.y, r, 0, Math.PI * 2)
        ctx!.fillStyle = color
        ctx!.fill()

        ctx!.font = `${isHover || isSelected ? '600' : '400'} 11px Inter, sans-serif`
        ctx!.fillStyle = isHover || isSelected ? '#e6edf3' : '#8b949e'
        ctx!.textAlign = 'center'
        ctx!.fillText(n.node.label, n.x, n.y + r + 14)
      }

      ctx!.restore()
      animRef.current = requestAnimationFrame(tick)
    }

    animRef.current = requestAnimationFrame(tick)
    return () => { running = false; cancelAnimationFrame(animRef.current) }
  }, [memoryWorld, hoveredNode, selectedNode])

  const toWorld = useCallback((e: React.MouseEvent) => {
    const canvas = canvasRef.current
    if (!canvas) return { wx: 0, wy: 0 }
    const rect = canvas.getBoundingClientRect()
    const tx = transformRef.current
    return {
      wx: (e.clientX - rect.left - tx.x) / tx.scale,
      wy: (e.clientY - rect.top - tx.y) / tx.scale,
    }
  }, [])

  const findNode = useCallback((wx: number, wy: number): GraphNode | null => {
    for (const n of nodesRef.current) {
      const dx = wx - n.x
      const dy = wy - n.y
      if (dx * dx + dy * dy < 144) return n.node // 12px hit radius
    }
    return null
  }, [])

  const handleMouseDown = useCallback((e: React.MouseEvent) => {
    const { wx, wy } = toWorld(e)
    const node = findNode(wx, wy)
    if (node) {
      draggedRef.current = node.id
    } else {
      panningRef.current = { startX: e.clientX, startY: e.clientY, startTx: transformRef.current.x, startTy: transformRef.current.y }
    }
  }, [toWorld, findNode])

  const handleMouseMove = useCallback((e: React.MouseEvent) => {
    if (draggedRef.current) {
      const { wx, wy } = toWorld(e)
      const n = nodesRef.current.find(nd => nd.node.id === draggedRef.current)
      if (n) { n.x = wx; n.y = wy; n.vx = 0; n.vy = 0 }
    } else if (panningRef.current) {
      transformRef.current.x = panningRef.current.startTx + (e.clientX - panningRef.current.startX)
      transformRef.current.y = panningRef.current.startTy + (e.clientY - panningRef.current.startY)
    } else {
      const { wx, wy } = toWorld(e)
      setHoveredNode(findNode(wx, wy))
    }
  }, [toWorld, findNode])

  const handleMouseUp = useCallback((e: React.MouseEvent) => {
    if (draggedRef.current) {
      draggedRef.current = null
      return
    }
    panningRef.current = null
    // Click to select
    const { wx, wy } = toWorld(e)
    const node = findNode(wx, wy)
    setSelectedNode(node ? (selectedNode?.id === node.id ? null : node) : null)
  }, [toWorld, findNode, selectedNode])

  const handleWheel = useCallback((e: React.WheelEvent) => {
    e.preventDefault()
    const delta = e.deltaY > 0 ? 0.9 : 1.1
    const tx = transformRef.current
    tx.scale = Math.max(0.3, Math.min(3, tx.scale * delta))
  }, [])

  if (!memoryWorld) {
    return (
      <div className="flex items-center justify-center h-full text-[#8b949e]">
        Loading graph data...
      </div>
    )
  }

  return (
    <div className="flex flex-col h-[calc(100vh-3rem)]">
      {/* Header */}
      <div className="flex items-center justify-between mb-4">
        <div>
          <h1 className="text-xl font-semibold">Memory World</h1>
          <p className="text-xs text-[#8b949e] mt-0.5">
            {memoryWorld.stats.total_nodes} nodes · {memoryWorld.stats.total_edges} edges · Generated {new Date(memoryWorld.generated_at).toLocaleString()}
          </p>
        </div>
        {/* Legend */}
        <div className="flex items-center gap-4 text-[11px] text-[#8b949e]">
          {Object.entries(NODE_COLORS).map(([type, color]) => (
            <div key={type} className="flex items-center gap-1.5">
              <div className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor: color }} />
              <span className="capitalize">{type}</span>
            </div>
          ))}
        </div>
      </div>

      {/* Canvas area */}
      <div className="flex-1 flex gap-4 min-h-0">
        <div className="flex-1 bg-[#161b22] border border-[#30363d] rounded-lg overflow-hidden relative">
          <canvas
            ref={canvasRef}
            className="absolute inset-0 w-full h-full cursor-grab active:cursor-grabbing"
            onMouseDown={handleMouseDown}
            onMouseMove={handleMouseMove}
            onMouseUp={handleMouseUp}
            onWheel={handleWheel}
            onMouseLeave={() => { setHoveredNode(null); panningRef.current = null; draggedRef.current = null }}
          />

          {/* Tooltip */}
          {hoveredNode && (
            <div className="absolute bottom-4 left-4 bg-[#1c2128]/95 border border-[#30363d] rounded-lg px-3 py-2 text-xs backdrop-blur-sm pointer-events-none">
              <div className="font-medium text-[#e6edf3]">{hoveredNode.label}</div>
              <div className="text-[#8b949e] mt-0.5">
                {hoveredNode.type}
                {hoveredNode.data && Object.keys(hoveredNode.data).length > 0 && (
                  <span> · {JSON.stringify(hoveredNode.data).slice(0, 80)}</span>
                )}
              </div>
            </div>
          )}
        </div>

        {/* Side panel */}
        {selectedNode && (
          <div className="w-72 bg-[#1c2128] border border-[#30363d] rounded-lg p-4 overflow-y-auto">
            <div className="flex items-center gap-2 mb-4">
              <div className="w-4 h-4 rounded-full" style={{ backgroundColor: getNodeColor(selectedNode.type) }} />
              <h2 className="font-semibold">{selectedNode.label}</h2>
            </div>
            <div className="space-y-3 text-sm">
              <div>
                <span className="text-[#484f58] text-xs uppercase tracking-wider">Type</span>
                <div className="text-[#e6edf3] capitalize">{selectedNode.type}</div>
              </div>
              {selectedNode.authority && (
                <div>
                  <span className="text-[#484f58] text-xs uppercase tracking-wider">Authority</span>
                  <div className="text-[#e6edf3]">{selectedNode.authority}</div>
                </div>
              )}
              {selectedNode.risk && (
                <div>
                  <span className="text-[#484f58] text-xs uppercase tracking-wider">Risk</span>
                  <div className="text-[#e6edf3]">{selectedNode.risk}</div>
                </div>
              )}
              {selectedNode.active !== undefined && (
                <div>
                  <span className="text-[#484f58] text-xs uppercase tracking-wider">Status</span>
                  <div className={selectedNode.active ? 'text-[#3fb950]' : 'text-[#8b949e]'}>
                    {selectedNode.active ? 'Active' : 'Inactive'}
                  </div>
                </div>
              )}

              {/* Connected edges */}
              <div>
                <span className="text-[#484f58] text-xs uppercase tracking-wider">Connections</span>
                <div className="mt-1 space-y-1">
                  {memoryWorld.edges
                    .filter((e: any) => e.source === selectedNode.id || e.target === selectedNode.id)
                    .map((e: any, i: number) => {
                      const otherId = e.source === selectedNode.id ? e.target : e.source
                      const other = memoryWorld.nodes.find((n: GraphNode) => n.id === otherId)
                      const dir = e.source === selectedNode.id ? '→' : '←'
                      return (
                        <div key={i} className="text-xs text-[#8b949e]">
                          <span className="text-[#30363d]">{dir}</span>{' '}
                          <span className="text-[#e6edf3]">{other?.label ?? otherId}</span>
                          <span className="text-[#484f58] ml-1">({e.type})</span>
                        </div>
                      )
                    })}
                </div>
              </div>

              {/* Raw data */}
              {selectedNode.data && Object.keys(selectedNode.data).length > 0 && (
                <div>
                  <span className="text-[#484f58] text-xs uppercase tracking-wider">Data</span>
                  <pre className="text-[11px] text-[#8b949e] mt-1 bg-[#0d1117] rounded p-2 overflow-x-auto font-mono">
                    {JSON.stringify(selectedNode.data, null, 2)}
                  </pre>
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
