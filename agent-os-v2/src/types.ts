export interface GraphNode {
  id: string
  type: 'agent' | 'model' | 'project' | 'resource' | 'rule' | 'tool'
  label: string
  authority?: string
  risk?: string
  active?: boolean
  data?: Record<string, unknown>
}

export interface GraphEdge {
  source: string
  target: string
  type: string
  label?: string
}

export interface MemoryWorldData {
  generated_at: string
  exporter_version: string
  stats: {
    total_nodes: number
    total_edges: number
    node_types: Record<string, number>
    edge_types: Record<string, number>
  }
  nodes: GraphNode[]
  edges: GraphEdge[]
}

export interface AgentInfo {
  id: string
  label: string
  domain: string
  authority: string
  risk: string
  active: boolean
  channel: string
}

export interface TokenStats {
  calls: number
  cost: number
  in_tokens: number
  out_tokens: number
  local_calls: number
  local_tokens: number
  local_pct: number
}

export interface ProjectInfo {
  id: string
  label: string
  active: boolean
  file_count: number
  path: string
}
