from pathlib import Path
p=Path('/home/spatula/Projects/ZenNew/agent-os.html')
s=p.read_text()
s=s.replace('📊 Local Worker Value Graph','📈 Token Flow Over Time')
start=s.index("  const localGraphEl = ...[truncated]