from pathlib import Path
p=Path('/home/spatula/Projects/ZenNew/agent-os.html')
s=p.read_text()
anchor="""  const colorFor = item => item.color === 'cyan' ? 'var(--cyan)' : item.color === 'green' ? 'va...[truncated]