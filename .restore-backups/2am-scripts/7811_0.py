from pathlib import Path
p=Path('/home/spatula/Projects/ZenNew/agent-os.html')
s=p.read_text()
s=s.replace('''        <div class="routing-legend">
          ${routing.map(item => `<div class="routing-...[truncated]