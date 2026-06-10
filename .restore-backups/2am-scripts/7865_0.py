from pathlib import Path
p=Path('/home/spatula/Projects/ZenNew/agent-os.html')
s=p.read_text()
old=s[s.index('    <div class="os-top-bar">', s.index('<div id="page-tokens"')):s.index('    <!-- Main st...[truncated]