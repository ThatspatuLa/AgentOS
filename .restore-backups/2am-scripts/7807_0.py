from pathlib import Path
p=Path('/home/spatula/Projects/ZenNew/agent-os.html')
s=p.read_text()
s=s.replace('<div class="os-cards-grid" id="tokens-stats" style="margin-bottom:20px"></div>', '<div class...[truncated]