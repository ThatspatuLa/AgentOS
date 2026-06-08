# ZenNew - Direct Hermes-Discord Bridge

Clean, minimal Discord bridge. No gating. Hermes has full capability.

## Project Structure

```
ZenNew/
├── bot/
│   └── main.py          # Bot entry point
├── .env                 # Configuration (secrets - never commit)
├── requirements.txt     # Python dependencies
└── README.md            # This file
```

## Setup

1. Install dependencies:
   ```bash
   cd ~/Projects/ZenNew
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. Configure `.env`:
   - `DISCORD_TOKEN`: Your bot token from Discord Developer Portal
   - `ALLOWED_USER_ID`: Your Discord user ID

3. Run the bot:
   ```bash
   cd ~/Projects/ZenNew
   source .venv/bin/activate
   python3 bot/main.py
   ```

## Run as systemd service (auto-start)

```bash
# Create service file
sudo nano /etc/systemd/system/zen-new.service
```

```ini
[Unit]
Description=ZenNew Discord Bridge
After=network.target

[Service]
Type=simple
User=spatula
WorkingDirectory=/home/spatula/Projects/ZenNew
ExecStart=/home/spatula/Projects/ZenNew/.venv/bin/python3 bot/main.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable zen-new
sudo systemctl start zen-new
sudo systemctl status zen-new
```

## Logs

```bash
journalctl -u zen-new -n 80 --no-pager
```

## Discord Commands

| Command | Description |
|---------|-------------|
| `!ping` | Test connection |
| `!status` | Show bridge status |
| (any message) | Forward to Hermes for processing |
