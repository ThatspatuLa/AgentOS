#!/bin/bash
# Setup Discord integration for Hermes Gateway
# Run this script to configure Discord

echo "======================================"
echo "  Hermes Gateway - Discord Setup"
echo "======================================"
echo ""

# Check if Discord token already exists
if grep -q "DISCORD_TOKEN=" ~/.hermes/.env 2>/dev/null && ! grep -q "DISCORD_TOKEN=your" ~/.hermes/.env 2>/dev/null; then
    echo "✅ DISCORD_TOKEN already configured in .env"
else
    echo "Adding DISCORD_TOKEN to ~/.hermes/.env..."
    # Remove placeholder if exists
    sed -i '/^DISCORD_TOKEN=your_d/d' ~/.hermes/.env 2>/dev/null
    sed -i '/^DISCORD_ALLOWED_USERS=your_u/d' ~/.hermes/.env 2>/dev/null
    sed -i '/# === DISCORD INTEGRATION ===/d' ~/.hermes/.env 2>/dev/null
    
    cat >> ~/.hermes/.env << 'EOF'

# =============================================================================
# DISCORD INTEGRATION
# =============================================================================
DISCORD_TOKEN=MTUwMD...NEI
DISCORD_ALLOWED_USERS=1022146083796816012
EOF
    echo "✅ Discord credentials added to .env"
fi

echo ""
echo "Updating Discord configuration in config.yaml..."

# Update config.yaml for Discord
cat > /tmp/discord_config_patch.yaml << 'EOF'
discord:
  require_mention: false
  free_response_channels: 'zen-chat,kiyosaki-chat,minato-chat,rin-chat,toji-chat,kazuki-chat'
  allowed_channels: ''
  auto_thread: false
  thread_require_mention: false
  history_backfill: true
  history_backfill_limit: 10
  reactions: true
  channel_prompts:
    zen-chat: 'You are Zen. Follow the instructions in ~/Obsidian/ZenVault/00_System/Project Instructions/Zen Instructions.md. Be tactical, direct, structured, practical, unsentimental, evidence-driven.'
    kiyosaki-chat: 'You are Kiyosaki. Follow the instructions in ~/Obsidian/ZenVault/00_System/Project Instructions/Kiyosaki Instructions.md. Focus on trading, risk, crypto strategy, survivability.'
    minato-chat: 'You are Minato. Follow the instructions in ~/Obsidian/ZenVault/00_System/Project Instructions/Minato Instructions.md. Focus on websites, monetization, client value.'
    rin-chat: 'You are Rin. Follow the instructions in ~/Obsidian/ZenVault/00_System/Project Instructions/Rin Instructions.md. Be a tactical assistant with sharp structure and cross-project awareness.'
    toji-chat: 'You are Toji. Follow the instructions in ~/Obsidian/ZenVault/00_System/Project Instructions/Toji Instructions.md. Focus on gym, body discipline, progression, recovery.'
    kazuki-chat: 'You are Kazuki. Follow the instructions in ~/Obsidian/ZenVault/00_System/Project Instructions/Kazuki Instructions.md. Focus on guitar and deliberate skill development.'
  dm_role_auth_guild: ''
  server_actions: ''
  allow_any_attachment: true
  max_attachment_bytes: 33554432
EOF

echo "✅ Discord config prepared"
echo ""
echo "======================================"
echo "  Next Steps (run these manually):"
echo "======================================"
echo ""
echo "1. Open config.yaml for editing:"
echo "   hermes config edit"
echo ""
echo "2. Find the 'discord:' section and paste the config from:"
echo "   /tmp/discord_config_patch.yaml"
echo ""
echo "3. Start the gateway:"
echo "   hermes gateway start"
echo ""
echo "4. Check status:"
echo "   hermes gateway status"
echo ""
echo "5. Test by sending a message in any Discord channel!"
echo "   Channels: zen-chat, kiyosaki-chat, minato-chat,"
echo "             rin-chat, toji-chat, kazuki-chat"
echo ""
