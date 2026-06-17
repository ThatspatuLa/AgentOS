#!/usr/bin/env python3
"""
ZenNew - Direct Hermes-Discord Bridge with Session Mapping
Each Discord channel maps to a specific Hermes session with its own instructions.
Webhook-based: messages are queued for Hermes to pick up.
"""

import os
import json
import asyncio
from datetime import datetime
from pathlib import Path
import discord
from discord.ext import commands
from dotenv import load_dotenv

load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "").strip()
ALLOWED_USER_ID = int(os.getenv("ALLOWED_USER_ID", "0").strip())
COMMAND_PREFIX = os.getenv("COMMAND_PREFIX", "!").strip()
OBSIDIAN_VAULT = os.getenv("OBSIDIAN_VAULT_DIR", "/home/spatula/Obsidian/ZenVault").strip()
WEBHOOK_PORT = int(os.getenv("WEBHOOK_PORT", "8765").strip())

# Channel → Session Mapping
CHANNEL_SESSION_MAP = {
    1513076747430793289: "zen",
    1516644810834841672: "zen-os",
    1500437316379086880: "kiyosaki",
    1500437358934233219: "minato",
    1500437397916356608: "rin",
    1500437438680793209: "toji",
    1500437483429564476: "kazuki",
}

# Session → Instruction File
SESSION_INSTRUCTIONS = {
    "zen": "Zen Instructions.md",
    "zen-os": "Zen OS Instructions.md",
    "kiyosaki": "Kiyosaki Instructions.md",
    "minato": "Minato Instructions.md",
    "rin": "Rin Instructions.md",
    "toji": "Toji Instructions.md",
    "kazuki": "Kazuki Instructions.md",
}

# Message queue directory
QUEUE_DIR = Path("/tmp/zennew_queue")
QUEUE_DIR.mkdir(exist_ok=True)

# Friendly Discord shortcuts for Hermes gateway model switching.
# These are session-scoped by default; they do not rewrite global config.yaml.
LLM_SWITCH_COMMANDS = {
    "owl": "/model openrouter/owl-alpha --provider openrouter",
    "owl-alpha": "/model openrouter/owl-alpha --provider openrouter",
    "owla": "/model openrouter/owl-alpha --provider openrouter",
    "codex": "/model gpt-5.5 --provider openai-codex",
    "gpt-5.5": "/model gpt-5.5 --provider openai-codex",
}

LLM_DISPLAY = {
    "owl": "Owl Alpha via OpenRouter (`openrouter/owl-alpha`)",
    "codex": "Codex (`gpt-5.5` via `openai-codex`)",
}

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix=COMMAND_PREFIX, intents=intents)


def get_session(channel_id: int) -> str:
    """Get session name from channel ID."""
    return CHANNEL_SESSION_MAP.get(channel_id, "zen")


def get_instruction_file(session: str) -> str:
    """Get instruction file path for a session."""
    filename = SESSION_INSTRUCTIONS.get(session, "Zen Instructions.md")
    return str(Path(OBSIDIAN_VAULT) / "00_System" / "Project Instructions" / filename)


def queue_message_for_hermes(channel_id: int, channel_name: str, user: str, message: str) -> str:
    """Queue a message for Hermes to process. Returns the queue file path."""
    session = get_session(channel_id)
    instruction_file = get_instruction_file(session)

    payload = {
        "timestamp": datetime.utcnow().isoformat(),
        "session": session,
        "channel_id": channel_id,
        "channel_name": channel_name,
        "user": user,
        "message": message,
        "instruction_file": instruction_file,
        "status": "pending",
        "id": f"{channel_id}_{int(datetime.utcnow().timestamp() * 1000)}",
    }

    queue_file = QUEUE_DIR / f"{payload['id']}.json"
    with open(queue_file, "w") as f:
        json.dump(payload, f, indent=2)

    return str(queue_file)


async def check_hermes_response(queue_file: str) -> str | None:
    """Check if Hermes has responded to a queued message."""
    response_file = Path(queue_file).with_suffix(".response.json")
    if response_file.exists():
        with open(response_file) as f:
            data = json.load(f)
        # Clean up
        response_file.unlink(missing_ok=True)
        Path(queue_file).unlink(missing_ok=True)
        return data.get("response", "No response content.")
    return None


@bot.event
async def on_ready():
    print(f"✅ ZenNew connected as {bot.user}")
    print(f"📡 Allowed user ID: {ALLOWED_USER_ID}")
    print(f"🎯 Prefix: {COMMAND_PREFIX}")
    print(f"📂 Obsidian: {OBSIDIAN_VAULT}")
    print(f"🔗 Channels:")
    for cid, session in CHANNEL_SESSION_MAP.items():
        ch = bot.get_channel(cid)
        name = ch.name if ch else "???"
        print(f"   #{name} ({cid}) → {session} session")
    print(f"📨 Message queue: {QUEUE_DIR}")


@bot.event
async def on_message(message: discord.Message):
    if message.author == bot.user:
        return

    if ALLOWED_USER_ID and message.author.id != ALLOWED_USER_ID:
        return

    if not message.content.strip():
        return

    # Let Discord commands (!ping, !status, !llm, etc.) run locally instead of
    # forwarding them to Hermes as ordinary chat messages first.
    if message.content.strip().startswith(COMMAND_PREFIX):
        await bot.process_commands(message)
        return

    session = get_session(message.channel.id)

    async with message.channel.typing():
        # Queue the message for Hermes
        queue_file = queue_message_for_hermes(
            channel_id=message.channel.id,
            channel_name=message.channel.name,
            user=str(message.author),
            message=message.content,
        )

        # Wait for Hermes to respond (poll queue)
        response = None
        for _ in range(60):  # Wait up to 60 seconds
            await asyncio.sleep(1)
            response = await check_hermes_response(queue_file)
            if response:
                break

        if response is None:
            response = f"⏱️ No response yet. Queued for {session} session."

        # Send response
        if len(response) <= 2000:
            await message.reply(response)
        else:
            chunks = [response[i:i+1900] for i in range(0, len(response), 1900)]
            for i, chunk in enumerate(chunks):
                if i == 0:
                    await message.reply(chunk)
                else:
                    await message.channel.send(chunk)

    await bot.process_commands(message)


@bot.command(name="ping")
async def ping(ctx):
    session = get_session(ctx.channel.id)
    await ctx.reply(f"🏓 Pong! Session: **{session}** | Channel: **{ctx.channel.name}**")


@bot.command(name="status")
async def status(ctx):
    session = get_session(ctx.channel.id)
    instruction_file = get_instruction_file(session)

    embed = discord.Embed(title=f"🔗 ZenNew Bridge — {session.upper()} Session", color=0x00ff00)
    embed.add_field(name="Bot", value=bot.user.mention, inline=True)
    embed.add_field(name="Channel", value=ctx.channel.mention, inline=True)
    embed.add_field(name="Session", value=session, inline=True)
    embed.add_field(name="Instructions", value=instruction_file, inline=False)
    embed.add_field(name="Queue", value=str(QUEUE_DIR), inline=False)
    await ctx.reply(embed=embed)


@bot.command(name="sessions")
async def sessions(ctx):
    """List all channel → session mappings."""
    embed = discord.Embed(title="🔗 Session Mappings", color=0x0099ff)
    for cid, session in CHANNEL_SESSION_MAP.items():
        ch = bot.get_channel(cid)
        ch_name = ch.name if ch else "???"
        embed.add_field(name=f"#{ch_name}", value=f"→ {session}", inline=True)
    await ctx.reply(embed=embed)


@bot.command(name="llm")
async def llm(ctx, choice: str = "help"):
    """Switch the active Hermes model for this Discord session.

    Usage:
      !llm owl      -> openrouter/owl-alpha via OpenRouter
      !llm codex    -> gpt-5.5 via OpenAI Codex
      !llm current  -> ask Hermes to show the current model picker/status
      !llm help     -> show options
    """
    normalized = (choice or "help").strip().lower()

    if normalized in {"help", "?", "list", "options"}:
        embed = discord.Embed(title="🧠 LLM Switcher", color=0x7c3aed)
        embed.add_field(name="Owl Alpha", value="`!llm owl`", inline=True)
        embed.add_field(name="Codex", value="`!llm codex`", inline=True)
        embed.add_field(name="Current / picker", value="`!llm current`", inline=True)
        embed.add_field(
            name="Scope",
            value="Switches this Discord/Hermes session only. Use Hermes `/model ... --global` separately if you want to persist globally.",
            inline=False,
        )
        await ctx.reply(embed=embed)
        return

    if normalized in {"current", "status", "show"}:
        hermes_command = "/model"
        label = "current model / picker"
    else:
        hermes_command = LLM_SWITCH_COMMANDS.get(normalized)
        label = LLM_DISPLAY.get("owl" if normalized.startswith("owl") else normalized, normalized)

    if not hermes_command:
        await ctx.reply(
            f"Unknown LLM choice: `{choice}`\n"
            "Use `!llm owl`, `!llm codex`, or `!llm current`."
        )
        return

    async with ctx.typing():
        queue_file = queue_message_for_hermes(
            channel_id=ctx.channel.id,
            channel_name=ctx.channel.name,
            user=str(ctx.author),
            message=hermes_command,
        )

        response = None
        for _ in range(60):
            await asyncio.sleep(1)
            response = await check_hermes_response(queue_file)
            if response:
                break

    if response is None:
        await ctx.reply(f"⏱️ Queued LLM switch to {label}; Hermes has not responded yet.")
        return

    await ctx.reply(f"🧠 Requested {label}.\n\n{response[:1800]}")


def main():
    if not DISCORD_TOKEN:
        print("❌ ERROR: DISCORD_TOKEN not set in .env")
        return 1
    if not ALLOWED_USER_ID:
        print("⚠️ WARNING: ALLOWED_USER_ID not set - bot responds to everyone")
    print("🚀 Starting ZenNew Direct Bridge with Session Mapping...")
    bot.run(DISCORD_TOKEN)
    return 0


if __name__ == "__main__":
    exit(main())
