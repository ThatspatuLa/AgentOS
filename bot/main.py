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
    1500437316379086880: "kiyosaki",
    1500437358934233219: "minato",
    1500437397916356608: "rin",
    1500437438680793209: "toji",
    1500437483429564476: "kazuki",
}

# Session → Instruction File
SESSION_INSTRUCTIONS = {
    "zen": "Zen Instructions.md",
    "kiyosaki": "Kiyosaki Instructions.md",
    "minato": "Minato Instructions.md",
    "rin": "Rin Instructions.md",
    "toji": "Toji Instructions.md",
    "kazuki": "Kazuki Instructions.md",
}

# Message queue directory
QUEUE_DIR = Path("/tmp/zennew_queue")
QUEUE_DIR.mkdir(exist_ok=True)

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
