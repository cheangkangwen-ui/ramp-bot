"""
Sends ramp reports (text + docx + PDF) to a Telegram group via Telethon.
Works for both company and macro reports — group_name is passed in by the caller.
"""

import asyncio
import os
from pathlib import Path

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.channels import CreateChannelRequest

TELEGRAM_API_ID   = 33919151
TELEGRAM_API_HASH = "dd0a935bd6545cf56910292ff4445c4e"


async def _get_or_create_group(tg, group_name: str, about: str):
    dialogs = await tg.get_dialogs()
    for d in dialogs:
        if d.name == group_name and getattr(d.entity, "megagroup", False):
            return d.entity
    result = await tg(CreateChannelRequest(
        title=group_name,
        about=about,
        megagroup=True,
    ))
    return result.chats[0]


async def _send(report_text: str, doc_path: str, pdf_path: str,
                label: str, asset: str, group_name: str, about: str):
    session_str = os.environ.get("TELEGRAM_SESSION", "")
    session = StringSession(session_str) if len(session_str) > 20 else session_str

    tg = TelegramClient(session, TELEGRAM_API_ID, TELEGRAM_API_HASH)
    await tg.connect()

    if not await tg.is_user_authorized():
        await tg.disconnect()
        raise RuntimeError("Telegram session not authorised. Check TELEGRAM_SESSION in .env.")

    try:
        group = await _get_or_create_group(tg, group_name, about)

        header = f"RAMP REPORT: {label} ({asset.upper()})\n{'=' * 50}\n\n"
        full_text = header + report_text

        chunk_size = 4000
        chunks = []
        remaining = full_text
        while len(remaining) > chunk_size:
            split_at = remaining.rfind("\n", 0, chunk_size)
            if split_at == -1:
                split_at = chunk_size
            chunks.append(remaining[:split_at])
            remaining = remaining[split_at:].lstrip("\n")
        if remaining:
            chunks.append(remaining)

        first_msg = None
        for i, chunk in enumerate(chunks):
            if len(chunks) > 1:
                chunk = f"[{i + 1}/{len(chunks)}]\n\n" + chunk
            sent = await tg.send_message(group, chunk)
            if i == 0:
                first_msg = sent
            await asyncio.sleep(0.5)

        if first_msg:
            await tg.pin_message(group, first_msg.id, notify=False)

        if doc_path and Path(doc_path).exists():
            await tg.send_file(group, doc_path, caption=f"Report: {label} ({asset.upper()})")
        if pdf_path and Path(pdf_path).exists():
            await tg.send_file(group, pdf_path, caption=f"PDF: {label} ({asset.upper()})")

        print(f"  Sent {len(chunks)} message(s) + docx + pdf to '{group_name}'.")
    finally:
        await tg.disconnect()


def send_to_group(report_text: str, doc_path: str, pdf_path: str,
                  label: str, asset: str, group_name: str, about: str = "Ramp research reports"):
    """Synchronous wrapper — call from a thread executor."""
    asyncio.run(_send(report_text, doc_path, pdf_path, label, asset, group_name, about))
