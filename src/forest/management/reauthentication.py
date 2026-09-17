"""Shared current-password verification for management module contracts."""

from __future__ import annotations

import asyncio

from forest.errors import ForestError
from forest.security import verify_password


async def reauthenticate_admin(store, password_slots, subject_id: str, password: str) -> str:
    async with store.pool.connection() as conn:
        account = await (
            await conn.execute(
                "SELECT password_hash,disabled,role FROM web_accounts WHERE id=%s",
                (subject_id,),
            )
        ).fetchone()
    encoded = account["password_hash"] if account else "malformed"
    async with password_slots:
        valid = await asyncio.to_thread(verify_password, password, encoded)
    if (
        not valid
        or account is None
        or account["disabled"]
        or account["role"] != "admin"
    ):
        raise ForestError("REAUTHENTICATION_FAILED", "Current password is incorrect", 403)
    return account["password_hash"]
