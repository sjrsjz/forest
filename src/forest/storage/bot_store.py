"""Minimal fixed-identity PostgreSQL handle for the clean Bot runtime."""

from __future__ import annotations

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool


class BotStore:
    """Own a Bot-bound pool without legacy schemas, migrations, or business methods."""

    def __init__(self, dsn: str, bot_id: str) -> None:
        try:
            size = len(bot_id.encode("utf-8")) if isinstance(bot_id, str) else 0
        except UnicodeError:
            size = 0
        if (
            not isinstance(bot_id, str)
            or not 1 <= size <= 128
            or any(ord(char) < 32 or ord(char) == 127 for char in bot_id)
        ):
            raise ValueError("bot_id must contain 1 to 128 printable UTF-8 bytes")
        self._bot_id = bot_id
        self.pool = AsyncConnectionPool(
            dsn,
            min_size=1,
            max_size=8,
            open=False,
            timeout=10,
            kwargs={
                "autocommit": True,
                "row_factory": dict_row,
                "options": "-csearch_path=pg_catalog",
            },
        )

    @property
    def bot_id(self) -> str:
        return self._bot_id

    async def open(self) -> None:
        await self.pool.open()
        await self.pool.wait(timeout=15)

    async def close(self) -> None:
        await self.pool.close()
