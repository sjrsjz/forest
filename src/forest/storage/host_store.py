"""Fixed host-identity PostgreSQL handle, independent of deployment identity."""

from __future__ import annotations

from contextlib import asynccontextmanager

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool


class HostStore:
    def __init__(self, dsn: str, host_id: str) -> None:
        if not isinstance(host_id, str) or not host_id.startswith("host/") or (
            len(host_id) != 37 or any(char not in "0123456789abcdef" for char in host_id[5:])
        ):
            raise ValueError("host_id must be a stable host/<32 lowercase hex> identity")
        self._host_id = host_id
        self.pool = AsyncConnectionPool(
            dsn, min_size=1, max_size=4, open=False, timeout=10,
            kwargs={
                "autocommit": True,
                "row_factory": dict_row,
                "options": "-csearch_path=pg_catalog",
            },
        )

    @property
    def host_id(self) -> str:
        return self._host_id

    async def open(self) -> None:
        await self.pool.open()
        await self.pool.wait(timeout=15)

    async def close(self) -> None:
        await self.pool.close()

    @asynccontextmanager
    async def transaction(self, conn=None):
        if conn is not None:
            yield conn
            return
        async with self.pool.connection() as owned, owned.transaction():
            yield owned
