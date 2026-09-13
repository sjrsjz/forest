"""Frozen writer fence for the configuration content cutover, starting at 042.

Both this source and the unchanged v1 helper are v2 checksum dependencies.
Future writer/algorithm changes require another versioned module.
"""

from forest.storage.migration_v1 import install_writer_gate_in as install_v1_gate_in

WRITER_GENERATION = 2


async def install_writer_gate_in(conn):
    """After Host retirement, raise the gate inside the migration transaction."""
    await install_v1_gate_in(conn)
    await conn.execute(
        "UPDATE public.bot_schema_writer_gate "
        "SET minimum_generation=GREATEST(minimum_generation,2) WHERE singleton"
    )
