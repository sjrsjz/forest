"""Writer fence for suppressed input occurrence receipts, starting at 070."""

from forest.storage.migration_v9 import install_writer_gate_in as install_v9_gate_in

WRITER_GENERATION = 10


async def install_writer_gate_in(conn):
    """Retain every earlier gate and atomically exclude pre-070 writers."""
    await install_v9_gate_in(conn)
    await conn.execute(
        "UPDATE public.bot_schema_writer_gate "
        "SET minimum_generation=GREATEST(minimum_generation,10) WHERE singleton"
    )
