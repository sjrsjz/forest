"""Writer fence for the host-owned Core Shell grammar, starting at 067."""

from forest.storage.migration_v6 import install_writer_gate_in as install_v6_gate_in

WRITER_GENERATION = 7


async def install_writer_gate_in(conn):
    """Retain every earlier gate and atomically exclude pre-067 writers."""
    await install_v6_gate_in(conn)
    await conn.execute(
        "UPDATE public.bot_schema_writer_gate "
        "SET minimum_generation=GREATEST(minimum_generation,7) WHERE singleton"
    )
