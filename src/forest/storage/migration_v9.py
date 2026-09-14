"""Writer fence for durable module enable/disable state, starting at 069."""

from forest.storage.migration_v8 import install_writer_gate_in as install_v8_gate_in

WRITER_GENERATION = 9


async def install_writer_gate_in(conn):
    """Retain every earlier gate and atomically exclude pre-069 writers."""
    await install_v8_gate_in(conn)
    await conn.execute(
        "UPDATE public.bot_schema_writer_gate "
        "SET minimum_generation=GREATEST(minimum_generation,9) WHERE singleton"
    )
