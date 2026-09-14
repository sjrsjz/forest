"""Writer fence for the Job definition payload cutover, starting at 060."""

from forest.storage.migration_v2 import install_writer_gate_in as install_v2_gate_in

WRITER_GENERATION = 3


async def install_writer_gate_in(conn):
    """Retain every earlier gate and atomically exclude pre-060 writers."""
    await install_v2_gate_in(conn)
    await conn.execute(
        "UPDATE public.bot_schema_writer_gate "
        "SET minimum_generation=GREATEST(minimum_generation,3) WHERE singleton"
    )
