"""Writer fence for atomic business-command registry publication, starting at 072."""

from forest.storage.migration_v11 import install_writer_gate_in as install_v11_gate_in

WRITER_GENERATION = 12


async def install_writer_gate_in(conn):
    """Retain every earlier gate and atomically exclude pre-072 writers."""
    await install_v11_gate_in(conn)
    await conn.execute(
        "UPDATE public.bot_schema_writer_gate "
        "SET minimum_generation=GREATEST(minimum_generation,12) WHERE singleton"
    )
