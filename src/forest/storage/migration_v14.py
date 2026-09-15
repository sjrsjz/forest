"""Writer fence for Actor-owned business-command execution, starting at 075."""

from forest.storage.migration_v13 import install_writer_gate_in as install_v13_gate_in

WRITER_GENERATION = 14


async def install_writer_gate_in(conn):
    await install_v13_gate_in(conn)
    await conn.execute(
        "UPDATE public.bot_schema_writer_gate "
        "SET minimum_generation=GREATEST(minimum_generation,14) WHERE singleton"
    )
