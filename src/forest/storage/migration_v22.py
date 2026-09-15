"""Writer fence for Actor-owned message-handler execution, starting at 083."""

from forest.storage.migration_v21 import install_writer_gate_in as install_v21_gate_in

WRITER_GENERATION = 22


async def install_writer_gate_in(conn):
    await install_v21_gate_in(conn)
    await conn.execute(
        "UPDATE public.bot_schema_writer_gate "
        "SET minimum_generation=GREATEST(minimum_generation,22) WHERE singleton"
    )
