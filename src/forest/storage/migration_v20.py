"""Writer fence for per-Binding ordinary-message handler policy, starting at 081."""

from forest.storage.migration_v19 import install_writer_gate_in as install_v19_gate_in

WRITER_GENERATION = 20


async def install_writer_gate_in(conn):
    await install_v19_gate_in(conn)
    await conn.execute(
        "UPDATE public.bot_schema_writer_gate "
        "SET minimum_generation=GREATEST(minimum_generation,20) WHERE singleton"
    )
