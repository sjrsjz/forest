"""Writer fence for Bot-lifetime multimodal embedding identity."""

from forest.storage.migration_v25 import install_writer_gate_in as install_v25_gate_in

WRITER_GENERATION = 26


async def install_writer_gate_in(conn):
    await install_v25_gate_in(conn)
    await conn.execute(
        "UPDATE public.bot_schema_writer_gate "
        "SET minimum_generation=GREATEST(minimum_generation,26) WHERE singleton"
    )
