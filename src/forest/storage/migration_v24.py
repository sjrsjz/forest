"""Writer fence for ordinary-message Handler capacity governance."""

from forest.storage.migration_v23 import install_writer_gate_in as install_v23_gate_in

WRITER_GENERATION = 24


async def install_writer_gate_in(conn):
    await install_v23_gate_in(conn)
    await conn.execute(
        "UPDATE public.bot_schema_writer_gate "
        "SET minimum_generation=GREATEST(minimum_generation,24) WHERE singleton"
    )
