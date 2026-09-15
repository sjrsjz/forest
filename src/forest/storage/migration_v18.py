"""Writer fence for durable business-command admission limits, starting at 079."""

from forest.storage.migration_v17 import install_writer_gate_in as install_v17_gate_in

WRITER_GENERATION = 18


async def install_writer_gate_in(conn):
    await install_v17_gate_in(conn)
    await conn.execute(
        "UPDATE public.bot_schema_writer_gate "
        "SET minimum_generation=GREATEST(minimum_generation,18) WHERE singleton"
    )
