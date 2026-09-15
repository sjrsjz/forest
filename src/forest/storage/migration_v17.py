"""Writer fence for business-command operations diagnostics, starting at 078."""

from forest.storage.migration_v16 import install_writer_gate_in as install_v16_gate_in

WRITER_GENERATION = 17


async def install_writer_gate_in(conn):
    await install_v16_gate_in(conn)
    await conn.execute(
        "UPDATE public.bot_schema_writer_gate "
        "SET minimum_generation=GREATEST(minimum_generation,17) WHERE singleton"
    )
