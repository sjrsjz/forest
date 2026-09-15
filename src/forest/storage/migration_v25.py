"""Writer fence for compatible ordinary-message Handler takeover."""

from forest.storage.migration_v24 import install_writer_gate_in as install_v24_gate_in

WRITER_GENERATION = 25


async def install_writer_gate_in(conn):
    await install_v24_gate_in(conn)
    await conn.execute(
        "UPDATE public.bot_schema_writer_gate "
        "SET minimum_generation=GREATEST(minimum_generation,25) WHERE singleton"
    )
