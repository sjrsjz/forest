"""Writer fence for message-handler diagnostics and revocation convergence."""

from forest.storage.migration_v22 import install_writer_gate_in as install_v22_gate_in

WRITER_GENERATION = 23


async def install_writer_gate_in(conn):
    await install_v22_gate_in(conn)
    await conn.execute(
        "UPDATE public.bot_schema_writer_gate "
        "SET minimum_generation=GREATEST(minimum_generation,23) WHERE singleton"
    )
