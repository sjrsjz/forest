"""One durable token account shared by every model call in a Run."""

from forest.errors import ForestError


async def account_in(conn, bot_id, run_id):
    # Existing active Runs acquire the account without forgetting pre-migration cost.
    await conn.execute(
        "INSERT INTO public.bot_agent_tree_budgets(bot_id,run_id,reserved_tokens,charged_tokens) "
        "SELECT %s,%s,COALESCE(sum(CASE WHEN b.status<>'SETTLED' THEN b.reserved_tokens ELSE 0 END),0),"
        "COALESCE(sum(b.charged_tokens),0) FROM public.bot_model_budget_reservations b "
        "JOIN public.bot_model_transport_attempts a USING(bot_id,transport_attempt_id) "
        "WHERE a.bot_id=%s AND a.run_id=%s ON CONFLICT DO NOTHING",
        (bot_id, run_id, bot_id, run_id),
    )
    return await (
        await conn.execute(
            "SELECT * FROM public.bot_agent_tree_budgets WHERE bot_id=%s AND run_id=%s FOR UPDATE",
            (bot_id, run_id),
        )
    ).fetchone()


async def reserve_in(conn, bot_id, run_id, amount, limit):
    row = await account_in(conn, bot_id, run_id)
    if row["reserved_tokens"] + row["charged_tokens"] + amount > limit:
        raise ForestError(
            "RUN_TOKEN_BUDGET_EXCEEDED",
            "Model calls exhausted this Run's token capacity",
            409,
        )
    await conn.execute(
        "UPDATE public.bot_agent_tree_budgets SET reserved_tokens=reserved_tokens+%s WHERE bot_id=%s AND run_id=%s",
        (amount, bot_id, run_id),
    )


async def settle_in(conn, bot_id, run_id, reserved, charged):
    await account_in(conn, bot_id, run_id)
    await conn.execute(
        "UPDATE public.bot_agent_tree_budgets SET reserved_tokens=reserved_tokens-%s,charged_tokens=charged_tokens+%s WHERE bot_id=%s AND run_id=%s",
        (reserved, charged, bot_id, run_id),
    )
