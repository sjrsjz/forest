import asyncio
import sys


def pytest_sessionstart(session):
    # Psycopg's asynchronous sockets require the selector loop on Windows.
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
