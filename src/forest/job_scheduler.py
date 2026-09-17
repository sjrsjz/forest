"""Bounded supervision for durable scheduled conversation triggers."""

from __future__ import annotations

import asyncio
from uuid import uuid4

from forest.errors import ForestError


class JobAuthorizationPort:
    """Translate a durable Job delegation into the deployment authority contract."""

    def __init__(self, authority, bot_id: str, store) -> None:
        self.authority = authority
        self.bot_id = bot_id
        self.store = store

    async def authorize(self, claim):
        grant = claim.delegation
        if grant["issuer_id"] != self.authority.issuer_id:
            raise ForestError("JOB_DELEGATION_REVOKED", "Job authority issuer changed", 403)
        request = self.store.authorization_request(claim)
        return await self.authority.issue_delegation(
            subject_id=grant["subject_id"],
            bot_id=self.bot_id,
            operation_id=request["operation_id"],
            payload=request["payload"],
            policy_revision=grant["policy_revision"],
            delegation_id=grant["delegation_id"],
            expected_account_revision=grant["account_revision"],
            expected_key_revision=grant["key_revision"],
        )


class JobDispatcher:
    def __init__(self, store, lease, *, authorize=None, on_change=None,
                 max_concurrency=4, poll_interval=1, operation_timeout=10):
        if (type(max_concurrency) is not int or not 1 <= max_concurrency <= 16
                or not 0.02 <= poll_interval <= 60 or not 1 <= operation_timeout <= 20):
            raise ValueError("Invalid Job dispatcher resource limits")
        self.store, self.lease_source = store, lease
        self.authorize, self.on_change = authorize, on_change
        self.max_concurrency, self.poll_interval = max_concurrency, poll_interval
        self.operation_timeout = operation_timeout
        self.owner_id = "job-dispatcher:" + str(uuid4())
        self.task = None
        self._jobs = set()
        self._wake = asyncio.Event()
        self._closed = False
        self._close_task = None
        self.errors = {}

    def _lease(self):
        return self.lease_source() if callable(self.lease_source) else self.lease_source

    @property
    def failure(self):
        if not self._closed and self.task is not None and self.task.done():
            return ForestError("JOB_DISPATCHER_STOPPED", "Job dispatcher stopped", 503)
        return None

    def _error(self, key, exc):
        if key not in self.errors and len(self.errors) >= 128:
            self.errors.pop(next(iter(self.errors)))
        self.errors[key] = exc.code if isinstance(exc, ForestError) else "JOB_DISPATCH_UNAVAILABLE"

    def notify(self):
        self._wake.set()

    def notify_change(self):
        """Wake both the Job scanner and its downstream conversation scheduler."""

        self.notify()
        if self.on_change is not None:
            self.on_change()

    async def start(self):
        if self._closed:
            raise ForestError("JOB_DISPATCHER_CLOSED", "Job dispatcher is closed", 503)
        if self.task is None:
            self.task = asyncio.create_task(self.run_forever(), name=self.owner_id)

    async def run_forever(self):
        while not self._closed:
            self._wake.clear()
            try:
                async with asyncio.timeout(self.operation_timeout):
                    lease = self._lease()
                    await self.store.reconcile(lease, limit=128)
                    await self.store.expand_due(lease, limit=32)
                    # Scanning is optional in standalone hosts without a current
                    # deployment authority. No expired creator ticket is reused.
                    if self.authorize is not None:
                        for _ in range(self.max_concurrency - len(self._jobs)):
                            claim = await self.store.claim_pending(lease, self.owner_id, ttl_seconds=30)
                            if claim is None:
                                break
                            task = asyncio.create_task(self._dispatch(claim), name="job-occurrence")
                            self._jobs.add(task)
                            task.add_done_callback(self._completed)
                    self.errors.pop("scan", None)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._error("scan", exc)
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self.poll_interval)
            except TimeoutError:
                pass

    def _completed(self, task):
        self._jobs.discard(task)
        if not task.cancelled() and task.exception() is not None:
            self._error("dispatch", task.exception())
        # Polling sets the minimum retry interval; repeated failure must not
        # manufacture an unbounded immediate rescan loop.

    async def _dispatch(self, claim):
        try:
            async with asyncio.timeout(self.operation_timeout):
                authorization = await self.authorize(claim)
                await self.store.dispatch(self._lease(), claim, authorization=authorization)
                if self.on_change is not None:
                    self.on_change()
                self.errors.pop("dispatch", None)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._error("dispatch", exc)
            try:
                async with asyncio.timeout(self.operation_timeout):
                    await self.store.defer(
                        self._lease(), claim, self.errors["dispatch"], delay_seconds=30,
                    )
            except asyncio.CancelledError:
                raise
            except Exception as deferred:
                # An unacknowledged commit is recovered from the same durable
                # occurrence after its lease expires; never invent another ID.
                self._error("defer", deferred)

    async def close(self):
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close(), name="close:" + self.owner_id)
        await asyncio.shield(self._close_task)

    async def _close(self):
        self._closed = True
        self._wake.set()
        if self.task is not None and self.task is not asyncio.current_task():
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        tasks = tuple(self._jobs)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._jobs.clear()
