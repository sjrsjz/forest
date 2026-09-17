"""Bot-scoped audit stream runtime and purpose-bound reader ports."""

from __future__ import annotations

from forest.storage.bot_audit import BotAuditStore


class AuditRuntime:
    """Own the audit query repository without owning producer business semantics."""

    def __init__(self, store, *, control=None):
        self._store = store
        self.store = BotAuditStore(store, control=control)

    def for_control(self, control):
        """Bind the same audit stream to a caller-specific authorization guard."""

        return BotAuditStore(self._store, control=control)
