"""Deployment audit HTTP projection."""

from __future__ import annotations

from fastapi import Query, Request

from forest.errors import ForestError
from forest.management.audit import DeploymentAudit
from forest.security import digest_token


def install_audit_routes(app, runtime) -> None:
    @app.get("/management/v1/audit")
    async def read_audit(
        request: Request,
        before: str | None = Query(default=None, min_length=1, max_length=512),
        limit: int = Query(default=50, ge=1, le=200),
        action: str | None = Query(default=None, min_length=1, max_length=256),
        resource_prefix: str | None = Query(default=None, min_length=1, max_length=256),
        actor_id: str | None = Query(default=None, min_length=1, max_length=256),
    ):
        if set(request.query_params) - {
            "before", "limit", "action", "resource_prefix", "actor_id"
        }:
            raise ForestError("AUDIT_QUERY_INVALID", "Unknown audit selector", 422)
        session = await runtime.principal(request, require_admin=True)
        return await DeploymentAudit(runtime.require_store()).read(
            subject_id=session["account_id"],
            session_token_hash=digest_token(request.cookies["forest_session"]),
            before=before,
            limit=limit,
            action=action,
            resource_prefix=resource_prefix,
            actor_id=actor_id,
        )
