from types import SimpleNamespace

import pytest

from forest.errors import ForestError
from forest.job_scheduler import JobAuthorizationPort


class Authority:
    issuer_id = "issuer-current"

    def __init__(self):
        self.requests = []

    async def issue_delegation(self, **values):
        self.requests.append(values)
        return {"authorization": "issued", **values}


class Store:
    def __init__(self):
        self.claims = []

    def authorization_request(self, claim):
        self.claims.append(claim)
        return {
            "operation_id": "job-occurrence:occurrence-1",
            "payload": {"occurrence_id": "occurrence-1"},
        }


def claim(*, issuer_id="issuer-current"):
    return SimpleNamespace(
        delegation={
            "issuer_id": issuer_id,
            "subject_id": "account-1",
            "policy_revision": 7,
            "delegation_id": "delegation-1",
            "account_revision": 3,
            "key_revision": 5,
        }
    )


async def test_job_authorization_port_translates_the_frozen_delegation():
    authority, store = Authority(), Store()
    target = claim()

    result = await JobAuthorizationPort(authority, "bot-a", store).authorize(target)

    assert store.claims == [target]
    assert authority.requests == [{
        "subject_id": "account-1",
        "bot_id": "bot-a",
        "operation_id": "job-occurrence:occurrence-1",
        "payload": {"occurrence_id": "occurrence-1"},
        "policy_revision": 7,
        "delegation_id": "delegation-1",
        "expected_account_revision": 3,
        "expected_key_revision": 5,
    }]
    assert result["authorization"] == "issued"


async def test_job_authorization_port_rejects_an_obsolete_issuer_before_store_use():
    authority, store = Authority(), Store()

    with pytest.raises(ForestError) as caught:
        await JobAuthorizationPort(authority, "bot-a", store).authorize(
            claim(issuer_id="issuer-obsolete")
        )

    assert caught.value.code == "JOB_DELEGATION_REVOKED"
    assert store.claims == []
    assert authority.requests == []
