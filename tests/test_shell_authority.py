"""Shell P0 qualification accepts only explicit verified platform principals."""

import pytest

from forest.shell_authority import is_shell_admin, qualify_priority


@pytest.mark.parametrize(
    "author,config",
    [
        ({"external_user_id": "1", "is_self": True, "identity_origin": "adapter_verified"},
         {"admin_user_ids": ["1"]}),
        ({"external_user_id": "1", "is_self": False, "identity_origin": "adapter_verified",
          "anonymous": {"id": "shared"}}, {"admin_user_ids": ["1"]}),
        ({"external_user_id": "1", "is_self": False, "identity_origin": "message_role"},
         {"admin_user_ids": ["1"]}),
        ({"external_user_id": "2", "is_self": False, "identity_origin": "adapter_verified"},
         {"admin_user_ids": ["1"]}),
        ({"external_user_id": "1", "is_self": False, "identity_origin": "adapter_verified"},
         {"admin_user_ids": "1"}),
    ],
)
def test_non_admin_identity_shapes_never_qualify(author, config):
    assert not is_shell_admin(author, config)
    assert qualify_priority(0, author, config) == {
        "requested_priority": 0,
        "priority": 1,
        "priority_eligible": False,
        "priority_policy": "current-binding-admin-v1",
    }


def test_verified_current_admin_keeps_p0_and_other_classes_are_unchanged():
    author = {"external_user_id": "1", "is_self": False, "identity_origin": "adapter_verified"}
    config = {"admin_user_ids": ["1"]}
    assert is_shell_admin(author, config)
    assert qualify_priority(0, author, config)["priority"] == 0
    assert qualify_priority(1, author, config)["priority"] == 1
    assert qualify_priority(2, author, config)["priority"] == 2
