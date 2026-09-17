"""Stable safe metadata projection shared by package-owning management modules."""

from __future__ import annotations


def describe_package(package) -> dict:
    management_ui = package.management_ui()
    storage_bundle = package.storage_bundle()
    component = package.component
    return {
        "package_digest": package.digest,
        "package_id": package.manifest["package_id"],
        "version": package.manifest["version"],
        "component_id": component["id"],
        "state_schema": component["state_schema"],
        "state_migrations": component.get("state_migrations", []),
        "message_handler_takeover": component.get("message_handler_takeover", "none"),
        "config_schema": package.schema(component["config_schema"]),
        "management_ui": management_ui[0] if management_ui else None,
        "storage": storage_bundle[0] if storage_bundle else None,
        "grants": component.get("capabilities", {}).get("requests", []),
        "services": [
            {"name": service["name"], "input_schema": package.schema(service["input_schema"])}
            for service in component["provides"]
        ],
    }
