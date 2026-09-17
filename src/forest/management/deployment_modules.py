"""Official deployment-scoped management services."""

from __future__ import annotations

from dataclasses import dataclass

from forest.management.accounts import AccountManager
from forest.management.audit import DeploymentAudit
from forest.management.deployment_accounts_contract import (
    DeploymentAccountsManagementContract,
)
from forest.management.deployment_audit_contract import DeploymentAuditManagementContract
from forest.management.deployment_backup_contract import DeploymentBackupManagementContract
from forest.management.deployment_bot_catalog_contract import (
    DeploymentBotCatalogManagementContract,
)
from forest.management.deployment_configuration_contract import (
    DeploymentConfigurationManagementContract,
)
from forest.management.deployment_package_registry_contract import (
    DeploymentPackageRegistryManagementContract,
)
from forest.management.deployment_restore_contract import DeploymentRestoreManagementContract
from forest.runtime_modules import (
    RuntimeManagementContribution,
    RuntimeModuleDescriptor,
    official_artifact,
)


@dataclass(frozen=True)
class DeploymentModuleHost:
    scope_kind: str
    scope_id: str
    store: object
    authority: object
    password_slots: object
    catalog: object
    fleet: object
    managed_available: bool
    backups: object
    verifications: object
    backup_imports: object
    restores: object
    activations: object


class DeploymentAuditRuntime:
    def __init__(self, store):
        self.repository = DeploymentAudit(store)
        self.management_contract = DeploymentAuditManagementContract()


class DeploymentAccountsRuntime:
    def __init__(self, host):
        self.store = host.store
        self.authority = host.authority
        self.password_slots = host.password_slots
        self.manager = AccountManager(host.store, host.authority.vault)
        self.management_contract = DeploymentAccountsManagementContract(self)


class DeploymentConfigurationRuntime:
    def __init__(self, store):
        self.management_contract = DeploymentConfigurationManagementContract(store)


class DeploymentBotCatalogRuntime:
    def __init__(self, host):
        self.catalog = host.catalog
        self.fleet = host.fleet
        self.managed_available = host.managed_available
        self.management_contract = DeploymentBotCatalogManagementContract(self)


class DeploymentPackageRegistryRuntime:
    def __init__(self, host):
        self.fleet = host.fleet
        self.management_contract = DeploymentPackageRegistryManagementContract(self)


class DeploymentBackupRuntime:
    def __init__(self, host):
        self.store = host.store
        self.authority = host.authority
        self.password_slots = host.password_slots
        self.backups = host.backups
        self.verifications = host.verifications
        self.backup_imports = host.backup_imports
        self.management_contract = DeploymentBackupManagementContract(self)


class DeploymentRestoreRuntime:
    def __init__(self, host):
        self.store = host.store
        self.password_slots = host.password_slots
        self.restores = host.restores
        self.activations = host.activations
        self.backups = host.backups
        self.verifications = host.verifications
        self.management_contract = DeploymentRestoreManagementContract(self)


def _audit(host, _modules):
    return DeploymentAuditRuntime(host.store)


def _accounts(host, _modules):
    return DeploymentAccountsRuntime(host)


def _configuration(host, _modules):
    return DeploymentConfigurationRuntime(host.store)


def _bot_catalog(host, _modules):
    return DeploymentBotCatalogRuntime(host)


def _package_registry(host, _modules):
    return DeploymentPackageRegistryRuntime(host)


def _backup(host, _modules):
    return DeploymentBackupRuntime(host)


def _restore(host, _modules):
    return DeploymentRestoreRuntime(host)


OFFICIAL_DEPLOYMENT_MODULES = (
    RuntimeModuleDescriptor(
        "deployment.package-registry",
        (),
        _package_registry,
        official_artifact("deployment"),
        management=(
            RuntimeManagementContribution(
                "packages",
                "包工件",
                ("deployment",),
                "/static/modules/deployment.package-registry.html",
                "/static/package-registry.js",
                group="runtime",
                order=20,
            ),
        ),
    ),
    RuntimeModuleDescriptor(
        "deployment.bot-catalog",
        (),
        _bot_catalog,
        official_artifact("deployment"),
        management=(
            RuntimeManagementContribution(
                "bots",
                "Bot 目录",
                ("deployment",),
                "/static/modules/deployment.bot-catalog.html",
                "/static/bot-catalog.js",
                group="runtime",
                order=10,
            ),
        ),
    ),
    RuntimeModuleDescriptor(
        "deployment.backup",
        ("deployment.package-registry",),
        _backup,
        official_artifact("deployment"),
        management=(
            RuntimeManagementContribution(
                "backups",
                "备份",
                ("deployment",),
                "/static/modules/deployment.backup.html",
                "/static/deployment-backup.js",
                group="storage",
                order=10,
            ),
        ),
    ),
    RuntimeModuleDescriptor(
        "deployment.restore",
        ("deployment.backup",),
        _restore,
        official_artifact("deployment"),
        management=(
            RuntimeManagementContribution(
                "restore",
                "恢复",
                ("deployment",),
                "/static/modules/deployment.restore.html",
                "/static/deployment-restore.js",
                group="storage",
                order=20,
            ),
        ),
    ),
    RuntimeModuleDescriptor(
        "deployment.configuration",
        (),
        _configuration,
        official_artifact("deployment"),
        management=(
            RuntimeManagementContribution(
                "configuration",
                "部署设置",
                ("deployment",),
                "/static/modules/deployment.configuration.html",
                "/static/deployment-configuration.js",
                group="configuration",
                order=10,
            ),
        ),
    ),
    RuntimeModuleDescriptor(
        "deployment.accounts",
        (),
        _accounts,
        official_artifact("deployment"),
        management=(
            RuntimeManagementContribution(
                "accounts",
                "安全与账号",
                ("deployment",),
                "/static/modules/deployment.accounts.html",
                "/static/accounts-panel.js",
                group="security",
                order=10,
            ),
        ),
    ),
    RuntimeModuleDescriptor(
        "deployment.audit",
        (),
        _audit,
        official_artifact("deployment"),
        management=(
            RuntimeManagementContribution(
                "audit",
                "部署审计",
                ("deployment",),
                "/static/modules/deployment.audit.html",
                "/static/deployment-audit.js",
                group="security",
                order=20,
            ),
        ),
    ),
)
