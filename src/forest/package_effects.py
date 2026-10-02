"""Host effect drivers attached to an immutable package Generation."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from forest.errors import ForestError
from forest.package_capabilities import PackageCallScope
from forest.runtime_contracts import RuntimeServiceInvocation
from forest.runtime_generation import BoundGenerationIdentity


class PackageEffectDriver(Protocol):
    """Lifecycle shared by every host effect attached to one Generation."""

    async def start(self) -> None: ...

    async def quiesce(self) -> None: ...

    async def resume(self) -> None: ...

    async def close(self) -> None: ...


@runtime_checkable
class PackageServiceEffectDriver(PackageEffectDriver, Protocol):
    """Effect port for a JSON service declared by a package."""

    async def invoke(
        self,
        payload: dict,
        *,
        scope: PackageCallScope,
        invocation: RuntimeServiceInvocation,
    ) -> dict: ...


@dataclass(frozen=True)
class PackageArtifactDownload:
    content: bytes
    media_type: str
    filename: str


@runtime_checkable
class PackageArtifactDownloadDriver(PackageServiceEffectDriver, Protocol):
    async def download(self, payload: dict, *, max_bytes: int) -> PackageArtifactDownload: ...


PackageIngressHandler = Callable[..., Awaitable[dict]]


@runtime_checkable
class PackageIngressEffectDriver(PackageServiceEffectDriver, Protocol):
    """Generation-owned external source that feeds one private Worker port.

    ``start`` may connect and buffer, but the driver cannot deliver frames until
    ``activate_ingress``. ``freeze_ingress`` must close the producer gate before
    its first suspension; the package then waits for callbacks already handed to
    the Worker.
    """

    def bind_ingress(self, handler: PackageIngressHandler) -> None: ...

    def activate_ingress(self) -> None: ...

    async def freeze_ingress(self) -> None: ...


PackageEffectDriverFactory = Callable[
    [dict, int, BoundGenerationIdentity, Mapping[str, Any]],
    PackageEffectDriver,
]


@dataclass(frozen=True)
class PackageEffectDriverRegistry:
    """Deployment-owned allowlist of effect kinds; packages select by declaration."""

    factories: Mapping[str, PackageEffectDriverFactory]

    def resolve(self, declarations: tuple[dict, ...]) -> dict[str, PackageEffectDriverFactory]:
        resolved: dict[str, PackageEffectDriverFactory] = {}
        for declaration in declarations:
            service_id = declaration["name"]
            factory = self.factories.get(declaration["kind"])
            if factory is None:
                raise ForestError(
                    "PACKAGE_EFFECT_DRIVER_UNAVAILABLE",
                    "Package declares an effect kind with no installed driver",
                    503,
                    details={"kind": declaration["kind"], "service_id": service_id},
                )
            resolved[service_id] = factory
        return resolved


EMPTY_PACKAGE_EFFECT_DRIVERS = PackageEffectDriverRegistry({})
