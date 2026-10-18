"""Generation-owned bounded HTTP I/O for reviewed external packages."""

from __future__ import annotations

import asyncio
import ipaddress
import socket
from urllib.parse import urlsplit

import httpx

from forest.errors import ForestError


class HttpEffectDriver:
    def __init__(self, configuration, identity, *, runtime, payloads, credential_resolver,
                 client_factory=httpx.AsyncClient):
        self.policy = configuration["transport"]
        self.identity = identity
        self.runtime, self.payloads = runtime, payloads
        self.credential_resolver = credential_resolver
        self.client_factory = client_factory
        self.client = None
        self.accepting = False

    async def start(self):
        self.client = self.client_factory(
            trust_env=False, follow_redirects=False,
            timeout=self.policy["timeout_seconds"],
            limits=httpx.Limits(max_connections=4, max_keepalive_connections=0),
        )
        self.accepting = True

    async def _target(self, value):
        try:
            parsed = urlsplit(value)
            if (parsed.scheme not in {"http", "https"} or not parsed.hostname
                    or parsed.username is not None or parsed.password is not None
                    or parsed.fragment or "\\" in value
                    or any(ord(c) <= 32 or ord(c) == 127 for c in value)):
                raise ValueError
            url = httpx.URL(value)
            origin = str(url.copy_with(path="/", query=None)).rstrip("/")
            named = origin in self.policy["allowed_origins"]
            host = parsed.hostname.encode("idna").decode("ascii")
            private = host in self.policy["allowed_private_hosts"]
            if not named and not self.policy["allow_public_urls"] and not private:
                raise ValueError
            port = parsed.port or (443 if parsed.scheme == "https" else 80)
            addresses = await asyncio.to_thread(socket.getaddrinfo, host, port,
                                                type=socket.SOCK_STREAM)
            if not addresses or (not private and any(
                    not ipaddress.ip_address(row[4][0]).is_global for row in addresses)):
                raise ValueError
            return url.copy_with(host=addresses[0][4][0]), url.netloc.decode("ascii"), host, named
        except (ValueError, UnicodeError, OSError, httpx.InvalidURL):
            raise ForestError("HTTP_TARGET_FORBIDDEN", "HTTP target is outside the configured policy", 403) from None

    async def invoke(self, payload, *, scope, invocation):
        if (not self.accepting or self.client is None
                or scope.component_id != self.identity.component_id
                or scope.generation_id != self.identity.generation_id):
            raise ForestError("RUNTIME_GENERATION_NOT_ACTIVE", "HTTP owner is not active", 409)
        await self.runtime.verify_generation_member(
            component_id=self.identity.component_id, generation_id=self.identity.generation_id,
        )
        try:
            target, authority, hostname, named = await self._target(payload["url"])
        except ForestError as error:
            return self._rejected(error.code)
        headers = dict(payload["headers"])
        reserved = {"host", "authorization", "proxy-authorization", "cookie", "content-length",
                    "transfer-encoding", "connection", "accept-encoding"}
        if any(key.lower() in reserved or "\r" in key or "\n" in key
               or "\r" in value or "\n" in value for key, value in headers.items()):
            return self._rejected("HTTP_HEADERS_FORBIDDEN")
        headers.update({"Host": authority, "Accept-Encoding": "identity"})
        credential_ref = self.policy["credential_ref"]
        if credential_ref is not None and named:
            if self.credential_resolver is None:
                return self._rejected("HTTP_CREDENTIAL_UNAVAILABLE")
            try:
                key = await self.credential_resolver(credential_ref, self.policy["credential_purpose"])
            except ForestError as error:
                return self._rejected(error.code)
            headers["Authorization"] = "Bearer " + key
        try:
            async with asyncio.timeout(self.policy["timeout_seconds"]):
                async with self.client.stream(
                    payload["method"], target, headers=headers,
                    content=payload["body"].encode("utf-8") if payload["body"] is not None else None,
                    extensions={"sni_hostname": hostname},
                ) as response:
                    content = bytearray()
                    async for chunk in response.aiter_raw(chunk_size=16384):
                        content.extend(chunk)
                        if len(content) > self.policy["max_response_bytes"]:
                            return self._failure(payload, "HTTP_RESPONSE_TOO_LARGE")
                    visible_headers = {k: v for k, v in response.headers.items()
                                       if k.lower() in {"content-type", "content-encoding", "location", "retry-after", "etag", "last-modified"}}
                    epoch = await self.payloads.epoch()
                    saved = await self.payloads.put_bytes(
                        owner_component_id=self.identity.component_id, owner_kind="http.response",
                        owner_id=invocation.operation_id, media_type="application/octet-stream",
                        content=bytes(content), expected_deletion_epoch=epoch,
                    )
                    return {"outcome": "RECEIVED", "status_code": response.status_code,
                            "headers": visible_headers, "byte_length": len(content),
                            "body": {"payload_id": saved.payload_id, "digest": saved.content_digest,
                                     "owner_component_id": self.identity.component_id,
                                     "owner_kind": "http.response", "owner_id": invocation.operation_id},
                            "error_code": None}
        except (httpx.ConnectError, httpx.ConnectTimeout):
            return self._rejected("HTTP_CONNECT_FAILED")
        except (httpx.HTTPError, TimeoutError, OSError):
            return self._failure(payload, "HTTP_TRANSPORT_FAILED")
        except ForestError as error:
            return self._failure(payload, error.code)

    @staticmethod
    def _rejected(code):
        return {"outcome": "FAILED", "status_code": None, "headers": {},
                "byte_length": 0, "body": None, "error_code": code}

    @staticmethod
    def _failure(payload, code):
        return {"outcome": "FAILED" if payload["read_only"] else "UNKNOWN",
                "status_code": None, "headers": {}, "byte_length": 0, "body": None,
                "error_code": code}

    async def quiesce(self):
        self.accepting = False

    async def resume(self):
        self.accepting = True

    async def close(self):
        self.accepting = False
        client, self.client = self.client, None
        if client is not None:
            await client.aclose()


def http_effect_driver_factory(*, runtime, payloads, credential_resolver, **options):
    def create(configuration, configuration_revision, identity, declaration):
        if declaration["kind"] != "network.http":
            raise ForestError("PACKAGE_EFFECT_DRIVER_BINDING_INVALID", "Not an HTTP I/O declaration", 500)
        return HttpEffectDriver(configuration, identity, runtime=runtime, payloads=payloads,
                                credential_resolver=credential_resolver, **options)
    return create
