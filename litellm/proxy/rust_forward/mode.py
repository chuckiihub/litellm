from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final, TypeAlias

from pydantic import TypeAdapter

from litellm.rust_bridge.configuration import environment_switch

GATEWAY_FD: Final = "_LITELLM_GATEWAY_FD"
METRICS_HOST: Final = "_LITELLM_GATEWAY_METRICS_HOST"
PROXY_HEADERS: Final = "_LITELLM_GATEWAY_PROXY_HEADERS"
TRUSTED_PROXIES: Final = "FORWARDED_ALLOW_IPS"
DEFAULT_TRUSTED_PROXIES: Final = "127.0.0.1"
RUST_HOP: Final = "127.0.0.1"

_FD: Final = TypeAdapter(int)
_ENABLED: Final = TypeAdapter(bool)


@dataclass(frozen=True, slots=True)
class Disabled:
    pass


@dataclass(frozen=True, slots=True)
class Supervisor:
    pass


@dataclass(frozen=True, slots=True)
class Supervised:
    listener_fd: int
    metrics_host: str
    proxy_headers: bool
    trusted_proxies: str


RustForwardMode: TypeAlias = Disabled | Supervisor | Supervised


def rust_forward_mode(environ: Mapping[str, str]) -> RustForwardMode:
    fd: Final = environ.get(GATEWAY_FD)
    if fd is not None:
        return Supervised(
            _FD.validate_python(fd),
            environ[METRICS_HOST],
            _ENABLED.validate_python(environ.get(PROXY_HEADERS, "true")),
            environ.get(TRUSTED_PROXIES, DEFAULT_TRUSTED_PROXIES),
        )
    if environment_switch(environ):
        return Supervisor()
    return Disabled()


@dataclass(frozen=True, slots=True)
class ServerOverrides:
    fd: int
    proxy_headers: bool
    metrics_host: str


def server_overrides(mode: Supervised) -> ServerOverrides:
    """The private listener the supervised server binds instead of its own flags.

    Proxy headers stay off at the server, since it would judge Rust's loopback address instead of the real peer.
    ``peer.wrap`` applies the operator's trust settings to the restored peer.
    """
    return ServerOverrides(fd=mode.listener_fd, proxy_headers=False, metrics_host=mode.metrics_host)
