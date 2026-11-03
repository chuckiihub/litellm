from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Final, TypeAlias

from pydantic import TypeAdapter
from typing_extensions import assert_never

from litellm.rust_bridge.configuration import environment_switch

GATEWAY_FD: Final = "_LITELLM_GATEWAY_FD"
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
    proxy_headers: bool
    trusted_proxies: str


RustForwardMode: TypeAlias = Disabled | Supervisor | Supervised


def supervised_environment(listener_fd: int, proxy_headers: bool, trusted_proxies: str | None) -> Mapping[str, str]:
    return MappingProxyType(
        {
            GATEWAY_FD: str(listener_fd),
            PROXY_HEADERS: str(proxy_headers).lower(),
            **({} if trusted_proxies is None else {TRUSTED_PROXIES: trusted_proxies}),
        }
    )


def rust_forward_mode(environ: Mapping[str, str]) -> RustForwardMode:
    fd: Final = environ.get(GATEWAY_FD)
    if fd is not None:
        return Supervised(
            _FD.validate_python(fd),
            _ENABLED.validate_python(environ.get(PROXY_HEADERS, "true")),
            environ.get(TRUSTED_PROXIES, DEFAULT_TRUSTED_PROXIES),
        )
    if environment_switch(environ):
        return Supervisor()
    return Disabled()


# A supervised server's TCP peer is always Rust on loopback, so server-level proxy headers would trust any
# client's forwarding headers. They stay off there and ``peer.wrap`` applies the operator's trust instead.


def uvicorn_options(mode: RustForwardMode, factory: str) -> Mapping[str, object]:
    match mode:
        case Supervised(listener_fd):
            return MappingProxyType({"app": factory, "factory": True, "fd": listener_fd, "proxy_headers": False})
        case Disabled() | Supervisor():
            return MappingProxyType({})
        case _:
            assert_never(mode)


def uvicorn_flags(mode: RustForwardMode) -> tuple[str, ...]:
    match mode:
        case Supervised(listener_fd):
            return ("--fd", str(listener_fd), "--no-proxy-headers")
        case Disabled() | Supervisor():
            return ()
        case _:
            assert_never(mode)


def gunicorn_bind(mode: RustForwardMode, public: str) -> str:
    match mode:
        case Supervised(listener_fd):
            return f"fd://{listener_fd}"
        case Disabled() | Supervisor():
            return public
        case _:
            assert_never(mode)


def server_proxy_headers(mode: RustForwardMode) -> bool:
    match mode:
        case Supervised():
            return False
        case Disabled() | Supervisor():
            return True
        case _:
            assert_never(mode)
