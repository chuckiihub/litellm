import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final

from litellm.rust_bridge.configuration import ENV_NAME


@dataclass(frozen=True, slots=True)
class PublicListener:
    host: str
    port: int
    keepalive_seconds: int
    shutdown_drain_seconds: float
    proxy_headers: bool
    trusted_proxies: str | None


@dataclass(frozen=True, slots=True)
class StartupError:
    reason: str


def startup_check(binary: str | None, unsupported: Mapping[str, object]) -> str | StartupError:
    flag: Final = next((name for name, value in unsupported.items() if value), None)
    if flag is not None:
        return StartupError(f"{ENV_NAME} does not support {flag}")
    if os.name != "posix":
        return StartupError(f"{ENV_NAME} requires POSIX inherited sockets and process signals")
    if binary is None:
        return StartupError(
            f"{ENV_NAME} requires the litellm-gateway binary. Install litellm[proxy] in this environment"
        )
    return binary


def uvicorn_default_keepalive() -> int:
    import uvicorn

    return uvicorn.Config(app="", log_config=None).timeout_keep_alive


def shutdown_drain_seconds(graceful_timeout: float) -> float:
    from litellm.constants import GRACEFUL_SHUTDOWN_TEARDOWN_HEADROOM_SECONDS

    return graceful_timeout + GRACEFUL_SHUTDOWN_TEARDOWN_HEADROOM_SECONDS
