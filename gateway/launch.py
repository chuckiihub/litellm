"""Gateway supervisor: assemble DATABASE_URL, start the in-container PgBouncer, then run uvicorn.

``gateway/main.py`` assembles ``DATABASE_URL`` inside every uvicorn worker, which
is fine for a plain Postgres URL but not for the pooler: PgBouncer must be
started exactly once per pod, before the workers fork, and the workers must be
handed the loopback URL it listens on. A pre-existing ``DATABASE_URL`` wins in
``DatabaseURLSettings.apply_to_env`` under password auth, and one marked pooled
wins under token auth too, so exporting it here is enough for every worker to
pick the pooled URL up unchanged.

LiteLLM imports wait for ``main``: every uvicorn worker imports this module again
before it can answer its first 5 s health ping, so a slow host would kill each one.
Workers then load the gateway app through ``worker_app`` without garbage collection
pauses, which hold the GIL long enough to miss a ping.

Run with:
    python -m gateway.launch --workers 4 --host 0.0.0.0 --port 4000
"""

import os
import sys
import sysconfig
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Final

from typing_extensions import assert_never
from uvicorn.main import main as uvicorn_main

from litellm_cli.main import without_gc_pauses

if TYPE_CHECKING:
    from litellm.proxy.db.db_url_settings import DatabaseURLSettings
    from litellm.proxy.db.pgbouncer import PgBouncerError, PgBouncerSettings
    from litellm.proxy.rust_forward.listener import PublicListener
    from litellm.proxy.rust_forward.mode import ServerOverrides
    from litellm.proxy.rust_forward.peer import ASGIApp

GATEWAY_APP: Final = "gateway.launch:worker_app"
KEEPALIVE_FLAG: Final = "--timeout-keep-alive"


def uvicorn_argv(
    argv: Sequence[str], environ: Mapping[str, str], forwarded: "ServerOverrides | None"
) -> tuple[str, ...]:
    """Honor ``KEEPALIVE_TIMEOUT`` like ``proxy_cli.py`` does, unless the flag was passed explicitly."""
    keepalive: Final = environ.get("KEEPALIVE_TIMEOUT")
    inherited_socket: Final = (
        ()
        if forwarded is None
        else (
            "--fd",
            str(forwarded.fd),
            "--proxy-headers" if forwarded.proxy_headers else "--no-proxy-headers",
        )
    )
    if keepalive is None or any(arg == KEEPALIVE_FLAG or arg.startswith(f"{KEEPALIVE_FLAG}=") for arg in argv):
        return (GATEWAY_APP, "--factory", *argv, *inherited_socket)
    return (GATEWAY_APP, "--factory", *argv, KEEPALIVE_FLAG, keepalive, *inherited_socket)


def pool_database_url(
    settings: "DatabaseURLSettings",
    pgbouncer: "PgBouncerSettings",
    environ: Mapping[str, str],
) -> "str | PgBouncerError | None":
    """Start the in-container PgBouncer and return its loopback URL, or None when ``pgbouncer.enabled`` is off.

    The upstream URL is whatever ``apply_to_env`` assembled from the discrete
    ``DATABASE_*`` vars (or an operator-pinned ``DATABASE_URL``). Under token
    auth the pooler mints and renews the upstream token itself.
    """
    from litellm.proxy.db.pgbouncer import PgBouncerError, start_in_container_pgbouncer

    if not pgbouncer.enabled:
        return None
    upstream_url: Final = environ.get("DATABASE_URL")
    if upstream_url is None:
        return PgBouncerError("LITELLM_PGBOUNCER_ENABLED is set but no DATABASE_URL could be assembled")
    return start_in_container_pgbouncer(pgbouncer, upstream_url, token_auth=settings.token_auth())


def worker_app() -> "ASGIApp":
    with without_gc_pauses():
        from gateway.main import app
        from litellm.proxy.rust_forward.mode import rust_forward_mode
        from litellm.proxy.rust_forward.peer import wrap
    return wrap(app, rust_forward_mode(os.environ))


def _serve(argv: Sequence[str]) -> None:
    uvicorn_main(tuple(argv), prog_name="uvicorn")


def public_listener(argv: Sequence[str], binary: str | None) -> "tuple[str, PublicListener]":
    """Validate the uvicorn flags for Rust forwarding and return the binary and the public listener."""
    from pydantic import TypeAdapter

    from litellm.proxy.rust_forward.listener import PublicListener, StartupError, shutdown_drain_seconds, startup_check
    from litellm.proxy.shutdown.graceful_shutdown_manager import GracefulShutdownManager

    params: Final[Mapping[str, object]] = uvicorn_main.make_context(
        "uvicorn", list(uvicorn_argv(argv, os.environ, None))
    ).params
    checked: Final = startup_check(
        binary,
        {
            "--ssl-keyfile": params["ssl_keyfile"],
            "--ssl-certfile": params["ssl_certfile"],
            "--ssl-ciphers": any(arg.split("=")[0] == "--ssl-ciphers" for arg in argv),
            "--uds": params["uds"] is not None,
            "--fd": params["fd"] is not None,
        },
    )
    if isinstance(checked, StartupError):
        sys.exit(checked.reason)
    graceful_timeout: Final = TypeAdapter[int | None](int | None).validate_python(params["timeout_graceful_shutdown"])
    return checked, PublicListener(
        host=TypeAdapter(str).validate_python(params["host"]),
        port=TypeAdapter(int).validate_python(params["port"]),
        keepalive_seconds=TypeAdapter(int).validate_python(params["timeout_keep_alive"]),
        shutdown_drain_seconds=shutdown_drain_seconds(
            GracefulShutdownManager.get_timeout() if graceful_timeout is None else graceful_timeout
        ),
        proxy_headers=TypeAdapter(bool).validate_python(params["proxy_headers"]),
        trusted_proxies=TypeAdapter[str | None](str | None).validate_python(params["forwarded_allow_ips"]),
    )


def configure_database() -> None:
    from litellm.proxy.db.db_url_settings import DatabaseURLSettings
    from litellm.proxy.db.pgbouncer import PgBouncerError, PgBouncerSettings, export_pooled_database_url

    settings: Final = DatabaseURLSettings.from_env()
    settings.apply_to_env()
    pooled_url: Final = pool_database_url(settings, PgBouncerSettings(), os.environ)
    if isinstance(pooled_url, PgBouncerError):
        sys.exit(f"LiteLLM gateway: in-container pgbouncer could not start: {pooled_url.reason}")
    if pooled_url is not None:
        export_pooled_database_url(pooled_url)


def main(argv: Sequence[str], serve: Callable[[Sequence[str]], None] = _serve) -> None:
    from litellm.proxy.rust_forward.mode import Disabled, Supervised, Supervisor, rust_forward_mode, server_overrides
    from litellm.proxy.rust_forward.supervisor import gateway_binary, supervise

    mode: Final = rust_forward_mode(os.environ)
    match mode:
        case Disabled():
            configure_database()
            serve(uvicorn_argv(argv, os.environ, None))
        case Supervisor():
            binary, listener = public_listener(argv, gateway_binary(sysconfig.get_path("scripts")))
            configure_database()
            command: Final = (sys.executable, "-m", "gateway.launch", *argv)
            sys.exit(supervise(binary, command, listener, os.environ))
        case Supervised():
            serve(uvicorn_argv(argv, os.environ, server_overrides(mode)))
        case _:
            assert_never(mode)


if __name__ == "__main__":
    main(sys.argv[1:])
