from collections.abc import Mapping
from typing import Final

import pytest
import uvicorn
from click.testing import CliRunner
from gunicorn.config import Config as GunicornConfig

from litellm.constants import GRACEFUL_SHUTDOWN_TEARDOWN_HEADROOM_SECONDS
from litellm.proxy.proxy_cli import ProxyInitializationHelpers, run_server
from litellm.proxy.rust_forward.listener import PublicListener, StartupError, startup_check
from litellm.proxy.rust_forward.supervisor import rust_spec


@pytest.mark.parametrize(
    "flag", ["--ssl_keyfile_path", "--ssl_certfile_path", "--ciphers", "--run_hypercorn", "--run_granian"]
)
def test_unsupported_modes_fail_before_starting_children(flag: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LITELLM_RUST", "true")
    monkeypatch.delenv("_LITELLM_GATEWAY_FD", raising=False)
    values: Final = ["unused"] if flag.startswith("--ssl_") or flag == "--ciphers" else []
    result: Final = CliRunner().invoke(run_server, [flag, *values])
    assert result.exit_code != 0
    assert f"LITELLM_RUST does not support {flag}" in result.output


def test_missing_binary_gives_install_instruction() -> None:
    assert startup_check(None, {}) == StartupError(
        "LITELLM_RUST requires the litellm-gateway binary. Install litellm[proxy] in this environment"
    )


def _cli_listener(argv: list[str]) -> tuple[Mapping[str, object], PublicListener]:
    params: Final = run_server.make_context("litellm", ["--port", "45999", *argv]).params
    return params, ProxyInitializationHelpers.public_listener(
        str(params["host"]), int(str(params["port"])), params["keepalive_timeout"], params["run_gunicorn"] is True
    )


def _uvicorn_keepalive(options: Mapping[str, object]) -> int:
    configured: Final = {"timeout_keep_alive": options["timeout_keep_alive"]} if "timeout_keep_alive" in options else {}
    return uvicorn.Config(app="", log_config=None, **configured).timeout_keep_alive


@pytest.mark.parametrize(
    ("argv", "environ"),
    [(["--keepalive_timeout", "2"], {}), ([], {"KEEPALIVE_TIMEOUT": "7"}), ([], {})],
)
def test_rust_retires_connections_by_the_keepalive_uvicorn_runs_with(
    argv: list[str], environ: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("KEEPALIVE_TIMEOUT", raising=False)
    for name, value in environ.items():
        monkeypatch.setenv(name, value)
    params, listener = _cli_listener(argv)
    uvicorn_options: Final = ProxyInitializationHelpers.get_default_unvicorn_init_args(
        "127.0.0.1", 45555, keepalive_timeout=params["keepalive_timeout"]
    )
    sent: Final = rust_spec("gateway", 45555, listener, {}).env["LITELLM_FORWARD_UPSTREAM_KEEPALIVE"]
    assert sent == str(_uvicorn_keepalive(uvicorn_options))


def test_gunicorn_workers_keep_gunicorns_keepalive_and_rust_follows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("KEEPALIVE_TIMEOUT", raising=False)
    _, listener = _cli_listener(["--run_gunicorn", "--keepalive_timeout", "9"])
    assert listener.keepalive_seconds == GunicornConfig().keepalive


def test_rust_drains_for_pythons_graceful_timeout_plus_teardown(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GRACEFUL_SHUTDOWN_TIMEOUT", "12.5")
    _, listener = _cli_listener([])
    uvicorn_options: Final = ProxyInitializationHelpers.get_default_unvicorn_init_args("127.0.0.1", 45555)
    sent: Final = float(rust_spec("gateway", 45555, listener, {}).env["LITELLM_FORWARD_SHUTDOWN_DRAIN"])
    assert sent == uvicorn_options["timeout_graceful_shutdown"] + GRACEFUL_SHUTDOWN_TEARDOWN_HEADROOM_SECONDS
