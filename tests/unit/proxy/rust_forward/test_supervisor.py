import functools
import signal
from pathlib import Path
from typing import Final

import pytest

from litellm.constants import GRACEFUL_SHUTDOWN_TEARDOWN_HEADROOM_SECONDS
from litellm.proxy.proxy_cli import run_server
from litellm.proxy.rust_forward.mode import Supervised, rust_forward_mode
from litellm.proxy.rust_forward.server import Listener, PythonServer, Server
from litellm.proxy.rust_forward.supervisor import (
    Child,
    Event,
    ExitCode,
    Exited,
    Finished,
    Running,
    Signal,
    StartupError,
    State,
    Stop,
    gateway_binary,
    python_spec,
    rust_spec,
    startup_check,
    step,
    supervise,
)

TERM: Final = signal.SIGTERM
INT: Final = signal.SIGINT


def _server(
    server: Server = Server.UVICORN, listener: Listener = Listener.TCP, trusted_proxies: str | None = None
) -> PythonServer:
    return PythonServer(server, listener, "0.0.0.0", 4000, 2, 2.5, False, trusted_proxies)


def test_python_child_serves_the_private_socket_with_the_operators_trust() -> None:
    spec: Final = python_spec(("python",), 17, 45555, _server(trusted_proxies="10.0.0.5"), {"KEEP": "1"})
    assert rust_forward_mode(spec.env) == Supervised(17, False, "10.0.0.5")
    assert (spec.env["KEEP"], spec.pass_fds) == ("1", (17,))


def test_python_child_keeps_the_public_host_and_its_options() -> None:
    arguments: Final = ("--host", "0.0.0.0", "--port", "4000", "--num_workers", "2", "--reload")
    spec: Final = python_spec(arguments, 17, 45555, _server(), {})
    params: Final = run_server.make_context("litellm", list(spec.argv)).params
    assert (params["host"], params["port"]) == ("0.0.0.0", 45555)
    assert (params["num_workers"], params["reload"]) == (2, True)


def test_rust_child_serves_public_port_and_forwards_to_private_listener() -> None:
    spec: Final = rust_spec("gateway", 45555, _server(), {"RUST_LOG": "info"})
    assert spec.argv == ("gateway",)
    assert spec.pass_fds == ()
    assert dict(spec.env) == {
        "RUST_LOG": "info",
        "LITELLM_FORWARD_UPSTREAM": "http://127.0.0.1:45555",
        "LITELLM_FORWARD_HOST": "0.0.0.0",
        "LITELLM_FORWARD_PORT": "4000",
        "LITELLM_FORWARD_UPSTREAM_KEEPALIVE": "2",
        "LITELLM_FORWARD_SHUTDOWN_DRAIN": str(2.5 + GRACEFUL_SHUTDOWN_TEARDOWN_HEADROOM_SECONDS),
    }


@pytest.mark.parametrize(
    ("server", "reason"),
    [
        (_server(Server.HYPERCORN), "LITELLM_RUST does not support Hypercorn"),
        (_server(Server.GRANIAN), "LITELLM_RUST does not support Granian"),
        (_server(listener=Listener.TLS), "LITELLM_RUST does not support TLS on the public port"),
        (_server(listener=Listener.UNIX_SOCKET), "LITELLM_RUST does not support a Unix socket on the public port"),
        (
            _server(Server.GUNICORN, Listener.INHERITED_FD),
            "LITELLM_RUST does not support an inherited file descriptor on the public port",
        ),
    ],
)
def test_unsupported_servers_are_refused_before_anything_starts(
    server: PythonServer, reason: str, capsys: pytest.CaptureFixture[str]
) -> None:
    assert startup_check(server, "gateway") == StartupError(reason)
    prepared: Final[list[bool]] = []
    assert supervise(server, ("python",), {}, before_start=lambda: prepared.append(True)) == ExitCode.CHILD_FAILED
    assert (capsys.readouterr().err, prepared) == (f"{reason}\n", [])


@pytest.mark.parametrize("server", [Server.UVICORN, Server.GUNICORN])
def test_plain_tcp_uvicorn_and_gunicorn_can_be_forwarded(server: Server) -> None:
    assert startup_check(_server(server), "gateway") == "gateway"


def test_missing_binary_gives_install_instruction() -> None:
    assert startup_check(_server(), None) == StartupError(
        "LITELLM_RUST requires the litellm-gateway binary. Install litellm[proxy] in this environment"
    )


def test_binary_is_found_only_in_the_environment_scripts_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    scripts: Final = tmp_path / "scripts"
    elsewhere: Final = tmp_path / "elsewhere"
    scripts.mkdir()
    elsewhere.mkdir()
    binary: Final = elsewhere / "litellm-gateway"
    binary.write_text("#!/bin/sh\nexit 0\n")
    binary.chmod(0o755)
    monkeypatch.setenv("PATH", str(elsewhere))
    assert gateway_binary(str(scripts)) is None
    assert gateway_binary(str(elsewhere)) == str(binary)


def _fold(events: tuple[Event, ...]) -> tuple[State, tuple[Signal, ...]]:
    def advance(acc: tuple[State, tuple[Signal, ...]], event: Event) -> tuple[State, tuple[Signal, ...]]:
        state, sent = acc
        following, action = step(state, event)
        return following, sent if action is None else (*sent, action)

    return functools.reduce(advance, events, (Running(), ()))


RUST: Final = Child.RUST
PYTHON: Final = Child.PYTHON


@pytest.mark.parametrize(
    ("events", "signals", "code"),
    [
        (
            (Stop(TERM), Exited(RUST, 0), Exited(PYTHON, 0)),
            (Signal(RUST, TERM), Signal(PYTHON, TERM)),
            ExitCode.CLEAN,
        ),
        ((Exited(PYTHON, 0),), (), ExitCode.CHILD_FAILED),
        ((Exited(RUST, 1),), (), ExitCode.CHILD_FAILED),
        ((Stop(TERM), Exited(PYTHON, 0)), (Signal(RUST, TERM),), ExitCode.CHILD_FAILED),
        ((Stop(TERM), Exited(RUST, 1)), (Signal(RUST, TERM),), ExitCode.CHILD_FAILED),
        (
            (Stop(TERM), Exited(RUST, 0), Exited(PYTHON, 1)),
            (Signal(RUST, TERM), Signal(PYTHON, TERM)),
            ExitCode.CHILD_FAILED,
        ),
        (
            (Stop(INT), Stop(INT), Exited(RUST, 0), Exited(PYTHON, 0)),
            (Signal(RUST, INT), Signal(PYTHON, INT)),
            ExitCode.CLEAN,
        ),
        (
            (Exited(RUST, 1), Stop(TERM), Exited(PYTHON, 0)),
            (),
            ExitCode.CHILD_FAILED,
        ),
    ],
)
def test_supervisor_drains_rust_before_python_and_fails_on_unasked_exit(
    events: tuple[Event, ...], signals: tuple[Signal, ...], code: ExitCode
) -> None:
    assert _fold(events) == (Finished(code), signals)
