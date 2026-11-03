import functools
import signal
import sys
from pathlib import Path
from typing import Final

import pytest

from litellm.proxy.proxy_cli import run_server
from litellm.proxy.rust_forward.listener import PublicListener
from litellm.proxy.rust_forward.mode import Supervised, rust_forward_mode
from litellm.proxy.rust_forward.supervisor import (
    Child,
    Event,
    ExitCode,
    Exited,
    Finished,
    Running,
    Signal,
    State,
    Stop,
    cli_command,
    gateway_binary,
    python_spec,
    rust_spec,
    step,
)

TERM: Final = signal.SIGTERM
INT: Final = signal.SIGINT


def _listener(proxy_headers: bool = True, trusted_proxies: str | None = None) -> PublicListener:
    return PublicListener("0.0.0.0", 4000, 5, 40.0, proxy_headers, trusted_proxies)


@pytest.mark.parametrize(
    ("environ", "listener", "expected"),
    [
        ({}, _listener(), Supervised(17, "0.0.0.0", True, "127.0.0.1")),
        ({"FORWARDED_ALLOW_IPS": "10.0.0.5"}, _listener(), Supervised(17, "0.0.0.0", True, "10.0.0.5")),
        ({"FORWARDED_ALLOW_IPS": "10.0.0.5"}, _listener(True, "*"), Supervised(17, "0.0.0.0", True, "*")),
        ({}, _listener(False), Supervised(17, "0.0.0.0", False, "127.0.0.1")),
    ],
)
def test_python_child_keeps_the_trust_python_alone_would_use(
    environ: dict[str, str], listener: PublicListener, expected: Supervised
) -> None:
    spec: Final = python_spec(("python",), 17, 45555, listener, environ)
    assert rust_forward_mode(spec.env) == expected


def test_python_child_listens_privately_and_keeps_options() -> None:
    arguments: Final = ("--host", "0.0.0.0", "--port", "4000", "--num_workers", "2", "--reload")
    spec: Final = python_spec(arguments, 17, 45555, _listener(), {})
    params: Final = run_server.make_context("litellm", list(spec.argv)).params
    assert params["host"] == "127.0.0.1"
    assert params["port"] == 45555
    assert params["num_workers"] == 2
    assert params["reload"] is True
    assert spec.pass_fds == (17,)
    assert cli_command(()) == (sys.executable, "-m", "litellm.proxy.proxy_cli")


def test_rust_child_serves_public_port_and_forwards_to_private_listener() -> None:
    spec: Final = rust_spec(
        "gateway", 45555, PublicListener("0.0.0.0", 4000, 2, 12.5, True, None), {"RUST_LOG": "info"}
    )
    assert spec.argv == ("gateway",)
    assert spec.pass_fds == ()
    assert dict(spec.env) == {
        "RUST_LOG": "info",
        "LITELLM_FORWARD_UPSTREAM": "http://127.0.0.1:45555",
        "LITELLM_FORWARD_HOST": "0.0.0.0",
        "LITELLM_FORWARD_PORT": "4000",
        "LITELLM_FORWARD_UPSTREAM_KEEPALIVE": "2",
        "LITELLM_FORWARD_SHUTDOWN_DRAIN": "12.5",
    }


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
