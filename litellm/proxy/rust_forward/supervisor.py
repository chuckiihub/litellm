import asyncio
import contextlib
import os
import shutil
import signal
import socket
import sys
import sysconfig
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum, IntEnum
from types import MappingProxyType
from typing import Final, TypeAlias

from pydantic import TypeAdapter
from typing_extensions import assert_never

from litellm.constants import GRACEFUL_SHUTDOWN_TEARDOWN_HEADROOM_SECONDS
from litellm.proxy.rust_forward.mode import RUST_HOP, supervised_environment
from litellm.proxy.rust_forward.server import Listener, PythonServer, Server
from litellm.rust_bridge.configuration import ENV_NAME

_ADDRESS: Final = TypeAdapter(tuple[str, int])


def gateway_binary(scripts_dir: str) -> str | None:
    name: Final = "litellm-gateway.exe" if os.name == "nt" else "litellm-gateway"
    return shutil.which(name, path=scripts_dir)


@dataclass(frozen=True, slots=True)
class StartupError:
    reason: str


def startup_check(server: PythonServer, binary: str | None) -> str | StartupError:
    if server.server in (Server.HYPERCORN, Server.GRANIAN):
        return StartupError(f"{ENV_NAME} does not support {server.server.value}")
    if server.listener is not Listener.TCP:
        return StartupError(f"{ENV_NAME} does not support {server.listener.value} on the public port")
    if os.name != "posix":
        return StartupError(f"{ENV_NAME} requires POSIX inherited sockets and process signals")
    if binary is None:
        return StartupError(
            f"{ENV_NAME} requires the litellm-gateway binary. Install litellm[proxy] in this environment"
        )
    return binary


class Child(Enum):
    RUST = "rust"
    PYTHON = "python"


DRAIN_ORDER: Final = (Child.RUST, Child.PYTHON)


class ExitCode(IntEnum):
    CLEAN = 0
    CHILD_FAILED = 1


@dataclass(frozen=True, slots=True)
class Stop:
    signum: signal.Signals


@dataclass(frozen=True, slots=True)
class Exited:
    child: Child
    code: int


@dataclass(frozen=True, slots=True)
class Running:
    pass


@dataclass(frozen=True, slots=True)
class Draining:
    signum: signal.Signals
    signalled: Child
    remaining: tuple[Child, ...]


@dataclass(frozen=True, slots=True)
class Finished:
    code: ExitCode


@dataclass(frozen=True, slots=True)
class Signal:
    child: Child
    signum: signal.Signals


Event: TypeAlias = Stop | Exited
State: TypeAlias = Running | Draining | Finished


def _signal_next(signum: signal.Signals, order: tuple[Child, ...]) -> tuple[State, Signal | None]:
    if not order:
        return Finished(ExitCode.CLEAN), None
    return Draining(signum, order[0], order[1:]), Signal(order[0], signum)


def step(state: State, event: Event) -> tuple[State, Signal | None]:
    match state:
        case Running():
            match event:
                case Stop(signum):
                    return _signal_next(signum, DRAIN_ORDER)
                case Exited():
                    return Finished(ExitCode.CHILD_FAILED), None
                case _:
                    assert_never(event)
        case Draining(signum, signalled, remaining):
            match event:
                case Stop():
                    return state, None
                case Exited(child, code) if child is signalled and code == 0:
                    return _signal_next(signum, remaining)
                case Exited():
                    return Finished(ExitCode.CHILD_FAILED), None
                case _:
                    assert_never(event)
        case Finished():
            return state, None
        case _:
            assert_never(state)


@dataclass(frozen=True, slots=True)
class ChildSpec:
    argv: tuple[str, ...]
    env: Mapping[str, str]
    pass_fds: tuple[int, ...]


def python_spec(
    command: Sequence[str], listener_fd: int, upstream_port: int, server: PythonServer, environ: Mapping[str, str]
) -> ChildSpec:
    return ChildSpec(
        argv=(*command, "--port", str(upstream_port)),
        env=MappingProxyType(
            {**environ, **supervised_environment(listener_fd, server.proxy_headers, server.trusted_proxies)}
        ),
        pass_fds=(listener_fd,),
    )


def rust_spec(binary: str, upstream_port: int, server: PythonServer, environ: Mapping[str, str]) -> ChildSpec:
    return ChildSpec(
        argv=(binary,),
        env=MappingProxyType(
            {
                **environ,
                "LITELLM_FORWARD_UPSTREAM": f"http://{RUST_HOP}:{upstream_port}",
                "LITELLM_FORWARD_HOST": server.host,
                "LITELLM_FORWARD_PORT": str(server.port),
                "LITELLM_FORWARD_UPSTREAM_KEEPALIVE": str(server.keepalive_seconds),
                "LITELLM_FORWARD_SHUTDOWN_DRAIN": str(
                    server.graceful_timeout_seconds + GRACEFUL_SHUTDOWN_TEARDOWN_HEADROOM_SECONDS
                ),
            }
        ),
        pass_fds=(),
    )


async def _spawn(spec: ChildSpec) -> asyncio.subprocess.Process:
    return await asyncio.create_subprocess_exec(
        *spec.argv, env=dict(spec.env), pass_fds=spec.pass_fds, start_new_session=True
    )


def _terminate_group(process: asyncio.subprocess.Process) -> None:
    with contextlib.suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGTERM)


async def _report_exit(child: Child, process: asyncio.subprocess.Process, events: asyncio.Queue[Event]) -> None:
    events.put_nowait(Exited(child, await process.wait()))


async def _drive(
    state: State, events: asyncio.Queue[Event], children: Mapping[Child, asyncio.subprocess.Process]
) -> ExitCode:
    if isinstance(state, Finished):
        return state.code
    following, action = step(state, await events.get())
    if action is not None:
        with contextlib.suppress(ProcessLookupError):
            children[action.child].send_signal(action.signum)
    return await _drive(following, events, children)


async def _supervise(binary: str, command: Sequence[str], server: PythonServer, environ: Mapping[str, str]) -> ExitCode:
    loop: Final = asyncio.get_running_loop()
    events: Final[asyncio.Queue[Event]] = asyncio.Queue()
    for signum in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signum, events.put_nowait, Stop(signum))
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as private:
        private.bind((RUST_HOP, 0))
        private.listen()
        _, upstream_port = _ADDRESS.validate_python(private.getsockname())
        python: Final = await _spawn(python_spec(command, private.fileno(), upstream_port, server, environ))
        try:
            rust: Final = await _spawn(rust_spec(binary, upstream_port, server, environ))
        except OSError:
            _terminate_group(python)
            await python.wait()
            raise
    children: Final = MappingProxyType({Child.RUST: rust, Child.PYTHON: python})
    waiters: Final = tuple(
        asyncio.create_task(_report_exit(child, process, events)) for child, process in children.items()
    )
    try:
        return await _drive(Running(), events, children)
    finally:
        for process in children.values():
            _terminate_group(process)
        await asyncio.gather(*waiters)


def _nothing_to_prepare() -> None:
    pass


def supervise(
    server: PythonServer,
    command: Sequence[str],
    environ: Mapping[str, str],
    before_start: Callable[[], None] = _nothing_to_prepare,
) -> int:
    """Serve ``server``'s public port from Rust and run ``command`` behind it, or say why that cannot start.

    ``before_start`` runs only once forwarding is known to be possible, so a refused start leaves nothing behind.
    """
    checked: Final = startup_check(server, gateway_binary(sysconfig.get_path("scripts")))
    if isinstance(checked, StartupError):
        sys.stderr.write(f"{checked.reason}\n")
        return ExitCode.CHILD_FAILED
    before_start()
    try:
        return asyncio.run(_supervise(checked, command, server, environ))
    except OSError as error:
        sys.stderr.write(f"Rust gateway startup failed: {error}\n")
        return ExitCode.CHILD_FAILED
