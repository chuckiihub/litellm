from typing import Final

import pytest

from litellm.proxy.proxy_cli import ProxyInitializationHelpers
from litellm.proxy.rust_forward.mode import Disabled, Supervised, Supervisor, rust_forward_mode, server_overrides


@pytest.mark.parametrize(
    ("environ", "expected"),
    [
        ({}, Disabled()),
        ({"LITELLM_RUST": "false"}, Disabled()),
        ({"LITELLM_RUST": "off"}, Disabled()),
        ({"LITELLM_RUST": "0"}, Disabled()),
        ({"LITELLM_RUST": "true"}, Supervisor()),
        ({"LITELLM_RUST": "1"}, Supervisor()),
        ({"LITELLM_RUST": "yes"}, Supervisor()),
        ({"LITELLM_RUST": "on"}, Supervisor()),
        ({"LITELLM_RUST": " 1 "}, Supervisor()),
        ({"LITELLM_RUST": "enabled"}, Disabled()),
        (
            {
                "LITELLM_RUST": "true",
                "_LITELLM_GATEWAY_FD": "17",
                "_LITELLM_GATEWAY_METRICS_HOST": "0.0.0.0",
                "FORWARDED_ALLOW_IPS": "127.0.0.1,10.0.0.5",
            },
            Supervised(17, "0.0.0.0", True, "127.0.0.1,10.0.0.5"),
        ),
    ],
)
def test_mode_is_resolved_from_environment(environ: dict[str, str], expected: object) -> None:
    assert rust_forward_mode(environ) == expected


@pytest.mark.parametrize("mode", [None, Supervised(17, "0.0.0.0", True, "127.0.0.1")])
def test_inherited_socket_reaches_uvicorn(mode: Supervised | None) -> None:
    options: Final = ProxyInitializationHelpers.get_default_unvicorn_init_args(
        "127.0.0.1", 45555, forwarded=None if mode is None else server_overrides(mode)
    )
    assert options.get("fd") == (None if mode is None else mode.listener_fd)
    assert options.get("proxy_headers", True) is (mode is None)
