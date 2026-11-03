import os

from litellm.proxy.rust_forward.mode import rust_forward_mode
from litellm.proxy.rust_forward.peer import ASGIApp, wrap


def app() -> ASGIApp:
    from litellm_cli.main import proxy_worker_app

    return wrap(proxy_worker_app(), rust_forward_mode(os.environ))
