# Rust forwarding, Python half

With `LITELLM_RUST` on, the supervisor here runs the Rust gateway on the public port and Python on a private loopback listener. This package is the Python half of forwarding and pairs with the crate `litellm-rust/crates/gateway-forward`. Once Rust serves every route, both get deleted together, along with one dispatch branch in each entry point

Every forwarding decision lives here. Entry points (`litellm/proxy/proxy_cli.py`, `gateway/launch.py`, `litellm_cli/main.py`) only translate between their own flags and this package:

- In the supervisor they describe what they would serve with as a `server.PythonServer` and hand it to `supervisor.supervise`, which decides whether forwarding can start, what Rust and the Python child receive, and how to report a refusal
- In the supervised child they merge `mode.uvicorn_options`, `mode.uvicorn_flags` or `mode.gunicorn_bind` and `mode.server_proxy_headers` into their server settings, and wrap the app with `peer.wrap` in their worker factory. These are identities when forwarding is off, so entry points never branch on it

Each environment contract is written and read in one place. `mode.supervised_environment` and `mode.rust_forward_mode` are the two halves of the Python child's, and `supervisor.rust_spec` writes the `LITELLM_FORWARD_*` variables that `gateway-forward`'s `settings.rs` reads. Python owns every value Rust needs, so a new one gets a new variable in `rust_spec`

Python must see each request as if the client had connected directly. `x-litellm-rust-peer` is the only Rust-specific input, and `peer.py` honors it only from the loopback hop. The proxy app itself never knows Rust is in front of it
