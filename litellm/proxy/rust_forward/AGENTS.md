# Rust forwarding, Python half

With `LITELLM_RUST` on, the supervisor here runs the Rust gateway on the public port and Python on a private loopback listener. This package is the Python half of forwarding and pairs with the crate `litellm-rust/crates/gateway-forward`. Once Rust serves every route, both get deleted together, along with one dispatch branch in each entry point

- Nothing outside this package imports Rust forwarding concepts, except the dispatch, the flag adapter and the app wrap in `litellm/proxy/proxy_cli.py` and `gateway/launch.py`. The proxy app itself never knows Rust is in front of it
- Python must see each request as if the client had connected directly. `x-litellm-rust-peer` is the only Rust-specific input, and `peer.py` honors it only from the loopback hop
- Python owns every setting Rust needs. The supervisor resolves the host, port, keepalive and shutdown drain Python actually runs with and hands them to Rust as the `LITELLM_FORWARD_*` variables in `rust_spec`. Rust never reads Python's own settings, so a new value Rust needs gets a new variable there
- Entry points turn their own flags into a `PublicListener`, apply `server_overrides` in the supervised child, and wrap the app with `peer.wrap` where they load it
