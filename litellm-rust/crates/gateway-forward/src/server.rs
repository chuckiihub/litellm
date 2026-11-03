use std::{convert::Infallible, io, net::SocketAddr, pin::pin, time::Duration};

use axum::{body::Body, extract::Request};
use hyper::{body::Incoming, service::service_fn};
use hyper_util::{
    rt::{TokioExecutor, TokioIo, TokioTimer},
    server::{conn::auto, graceful::GracefulShutdown},
};
use tokio::net::TcpListener;

use crate::{Forward, forward::MAX_HEADERS};

/// The client's TCP connection to the public listener.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ClientSocket {
    pub peer: SocketAddr,
    pub local: SocketAddr,
}

/// Serves each public connection with hyper directly, because `axum::serve` cannot configure
/// the connection timeouts uvicorn applied to the same clients.
pub(crate) async fn serve(
    forward: Forward,
    listener: TcpListener,
    shutdown: impl Future<Output = ()>,
    drain: Duration,
) {
    let connections = forward.connections().clone();
    let _close_on_drop = connections.close_on_drop();
    let keepalive = forward.keepalive();
    let builder = builder(keepalive);
    let graceful = GracefulShutdown::new();
    let mut shutdown = pin!(shutdown);
    loop {
        let (stream, peer) = tokio::select! {
            () = &mut shutdown => break,
            accepted = listener.accept() => match accepted {
                Ok(accepted) => accepted,
                Err(error) => {
                    back_off(error).await;
                    continue;
                }
            },
        };
        let local = match stream.local_addr() {
            Ok(local) => local,
            Err(error) => {
                tracing::debug!(%error, "client connection closed before it was served");
                continue;
            }
        };
        if let Err(error) = stream.set_nodelay(true) {
            tracing::warn!(%error, "could not disable Nagle on a client connection");
        }
        let socket = ClientSocket { peer, local };
        let forward = forward.clone();
        let service = service_fn(move |request: Request<Incoming>| {
            let forward = forward.clone();
            async move { Ok::<_, Infallible>(forward.handle(socket, request.map(Body::new)).await) }
        });
        let builder = builder.clone();
        let watcher = graceful.watcher();
        let draining = connections.clone();
        connections.spawn(async move {
            // The auto builder reads the first bytes to pick HTTP/1 or HTTP/2 before any hyper
            // timer runs, so the wait for a first request needs its own limit.
            tokio::select! {
                first = tokio::time::timeout(keepalive, stream.readable()) => {
                    if first.is_err() {
                        return;
                    }
                }
                () = draining.draining() => return,
            }
            let connection = builder.serve_connection_with_upgrades(TokioIo::new(stream), service);
            if let Err(error) = watcher.watch(connection.into_owned()).await {
                tracing::debug!(%error, "client connection ended with an error");
            }
        });
    }
    drop(listener);
    connections.begin_shutdown();
    let drained = async {
        graceful.shutdown().await;
        connections.wait().await;
    };
    if tokio::time::timeout(drain, drained).await.is_err() {
        tracing::warn!(
            ?drain,
            "closing connections still open after the shutdown drain"
        );
    }
    connections.close();
    connections.wait().await;
}

// uvicorn closes a client connection that sits idle for `--keepalive_timeout` between requests.
// Without the same limit, a client that vanishes without closing (NAT expiry, a crashed host)
// would hold its socket for the life of the process until the gateway runs out of descriptors.
// Operators already tune that timeout against their load balancer, so the public side keeps
// Python's value instead of introducing its own. hyper only runs the header timer while a
// connection waits for its next request, so it never cuts a long stream, upload or WebSocket.
// HTTP/2 has no idle timer, so unanswered pings retire connections to clients that are gone.
fn builder(keepalive: Duration) -> auto::Builder<TokioExecutor> {
    let mut builder = auto::Builder::new(TokioExecutor::new());
    builder
        .http1()
        .max_headers(MAX_HEADERS)
        .timer(TokioTimer::new())
        .header_read_timeout(keepalive);
    builder
        .http2()
        .timer(TokioTimer::new())
        .keep_alive_interval(keepalive)
        .keep_alive_timeout(keepalive);
    builder
}

async fn back_off(error: io::Error) {
    if matches!(
        error.kind(),
        io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
    ) {
        return;
    }
    tracing::error!(%error, "could not accept a client connection");
    tokio::time::sleep(Duration::from_secs(1)).await;
}
