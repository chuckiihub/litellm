use std::{net::SocketAddr, time::Duration};

use axum::{
    Router,
    body::Body,
    extract::{ConnectInfo, DefaultBodyLimit, Request, State},
    http::{StatusCode, Uri},
    response::Response,
    serve::ListenerExt,
};
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::{TokioExecutor, TokioTimer},
};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::{Rejection, Upstream, rewrite, websocket::Upgrades};

// uvicorn sets no response header limit and Python copies every provider header into its
// response, so hyper's default of 100 would turn ordinary responses into 502s.
const PYTHON_RESPONSE_HEADERS: usize = 1000;

#[derive(Clone)]
pub struct Forward {
    upstream: Uri,
    client: Client<HttpConnector, Body>,
    upgrades: Upgrades,
}

impl Forward {
    /// `upstream_keepalive` is how long the upstream keeps an idle connection open. Pooled
    /// connections are dropped well before that, so a request never lands on one it is closing.
    pub fn new(upstream: &Upstream, upstream_keepalive: Duration) -> Self {
        let mut connector = HttpConnector::new();
        connector.set_nodelay(true);
        let client = Client::builder(TokioExecutor::new())
            .http1_max_headers(PYTHON_RESPONSE_HEADERS)
            .pool_timer(TokioTimer::new())
            .pool_idle_timeout(upstream_keepalive / 2)
            .build(connector);
        Self {
            upstream: upstream.uri().clone(),
            client,
            upgrades: Upgrades::default(),
        }
    }

    pub fn router(self) -> Router {
        Router::new()
            .fallback(forward)
            .layer(DefaultBodyLimit::disable())
            .with_state(self)
    }

    /// Serves until `shutdown` resolves, then lets open requests and WebSockets finish for up to
    /// `drain` before closing whatever is left. Python shuts down alongside and ends long streams
    /// itself, so `drain` only backstops a Python that never does.
    pub async fn serve(
        self,
        listener: TcpListener,
        shutdown: impl Future<Output = ()> + Send + 'static,
        drain: Duration,
    ) -> std::io::Result<()> {
        let upgrades = self.upgrades.clone();
        let _close_on_drop = upgrades.close_on_drop();
        let stopping = CancellationToken::new();
        let stop = stopping.clone();
        let draining = upgrades.clone();
        let listener = listener.tap_io(|stream| {
            if let Err(error) = stream.set_nodelay(true) {
                tracing::warn!(%error, "could not disable Nagle on a client connection");
            }
        });
        let server = axum::serve(
            listener,
            self.router()
                .into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(async move {
            shutdown.await;
            draining.begin_shutdown();
            stop.cancel();
        });
        let drained = async {
            let result = server.await;
            if result.is_err() {
                upgrades.close();
            }
            upgrades.wait().await;
            result
        };
        let deadline = async {
            stopping.cancelled().await;
            tokio::time::sleep(drain).await;
            tracing::warn!(
                ?drain,
                "closing connections still open after the shutdown drain"
            );
        };
        let result = tokio::select! {
            result = drained => result,
            () = deadline => Ok(()),
        };
        upgrades.close();
        upgrades.wait().await;
        result
    }
}

async fn forward(
    State(state): State<Forward>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    mut request: Request,
) -> Result<Response, Rejection> {
    let downstream = state.upgrades.take_handshake(&mut request)?;
    let request = rewrite::upstream_request(request, &state.upstream, peer, downstream.is_some())?;
    let response = state.client.request(request);
    let response = match downstream {
        Some(_) => state.upgrades.until_closed(response).await?,
        None => response.await,
    }
    .map_err(Rejection::Unreachable)?;
    if response.status() == StatusCode::SWITCHING_PROTOCOLS {
        return state.upgrades.switch(downstream, response).await;
    }
    Ok(rewrite::downstream_response(response).map(Body::new))
}
