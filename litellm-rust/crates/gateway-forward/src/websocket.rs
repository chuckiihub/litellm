use axum::{
    body::Body,
    extract::Request,
    http::{Method, Version},
    response::Response,
};
use hyper::{
    body::Incoming,
    upgrade::{OnUpgrade, Upgraded},
};
use hyper_util::rt::TokioIo;
use tokio::io::AsyncWriteExt;

use crate::{Rejection, connections::Connections, rewrite};

pub(crate) fn take_handshake(
    connections: &Connections,
    request: &mut Request,
) -> Result<Option<OnUpgrade>, Rejection> {
    if request.method() != Method::GET
        || request.version() != Version::HTTP_11
        || !rewrite::is_websocket_handshake(request.headers())
    {
        return Ok(None);
    }
    if connections.is_draining() {
        return Err(Rejection::ShuttingDown);
    }
    Ok(Some(hyper::upgrade::on(request)))
}

pub(crate) async fn switch(
    connections: &Connections,
    downstream: Option<OnUpgrade>,
    mut response: Response<Incoming>,
) -> Result<Response, Rejection> {
    let Some(downstream) = downstream else {
        return Err(Rejection::UnrequestedUpgrade);
    };
    if !rewrite::is_websocket_handshake(response.headers()) {
        return Err(Rejection::UnrequestedUpgrade);
    }
    let upstream = connections
        .until_closed(hyper::upgrade::on(&mut response))
        .await
        .ok_or(Rejection::ShuttingDown)?
        .map_err(Rejection::UpgradeFailed)?;
    connections.spawn(async move {
        match downstream.await {
            Ok(downstream) => relay(downstream, upstream).await,
            Err(_) => tracing::debug!("websocket upgrade did not complete"),
        }
    });
    Ok(rewrite::switched_response(response).map(|_| Body::empty()))
}

// uvicorn never half-closes a WebSocket, so Python's EOF means the socket is gone and the relay
// ends. The client's EOF only half-closes Python's side, since Python may still send its close
// frame.
async fn relay(downstream: Upgraded, upstream: Upgraded) {
    let (mut client_read, mut client_write) = tokio::io::split(TokioIo::new(downstream));
    let (mut python_read, mut python_write) = tokio::io::split(TokioIo::new(upstream));
    let to_python = async {
        if tokio::io::copy(&mut client_read, &mut python_write)
            .await
            .is_ok()
        {
            python_write.shutdown().await.ok();
            std::future::pending::<()>().await;
        }
    };
    let to_client = async {
        tokio::io::copy(&mut python_read, &mut client_write)
            .await
            .ok();
        client_write.shutdown().await.ok();
    };
    tokio::select! {
        () = to_python => tracing::debug!("websocket client disconnected"),
        () = to_client => {}
    }
}
