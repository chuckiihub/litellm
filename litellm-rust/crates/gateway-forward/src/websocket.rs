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
use tokio_util::{
    sync::{CancellationToken, DropGuard},
    task::TaskTracker,
};

use crate::{Rejection, rewrite};

#[derive(Clone, Default)]
pub(crate) struct Upgrades {
    tasks: TaskTracker,
    draining: CancellationToken,
    closed: CancellationToken,
}

impl Upgrades {
    pub fn begin_shutdown(&self) {
        self.draining.cancel();
    }

    pub fn take_handshake(&self, request: &mut Request) -> Result<Option<OnUpgrade>, Rejection> {
        if request.method() != Method::GET
            || request.version() != Version::HTTP_11
            || !rewrite::is_websocket_handshake(request.headers())
        {
            return Ok(None);
        }
        if self.draining.is_cancelled() {
            return Err(Rejection::ShuttingDown);
        }
        Ok(Some(hyper::upgrade::on(request)))
    }

    pub async fn until_closed<T>(&self, future: impl Future<Output = T>) -> Result<T, Rejection> {
        tokio::select! {
            _ = self.closed.cancelled() => Err(Rejection::ShuttingDown),
            output = future => Ok(output),
        }
    }

    pub async fn switch(
        &self,
        downstream: Option<OnUpgrade>,
        mut response: Response<Incoming>,
    ) -> Result<Response, Rejection> {
        let Some(downstream) = downstream else {
            return Err(Rejection::UnrequestedUpgrade);
        };
        if !rewrite::is_websocket_handshake(response.headers()) {
            return Err(Rejection::UnrequestedUpgrade);
        }
        let upstream = self
            .until_closed(hyper::upgrade::on(&mut response))
            .await?
            .map_err(Rejection::UpgradeFailed)?;
        self.relay(downstream, upstream);
        Ok(rewrite::switched_response(response).map(|_| Body::empty()))
    }

    pub fn close(&self) {
        self.closed.cancel();
    }

    pub fn close_on_drop(&self) -> DropGuard {
        self.closed.clone().drop_guard()
    }

    pub async fn wait(&self) {
        self.tasks.close();
        self.tasks.wait().await;
    }

    fn relay(&self, downstream: OnUpgrade, upstream: Upgraded) {
        let closed = self.closed.clone();
        self.tasks.spawn(async move {
            tokio::select! {
                _ = closed.cancelled() => {}
                _ = async {
                    let Ok(downstream) = downstream.await else {
                        tracing::debug!("websocket upgrade did not complete");
                        return;
                    };
                    let mut downstream = TokioIo::new(downstream);
                    let mut upstream = TokioIo::new(upstream);
                    if let Err(error) = tokio::io::copy_bidirectional(&mut downstream, &mut upstream).await {
                        tracing::debug!(%error, "websocket relay disconnected");
                    }
                } => {}
            }
        });
    }
}
