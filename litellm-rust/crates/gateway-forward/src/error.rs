use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid forwarding upstream URI")]
    UpstreamUri(#[source] axum::http::uri::InvalidUri),
    #[error(
        "LITELLM_FORWARD_UPSTREAM must be a loopback IPv4 http origin without a path, query or credentials"
    )]
    Upstream,
    #[error("{0} must be set in forward mode")]
    MissingSetting(String),
    #[error("invalid forwarding setting: {0}")]
    InvalidSetting(String),
    #[error("forwarding gateway I/O failed: {0}")]
    Io(#[from] std::io::Error),
}

/// Why a request got an answer from Rust instead of from Python.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Rejection {
    #[error("request authority is not a valid Host header")]
    InvalidAuthority,
    #[error("python proxy unreachable: {0}")]
    Unreachable(#[source] hyper_util::client::legacy::Error),
    #[error("python proxy switched protocols without being asked to")]
    UnrequestedUpgrade,
    #[error("python proxy websocket upgrade failed: {0}")]
    UpgradeFailed(#[source] hyper::Error),
    #[error("forwarding gateway is shutting down")]
    ShuttingDown,
    #[error("could not build the request for the python proxy")]
    Internal,
}

impl Rejection {
    fn status(&self) -> StatusCode {
        match self {
            Self::InvalidAuthority => StatusCode::BAD_REQUEST,
            Self::Unreachable(_) | Self::UnrequestedUpgrade | Self::UpgradeFailed(_) => {
                StatusCode::BAD_GATEWAY
            }
            Self::ShuttingDown => StatusCode::SERVICE_UNAVAILABLE,
            Self::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl IntoResponse for Rejection {
    fn into_response(self) -> Response {
        let status = self.status();
        if status.is_server_error() {
            tracing::warn!(error = %self, "request not forwarded");
        }
        status.into_response()
    }
}
