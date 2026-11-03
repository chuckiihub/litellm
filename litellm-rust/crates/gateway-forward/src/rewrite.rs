use std::net::SocketAddr;

use axum::{
    body::HttpBody,
    extract::Request,
    http::{HeaderMap, HeaderValue, Response, Uri, Version, header},
};

use crate::Rejection;

pub const PEER_HEADER: &str = "x-litellm-rust-peer";

/// The request Python receives: the client's method, target, headers and body unchanged, sent to
/// `upstream` over HTTP/1.1 without the client connection's own headers, plus the client's TCP
/// peer in `PEER_HEADER`, which replaces any copy the client sent.
pub(crate) fn upstream_request(
    mut request: Request,
    upstream: &Uri,
    peer: SocketAddr,
    websocket: bool,
) -> Result<Request, Rejection> {
    // HTTP/2 carries the public host in :authority, which retargeting at the upstream replaces.
    if let Some(authority) = request.uri().authority()
        && !request.headers().contains_key(header::HOST)
    {
        let host =
            HeaderValue::from_str(authority.as_str()).map_err(|_| Rejection::InvalidAuthority)?;
        request.headers_mut().insert(header::HOST, host);
    }
    let mut uri = upstream.clone().into_parts();
    uri.path_and_query = request.uri().path_and_query().cloned();
    *request.uri_mut() = Uri::from_parts(uri).map_err(|_| Rejection::Internal)?;
    *request.version_mut() = Version::HTTP_11;
    let chunked = request.body().size_hint().exact().is_none();
    let headers = request.headers_mut();
    strip_hop_by_hop(headers);
    if chunked {
        // hyper sends no body for a GET with an unknown length unless told it is chunked.
        headers.insert(
            header::TRANSFER_ENCODING,
            HeaderValue::from_static("chunked"),
        );
    }
    if websocket {
        restore_websocket_handshake(headers);
    }
    headers.insert(
        PEER_HEADER,
        HeaderValue::from_str(&peer.to_string()).map_err(|_| Rejection::Internal)?,
    );
    Ok(request)
}

/// The response the client receives: Python's, without the upstream connection's own headers.
pub(crate) fn downstream_response<B>(mut response: Response<B>) -> Response<B> {
    strip_hop_by_hop(response.headers_mut());
    response
}

/// Python's 101 as the client receives it, still naming the protocol both sides switch to.
pub(crate) fn switched_response<B>(mut response: Response<B>) -> Response<B> {
    strip_hop_by_hop(response.headers_mut());
    restore_websocket_handshake(response.headers_mut());
    response
}

pub(crate) fn is_websocket_handshake(headers: &HeaderMap) -> bool {
    headers
        .get(header::UPGRADE)
        .is_some_and(|value| value.as_bytes().eq_ignore_ascii_case(b"websocket"))
        && headers
            .get_all(header::CONNECTION)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .flat_map(|value| value.split(','))
            .any(|token| token.trim().eq_ignore_ascii_case("upgrade"))
}

// Per-connection headers (RFC 9110 section 7.6.1) describe the client's connection to Rust, not
// Rust's connection to Python, so a proxy must drop them instead of forwarding them.
fn strip_hop_by_hop(headers: &mut HeaderMap) {
    let connection_headers: Vec<_> = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|name| header::HeaderName::from_bytes(name.trim().as_bytes()).ok())
        .collect();
    for name in connection_headers {
        headers.remove(name);
    }
    for name in [
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
        "proxy-connection",
    ] {
        headers.remove(name);
    }
}

// Stripping drops the handshake's Connection and Upgrade, so each hop gets them back.
fn restore_websocket_handshake(headers: &mut HeaderMap) {
    headers.insert(header::CONNECTION, HeaderValue::from_static("upgrade"));
    headers.insert(header::UPGRADE, HeaderValue::from_static("websocket"));
}
