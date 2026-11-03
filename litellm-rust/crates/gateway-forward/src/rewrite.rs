use axum::{
    body::HttpBody,
    extract::Request,
    http::{HeaderMap, HeaderValue, Response, Uri, Version, header},
};

use crate::{Rejection, server::ClientSocket};

pub const PEER_HEADER: &str = "x-litellm-rust-peer";

/// The request Python receives: the client's method, target, headers and body unchanged, sent to
/// `upstream` over HTTP/1.1 without the client connection's own headers, plus the client's TCP
/// peer in `PEER_HEADER`, which replaces any copy the client sent.
pub(crate) fn upstream_request(
    mut request: Request,
    upstream: &Uri,
    client: ClientSocket,
    websocket: bool,
) -> Result<Request, Rejection> {
    if !only_chunked(request.headers()) {
        return Err(Rejection::UnsupportedTransferCoding);
    }
    let path_and_query = request
        .uri()
        .path_and_query()
        .cloned()
        .ok_or(Rejection::NoPath)?;
    let version = request.version();
    let host = public_host(&request, client)?;
    let headers = request.headers_mut();
    if let Some(host) = host {
        headers.insert(header::HOST, host);
    }
    if version == Version::HTTP_2 {
        join_cookie_crumbs(headers);
    }
    let mut uri = upstream.clone().into_parts();
    uri.path_and_query = Some(path_and_query);
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
        HeaderValue::from_str(&client.peer.to_string()).map_err(|_| Rejection::Internal)?,
    );
    Ok(request)
}

// Python builds absolute URLs from Host and, without a usable one, from the address it listens
// on, which behind Rust is the private loopback port. HTTP/2 carries the host in :authority, which
// retargeting at the upstream replaces, and a client that sent no usable Host gets the public
// address it connected to, which is what Python used before. h11 rejects HTTP/1.1 without Host.
fn public_host(request: &Request, client: ClientSocket) -> Result<Option<HeaderValue>, Rejection> {
    let host = request.headers().get(header::HOST);
    if host.is_some_and(|host| !host.is_empty()) {
        return Ok(None);
    }
    if let Some(authority) = request.uri().authority()
        && request.version() == Version::HTTP_2
    {
        return HeaderValue::from_str(authority.as_str())
            .map(Some)
            .map_err(|_| Rejection::InvalidAuthority);
    }
    if host.is_none() && request.version() == Version::HTTP_11 {
        return Err(Rejection::MissingHost);
    }
    HeaderValue::from_str(&client.local.to_string())
        .map(Some)
        .map_err(|_| Rejection::Internal)
}

// RFC 9113 section 8.2.3: HTTP/2 may split Cookie into one field per cookie, and they must be
// joined back into one before the request crosses to HTTP/1.1.
fn join_cookie_crumbs(headers: &mut HeaderMap) {
    let crumbs: Vec<_> = headers
        .get_all(header::COOKIE)
        .iter()
        .map(HeaderValue::as_bytes)
        .collect();
    if crumbs.len() < 2 {
        return;
    }
    let Ok(cookie) = HeaderValue::from_bytes(&crumbs.join(&b"; "[..])) else {
        return;
    };
    headers.insert(header::COOKIE, cookie);
}

// hyper only decodes chunked framing, so anything else would reach Python still coded but framed
// as a plain body. uvicorn rejects it with the 400 it sends for every h11 parse error.
fn only_chunked(headers: &HeaderMap) -> bool {
    headers
        .get_all(header::TRANSFER_ENCODING)
        .iter()
        .flat_map(|value| value.as_bytes().split(|byte| *byte == b','))
        .all(|coding| coding.trim_ascii().eq_ignore_ascii_case(b"chunked"))
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

const HOP_BY_HOP: [&str; 9] = [
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "proxy-connection",
];

// Per-connection headers (RFC 9110 section 7.6.1) describe the client's connection to Rust, not
// Rust's connection to Python, so a proxy must drop them instead of forwarding them. The rest
// keep their order, which `HeaderMap::remove` would not, because Python reads them as a list.
fn strip_hop_by_hop(headers: &mut HeaderMap) {
    let named: Vec<_> = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|name| header::HeaderName::from_bytes(name.trim().as_bytes()).ok())
        .collect();
    let is_hop_by_hop =
        |name: &header::HeaderName| HOP_BY_HOP.contains(&name.as_str()) || named.contains(name);
    if !headers.keys().any(is_hop_by_hop) {
        return;
    }
    *headers = headers
        .iter()
        .filter(|(name, _)| !is_hop_by_hop(name))
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect();
}

// Stripping drops the handshake's Connection and Upgrade, so each hop gets them back.
fn restore_websocket_handshake(headers: &mut HeaderMap) {
    headers.insert(header::CONNECTION, HeaderValue::from_static("upgrade"));
    headers.insert(header::UPGRADE, HeaderValue::from_static("websocket"));
}
