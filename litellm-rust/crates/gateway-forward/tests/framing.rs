mod support;

use std::net::SocketAddr;

use axum::http::StatusCode;
use rstest::rstest;
use support::{Gateway, Head, Raw, raw_upstream, within};
use tokio::sync::mpsc;

struct Captured {
    head: Head,
    body: Vec<u8>,
}

/// A keep-alive upstream that decodes framing like h11 and reports every request it reads.
async fn capturing() -> (SocketAddr, mpsc::UnboundedReceiver<Captured>) {
    let (sender, receiver) = mpsc::unbounded_channel();
    let address = raw_upstream(move |mut python: Raw| {
        let sender = sender.clone();
        async move {
            while let Some(head) = python.request_head().await {
                let body = match head.header("transfer-encoding") {
                    Some(_) => python.chunked_body().await,
                    None => python.body(head.content_length().unwrap_or(0)).await,
                };
                sender.send(Captured { head, body }).unwrap();
                python
                    .send("HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
                    .await;
            }
        }
    })
    .await;
    (address, receiver)
}

/// RFC 9112 section 6.3: Transfer-Encoding overrides Content-Length. Forwarding both would let
/// Python split the stream at a different place than Rust did.
#[rstest]
#[tokio::test]
async fn a_request_framed_two_ways_reaches_python_framed_by_transfer_encoding() {
    let (upstream, mut received) = capturing().await;
    let gateway = Gateway::start(upstream).await;
    let mut raw = gateway.connect().await;
    raw.send("POST /v1/files HTTP/1.1\r\nhost: x\r\ncontent-length: 3\r\ntransfer-encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\nGET /next HTTP/1.1\r\nhost: x\r\n\r\n")
        .await;
    assert_eq!(
        within(raw.response_head()).await.unwrap().status,
        StatusCode::OK
    );
    raw.body(2).await;
    assert_eq!(
        within(raw.response_head()).await.unwrap().status,
        StatusCode::OK
    );

    let upload = received.recv().await.unwrap();
    assert_eq!(upload.head.header("content-length"), None);
    assert_eq!(
        upload.head.header("transfer-encoding"),
        Some(&b"chunked"[..])
    );
    assert_eq!(upload.body, b"hello");
    assert_eq!(received.recv().await.unwrap().head.path, "/next");
}

#[rstest]
#[case::conflicting_content_lengths("content-length: 4\r\ncontent-length: 40\r\n")]
#[case::content_length_list("content-length: 4, 40\r\n")]
#[case::signed_content_length("content-length: +4\r\n")]
#[case::chunked_not_last("transfer-encoding: chunked, identity\r\ncontent-length: 4\r\n")]
#[case::unknown_coding("transfer-encoding: xchunked\r\ncontent-length: 4\r\n")]
#[case::space_before_colon("transfer-encoding : chunked\r\ncontent-length: 4\r\n")]
#[case::folded_transfer_encoding("transfer-encoding:\r\n chunked\r\ncontent-length: 4\r\n")]
#[case::bare_cr("x-a: b\rx-b: c\r\ncontent-length: 4\r\n")]
#[case::nul_byte("x-a: b\x00c\r\ncontent-length: 4\r\n")]
#[tokio::test]
async fn ambiguous_framing_never_reaches_python(#[case] headers: &str) {
    let (upstream, mut received) = capturing().await;
    let gateway = Gateway::start(upstream).await;
    let mut raw = gateway.connect().await;
    raw.send(format!(
        "POST /v1/files HTTP/1.1\r\nhost: x\r\n{headers}\r\nbodyGET /smuggled HTTP/1.1\r\nhost: x\r\n\r\n"
    ))
    .await;
    assert_eq!(
        within(raw.response_head()).await.unwrap().status,
        StatusCode::BAD_REQUEST
    );
    within(raw.closed()).await;
    assert!(received.try_recv().is_err(), "python received a request");
}

#[rstest]
#[case::bare_lf("4\nbody\n0\n\n")]
#[case::invalid_size("zz\r\nbody\r\n0\r\n\r\n")]
#[case::size_overflow("10000000000000004\r\nbody\r\n0\r\n\r\n")]
#[tokio::test]
async fn a_malformed_request_body_is_a_400_not_a_502(#[case] chunks: &str) {
    let upstream = raw_upstream(|mut python| async move { python.closed().await }).await;
    let gateway = Gateway::start(upstream).await;
    let mut raw = gateway.connect().await;
    raw.send(format!(
        "POST /v1/files HTTP/1.1\r\nhost: x\r\ntransfer-encoding: chunked\r\n\r\n{chunks}"
    ))
    .await;
    assert_eq!(
        within(raw.response_head()).await.unwrap().status,
        StatusCode::BAD_REQUEST
    );
}

/// hyper decodes only the chunked layer, so any other coding would reach Python still coded but
/// framed as a plain body. uvicorn answers these with a 501 itself.
#[rstest]
#[case::listed_before_chunked("transfer-encoding: gzip, chunked\r\n")]
#[case::split_across_fields("transfer-encoding: gzip\r\ntransfer-encoding: chunked\r\n")]
#[tokio::test]
async fn a_transfer_coding_other_than_chunked_is_a_501_python_never_sees(#[case] headers: &str) {
    let (upstream, mut received) = capturing().await;
    let gateway = Gateway::start(upstream).await;
    let mut raw = gateway.connect().await;
    raw.send(format!(
        "POST /v1/files HTTP/1.1\r\nhost: x\r\n{headers}\r\n4\r\nbody\r\n0\r\n\r\n"
    ))
    .await;
    assert_eq!(
        within(raw.response_head()).await.unwrap().status,
        StatusCode::NOT_IMPLEMENTED
    );
    assert!(
        received.try_recv().is_err(),
        "python received a still-coded body"
    );
}
