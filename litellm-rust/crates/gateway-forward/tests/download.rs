mod support;

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{body::Body, extract::Request, http::StatusCode, response::Response};
use http_body_util::BodyExt;
use rstest::rstest;
use support::{Gateway, client, raw_upstream, recording, within};
use tokio::sync::Notify;

#[rstest]
#[tokio::test]
async fn streams_a_body_without_a_content_length_as_python_produces_it() {
    let first: &[u8] = &[0xff, 0xfb, 0x90, 0x64, 0x00, 0x0d, 0x0a];
    let rest = vec![0xa5; 64 * 1024];
    let finish = Arc::new(Notify::new());
    let finished = finish.clone();
    let tail = rest.clone();
    let upstream = raw_upstream(move |mut python| {
        let finished = finished.clone();
        let tail = tail.clone();
        async move {
            python.request_head().await.unwrap();
            python
                .send([&b"HTTP/1.1 200 OK\r\ncontent-type: audio/mpeg\r\ntransfer-encoding: chunked\r\n\r\n7\r\n"[..], first, b"\r\n"].concat())
                .await;
            finished.notified().await;
            python
                .send([format!("{:x}\r\n", tail.len()).as_bytes(), &tail, b"\r\n0\r\n\r\n"].concat())
                .await;
        }
    })
    .await;
    let gateway = Gateway::start(upstream).await;
    let mut response = within(
        client().request(
            Request::post(gateway.url("/v1/audio/speech"))
                .body(Body::empty())
                .unwrap(),
        ),
    )
    .await
    .unwrap();

    assert!(!response.headers().contains_key("content-length"));
    let frame = within(response.body_mut().frame()).await.unwrap().unwrap();
    assert_eq!(frame.into_data().unwrap(), first);
    finish.notify_one();
    let body = within(response.into_body().collect())
        .await
        .unwrap()
        .to_bytes();
    assert_eq!(body, rest);
}

#[rstest]
#[tokio::test]
async fn range_requests_and_partial_content_pass_unchanged() {
    let (upstream, mut received) = recording(|| -> Response {
        Response::builder()
            .status(StatusCode::PARTIAL_CONTENT)
            .header("content-range", "bytes 2-5/10")
            .header("accept-ranges", "bytes")
            .body(Body::from("2345"))
            .unwrap()
    })
    .await;
    let gateway = Gateway::start(upstream).await;
    let mut raw = gateway.connect().await;
    raw.send("GET /v1/videos/video_1/content HTTP/1.1\r\nhost: x\r\nrange: bytes=2-5\r\nif-range: \"v1\"\r\n\r\n")
        .await;

    let head = within(raw.response_head()).await.unwrap();
    assert_eq!(head.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(head.header("content-range"), Some(&b"bytes 2-5/10"[..]));
    assert_eq!(raw.body(head.content_length().unwrap()).await, b"2345");
    let request = received.recv().await.unwrap();
    assert_eq!(request.headers["range"], "bytes=2-5");
    assert_eq!(request.headers["if-range"], "\"v1\"");
}

#[rstest]
#[tokio::test]
async fn a_large_download_is_paced_by_the_client_and_keeps_its_content_length() {
    const LENGTH: usize = 128 * 1024 * 1024;
    let written = Arc::new(AtomicUsize::new(0));
    let writing = written.clone();
    let upstream = raw_upstream(move |mut python| {
        let writing = writing.clone();
        async move {
            python.request_head().await.unwrap();
            python
                .send(format!("HTTP/1.1 200 OK\r\ncontent-type: application/octet-stream\r\ncontent-length: {LENGTH}\r\n\r\n"))
                .await;
            let chunk = [0xa5; 64 * 1024];
            while writing.load(Ordering::SeqCst) < LENGTH {
                python.send(chunk).await;
                writing.fetch_add(chunk.len(), Ordering::SeqCst);
            }
        }
    })
    .await;
    let gateway = Gateway::start(upstream).await;
    let mut raw = gateway.connect().await;
    raw.send("GET /v1/files/file-1/content HTTP/1.1\r\nhost: x\r\n\r\n")
        .await;

    let head = within(raw.response_head()).await.unwrap();
    assert_eq!(head.content_length(), Some(LENGTH));
    assert_eq!(head.header("transfer-encoding"), None);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        written.load(Ordering::SeqCst) < LENGTH / 4,
        "the forwarder buffered a download the client was not reading"
    );
    let mut remaining = LENGTH;
    while remaining > 0 {
        remaining -= within(raw.body(remaining.min(1 << 20))).await.len();
    }
}
