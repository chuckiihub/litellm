mod support;

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    body::Body,
    extract::Request,
    http::{StatusCode, Version},
    response::{IntoResponse, Response},
};
use http_body_util::BodyExt;
use rstest::rstest;
use support::{Gateway, client, h2, raw_upstream, recording, send, within};
use tokio::{sync::mpsc, time::Instant};

const SSE_HEAD: &str = "HTTP/1.1 200 OK\r\n\
    date: Tue, 03 Nov 2026 00:00:00 GMT\r\n\
    server: uvicorn\r\n\
    cache-control: no-cache, no-transform\r\n\
    connection: keep-alive\r\n\
    content-type: text/event-stream\r\n\
    mcp-session-id: session-1\r\n\
    x-accel-buffering: no\r\n\
    transfer-encoding: chunked\r\n\r\n";

const PING: &str = ": ping - 2026-11-03 00:20:00.000000+00:00\r\n\r\n";

const MESSAGE: &str = "event: message\r\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/tools/list_changed\"}\r\n\r\n";

fn chunk(data: &str) -> String {
    format!("{:x}\r\n{data}\r\n", data.len())
}

/// An MCP client holds a GET stream open for server-initiated messages, which may not come for
/// many minutes, so neither hop may close a stream for being idle.
#[rstest]
#[tokio::test(start_paused = true)]
async fn an_idle_mcp_stream_stays_open_in_both_directions() {
    let idle = Duration::from_secs(20 * 60);
    let (session, mut sessions) = mpsc::unbounded_channel();
    let upstream = raw_upstream(move |mut python| {
        let session = session.clone();
        async move {
            let head = python.request_head().await.unwrap();
            session
                .send(head.header("mcp-session-id").map(<[u8]>::to_vec))
                .unwrap();
            python.send(SSE_HEAD).await;
            tokio::time::sleep(idle).await;
            python.send(chunk(PING)).await;
            python.send(chunk(MESSAGE)).await;
            python.send("0\r\n\r\n").await;
        }
    })
    .await;
    let gateway = Gateway::start(upstream).await;
    let started = Instant::now();

    let response = client()
        .request(
            Request::get(gateway.url("/github/mcp"))
                .header("accept", "text/event-stream")
                .header("mcp-session-id", "session-1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.headers()["mcp-session-id"], "session-1");
    assert_eq!(response.headers()["x-accel-buffering"], "no");
    assert!(!response.headers().contains_key("connection"));
    assert_eq!(
        sessions.recv().await.unwrap().as_deref(),
        Some(&b"session-1"[..])
    );
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body, [PING, MESSAGE].concat());
    assert!(started.elapsed() >= idle);
}

/// HTTP/2 has no idle timer, so the gateway's pings run through every exchange, and a stream that
/// goes quiet must outlive any number of pings its client answers.
#[rstest]
#[tokio::test]
async fn an_idle_http2_stream_outlives_the_keepalive_pings_its_client_answers() {
    let keepalive = Duration::from_millis(100);
    let upstream = raw_upstream(move |mut python| async move {
        python.request_head().await.unwrap();
        python.send(SSE_HEAD).await;
        tokio::time::sleep(keepalive * 10).await;
        python.send(chunk(MESSAGE)).await;
        python.send("0\r\n\r\n").await;
    })
    .await;
    let gateway = Gateway::with_keepalive(upstream, keepalive).await;

    let response = within(send(
        &gateway,
        Version::HTTP_2,
        Request::get(gateway.url("/github/mcp"))
            .header("accept", "text/event-stream")
            .body(Body::empty())
            .unwrap(),
    ))
    .await;

    assert_eq!(response.headers()["mcp-session-id"], "session-1");
    let body = within(response.into_body().collect())
        .await
        .unwrap()
        .to_bytes();
    assert_eq!(body, MESSAGE);
}

/// A legacy MCP SSE client holds its GET stream on one HTTP/2 connection and POSTs the messages
/// that Python answers on that stream over the same connection, so the held stream must not
/// stall the streams beside it.
#[rstest]
#[tokio::test]
async fn a_held_stream_does_not_block_requests_multiplexed_beside_it() {
    let (feed, fed) = mpsc::unbounded_channel::<Vec<u8>>();
    let fed = Arc::new(Mutex::new(Some(fed)));
    let upstream = raw_upstream(move |mut python| {
        let feed = feed.clone();
        let fed = fed.clone();
        async move {
            let head = python.request_head().await.unwrap();
            if head.path == "/mcp/sse/messages" {
                feed.send(python.body(head.content_length().unwrap()).await)
                    .unwrap();
                python
                    .send("HTTP/1.1 202 Accepted\r\ncontent-length: 8\r\n\r\nAccepted")
                    .await;
                return;
            }
            let mut fed = fed.lock().unwrap().take().unwrap();
            python
                .send("HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\n")
                .await;
            while let Some(message) = fed.recv().await {
                let event = format!("data: {}\r\n\r\n", String::from_utf8(message).unwrap());
                python.send(chunk(&event)).await;
            }
        }
    })
    .await;
    let gateway = Gateway::start(upstream).await;
    let mut sender = h2(&gateway).await;
    let mut held = within(
        sender.send_request(
            Request::get("http://public.example/mcp/sse")
                .body(Body::empty())
                .unwrap(),
        ),
    )
    .await
    .unwrap();

    for message in ["1", "2"] {
        let accepted = within(
            sender.send_request(
                Request::post("http://public.example/mcp/sse/messages")
                    .body(Body::from(message))
                    .unwrap(),
            ),
        )
        .await
        .unwrap();
        assert_eq!(accepted.status(), StatusCode::ACCEPTED);
        let event = within(held.body_mut().frame()).await.unwrap().unwrap();
        assert_eq!(
            event.into_data().unwrap(),
            format!("data: {message}\r\n\r\n")
        );
    }
}

#[rstest]
#[tokio::test]
async fn ends_a_session_with_a_bodiless_delete() {
    let (upstream, mut received) = recording(|| -> Response {
        ([("mcp-session-id", "session-1")], StatusCode::NO_CONTENT).into_response()
    })
    .await;
    let gateway = Gateway::start(upstream).await;
    let mut raw = gateway.connect().await;
    raw.send("DELETE /mcp HTTP/1.1\r\nhost: x\r\nmcp-session-id: session-1\r\n\r\n")
        .await;

    let head = within(raw.response_head()).await.unwrap();
    assert_eq!(head.status, StatusCode::NO_CONTENT);
    assert_eq!(head.header("mcp-session-id"), Some(&b"session-1"[..]));
    let request = received.recv().await.unwrap();
    assert_eq!(request.method, "DELETE");
    assert_eq!(request.headers["mcp-session-id"], "session-1");
    assert!(!request.headers.contains_key("transfer-encoding"));
    assert!(request.body.is_empty());
}
