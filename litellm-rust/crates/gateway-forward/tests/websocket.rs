mod support;

use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use axum::{
    Router,
    http::{HeaderValue, StatusCode},
    response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt};
use litellm_gateway_forward::PEER_HEADER;
use rstest::rstest;
use support::{DRAIN, Gateway, Head, Raw, raw_upstream, serve, within};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::mpsc,
};
use tokio_tungstenite::{
    WebSocketStream, accept_hdr_async, client_async,
    tungstenite::{
        Error, Message,
        client::IntoClientRequest,
        handshake::{
            derive_accept_key,
            server::{ErrorResponse, Request as Handshake, Response as Accepted},
        },
        protocol::{CloseFrame, frame::coding::CloseCode},
    },
};

const KEY: &str = "dGhlIHNhbXBsZSBub25jZQ==";

async fn connect(
    address: SocketAddr,
    connection: &'static str,
) -> Result<WebSocketStream<TcpStream>, Box<Error>> {
    let mut request = format!("ws://{address}/v1/realtime?model=m")
        .into_client_request()
        .unwrap();
    let headers = request.headers_mut();
    headers.insert("authorization", HeaderValue::from_static("Bearer test"));
    headers.insert(
        "sec-websocket-protocol",
        HeaderValue::from_static("realtime"),
    );
    headers.insert("connection", HeaderValue::from_static(connection));
    let stream = TcpStream::connect(address).await.unwrap();
    let (socket, response) = client_async(request, stream).await.map_err(Box::new)?;
    assert_eq!(response.headers()["sec-websocket-protocol"], "realtime");
    Ok(socket)
}

/// Greets, echoes every message, and closes with 4000 when told to "bye".
#[expect(
    clippy::result_large_err,
    reason = "tungstenite's handshake callback returns ErrorResponse"
)]
async fn echo() -> SocketAddr {
    raw_upstream(|python| async move {
        let callback = |request: &Handshake, mut response: Accepted| {
            assert_eq!(request.uri(), "/v1/realtime?model=m");
            assert_eq!(request.headers()["authorization"], "Bearer test");
            assert!(request.headers().contains_key(PEER_HEADER));
            response.headers_mut().insert(
                "sec-websocket-protocol",
                HeaderValue::from_static("realtime"),
            );
            Ok::<_, ErrorResponse>(response)
        };
        let mut socket = accept_hdr_async(python.into_stream(), callback)
            .await
            .unwrap();
        socket.send(Message::text("greeting")).await.unwrap();
        while let Some(Ok(message)) = socket.next().await {
            if message == Message::text("bye") {
                socket
                    .close(Some(CloseFrame {
                        code: CloseCode::Library(4000),
                        reason: "done".into(),
                    }))
                    .await
                    .unwrap();
                continue;
            }
            if message.is_close() {
                break;
            }
            socket.send(message).await.unwrap();
        }
    })
    .await
}

fn handshake(target: &str, headers: &str) -> String {
    format!(
        "GET {target} HTTP/1.1\r\nhost: public.example\r\nconnection: Upgrade\r\nupgrade: websocket\r\nsec-websocket-version: 13\r\nsec-websocket-key: {KEY}\r\n{headers}\r\n"
    )
}

/// The 101 uvicorn writes for `handshake`, with `headers` added.
fn switching(handshake: &Head, headers: &str) -> String {
    let accept = derive_accept_key(handshake.header("sec-websocket-key").unwrap());
    format!(
        "HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\nconnection: Upgrade\r\nsec-websocket-accept: {accept}\r\n{headers}\r\n"
    )
}

fn values(head: &Head, name: &str) -> Vec<String> {
    head.headers
        .iter()
        .filter(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| String::from_utf8(value.clone()).unwrap())
        .collect()
}

/// Accepts with `headers` in the 101, hands the handshake head to the test, then echoes bytes.
async fn byte_echo(headers: &'static str) -> (SocketAddr, mpsc::UnboundedReceiver<Head>) {
    let (sender, handshakes) = mpsc::unbounded_channel();
    let upstream = raw_upstream(move |mut python: Raw| {
        let sender = sender.clone();
        async move {
            let handshake = python.request_head().await.unwrap();
            python.send(switching(&handshake, headers)).await;
            sender.send(handshake).unwrap();
            let mut stream = python.into_stream();
            let (mut read, mut write) = stream.split();
            tokio::io::copy(&mut read, &mut write).await.ok();
        }
    })
    .await;
    (upstream, handshakes)
}

async fn open(gateway: &Gateway, handshake: String) -> (TcpStream, Head) {
    let mut raw = gateway.connect().await;
    raw.send(handshake).await;
    let switched = within(raw.response_head()).await.unwrap();
    assert_eq!(switched.status, StatusCode::SWITCHING_PROTOCOLS);
    (raw.into_stream(), switched)
}

#[rstest]
#[tokio::test]
async fn relays_messages_both_ways(
    #[values("Upgrade", "keep-alive, Upgrade", "upgrade")] connection: &'static str,
    #[values(
        Message::text("hello"),
        Message::binary(vec![0, 1, 2]),
        Message::binary(vec![7; 4 * 1024 * 1024])
    )]
    message: Message,
) {
    let gateway = Gateway::start(echo().await).await;
    let mut socket = within(connect(gateway.address, connection)).await.unwrap();
    assert_eq!(
        socket.next().await.unwrap().unwrap(),
        Message::text("greeting")
    );
    socket.send(message.clone()).await.unwrap();
    assert_eq!(within(socket.next()).await.unwrap().unwrap(), message);
    socket.close(None).await.unwrap();
    while socket.next().await.is_some() {}
    drop(socket);
    within(gateway.shutdown()).await.unwrap();
}

#[rstest]
#[tokio::test]
async fn relays_a_close_python_initiates() {
    let gateway = Gateway::start(echo().await).await;
    let mut socket = within(connect(gateway.address, "Upgrade")).await.unwrap();
    socket.next().await.unwrap().unwrap();
    socket.send(Message::text("bye")).await.unwrap();
    let Message::Close(Some(frame)) = within(socket.next()).await.unwrap().unwrap() else {
        panic!("expected python's close frame");
    };
    assert_eq!(frame.code, CloseCode::Library(4000));
    assert_eq!(frame.reason, "done");
}

#[rstest]
#[tokio::test]
async fn passes_an_upstream_rejection_through() {
    let upstream = serve(
        Router::new()
            .fallback(|| async { (StatusCode::FORBIDDEN, "origin not allowed").into_response() }),
    )
    .await;
    let gateway = Gateway::start(upstream).await;
    let error = within(connect(gateway.address, "Upgrade"))
        .await
        .unwrap_err();
    let Error::Http(response) = *error else {
        panic!("expected the upstream rejection, got {error}");
    };
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(response.body().as_deref(), Some(&b"origin not allowed"[..]));
}

/// The idle timeout only covers a connection waiting for its next HTTP request, and a realtime
/// session may sit silent between turns.
#[rstest]
#[tokio::test]
async fn keeps_a_quiet_socket_open_past_the_keepalive() {
    let gateway = Gateway::with_keepalive(echo().await, Duration::from_millis(400)).await;
    let mut socket = within(connect(gateway.address, "Upgrade")).await.unwrap();
    socket.next().await.unwrap().unwrap();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    socket.send(Message::text("still here")).await.unwrap();
    assert_eq!(
        within(socket.next()).await.unwrap().unwrap(),
        Message::text("still here")
    );
}

/// Browsers carry the API key in a subprotocol, Python accepts with the first one it was offered,
/// and uvicorn negotiates permessage-deflate, so every handshake header must cross unchanged.
#[rstest]
#[tokio::test]
async fn handshake_headers_cross_unchanged_both_ways() {
    let (upstream, mut handshakes) = byte_echo(
        "sec-websocket-protocol: realtime\r\nsec-websocket-extensions: permessage-deflate; server_max_window_bits=12; client_max_window_bits=12\r\nset-cookie: a=1\r\nset-cookie: b=2\r\nx-litellm-call-id: abc\r\n",
    )
    .await;
    let gateway = Gateway::start(upstream).await;
    let target = "/v1/realtime?model=gpt-realtime&api-key=sk%2Fa%2Bb%3D&dup=1&dup=2";
    let (_socket, switched) = open(
        &gateway,
        handshake(
            target,
            "sec-websocket-protocol: realtime, openai-insecure-api-key.sk-1234, openai-beta.realtime-v1\r\nsec-websocket-protocol: second-line\r\nsec-websocket-extensions: permessage-deflate; client_max_window_bits\r\ncookie: token=abc\r\n",
        ),
    )
    .await;
    let handshake = within(handshakes.recv()).await.unwrap();

    assert_eq!(handshake.path, target);
    assert_eq!(
        values(&handshake, "sec-websocket-protocol"),
        [
            "realtime, openai-insecure-api-key.sk-1234, openai-beta.realtime-v1",
            "second-line"
        ]
    );
    assert_eq!(
        values(&handshake, "sec-websocket-extensions"),
        ["permessage-deflate; client_max_window_bits"]
    );
    assert_eq!(values(&handshake, "cookie"), ["token=abc"]);
    assert_eq!(values(&switched, "sec-websocket-protocol"), ["realtime"]);
    assert_eq!(
        values(&switched, "sec-websocket-extensions"),
        ["permessage-deflate; server_max_window_bits=12; client_max_window_bits=12"]
    );
    assert_eq!(values(&switched, "set-cookie"), ["a=1", "b=2"]);
    assert_eq!(values(&switched, "x-litellm-call-id"), ["abc"]);
}

#[rstest]
#[tokio::test]
async fn invents_no_subprotocol_or_extension() {
    let (upstream, mut handshakes) = byte_echo("").await;
    let gateway = Gateway::start(upstream).await;
    let (_socket, switched) = open(&gateway, handshake("/v1/realtime", "")).await;
    let handshake = within(handshakes.recv()).await.unwrap();
    for name in ["sec-websocket-protocol", "sec-websocket-extensions"] {
        assert!(handshake.header(name).is_none());
        assert!(switched.header(name).is_none());
    }
}

#[rstest]
#[case::capitalized_upgrade("Upgrade: WebSocket\r\nConnection: keep-alive, Upgrade\r\n")]
#[case::uppercase("UPGRADE: WEBSOCKET\r\nCONNECTION: UPGRADE\r\n")]
#[case::split_connection("Upgrade: websocket\r\nConnection: keep-alive\r\nConnection: Upgrade\r\n")]
#[tokio::test]
async fn accepts_any_spelling_of_the_upgrade_headers(#[case] upgrade: &str) {
    let (upstream, _handshakes) = byte_echo("").await;
    let gateway = Gateway::start(upstream).await;
    let (mut socket, _) = open(
        &gateway,
        format!(
            "GET /v1/realtime HTTP/1.1\r\nhost: x\r\n{upgrade}sec-websocket-version: 13\r\nsec-websocket-key: {KEY}\r\n\r\n"
        ),
    )
    .await;
    socket.write_all(b"ping").await.unwrap();
    let mut echoed = [0; 4];
    within(socket.read_exact(&mut echoed)).await.unwrap();
    assert_eq!(&echoed, b"ping");
}

/// Compressed frames set RSV1 and every client frame is masked, so the socket is relayed as bytes,
/// never re-framed.
#[rstest]
#[tokio::test]
async fn relays_socket_bytes_exactly() {
    let (upstream, _handshakes) = byte_echo("").await;
    let gateway = Gateway::start(upstream).await;
    let (socket, _) = open(&gateway, handshake("/v1/realtime", "")).await;
    let sent: Vec<u8> = (0..3_000_017u32)
        .map(|index| index.wrapping_mul(2_654_435_761).to_be_bytes()[1])
        .collect();
    let (mut read, mut write) = socket.into_split();
    let writing = sent.clone();
    let writer = tokio::spawn(async move {
        for chunk in writing.chunks(7919) {
            write.write_all(chunk).await.unwrap();
        }
        write
    });
    let mut echoed = vec![0; sent.len()];
    within(read.read_exact(&mut echoed)).await.unwrap();
    assert!(echoed == sent);
    drop(writer.await.unwrap());
}

/// uvicorn writes the 101, an error event and a close frame back to back and then closes, so they
/// share a segment with the FIN, and a client may send its first frame with the handshake.
#[rstest]
#[tokio::test]
async fn keeps_bytes_that_arrive_with_the_handshake() {
    let (early, mut received) = mpsc::unbounded_channel();
    let upstream = raw_upstream(move |mut python: Raw| {
        let early = early.clone();
        async move {
            let handshake = python.request_head().await.unwrap();
            let mut segment = switching(&handshake, "").into_bytes();
            segment.extend(b"\x81\x05error\x88\x08\x03\xf0denied");
            python.send(segment).await;
            early.send(within(python.body(6)).await).unwrap();
        }
    })
    .await;
    let gateway = Gateway::start(upstream).await;
    let mut raw = gateway.connect().await;
    raw.send(handshake("/v1/realtime", "") + "early!").await;
    assert_eq!(
        within(raw.response_head()).await.unwrap().status,
        StatusCode::SWITCHING_PROTOCOLS
    );
    assert_eq!(within(received.recv()).await.unwrap(), b"early!");
    assert_eq!(
        within(raw.until_closed()).await,
        b"\x81\x05error\x88\x08\x03\xf0denied"
    );
}

/// Python frees a realtime session's budget and parallel slot when its socket closes, so a client
/// that disappears without a close frame must still close Python's socket.
#[rstest]
#[case::fin(false)]
#[case::reset(true)]
#[tokio::test]
async fn a_vanished_client_closes_the_python_socket(#[case] reset: bool) {
    let (event, mut events) = mpsc::unbounded_channel();
    let upstream = raw_upstream(move |mut python: Raw| {
        let event = event.clone();
        async move {
            let handshake = python.request_head().await.unwrap();
            python.send(switching(&handshake, "")).await;
            event.send("open").unwrap();
            python.closed().await;
            event.send("closed").unwrap();
        }
    })
    .await;
    let gateway = Gateway::start(upstream).await;
    let (socket, _) = open(&gateway, handshake("/v1/realtime", "")).await;
    assert_eq!(within(events.recv()).await, Some("open"));
    if reset {
        #[expect(deprecated, reason = "a zero linger is how a test sends a TCP reset")]
        socket.set_linger(Some(Duration::ZERO)).unwrap();
    }
    drop(socket);
    assert_eq!(within(events.recv()).await, Some("closed"));
}

#[rstest]
#[case::fin(false)]
#[case::reset(true)]
#[tokio::test]
async fn a_vanished_python_ends_the_client_socket(#[case] reset: bool) {
    let upstream = raw_upstream(move |mut python: Raw| async move {
        let handshake = python.request_head().await.unwrap();
        python.send(switching(&handshake, "")).await;
        let socket = python.into_stream();
        if reset {
            #[expect(deprecated, reason = "a zero linger is how a test sends a TCP reset")]
            socket.set_linger(Some(Duration::ZERO)).unwrap();
        }
        drop(socket);
    })
    .await;
    let gateway = Gateway::start(upstream).await;
    let (mut socket, _) = open(&gateway, handshake("/v1/realtime", "")).await;
    let mut rest = Vec::new();
    assert!(matches!(
        within(socket.read_to_end(&mut rest)).await,
        Ok(0) | Err(_)
    ));
}

/// A browser sends a WebSocket over an open HTTP/2 connection as an RFC 8441 extended CONNECT
/// only when the server sets SETTINGS_ENABLE_CONNECT_PROTOCOL (0x8). Python cannot serve one, so
/// the forwarder must not offer it and browsers keep opening HTTP/1.1 upgrades.
#[rstest]
#[tokio::test]
async fn http2_never_offers_websockets_over_extended_connect() {
    let (upstream, _handshakes) = byte_echo("").await;
    let gateway = Gateway::start(upstream).await;
    let mut stream = TcpStream::connect(gateway.address).await.unwrap();
    stream
        .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n\x00\x00\x00\x04\x00\x00\x00\x00\x00")
        .await
        .unwrap();
    let mut frame = [0; 9];
    within(stream.read_exact(&mut frame)).await.unwrap();
    assert_eq!(frame[3], 0x4, "the server opens with SETTINGS");
    let mut settings = vec![0; usize::from(frame[1]) << 8 | usize::from(frame[2])];
    within(stream.read_exact(&mut settings)).await.unwrap();
    assert!(
        settings
            .chunks(6)
            .all(|setting| setting[..2] != [0x00, 0x08] || setting[2..] == [0, 0, 0, 0])
    );
}

#[rstest]
#[tokio::test]
async fn shutdown_closes_open_sockets_after_drain() {
    let gateway = Gateway::start(echo().await).await;
    let mut socket = within(connect(gateway.address, "Upgrade")).await.unwrap();
    socket.next().await.unwrap().unwrap();
    let started = Instant::now();
    within(gateway.shutdown()).await.unwrap();
    assert!(started.elapsed() >= DRAIN);
    assert!(!matches!(socket.next().await, Some(Ok(Message::Text(_)))));
}

#[rstest]
#[tokio::test]
async fn shutdown_relays_the_close_python_sends_while_draining() {
    let gateway = Gateway::start(echo().await).await;
    let mut socket = within(connect(gateway.address, "Upgrade")).await.unwrap();
    socket.next().await.unwrap().unwrap();
    let server = gateway.shutdown();
    tokio::time::sleep(Duration::from_millis(100)).await;
    socket.send(Message::text("bye")).await.unwrap();
    let Message::Close(Some(frame)) = within(socket.next()).await.unwrap().unwrap() else {
        panic!("expected python's close frame");
    };
    assert_eq!(frame.code, CloseCode::Library(4000));
    while socket.next().await.is_some() {}
    drop(socket);
    let started = Instant::now();
    within(server).await.unwrap();
    assert!(started.elapsed() < DRAIN / 2);
}

#[rstest]
#[tokio::test]
async fn refuses_new_sockets_while_draining() {
    let gateway = Gateway::start(echo().await).await;
    let mut open = within(connect(gateway.address, "Upgrade")).await.unwrap();
    open.next().await.unwrap().unwrap();
    let address = gateway.address;
    let server = gateway.shutdown();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        TcpStream::connect(address).await.is_err() || connect(address, "Upgrade").await.is_err()
    );
    open.send(Message::text("still open")).await.unwrap();
    assert_eq!(
        within(open.next()).await.unwrap().unwrap(),
        Message::text("still open")
    );
    drop(open);
    within(server).await.unwrap();
}

/// uvicorn never half-closes: once Python closes the socket nothing more can cross it, so the
/// relay must end instead of waiting for the client to close its side too.
#[rstest]
#[tokio::test]
async fn relay_ends_when_python_closes_while_the_client_holds_its_socket() {
    let upstream = raw_upstream(move |mut python: Raw| async move {
        let handshake = python.request_head().await.unwrap();
        python.send(switching(&handshake, "")).await;
    })
    .await;
    let gateway = Gateway::start(upstream).await;
    let (mut socket, _) = open(&gateway, handshake("/v1/realtime", "")).await;
    assert_eq!(within(socket.read(&mut [0; 1])).await.unwrap(), 0);
    let started = Instant::now();
    within(gateway.shutdown()).await.unwrap();
    assert!(started.elapsed() < DRAIN / 2);
    drop(socket);
}
