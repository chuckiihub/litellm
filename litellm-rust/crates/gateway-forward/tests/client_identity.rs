mod support;

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use axum::{body::Body, http::StatusCode, response::Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use litellm_gateway_forward::{Forward, PEER_HEADER};
use rstest::rstest;
use support::{DRAIN, Gateway, Raw, UVICORN_KEEPALIVE, h2, raw_upstream, recording, within};
use tokio::net::{TcpListener, TcpStream};

fn ok() -> Response {
    Response::new(Body::from("ok"))
}

#[rstest]
#[tokio::test]
async fn python_sees_forwarding_headers_untouched_and_the_tcp_peer_rust_saw() {
    let (upstream, mut received) = recording(ok).await;
    let gateway = Gateway::start(upstream).await;
    let mut raw = gateway.connect().await;
    raw.send(
        "GET /sso/callback HTTP/1.1\r\n\
         host: public.example\r\n\
         x-forwarded-for: 192.0.2.1\r\n\
         x-forwarded-for: 192.0.2.2\r\n\
         x-forwarded-proto: https\r\n\
         forwarded: for=192.0.2.1;proto=https\r\n\
         x-litellm-rust-peer: 10.1.2.3:1\r\n\r\n",
    )
    .await;
    assert_eq!(
        within(raw.response_head()).await.unwrap().status,
        StatusCode::OK
    );

    let request = received.recv().await.unwrap();
    let forwarded_for: Vec<_> = request.headers.get_all("x-forwarded-for").iter().collect();
    assert_eq!(forwarded_for, ["192.0.2.1", "192.0.2.2"]);
    assert_eq!(request.headers["x-forwarded-proto"], "https");
    assert_eq!(request.headers["forwarded"], "for=192.0.2.1;proto=https");
    let peers: Vec<_> = request.headers.get_all(PEER_HEADER).iter().collect();
    assert_eq!(peers, [raw.local_address().to_string().as_str()]);
}

/// Python builds redirects and OAuth metadata from Host, and without one from the address it
/// listens on, which behind Rust is its private loopback port. So a request without a usable Host
/// carries the public address the client connected to, except HTTP/1.1, which h11 rejects.
#[rstest]
#[case::http11_without_host("HTTP/1.1", "", false)]
#[case::http10_without_host("HTTP/1.0", "", true)]
#[case::http11_with_an_empty_host("HTTP/1.1", "host:\r\n", true)]
#[tokio::test]
async fn a_request_without_a_usable_host_never_names_the_private_upstream(
    #[case] version: &str,
    #[case] host: &str,
    #[case] forwarded: bool,
) {
    let (upstream, mut received) = recording(ok).await;
    let gateway = Gateway::start(upstream).await;
    let mut raw = gateway.connect().await;

    raw.send(format!("GET /sso/key/generate {version}\r\n{host}\r\n"))
        .await;

    let status = within(raw.response_head()).await.unwrap().status;
    let hosts = received.try_recv().ok().map(|request| {
        request
            .headers
            .get_all("host")
            .iter()
            .map(|value| value.to_str().unwrap().to_owned())
            .collect::<Vec<_>>()
    });
    let expected = if forwarded {
        (StatusCode::OK, Some(vec![gateway.address.to_string()]))
    } else {
        (StatusCode::BAD_REQUEST, None)
    };
    assert_eq!((status, hosts), expected);
}

#[rstest]
#[case::uppercase_with_port("PUBLIC.Example:4000")]
#[case::ipv6_literal("[::1]:4000")]
#[case::ipv4_with_port("192.0.2.10:8443")]
#[tokio::test]
async fn python_sees_the_host_header_byte_for_byte(#[case] host: &str) {
    let (upstream, mut received) = recording(ok).await;
    let gateway = Gateway::start(upstream).await;
    let mut raw = gateway.connect().await;

    raw.send(format!("GET /ui HTTP/1.1\r\nHost: {host}\r\n\r\n"))
        .await;

    assert_eq!(
        within(raw.response_head()).await.unwrap().status,
        StatusCode::OK
    );
    let request = received.recv().await.unwrap();
    let hosts: Vec<_> = request.headers.get_all("host").iter().collect();
    assert_eq!(hosts, [host]);
}

#[rstest]
#[case::name("http://public.example/v1/models", "public.example")]
#[case::uppercase_with_port("http://PUBLIC.Example:8443/ui", "PUBLIC.Example:8443")]
#[case::ipv6_literal("http://[::1]:4000/ui", "[::1]:4000")]
#[tokio::test]
async fn http2_clients_reach_python_with_their_public_host(#[case] uri: &str, #[case] host: &str) {
    let (upstream, mut received) = recording(ok).await;
    let gateway = Gateway::start(upstream).await;
    let mut sender = h2(&gateway).await;
    let request = axum::http::Request::get(uri).body(Body::empty()).unwrap();

    let response = within(sender.send_request(request)).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(received.recv().await.unwrap().headers["host"], host);
}

/// RFC 9113 section 8.2.3 lets an HTTP/2 client send one cookie field per crumb, and HTTP/1.1
/// allows only one Cookie header, which is all Python reads.
#[rstest]
#[tokio::test]
async fn http2_cookie_crumbs_reach_python_as_one_header() {
    let (upstream, mut received) = recording(ok).await;
    let gateway = Gateway::start(upstream).await;
    let mut sender = h2(&gateway).await;
    let request = axum::http::Request::get("http://public.example/ui/")
        .header("cookie", "a=1")
        .header("cookie", "token=secret")
        .body(Body::empty())
        .unwrap();

    let response = within(sender.send_request(request)).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let request = received.recv().await.unwrap();
    let cookies: Vec<_> = request.headers.get_all("cookie").iter().collect();
    assert_eq!(cookies, ["a=1; token=secret"]);
}

/// Python's IP allowlists and rate limits read the peer exactly as uvicorn would have seen it on
/// the public socket, so a dual-stack listener reports IPv4 clients in their mapped form.
#[rstest]
#[case::ipv6("[::1]:0", "[::1]", "[::1]")]
#[case::ipv4_on_a_dual_stack_listener("[::]:0", "127.0.0.1", "[::ffff:127.0.0.1]")]
#[tokio::test]
async fn ipv6_listeners_report_the_peer_bracketed_with_its_port(
    #[case] bind: &str,
    #[case] connect: &str,
    #[case] peer_ip: &str,
) {
    let (upstream, mut received) = recording(ok).await;
    let forward = Forward::new(
        &format!("http://{upstream}").parse().unwrap(),
        UVICORN_KEEPALIVE,
    );
    let public = TcpListener::bind(bind).await.unwrap();
    let port = public.local_addr().unwrap().port();
    tokio::spawn(forward.serve(public, std::future::pending(), DRAIN));
    let mut raw = Raw::new(
        TcpStream::connect(format!("{connect}:{port}"))
            .await
            .unwrap(),
    );

    raw.send("GET / HTTP/1.1\r\nhost: public.example\r\n\r\n")
        .await;

    assert_eq!(
        within(raw.response_head()).await.unwrap().status,
        StatusCode::OK
    );
    assert_eq!(
        received.recv().await.unwrap().headers[PEER_HEADER],
        format!("{peer_ip}:{}", raw.local_address().port()).as_str()
    );
}

/// Without the peer header Python would take Rust's loopback hop as the client, which allowlists
/// often trust, so no spelling, repetition or Connection token may remove or replace it.
#[rstest]
#[case::named_as_hop_by_hop(
    "connection: x-litellm-rust-peer\r\nx-litellm-rust-peer: 127.0.0.1:1\r\n"
)]
#[case::mixed_case_copies(
    "X-LiteLLM-Rust-Peer: 127.0.0.1:1\r\nx-litellm-rust-peer: 192.0.2.7:2\r\n"
)]
#[case::comma_joined("x-litellm-rust-peer: 127.0.0.1:1, 192.0.2.7:2\r\n")]
#[tokio::test]
async fn a_client_can_neither_strip_nor_spoof_the_peer(#[case] spoof: &str) {
    let (upstream, mut received) = recording(ok).await;
    let gateway = Gateway::start(upstream).await;
    let mut raw = gateway.connect().await;

    raw.send(format!(
        "GET /key/info HTTP/1.1\r\nhost: public.example\r\n{spoof}\r\n"
    ))
    .await;

    assert_eq!(
        within(raw.response_head()).await.unwrap().status,
        StatusCode::OK
    );
    let request = received.recv().await.unwrap();
    let peers: Vec<_> = request.headers.get_all(PEER_HEADER).iter().collect();
    assert_eq!(peers, [raw.local_address().to_string().as_str()]);
}

/// Pooled upstream connections carry requests from many clients in turn, so the peer belongs to
/// each request and never to the connection it travels on.
#[rstest]
#[tokio::test]
async fn each_request_carries_its_own_peer_over_a_shared_upstream_connection() {
    let connections = Arc::new(AtomicUsize::new(0));
    let accepted = connections.clone();
    let upstream = raw_upstream(move |mut python| {
        accepted.fetch_add(1, Ordering::SeqCst);
        async move {
            while let Some(head) = python.request_head().await {
                let peer = head.header(PEER_HEADER).unwrap().to_vec();
                python
                    .send(
                        [
                            format!("HTTP/1.1 200 OK\r\ncontent-length: {}\r\n\r\n", peer.len())
                                .as_bytes(),
                            &peer,
                        ]
                        .concat(),
                    )
                    .await;
            }
        }
    })
    .await;
    let gateway = Gateway::start(upstream).await;
    let mut clients = [
        gateway.connect().await,
        gateway.connect().await,
        gateway.connect().await,
    ];

    for index in [0, 1, 2, 2, 1, 0] {
        let raw = &mut clients[index];
        raw.send("GET /v1/models HTTP/1.1\r\nhost: public.example\r\n\r\n")
            .await;
        let head = within(raw.response_head()).await.unwrap();
        let peer = raw.body(head.content_length().unwrap()).await;
        assert_eq!(peer, raw.local_address().to_string().into_bytes());
    }
    assert_eq!(connections.load(Ordering::SeqCst), 1);
}

#[rstest]
#[tokio::test]
async fn http2_clients_reach_python_with_forwarding_headers_untouched_and_their_peer() {
    let (upstream, mut received) = recording(ok).await;
    let gateway = Gateway::start(upstream).await;
    let stream = TcpStream::connect(gateway.address).await.unwrap();
    let local = stream.local_addr().unwrap();
    let (mut sender, connection) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(stream))
            .await
            .unwrap();
    tokio::spawn(connection);
    let request = axum::http::Request::get("http://public.example/sso/callback")
        .header("x-forwarded-for", "192.0.2.1")
        .header("x-forwarded-for", "192.0.2.2")
        .header("x-forwarded-proto", "https")
        .header("forwarded", "for=192.0.2.1;proto=https")
        .header(PEER_HEADER, "127.0.0.1:1")
        .body(Body::empty())
        .unwrap();

    let response = within(sender.send_request(request)).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let request = received.recv().await.unwrap();
    let forwarded_for: Vec<_> = request.headers.get_all("x-forwarded-for").iter().collect();
    assert_eq!(forwarded_for, ["192.0.2.1", "192.0.2.2"]);
    assert_eq!(request.headers["x-forwarded-proto"], "https");
    assert_eq!(request.headers["forwarded"], "for=192.0.2.1;proto=https");
    let peers: Vec<_> = request.headers.get_all(PEER_HEADER).iter().collect();
    assert_eq!(peers, [local.to_string().as_str()]);
}

/// Unlike Cookie, Set-Cookie can never be joined into one field, so an HTTP/2 login response must
/// reach the browser with every cookie Python set, each in its own field and in order.
#[rstest]
#[tokio::test]
async fn http2_clients_receive_each_set_cookie_python_sent_in_order() {
    let (upstream, _received) = recording(|| {
        Response::builder()
            .status(StatusCode::SEE_OTHER)
            .header("location", "/ui/?login=success")
            .header("set-cookie", "token=a, b; Path=/; HttpOnly; Secure")
            .header("set-cookie", "state=; Max-Age=0")
            .header(
                "set-cookie",
                "session=x; Expires=Wed, 04 Nov 2026 10:00:00 GMT; Path=/",
            )
            .body(Body::empty())
            .unwrap()
    })
    .await;
    let gateway = Gateway::start(upstream).await;
    let mut sender = h2(&gateway).await;
    let request = axum::http::Request::get("http://public.example/sso/callback?code=x")
        .body(Body::empty())
        .unwrap();

    let response = within(sender.send_request(request)).await.unwrap();

    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    let cookies: Vec<_> = response.headers().get_all("set-cookie").iter().collect();
    assert_eq!(
        cookies,
        [
            "token=a, b; Path=/; HttpOnly; Secure",
            "state=; Max-Age=0",
            "session=x; Expires=Wed, 04 Nov 2026 10:00:00 GMT; Path=/",
        ]
    );
}
