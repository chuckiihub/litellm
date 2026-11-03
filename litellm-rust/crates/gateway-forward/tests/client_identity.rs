mod support;

use axum::{body::Body, http::StatusCode, response::Response};
use litellm_gateway_forward::{Forward, PEER_HEADER};
use rstest::rstest;
use support::{DRAIN, Gateway, Raw, UVICORN_KEEPALIVE, h2, recording, within};
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

#[rstest]
#[tokio::test]
async fn ipv6_peers_reach_python_bracketed_with_their_port() {
    let (upstream, mut received) = recording(ok).await;
    let forward = Forward::new(
        &format!("http://{upstream}").parse().unwrap(),
        UVICORN_KEEPALIVE,
    );
    let public = TcpListener::bind("[::1]:0").await.unwrap();
    let address = public.local_addr().unwrap();
    tokio::spawn(forward.serve(public, std::future::pending(), DRAIN));
    let mut raw = Raw::new(TcpStream::connect(address).await.unwrap());

    raw.send("GET / HTTP/1.1\r\nhost: [::1]:4000\r\n\r\n").await;

    assert_eq!(
        within(raw.response_head()).await.unwrap().status,
        StatusCode::OK
    );
    assert_eq!(
        received.recv().await.unwrap().headers[PEER_HEADER],
        format!("[::1]:{}", raw.local_address().port()).as_str()
    );
}
