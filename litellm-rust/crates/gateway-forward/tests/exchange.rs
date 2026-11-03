mod support;

use std::{
    convert::Infallible,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use axum::{
    Router,
    body::{Body, Bytes},
    extract::Request,
    http::{HeaderValue, StatusCode},
    response::Response,
};
use rstest::rstest;
use support::{Gateway, client, raw_upstream, recording, serve, within};

fn ok() -> Response {
    Response::new(Body::from("ok"))
}

#[rstest]
#[tokio::test]
async fn forwards_the_request_as_the_client_sent_it_minus_hop_headers() {
    let (upstream, mut received) = recording(|| {
        Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .header("connection", "x-upstream-hop")
            .header("x-upstream-hop", "remove")
            .header("x-response", "keep")
            .body(Body::from("python error"))
            .unwrap()
    })
    .await;
    let gateway = Gateway::start(upstream).await;
    let mut raw = gateway.connect().await;
    raw.send(
        "PATCH /v1/messages?x=a%2Fb&x=c HTTP/1.1\r\n\
         host: public.example\r\n\
         authorization: Bearer test\r\n\
         connection: x-remove, keep-alive\r\n\
         x-remove: remove\r\n\
         proxy-authorization: remove\r\n\
         te: trailers\r\n\
         content-length: 7\r\n\r\npayload",
    )
    .await;
    let head = within(raw.response_head()).await.unwrap();
    assert_eq!(head.status, StatusCode::UNAUTHORIZED);
    assert_eq!(head.header("x-response"), Some(&b"keep"[..]));
    assert_eq!(head.header("x-upstream-hop"), None);
    let body = raw.body(head.content_length().unwrap()).await;
    assert_eq!(body, b"python error");

    let request = received.recv().await.unwrap();
    assert_eq!(request.method, "PATCH");
    assert_eq!(request.uri, "/v1/messages?x=a%2Fb&x=c");
    assert_eq!(request.headers["host"], "public.example");
    assert_eq!(request.headers["authorization"], "Bearer test");
    assert!(!request.headers.contains_key("x-remove"));
    assert!(!request.headers.contains_key("proxy-authorization"));
    assert!(!request.headers.contains_key("te"));
    assert!(!request.headers.contains_key("connection"));
    assert_eq!(request.body, "payload");
}

#[rstest]
#[case::get("GET")]
#[case::post("POST")]
#[case::put("PUT")]
#[case::delete("DELETE")]
#[case::options("OPTIONS")]
#[case::extension("PURGE")]
#[tokio::test]
async fn forwards_every_method(#[case] method: &str) {
    let (upstream, mut received) = recording(ok).await;
    let gateway = Gateway::start(upstream).await;
    let response = client()
        .request(
            Request::builder()
                .method(method)
                .uri(gateway.url("/mcp"))
                .body(Body::from("body"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let request = received.recv().await.unwrap();
    assert_eq!(request.method, method);
    assert_eq!(request.body, "body");
}

#[rstest]
#[case::double_slash("//v1//models")]
#[case::encoded_slash("/v1/files/a%2Fb/content")]
#[case::encoded_query("/v1/models?q=%E2%9C%93&empty=&flag")]
#[case::utf8_bytes_escaped("/mcp/%F0%9F%A6%80")]
#[case::lowercase_encoded_slash("/v1/files/a%2fb/content")]
#[case::dot_segments("/openai/./v1/../v1/models")]
#[case::encoded_dot_segments("/openai/%2e%2e/v1/%2E/models")]
#[case::matrix_params("/langfuse/api;v=1/public;x=y/traces")]
#[case::plus_and_encoded_space("/gemini/v1beta/models?q=a+b&r=a%20b&s=%2B")]
#[case::empty_query("/v1/models?")]
#[case::question_marks_in_query("/v1/models??a=b?c")]
#[case::colons_in_segments(
    "/v1/projects/p/locations/l/publishers/google/models/m:streamGenerateContent?alt=sse"
)]
#[case::sub_delims("/~u/@:!$&'()*+,=/x")]
#[case::raw_bytes_browsers_leave_unescaped("/a{b}|c^d\\e\"f?g={}|^`")]
#[case::invalid_percent_escapes("/a%zz%00b?%")]
#[tokio::test]
async fn forwards_the_request_target_byte_for_byte(#[case] target: &str) {
    let (upstream, mut received) = recording(ok).await;
    let gateway = Gateway::start(upstream).await;
    let mut raw = gateway.connect().await;
    raw.send(format!("GET {target} HTTP/1.1\r\nhost: x\r\n\r\n"))
        .await;
    assert_eq!(
        within(raw.response_head()).await.unwrap().status,
        StatusCode::OK
    );
    assert_eq!(received.recv().await.unwrap().uri, target);
}

#[rstest]
#[tokio::test]
async fn absolute_form_targets_reach_python_as_origin_form() {
    let (upstream, mut received) = recording(ok).await;
    let gateway = Gateway::start(upstream).await;
    let mut raw = gateway.connect().await;
    raw.send(
        "GET http://elsewhere.example/v1/models?x=1 HTTP/1.1\r\nhost: elsewhere.example\r\n\r\n",
    )
    .await;
    assert_eq!(
        within(raw.response_head()).await.unwrap().status,
        StatusCode::OK
    );
    let request = received.recv().await.unwrap();
    assert_eq!(request.uri, "/v1/models?x=1");
}

#[rstest]
#[tokio::test]
async fn relays_a_response_with_more_headers_than_hyper_allows_by_default() {
    let upstream = serve(axum::Router::new().fallback(|| async {
        let headers = (0..150).map(|index| (format!("llm_provider-x-{index}"), "v"));
        let mut response = ok();
        for (name, value) in headers {
            response.headers_mut().insert(
                axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_static(value),
            );
        }
        response
    }))
    .await;
    let gateway = Gateway::start(upstream).await;
    let mut raw = gateway.connect().await;

    raw.send("GET /v1/chat/completions HTTP/1.1\r\nhost: public.example\r\n\r\n")
        .await;

    let head = within(raw.response_head()).await.unwrap();
    assert_eq!(head.status, StatusCode::OK);
    assert_eq!(head.header("llm_provider-x-149"), Some(&b"v"[..]));
}

#[rstest]
#[tokio::test]
async fn keeps_repeated_response_headers_and_opaque_header_bytes() {
    let (upstream, mut received) = recording(|| {
        Response::builder()
            .header("set-cookie", "a=1; Path=/")
            .header("set-cookie", "b=2; HttpOnly")
            .header("x-opaque", HeaderValue::from_bytes(b"caf\xe9").unwrap())
            .body(Body::empty())
            .unwrap()
    })
    .await;
    let gateway = Gateway::start(upstream).await;
    let mut raw = gateway.connect().await;
    raw.send(b"GET / HTTP/1.1\r\nhost: x\r\nx-opaque: caf\xe9\r\n\r\n")
        .await;
    let head = within(raw.response_head()).await.unwrap();
    let cookies: Vec<_> = head
        .headers
        .iter()
        .filter(|(name, _)| name == "set-cookie")
        .map(|(_, value)| value.as_slice())
        .collect();
    assert_eq!(cookies, [&b"a=1; Path=/"[..], b"b=2; HttpOnly"]);
    assert_eq!(head.header("x-opaque"), Some(&b"caf\xe9"[..]));
    assert_eq!(
        received.recv().await.unwrap().headers["x-opaque"].as_bytes(),
        b"caf\xe9"
    );
}

#[rstest]
#[tokio::test]
async fn answers_pipelined_requests_in_order() {
    let upstream = serve(
        Router::new().fallback(|request: Request| async move { request.uri().path().to_owned() }),
    )
    .await;
    let gateway = Gateway::start(upstream).await;
    let mut raw = gateway.connect().await;
    raw.send("GET /first HTTP/1.1\r\nhost: x\r\n\r\nHEAD /second HTTP/1.1\r\nhost: x\r\n\r\nGET /third HTTP/1.1\r\nhost: x\r\n\r\n")
        .await;
    let first = within(raw.response_head()).await.unwrap();
    assert_eq!(raw.body(first.content_length().unwrap()).await, b"/first");
    let second = within(raw.response_head()).await.unwrap();
    assert_eq!(second.content_length(), Some(7));
    let third = within(raw.response_head()).await.unwrap();
    assert_eq!(raw.body(third.content_length().unwrap()).await, b"/third");
}

#[rstest]
#[case::no_content(StatusCode::NO_CONTENT)]
#[case::not_modified(StatusCode::NOT_MODIFIED)]
#[tokio::test]
async fn bodiless_statuses_keep_the_connection_reusable(#[case] status: StatusCode) {
    let upstream = serve(Router::new().fallback(move |request: Request| async move {
        match request.uri().path() {
            "/next" => (StatusCode::OK, "next"),
            _ => (status, ""),
        }
    }))
    .await;
    let gateway = Gateway::start(upstream).await;
    let mut raw = gateway.connect().await;
    raw.send("GET /empty HTTP/1.1\r\nhost: x\r\n\r\nGET /next HTTP/1.1\r\nhost: x\r\n\r\n")
        .await;
    assert_eq!(within(raw.response_head()).await.unwrap().status, status);
    let next = within(raw.response_head()).await.unwrap();
    assert_eq!(raw.body(next.content_length().unwrap()).await, b"next");
}

#[rstest]
#[tokio::test]
async fn informational_responses_keep_the_connection_reusable() {
    let upstream = raw_upstream(|mut python| async move {
        while let Some(head) = python.request_head().await {
            match head.path.as_str() {
                "/hints" => {
                    python
                        .send("HTTP/1.1 103 Early Hints\r\nlink: </ui/app.css>; rel=preload\r\n\r\nHTTP/1.1 200 OK\r\ncontent-length: 5\r\n\r\nfinal")
                        .await
                }
                _ => {
                    python
                        .send("HTTP/1.1 200 OK\r\ncontent-length: 4\r\n\r\nnext")
                        .await
                }
            }
        }
    })
    .await;
    let gateway = Gateway::start(upstream).await;
    let mut raw = gateway.connect().await;
    raw.send("GET /hints HTTP/1.1\r\nhost: x\r\n\r\nGET /next HTTP/1.1\r\nhost: x\r\n\r\n")
        .await;
    let mut hinted = within(raw.response_head()).await.unwrap();
    while hinted.status.is_informational() {
        hinted = within(raw.response_head()).await.unwrap();
    }
    assert_eq!(hinted.status, StatusCode::OK);
    assert_eq!(raw.body(hinted.content_length().unwrap()).await, b"final");
    let next = within(raw.response_head()).await.unwrap();
    assert_eq!(raw.body(next.content_length().unwrap()).await, b"next");
}

#[rstest]
#[case::found(
    StatusCode::FOUND,
    "https://idp.example/authorize?state=a%2Fb&redirect_uri=https%3A%2F%2Fpublic.example%2Fsso%2Fcallback"
)]
#[case::see_other(StatusCode::SEE_OTHER, "/ui/?login=success")]
#[case::temporary(StatusCode::TEMPORARY_REDIRECT, "./mcp/oauth/authorize?client_id=x")]
#[tokio::test]
async fn redirects_reach_the_client_with_location_and_cookies_unchanged(
    #[case] status: StatusCode,
    #[case] location: &'static str,
) {
    let (upstream, _received) = recording(move || {
        Response::builder()
            .status(status)
            .header("location", location)
            .header(
                "set-cookie",
                "token=a%2Fb; Path=/; HttpOnly; Secure; SameSite=Lax",
            )
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
    let mut raw = gateway.connect().await;
    raw.send("GET /sso/key/generate HTTP/1.1\r\nhost: public.example\r\n\r\n")
        .await;
    let head = within(raw.response_head()).await.unwrap();
    assert_eq!(head.status, status);
    assert_eq!(head.header("location"), Some(location.as_bytes()));
    let cookies: Vec<_> = head
        .headers
        .iter()
        .filter(|(name, _)| name == "set-cookie")
        .map(|(_, value)| value.as_slice())
        .collect();
    assert_eq!(
        cookies,
        [
            &b"token=a%2Fb; Path=/; HttpOnly; Secure; SameSite=Lax"[..],
            b"state=; Max-Age=0",
            b"session=x; Expires=Wed, 04 Nov 2026 10:00:00 GMT; Path=/",
        ]
    );
}

#[rstest]
#[tokio::test]
async fn non_websocket_upgrades_reach_python_as_plain_requests() {
    let (upstream, mut received) = recording(ok).await;
    let gateway = Gateway::start(upstream).await;
    let mut raw = gateway.connect().await;
    raw.send("GET / HTTP/1.1\r\nhost: x\r\nconnection: upgrade, http2-settings\r\nupgrade: h2c\r\nhttp2-settings: AAMAAABkAAQAoAAAAAIAAAAA\r\n\r\n")
        .await;
    assert_eq!(
        within(raw.response_head()).await.unwrap().status,
        StatusCode::OK
    );
    let request = received.recv().await.unwrap();
    assert!(!request.headers.contains_key("upgrade"));
    assert!(!request.headers.contains_key("connection"));
    assert!(!request.headers.contains_key("http2-settings"));
}

#[rstest]
#[tokio::test]
async fn forwards_an_asterisk_form_options_request() {
    let (upstream, mut received) = recording(ok).await;
    let gateway = Gateway::start(upstream).await;
    let mut raw = gateway.connect().await;
    raw.send("OPTIONS * HTTP/1.1\r\nhost: x\r\n\r\n").await;
    assert_eq!(
        within(raw.response_head()).await.unwrap().status,
        StatusCode::OK
    );
    let request = received.recv().await.unwrap();
    assert_eq!(request.method, "OPTIONS");
    assert_eq!(request.uri, "*");
}

#[rstest]
#[tokio::test]
async fn an_authority_form_target_is_a_400_python_never_sees() {
    let (upstream, mut received) = recording(ok).await;
    let gateway = Gateway::start(upstream).await;
    let mut raw = gateway.connect().await;
    raw.send("CONNECT api.example:443 HTTP/1.1\r\nhost: api.example:443\r\n\r\n")
        .await;
    assert_eq!(
        within(raw.response_head()).await.unwrap().status,
        StatusCode::BAD_REQUEST
    );
    assert!(received.try_recv().is_err());
}

#[rstest]
#[tokio::test]
async fn a_client_connection_close_ends_only_the_client_connection() {
    let connections = Arc::new(AtomicUsize::new(0));
    let accepted = connections.clone();
    let upstream = raw_upstream(move |mut python| {
        accepted.fetch_add(1, Ordering::SeqCst);
        async move {
            while python.request_head().await.is_some() {
                python
                    .send("HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
                    .await;
            }
        }
    })
    .await;
    let gateway = Gateway::start(upstream).await;
    for _ in 0..2 {
        let mut raw = gateway.connect().await;
        raw.send("GET / HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n")
            .await;
        let head = within(raw.response_head()).await.unwrap();
        assert_eq!(head.header("connection"), Some(&b"close"[..]));
        assert_eq!(raw.body(2).await, b"ok");
        within(raw.closed()).await;
    }
    assert_eq!(connections.load(Ordering::SeqCst), 1);
}

#[rstest]
#[tokio::test]
async fn a_python_connection_close_ends_only_the_upstream_connection() {
    let connections = Arc::new(AtomicUsize::new(0));
    let accepted = connections.clone();
    let upstream = raw_upstream(move |mut python| {
        accepted.fetch_add(1, Ordering::SeqCst);
        async move {
            python.request_head().await.unwrap();
            python
                .send("HTTP/1.1 500 Internal Server Error\r\ncontent-length: 5\r\nconnection: close\r\n\r\nerror")
                .await;
        }
    })
    .await;
    let gateway = Gateway::start(upstream).await;
    let mut raw = gateway.connect().await;
    for _ in 0..2 {
        raw.send("GET / HTTP/1.1\r\nhost: x\r\n\r\n").await;
        let head = within(raw.response_head()).await.unwrap();
        assert_eq!(head.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(head.header("connection"), None);
        assert_eq!(raw.body(5).await, b"error");
    }
    assert_eq!(connections.load(Ordering::SeqCst), 2);
}

/// uvicorn answers HEAD on a streaming route with chunked framing and no body, so the client
/// must not be told the body is empty, and must not wait for one before its next request.
#[rstest]
#[tokio::test]
async fn head_answered_with_chunked_framing_claims_no_length_and_keeps_the_connection() {
    let upstream = raw_upstream(|mut python| async move {
        while let Some(head) = python.request_head().await {
            match head.path.as_str() {
                "/next" => {
                    python
                        .send("HTTP/1.1 200 OK\r\ncontent-length: 4\r\n\r\nnext")
                        .await
                }
                _ => {
                    python
                        .send("HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\n")
                        .await
                }
            }
        }
    })
    .await;
    let gateway = Gateway::start(upstream).await;
    let mut raw = gateway.connect().await;
    raw.send("HEAD /mcp HTTP/1.1\r\nhost: x\r\n\r\nGET /next HTTP/1.1\r\nhost: x\r\n\r\n")
        .await;
    let head = within(raw.response_head()).await.unwrap();
    assert_eq!(head.status, StatusCode::OK);
    assert_eq!(head.content_length(), None);
    let next = within(raw.response_head()).await.unwrap();
    assert_eq!(raw.body(next.content_length().unwrap()).await, b"next");
}

const GZIP: &[u8] = &[
    0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x4b, 0x49, 0x2c, 0x49, 0xb4, 0x52,
    0x30, 0xe4, 0xe2, 0x02, 0x00, 0xce, 0xcd, 0x78, 0x54, 0x09, 0x00, 0x00, 0x00,
];

#[rstest]
#[case::fixed_length(false)]
#[case::streamed(true)]
#[tokio::test]
async fn content_codings_cross_unchanged_both_ways(#[case] streamed: bool) {
    let (upstream, mut received) = recording(move || {
        let body = match streamed {
            true => Body::from_stream(futures_util::stream::iter(
                GZIP.chunks(5)
                    .map(|piece| Ok::<_, Infallible>(Bytes::from_static(piece))),
            )),
            false => Body::from(GZIP),
        };
        Response::builder()
            .header("content-encoding", "gzip")
            .header("vary", "Accept-Encoding")
            .body(body)
            .unwrap()
    })
    .await;
    let gateway = Gateway::start(upstream).await;
    let mut raw = gateway.connect().await;
    raw.send(
        [
            format!(
                "POST /v1/chat/completions HTTP/1.1\r\nhost: x\r\ncontent-encoding: gzip\r\naccept-encoding: gzip, br;q=0.5\r\ncontent-length: {}\r\n\r\n",
                GZIP.len()
            )
            .as_bytes(),
            GZIP,
        ]
        .concat(),
    )
    .await;

    let head = within(raw.response_head()).await.unwrap();
    assert_eq!(head.header("content-encoding"), Some(&b"gzip"[..]));
    assert_eq!(head.header("vary"), Some(&b"Accept-Encoding"[..]));
    let body = match head.content_length() {
        Some(length) => raw.body(length).await,
        None => within(raw.chunked_body()).await,
    };
    assert_eq!(body, GZIP);
    assert_eq!(head.content_length().is_none(), streamed);
    let request = received.recv().await.unwrap();
    assert_eq!(request.headers["content-encoding"], "gzip");
    assert_eq!(request.headers["accept-encoding"], "gzip, br;q=0.5");
    assert_eq!(request.body, GZIP);
}

#[rstest]
#[tokio::test]
async fn python_sees_no_accept_encoding_the_client_did_not_send() {
    let (upstream, mut received) = recording(ok).await;
    let gateway = Gateway::start(upstream).await;
    let mut raw = gateway.connect().await;
    raw.send("GET /metrics/ HTTP/1.1\r\nhost: x\r\n\r\n").await;
    within(raw.response_head()).await.unwrap();
    assert!(
        !received
            .recv()
            .await
            .unwrap()
            .headers
            .contains_key("accept-encoding")
    );
}
