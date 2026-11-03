use std::{
    pin::Pin,
    task::{Context, Poll, ready},
};

use axum::{
    body::Body,
    extract::Request,
    http::{StatusCode, header},
};
use hyper::body::{Frame, SizeHint};
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};

/// hyper answers a client's `expect: 100-continue` the first time its body is read, but uvicorn
/// answers it only once the app reads the body, so a request Python rejects up front never uploads
/// it. The body of such a request is held until Python sends its own 100, or until the returned
/// token releases it once Python's final response is in hand: Python may still read the body
/// then, and hyper writes that response's head before it next reads the client, so it never
/// sends a 100 after it.
pub(crate) fn hold_body_until_continue(request: &mut Request) -> Option<CancellationToken> {
    let expects_continue = request
        .headers()
        .get(header::EXPECT)
        .is_some_and(|value| value.as_bytes().eq_ignore_ascii_case(b"100-continue"));
    if !expects_continue {
        return None;
    }
    let released = CancellationToken::new();
    let continued = released.clone();
    hyper::ext::on_informational(request, move |response| {
        if response.status() == StatusCode::CONTINUE {
            continued.cancel();
        }
    });
    let body = std::mem::take(request.body_mut());
    *request.body_mut() = Body::new(Held {
        body,
        released: Some(Box::pin(released.clone().cancelled_owned())),
    });
    Some(released)
}

struct Held {
    body: Body,
    released: Option<Pin<Box<WaitForCancellationFutureOwned>>>,
}

impl hyper::body::Body for Held {
    type Data = <Body as hyper::body::Body>::Data;
    type Error = <Body as hyper::body::Body>::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        if let Some(released) = &mut self.released {
            ready!(released.as_mut().poll(context));
            self.released = None;
        }
        Pin::new(&mut self.body).poll_frame(context)
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}
