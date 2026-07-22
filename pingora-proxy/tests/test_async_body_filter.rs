// Copyright 2026 Cloudflare, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Tests that `upstream_response_body_filter` implementations can await async
//! work (e.g. offloaded computation or calls to external services) while
//! retaining the existing body mutation, pacing, and error semantics.

use async_trait::async_trait;
use bytes::Bytes;
use pingora_core::protocols::l4::stream::Stream as L4Stream;
use pingora_core::upstreams::peer::HttpPeer;
use pingora_error::{Error, ErrorType, Result};
use pingora_proxy::{ProxyHttp, Session};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

async fn session() -> (Session, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let client = TcpStream::connect(addr);
    let server = listener.accept();
    let (client, server) = tokio::join!(client, server);
    let mut client = client.unwrap();
    let (server, _) = server.unwrap();

    client
        .write_all(b"GET / HTTP/1.1\r\nhost: example.com\r\n\r\n")
        .await
        .unwrap();

    let mut session = Session::new_h1(Box::new(L4Stream::from(server)));
    session.read_request().await.unwrap();

    (session, client)
}

struct AsyncBodyFilter;

#[async_trait]
impl ProxyHttp for AsyncBodyFilter {
    type CTX = bool;

    fn new_ctx(&self) -> Self::CTX {
        false
    }

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        unreachable!("not used by these tests")
    }

    async fn upstream_response_body_filter(
        &self,
        _session: &mut Session,
        body: &mut Option<Bytes>,
        _end_of_stream: bool,
        ctx: &mut Self::CTX,
    ) -> Result<Option<Duration>> {
        // Yield to prove the filter can await without losing its ability to
        // mutate the body or return a pacing delay.
        tokio::task::yield_now().await;
        *ctx = true;
        *body = Some(Bytes::from_static(b"filtered"));
        Ok(Some(Duration::from_millis(7)))
    }
}

#[tokio::test]
async fn upstream_response_body_filter_can_do_async_work() {
    let (mut session, _client) = session().await;
    let filter = AsyncBodyFilter;
    let mut body = Some(Bytes::from_static(b"original"));
    let mut ctx = false;

    let delay = filter
        .upstream_response_body_filter(&mut session, &mut body, true, &mut ctx)
        .await
        .unwrap();

    assert!(ctx);
    assert_eq!(delay, Some(Duration::from_millis(7)));
    assert_eq!(body, Some(Bytes::from_static(b"filtered")));
}

struct FailingAsyncBodyFilter;

#[async_trait]
impl ProxyHttp for FailingAsyncBodyFilter {
    type CTX = ();

    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        unreachable!("not used by these tests")
    }

    async fn upstream_response_body_filter(
        &self,
        _session: &mut Session,
        _body: &mut Option<Bytes>,
        _end_of_stream: bool,
        _ctx: &mut Self::CTX,
    ) -> Result<Option<Duration>> {
        tokio::task::yield_now().await;
        Err(Error::explain(
            ErrorType::InternalError,
            "async body filter failed",
        ))
    }
}

#[tokio::test]
async fn upstream_response_body_filter_can_fail_after_async_work() {
    let (mut session, _client) = session().await;
    let filter = FailingAsyncBodyFilter;
    let mut body = Some(Bytes::from_static(b"original"));
    let mut ctx = ();

    let result = filter
        .upstream_response_body_filter(&mut session, &mut body, true, &mut ctx)
        .await;

    assert!(result.is_err());
}
