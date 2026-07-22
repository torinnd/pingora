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

//! Microbenchmark for the per-call cost of `upstream_response_body_filter`.
//!
//! Measures the filter dispatch in isolation, the way the proxy invokes it
//! (statically dispatched on the concrete `ProxyHttp` type): once with the
//! default (no override) and once with a trivial override. Run against a
//! sync and an async version of the trait to compare the per-chunk dispatch
//! cost; the workload is otherwise identical.
//!
//! NOTE: exactly two things differ between the sync and async versions of
//! this file: the `async` keyword on `OverriddenFilter`'s method, and the
//! `.await` in `call_filter`.

use async_trait::async_trait;
use bytes::Bytes;
use pingora_core::protocols::l4::stream::Stream as L4Stream;
use pingora_core::upstreams::peer::HttpPeer;
use pingora_error::Result;
use pingora_proxy::{ProxyHttp, Session};
use std::hint::black_box;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

const WARMUP: u64 = 100_000;
const ITERS: u64 = 2_000_000;

struct DefaultFilter;

#[async_trait]
impl ProxyHttp for DefaultFilter {
    type CTX = ();

    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        unreachable!("not used by this benchmark")
    }
}

struct OverriddenFilter;

#[async_trait]
impl ProxyHttp for OverriddenFilter {
    type CTX = ();

    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        unreachable!("not used by this benchmark")
    }

    fn upstream_response_body_filter(
        &self,
        _session: &mut Session,
        _body: &mut Option<Bytes>,
        _end_of_stream: bool,
        _ctx: &mut Self::CTX,
    ) -> Result<Option<Duration>> {
        Ok(None)
    }
}

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

async fn bench<F: ProxyHttp<CTX = ()> + Send + Sync>(name: &str, filter: &F) {
    let (mut session, _client) = session().await;
    let chunk = Bytes::from(vec![0u8; 4096]);

    for _ in 0..WARMUP {
        call_filter(filter, &mut session, &chunk).await;
    }

    let start = Instant::now();
    for _ in 0..ITERS {
        call_filter(filter, &mut session, &chunk).await;
    }
    let elapsed = start.elapsed();

    println!(
        "{name}: {:.1} ns/call ({ITERS} iters in {elapsed:?})",
        elapsed.as_nanos() as f64 / ITERS as f64
    );
}

async fn call_filter<F: ProxyHttp<CTX = ()> + Send + Sync>(
    filter: &F,
    session: &mut Session,
    chunk: &Bytes,
) {
    let mut body = Some(chunk.clone());
    let mut ctx = ();
    // The single line that differs between the sync and async versions of
    // this benchmark: the async trait method needs `.await`.
    let out = filter.upstream_response_body_filter(
        black_box(session),
        black_box(&mut body),
        false,
        &mut ctx,
    );
    black_box(out.unwrap());
}

fn main() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        bench("default (no override)", &DefaultFilter).await;
        bench("trivial override     ", &OverriddenFilter).await;
    });
}
