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

//! End-to-end proxy overhead benchmark for `upstream_response_body_filter`.
//!
//! Runs a real proxy over loopback against in-process h1 and h2c origins,
//! with a `ProxyHttp` implementation that does not override the body filter,
//! so it measures the cost every user pays for the filter's signature. A
//! counting run reports how many times the filter is invoked per request, so
//! per-call microbenchmark numbers can be scaled honestly.
//!
//! Workloads:
//! - throughput: large chunked bodies (maximizes filter calls per byte)
//! - request rate: small bodies, sequential and concurrent
//!
//! Compare runs of the identical benchmark against a sync and an async
//! version of the trait. Only the counting filter differs between those two
//! versions of this file (the `async` keyword and body).

use async_trait::async_trait;
use bytes::Bytes;
use once_cell::sync::Lazy;
use pingora_core::server::Server;
use pingora_core::upstreams::peer::HttpPeer;
use pingora_error::Result;
use pingora_proxy::{ProxyHttp, Session};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const LARGE_BODY_CHUNK: usize = 8 * 1024;
const LARGE_BODY_CHUNKS: usize = 1280; // 10 MiB total
const LARGE_DOWNLOADS: usize = 30;
const SMALL_BODY: usize = 1024;
const SMALL_REQUESTS: usize = 3000;
const SMALL_CONCURRENCY: usize = 16;

static FILTER_CALLS: AtomicU64 = AtomicU64::new(0);

/// Proxy that leaves upstream_response_body_filter at its default. This is
/// the configuration every user who does not use the hook runs.
struct NoopProxy {
    origin: u16,
    h2: bool,
}

#[async_trait]
impl ProxyHttp for NoopProxy {
    type CTX = ();

    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        let mut peer = HttpPeer::new(("127.0.0.1", self.origin), false, String::new());
        if self.h2 {
            peer.options.set_http_version(2, 2);
        }
        Ok(Box::new(peer))
    }
}

/// Same proxy with a trivial override that counts invocations. Used once per
/// scenario to report filter calls per request; not part of the timed runs.
struct CountingProxy {
    origin: u16,
    h2: bool,
}

#[async_trait]
impl ProxyHttp for CountingProxy {
    type CTX = ();

    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        let mut peer = HttpPeer::new(("127.0.0.1", self.origin), false, String::new());
        if self.h2 {
            peer.options.set_http_version(2, 2);
        }
        Ok(Box::new(peer))
    }

    fn upstream_response_body_filter(
        &self,
        _session: &mut Session,
        _body: &mut Option<Bytes>,
        _end_of_stream: bool,
        _ctx: &mut Self::CTX,
    ) -> Result<Option<Duration>> {
        FILTER_CALLS.fetch_add(1, Ordering::Relaxed);
        Ok(None)
    }
}

/// h1 origin. `/large` streams LARGE_BODY_CHUNKS chunks of LARGE_BODY_CHUNK
/// bytes with a flush per chunk; `/small` sends SMALL_BODY bytes at once.
fn start_h1_origin() -> u16 {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            tx.send(listener.local_addr().unwrap().port()).unwrap();
            loop {
                let (mut sock, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    sock.set_nodelay(true).ok();
                    let mut buf = vec![0u8; 4096];
                    loop {
                        // read one request head (keep-alive loop)
                        let mut head = Vec::new();
                        loop {
                            match sock.read(&mut buf).await {
                                Ok(0) | Err(_) => return,
                                Ok(n) => head.extend_from_slice(&buf[..n]),
                            }
                            if head.windows(4).any(|w| w == b"\r\n\r\n") {
                                break;
                            }
                        }
                        let head = String::from_utf8_lossy(&head);
                        if head.contains("/large") {
                            let total = LARGE_BODY_CHUNK * LARGE_BODY_CHUNKS;
                            let hdr = format!("HTTP/1.1 200 OK\r\nContent-Length: {total}\r\n\r\n");
                            if sock.write_all(hdr.as_bytes()).await.is_err() {
                                return;
                            }
                            let chunk = vec![0u8; LARGE_BODY_CHUNK];
                            for _ in 0..LARGE_BODY_CHUNKS {
                                if sock.write_all(&chunk).await.is_err() {
                                    return;
                                }
                                if sock.flush().await.is_err() {
                                    return;
                                }
                            }
                        } else {
                            let hdr =
                                format!("HTTP/1.1 200 OK\r\nContent-Length: {SMALL_BODY}\r\n\r\n");
                            let body = vec![0u8; SMALL_BODY];
                            if sock.write_all(hdr.as_bytes()).await.is_err()
                                || sock.write_all(&body).await.is_err()
                            {
                                return;
                            }
                        }
                    }
                });
            }
        });
    });
    rx.recv().unwrap()
}

/// h2c origin serving the same two paths over cleartext HTTP/2.
fn start_h2_origin() -> u16 {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            tx.send(listener.local_addr().unwrap().port()).unwrap();
            loop {
                let (sock, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let mut conn = match h2::server::handshake(sock).await {
                        Ok(c) => c,
                        Err(_) => return,
                    };
                    while let Some(req) = conn.accept().await {
                        let (req, mut respond) = match req {
                            Ok(r) => r,
                            Err(_) => return,
                        };
                        let large = req.uri().path().contains("/large");
                        tokio::spawn(async move {
                            let resp = http::Response::builder().status(200).body(()).unwrap();
                            let mut stream = match respond.send_response(resp, false) {
                                Ok(s) => s,
                                Err(_) => return,
                            };
                            if large {
                                let chunk = Bytes::from(vec![0u8; LARGE_BODY_CHUNK]);
                                for i in 0..LARGE_BODY_CHUNKS {
                                    stream.reserve_capacity(LARGE_BODY_CHUNK);
                                    let last = i == LARGE_BODY_CHUNKS - 1;
                                    if stream.send_data(chunk.clone(), last).is_err() {
                                        return;
                                    }
                                }
                            } else {
                                let body = Bytes::from(vec![0u8; SMALL_BODY]);
                                let _ = stream.send_data(body, true);
                            }
                        });
                    }
                });
            }
        });
    });
    rx.recv().unwrap()
}

fn start_proxy<P>(proxy: P, port: u16)
where
    P: ProxyHttp + Send + Sync + 'static,
    P::CTX: Send + Sync,
{
    std::thread::spawn(move || {
        let mut server = Server::new(None).unwrap();
        server.bootstrap();
        let mut service = pingora_proxy::http_proxy_service(&server.configuration, proxy);
        service.add_tcp(&format!("127.0.0.1:{port}"));
        server.add_service(service);
        server.run_forever();
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while std::net::TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(Instant::now() < deadline, "proxy did not start");
        std::thread::sleep(Duration::from_millis(50));
    }
}

static CLIENT: Lazy<reqwest::Client> = Lazy::new(|| {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .unwrap()
});

async fn run_throughput(port: u16) -> f64 {
    let total_bytes = (LARGE_BODY_CHUNK * LARGE_BODY_CHUNKS * LARGE_DOWNLOADS) as f64;
    let start = Instant::now();
    for _ in 0..LARGE_DOWNLOADS {
        let res = CLIENT
            .get(format!("http://127.0.0.1:{port}/large"))
            .send()
            .await
            .unwrap();
        assert!(res.status().is_success());
        let body = res.bytes().await.unwrap();
        assert_eq!(body.len(), LARGE_BODY_CHUNK * LARGE_BODY_CHUNKS);
    }
    total_bytes / start.elapsed().as_secs_f64() / (1024.0 * 1024.0)
}

async fn run_request_rate(port: u16) -> f64 {
    let start = Instant::now();
    let mut handles = Vec::new();
    for _ in 0..SMALL_CONCURRENCY {
        handles.push(tokio::spawn(async move {
            for _ in 0..(SMALL_REQUESTS / SMALL_CONCURRENCY) {
                let res = CLIENT
                    .get(format!("http://127.0.0.1:{port}/small"))
                    .send()
                    .await
                    .unwrap();
                assert!(res.status().is_success());
                let body = res.bytes().await.unwrap();
                assert_eq!(body.len(), SMALL_BODY);
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    SMALL_REQUESTS as f64 / start.elapsed().as_secs_f64()
}

/// Report the average filter calls per request via the counting proxy.
async fn run_call_count(port: u16, large: bool) -> f64 {
    let path = if large { "/large" } else { "/small" };
    let n = if large { 5 } else { 100 };
    FILTER_CALLS.store(0, Ordering::Relaxed);
    for _ in 0..n {
        let res = CLIENT
            .get(format!("http://127.0.0.1:{port}{path}"))
            .send()
            .await
            .unwrap();
        assert!(res.status().is_success());
        let _ = res.bytes().await.unwrap();
    }
    FILTER_CALLS.load(Ordering::Relaxed) as f64 / n as f64
}

fn main() {
    let h1_origin = start_h1_origin();
    let h2_origin = start_h2_origin();

    // Timed proxies (default no-op filter) and counting proxies, one port each.
    start_proxy(
        NoopProxy {
            origin: h1_origin,
            h2: false,
        },
        41880,
    );
    start_proxy(
        NoopProxy {
            origin: h2_origin,
            h2: true,
        },
        41881,
    );
    start_proxy(
        CountingProxy {
            origin: h1_origin,
            h2: false,
        },
        41882,
    );
    start_proxy(
        CountingProxy {
            origin: h2_origin,
            h2: true,
        },
        41883,
    );

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        // Warm up connections and pools.
        let _ = run_throughput(41880).await;
        let _ = run_request_rate(41880).await;
        let _ = run_throughput(41881).await;
        let _ = run_request_rate(41881).await;

        println!(
            "h1 upstream: {:6.1} MiB/s throughput, {:7.1} req/s ({:.1} filter calls/req large, {:.1} small)",
            run_throughput(41880).await,
            run_request_rate(41880).await,
            run_call_count(41882, true).await,
            run_call_count(41882, false).await,
        );
        println!(
            "h2 upstream: {:6.1} MiB/s throughput, {:7.1} req/s ({:.1} filter calls/req large, {:.1} small)",
            run_throughput(41881).await,
            run_request_rate(41881).await,
            run_call_count(41883, true).await,
            run_call_count(41883, false).await,
        );
    });
}
