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

//! Tests for holding the final upstream response header until an async body
//! verdict commits it.
//!
//! The end-to-end tests exercise both coalesced header/body writes and a
//! deliberately delayed body. In either case, the header must be held before
//! the held-body hook reaches its verdict.

use async_trait::async_trait;
use bytes::Bytes;
use once_cell::sync::Lazy;
use pingora_cache::{CacheKey, CachePhase, MemCache, NoCacheReason};
use pingora_core::modules::http::compression::ResponseCompressionBuilder;
use pingora_core::modules::http::HttpModules;
use pingora_core::server::Server;
use pingora_core::upstreams::peer::HttpPeer;
use pingora_error::Result;
use pingora_http::ResponseHeader;
use pingora_proxy::{HeldResponse, HeldResponseBody, ProxyHttp, ResponseCommitPolicy, Session};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

static CACHE_BACKEND: Lazy<MemCache> = Lazy::new(MemCache::new);

// ===== End-to-end tests through the proxy loop =====

/// Origin that sends the header and complete body in a single write, so the
/// proxy receives them in one read batch.
fn start_origin() -> u16 {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            tx.send(listener.local_addr().unwrap().port()).unwrap();
            loop {
                let (mut sock, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 4096];
                    let mut head = Vec::new();
                    loop {
                        let n = sock.read(&mut buf).await.unwrap();
                        if n == 0 {
                            return;
                        }
                        head.extend_from_slice(&buf[..n]);
                        if head.windows(4).any(|w| w == b"\r\n\r\n") {
                            break;
                        }
                    }
                    let head = String::from_utf8_lossy(&head);
                    if head.contains("/compress") {
                        let mut response = b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 1024\r\nConnection: close\r\n\r\n".to_vec();
                        response.extend(std::iter::repeat_n(b'a', 1024));
                        sock.write_all(&response).await.unwrap();
                        sock.shutdown().await.ok();
                        return;
                    }
                    if head.contains("/split") {
                        sock.write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 5\r\nConnection: close\r\n\r\n",
                        )
                        .await
                        .unwrap();
                        sock.flush().await.unwrap();
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        sock.write_all(b"HELLO").await.unwrap();
                        sock.shutdown().await.ok();
                        return;
                    }
                    let resp: &[u8] = if head.contains("/trailers") {
                        b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nTransfer-Encoding: chunked\r\nTrailer: x-checksum\r\nConnection: close\r\n\r\n5\r\nHELLO\r\n0\r\nx-checksum: ok\r\n\r\n"
                    } else if head.contains("/hints") {
                        b"HTTP/1.1 103 Early Hints\r\nLink: </style.css>; rel=preload\r\n\r\nHTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 5\r\nConnection: close\r\n\r\nHELLO"
                    } else if head.contains("/allow")
                        || head.contains("/broken")
                        || head.contains("/invalid")
                        || head.contains("/stream")
                    {
                        b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 5\r\nConnection: close\r\n\r\nHELLO"
                    } else if head.contains("/block-chunked") {
                        b"HTTP/1.0 200 OK\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\nEVIL!"
                    } else if head.contains("/block") {
                        b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 5\r\nConnection: close\r\n\r\nEVIL!"
                    } else {
                        b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n"
                    };
                    // Single write: header + body arrive at the proxy together.
                    sock.write_all(resp).await.unwrap();
                    sock.shutdown().await.ok();
                });
            }
        });
    });
    rx.recv().unwrap()
}

fn start_h2_origin() -> u16 {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            tx.send(listener.local_addr().unwrap().port()).unwrap();
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let mut connection = h2::server::handshake(socket).await.unwrap();
                    while let Some(request) = connection.accept().await {
                        let (request, mut respond) = request.unwrap();
                        let trailers = request.uri().path().ends_with("/trailers");
                        let response = http::Response::builder()
                            .status(200)
                            .header(http::header::CONTENT_TYPE, "text/plain")
                            .header(http::header::CONTENT_LENGTH, "5")
                            .body(())
                            .unwrap();
                        let mut stream = respond.send_response(response, false).unwrap();
                        stream
                            .send_data(Bytes::from_static(b"HELLO"), !trailers)
                            .unwrap();
                        if trailers {
                            let mut trailers = http::HeaderMap::new();
                            trailers.insert("x-checksum", "ok".parse().unwrap());
                            stream.send_trailers(trailers).unwrap();
                        }
                    }
                });
            }
        });
    });
    rx.recv().unwrap()
}

/// A scanner-style proxy that holds the response header until the body
/// verdict is ready.
struct Scanner {
    h1_origin_port: u16,
    h2_origin_port: u16,
}

#[derive(Default)]
struct ScannerCtx {
    path: String,
    buffer: Vec<u8>,
}

#[async_trait]
impl ProxyHttp for Scanner {
    type CTX = ScannerCtx;

    fn new_ctx(&self) -> Self::CTX {
        ScannerCtx::default()
    }

    fn init_downstream_modules(&self, modules: &mut HttpModules) {
        // Enabled compression asserts the required downstream module order:
        // final header before body.
        modules.add_module(ResponseCompressionBuilder::enable(6));
    }

    async fn upstream_peer(
        &self,
        session: &mut Session,
        ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        ctx.path = session.req_header().uri.path().to_string();
        let h2 = ctx.path.starts_with("/h2-");
        let port = if h2 {
            self.h2_origin_port
        } else {
            self.h1_origin_port
        };
        let mut peer = HttpPeer::new(("127.0.0.1", port), false, String::new());
        if h2 {
            peer.options.set_http_version(2, 2);
        }
        Ok(Box::new(peer))
    }

    fn response_commit_policy(&self, _session: &Session, _ctx: &Self::CTX) -> ResponseCommitPolicy {
        ResponseCommitPolicy::Hold
    }

    fn request_cache_filter(&self, session: &mut Session, _ctx: &mut Self::CTX) -> Result<()> {
        session
            .cache
            .enable(&*CACHE_BACKEND, None, None, None, None);
        Ok(())
    }

    fn cache_key_callback(&self, session: &Session, _ctx: &mut Self::CTX) -> Result<CacheKey> {
        Ok(CacheKey::new(
            "commit-gate",
            session.req_header().uri.path(),
        ))
    }

    async fn proxy_upstream_filter(
        &self,
        session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<bool> {
        // The gate must reassert its cache invariant after this hook.
        session
            .cache
            .enable(&*CACHE_BACKEND, None, None, None, None);
        Ok(true)
    }

    async fn response_filter(
        &self,
        _session: &mut Session,
        upstream_response: &mut ResponseHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        // The held header includes changes made by this filter.
        upstream_response.insert_header("x-response-filter", "ran")?;
        Ok(())
    }

    async fn held_upstream_response_body_filter(
        &self,
        mut response: HeldResponseBody<'_>,
        ctx: &mut Self::CTX,
    ) -> Result<Option<Duration>> {
        assert!(matches!(
            response.session().cache.phase(),
            CachePhase::Disabled(NoCacheReason::Custom("response commit gate"))
        ));
        // Buffer (and withhold) the body until the verdict.
        if let Some(body) = response.take_body() {
            ctx.buffer.extend_from_slice(&body);
        }
        if response.end_of_stream() {
            if ctx.path.ends_with("/broken") {
                // Simulate a buggy filter that never commits the held header.
                return Ok(None);
            }
            if ctx.path.ends_with("/stream") {
                let buffered = std::mem::take(&mut ctx.buffer);
                let mut committed = response.commit_for_streaming().await?;
                committed
                    .write_chunk(Bytes::copy_from_slice(&buffered[..2]))
                    .await?;
                committed
                    .write_chunk(Bytes::copy_from_slice(&buffered[2..]))
                    .await?;
                return Ok(None);
            }
            if ctx.path.ends_with("/invalid") {
                *response.body_mut() = Some(Bytes::from_static(b"invalid"));
                response.header_mut().set_status(103)?;
            } else if ctx.path.contains("/block") {
                *response.body_mut() = Some(Bytes::from_static(b"BLOCKED"));
                let header = response.header_mut();
                header.set_status(403)?;
                if header
                    .headers
                    .get(http::header::TRANSFER_ENCODING)
                    .is_none()
                {
                    header.insert_header(http::header::CONTENT_LENGTH, "7")?;
                }
            } else {
                *response.body_mut() = Some(Bytes::from(std::mem::take(&mut ctx.buffer)));
            }
            response.commit().await?;
        }
        Ok(None)
    }

    async fn held_upstream_response_trailer_filter(
        &self,
        response: HeldResponse<'_>,
        _upstream_trailers: &mut http::HeaderMap,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        let buffered = std::mem::take(&mut ctx.buffer);
        let mut committed = response.commit_for_streaming().await?;
        committed.write_chunk(Bytes::from(buffered)).await
    }
}

static PROXY: Lazy<u16> = Lazy::new(|| {
    let h1_origin_port = start_origin();
    let h2_origin_port = start_h2_origin();
    let proxy_port = 40_000 + (std::process::id() % 20_000) as u16;
    std::thread::spawn(move || {
        let mut server = Server::new(None).unwrap();
        server.bootstrap();
        let scanner = Scanner {
            h1_origin_port,
            h2_origin_port,
        };
        let mut proxy = pingora_proxy::http_proxy_service(&server.configuration, scanner);
        proxy.add_tcp(&format!("127.0.0.1:{proxy_port}"));
        server.add_service(proxy);
        server.run_forever();
    });
    // The listener binds inside run_forever; wait until it accepts.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if std::net::TcpStream::connect(("127.0.0.1", proxy_port)).is_ok() {
            return proxy_port;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "proxy did not start listening"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
});

fn client() -> reqwest::Client {
    // A response that never commits its held header should fail quickly.
    reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap()
}

#[tokio::test]
async fn e2e_held_header_committed_on_allow() {
    let port = *PROXY;
    let res = client()
        .get(format!("http://127.0.0.1:{port}/allow"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    // The committed header carries mutations made in response_filter.
    assert_eq!(
        res.headers().get("x-response-filter").unwrap(),
        &"ran".parse::<reqwest::header::HeaderValue>().unwrap()
    );
    assert_eq!(res.bytes().await.unwrap(), Bytes::from_static(b"HELLO"));
}

#[tokio::test]
async fn e2e_downstream_compression_sees_committed_header_first() {
    let port = *PROXY;
    let client = reqwest::Client::builder().gzip(false).build().unwrap();
    let res = client
        .get(format!("http://127.0.0.1:{port}/compress"))
        .header(reqwest::header::ACCEPT_ENCODING, "gzip")
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), reqwest::StatusCode::OK);
    assert_eq!(
        res.headers()
            .get(reqwest::header::CONTENT_ENCODING)
            .unwrap(),
        "gzip"
    );
    assert!(!res.bytes().await.unwrap().is_empty());
}

#[tokio::test]
async fn e2e_committed_capability_streams_body() {
    let port = *PROXY;
    let res = client()
        .get(format!("http://127.0.0.1:{port}/stream"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    assert_eq!(res.bytes().await.unwrap(), Bytes::from_static(b"HELLO"));
}

#[tokio::test]
async fn e2e_h2_upstream_held_header() {
    let port = *PROXY;
    let res = client()
        .get(format!("http://127.0.0.1:{port}/h2-allow"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    assert_eq!(res.bytes().await.unwrap(), Bytes::from_static(b"HELLO"));
}

#[tokio::test]
async fn e2e_h2_upstream_trailer_hook() {
    let port = *PROXY;
    let res = client()
        .get(format!("http://127.0.0.1:{port}/h2-trailers"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    assert_eq!(res.bytes().await.unwrap(), Bytes::from_static(b"HELLO"));
}

#[tokio::test]
async fn e2e_held_header_mutated_on_block() {
    let port = *PROXY;
    let res = client()
        .get(format!("http://127.0.0.1:{port}/block"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::FORBIDDEN);
    // The blocker mutates the captured, fully transformed header, so changes
    // made by response_filter are preserved.
    assert_eq!(res.headers().get("x-response-filter").unwrap(), "ran");
    assert_eq!(res.bytes().await.unwrap(), Bytes::from_static(b"BLOCKED"));
}

#[tokio::test]
async fn e2e_held_header_across_read_batches() {
    let port = *PROXY;
    let res = client()
        .get(format!("http://127.0.0.1:{port}/split"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    assert_eq!(res.bytes().await.unwrap(), Bytes::from_static(b"HELLO"));
}

#[tokio::test]
async fn e2e_informational_header_precedes_committed_header() {
    let port = *PROXY;
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    stream
        .write_all(b"GET /hints HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let response = String::from_utf8_lossy(&response);

    let hints = response.find("103 Early Hints").unwrap();
    let final_header = response.find("200 OK").unwrap();
    assert!(hints < final_header, "{response}");
}

#[tokio::test]
async fn e2e_header_only_response_passes_through() {
    let port = *PROXY;
    let res = client()
        .get(format!("http://127.0.0.1:{port}/empty"))
        .send()
        .await
        .unwrap();
    // The 204 is end-of-stream: never captured despite the armed deferral.
    assert_eq!(res.status(), reqwest::StatusCode::NO_CONTENT);
    // Passthrough headers go through response_filter as usual.
    assert!(res.headers().get("x-response-filter").is_some());
}

#[tokio::test]
async fn e2e_block_preserves_protocol_framing() {
    let port = *PROXY;
    let res = client()
        .get(format!("http://127.0.0.1:{port}/block-chunked"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::FORBIDDEN);
    assert_eq!(
        res.headers()
            .get(reqwest::header::TRANSFER_ENCODING)
            .unwrap(),
        "chunked"
    );
    assert_eq!(res.bytes().await.unwrap(), Bytes::from_static(b"BLOCKED"));
}

#[tokio::test]
async fn e2e_informational_header_mutation_fails() {
    let port = *PROXY;
    let res = client()
        .get(format!("http://127.0.0.1:{port}/invalid"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn e2e_trailer_hook_commits_held_response() {
    let port = *PROXY;
    let res = client()
        .get(format!("http://127.0.0.1:{port}/trailers"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    assert_eq!(res.bytes().await.unwrap(), Bytes::from_static(b"HELLO"));
}

#[tokio::test]
async fn e2e_never_committed_header_fails_fast() {
    let port = *PROXY;
    // A filter that defers but never reaches a verdict must produce a fast,
    // clean error (nothing was committed downstream), not a hanging client.
    let res = client()
        .get(format!("http://127.0.0.1:{port}/broken"))
        .send()
        .await
        .unwrap();
    // InternalError: the fault is the local filter's, not the upstream's.
    assert_eq!(res.status(), reqwest::StatusCode::INTERNAL_SERVER_ERROR);
}
