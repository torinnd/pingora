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

//! Holding the final upstream response header until a body verdict commits it.
//!
//! See [`ResponseCommitPolicy`]. A request that never returns
//! [`ResponseCommitPolicy::Hold`] stays `Immediate`, and each proxy call site
//! checks that state before doing anything else.

use super::*;

/// Whether Pingora may commit a final response header as soon as it arrives.
///
/// Experimental: this API may change or be removed until indicated otherwise.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum ResponseCommitPolicy {
    /// Use the normal response path.
    #[default]
    Immediate,
    /// Hold the final upstream response header until the application commits it
    /// from [`ProxyHttp::held_upstream_response_body_filter`] or
    /// [`ProxyHttp::held_upstream_response_trailer_filter`].
    ///
    /// Header-only responses and 101 Switching Protocols bypass the gate.
    Hold,
}

pub(crate) enum ResponseCommitState {
    Immediate,
    Armed,
    Held(Box<ResponseHeader>),
    Committed,
}

fn response_header_commits(header: &ResponseHeader) -> bool {
    !header.status.is_informational() || header.status == http::StatusCode::SWITCHING_PROTOCOLS
}

/// A response whose final header is held. It gives read access to the session
/// and the held header, but no way to write the response until committed.
///
/// Experimental: this API may change or be removed until indicated otherwise.
pub struct HeldResponse<'a, DS = ()>
where
    DS: DownstreamSession,
{
    session: &'a mut Session<DS>,
}

impl<'a, DS> HeldResponse<'a, DS>
where
    DS: DownstreamSession,
{
    fn new(session: &'a mut Session<DS>) -> Self {
        Self { session }
    }

    pub fn session(&self) -> &Session<DS> {
        self.session
    }

    /// The held header, after `response_filter` and per-protocol framing.
    pub fn header(&self) -> &ResponseHeader {
        let ResponseCommitState::Held(header) = &self.session.response_commit_state else {
            unreachable!("HeldResponse requires a held response header")
        };
        header
    }

    /// Callers that change the body must keep Content-Length and
    /// Transfer-Encoding consistent with it.
    pub fn header_mut(&mut self) -> &mut ResponseHeader {
        let ResponseCommitState::Held(header) = &mut self.session.response_commit_state else {
            unreachable!("HeldResponse requires a held response header")
        };
        header
    }

    /// Write the held header through the downstream modules.
    pub async fn commit(self) -> Result<()> {
        self.session.commit_held_response_header().await
    }

    /// Write the held header and return a capability for streaming body chunks.
    pub async fn commit_for_streaming(self) -> Result<CommittedResponse<'a, DS>> {
        self.session.commit_held_response_header().await?;
        Ok(CommittedResponse {
            session: self.session,
        })
    }
}

/// A held response together with the current body task. Committing for
/// streaming clears the task so the chunk cannot also be written by the proxy.
///
/// Experimental: this API may change or be removed until indicated otherwise.
pub struct HeldResponseBody<'a, DS = ()>
where
    DS: DownstreamSession,
{
    response: HeldResponse<'a, DS>,
    body: &'a mut Option<Bytes>,
    end_of_stream: bool,
}

impl<'a, DS> HeldResponseBody<'a, DS>
where
    DS: DownstreamSession,
{
    pub fn session(&self) -> &Session<DS> {
        self.response.session()
    }

    pub fn header(&self) -> &ResponseHeader {
        self.response.header()
    }

    pub fn header_mut(&mut self) -> &mut ResponseHeader {
        self.response.header_mut()
    }

    pub fn body(&self) -> Option<&Bytes> {
        self.body.as_ref()
    }

    /// The chunk must be `None` when returning without committing.
    pub fn body_mut(&mut self) -> &mut Option<Bytes> {
        self.body
    }

    pub fn take_body(&mut self) -> Option<Bytes> {
        self.body.take()
    }

    pub fn end_of_stream(&self) -> bool {
        self.end_of_stream
    }

    /// Commit the header and leave the current body task to the proxy.
    pub async fn commit(self) -> Result<()> {
        self.response.commit().await
    }

    /// Commit the header and clear the current body task.
    pub async fn commit_for_streaming(self) -> Result<CommittedResponse<'a, DS>> {
        self.body.take();
        self.response.commit_for_streaming().await
    }
}

/// Write access to the body of a committed held response.
///
/// Experimental: this API may change or be removed until indicated otherwise.
#[must_use = "use the committed response to stream body chunks"]
pub struct CommittedResponse<'a, DS = ()>
where
    DS: DownstreamSession,
{
    session: &'a mut Session<DS>,
}

impl<DS> CommittedResponse<'_, DS>
where
    DS: DownstreamSession,
{
    pub fn session(&self) -> &Session<DS> {
        self.session
    }

    /// Write one body chunk. The proxy writes end-of-stream after the held
    /// filter returns.
    pub async fn write_chunk(&mut self, body: Bytes) -> Result<()> {
        self.session.write_response_body(Some(body), false).await
    }
}

impl<DS> Session<DS>
where
    DS: DownstreamSession,
{
    pub(crate) fn set_response_commit_policy(&mut self, policy: ResponseCommitPolicy) {
        if policy == ResponseCommitPolicy::Hold {
            self.response_commit_state = ResponseCommitState::Armed;
        }
    }

    /// Disable caching for a gated request so every response reaches the
    /// upstream response filters.
    pub(crate) fn bypass_cache_for_response_commit(&mut self) {
        if !matches!(self.response_commit_state, ResponseCommitState::Immediate)
            && (self.cache.enabled() || self.cache.bypassing())
        {
            self.cache
                .disable(NoCacheReason::Custom("response commit gate"));
        }
    }

    /// Whether response tasks must go through [`Self::push_gated_task`].
    pub(crate) fn response_commit_gated(&self) -> bool {
        matches!(
            self.response_commit_state,
            ResponseCommitState::Armed | ResponseCommitState::Held(_)
        )
    }

    pub(crate) fn response_header_held(&self) -> bool {
        matches!(self.response_commit_state, ResponseCommitState::Held(_))
    }

    /// A gated request cannot retry once a final header is held or written,
    /// since the attempt's application, module and compression state is spent.
    pub(crate) fn response_commit_blocks_retry(&self) -> bool {
        match self.response_commit_state {
            ResponseCommitState::Immediate => false,
            ResponseCommitState::Held(_) => true,
            ResponseCommitState::Armed | ResponseCommitState::Committed => {
                self.response_written().is_some_and(response_header_commits)
            }
        }
    }

    pub(crate) fn rearm_response_commit_gate(&mut self) {
        if !matches!(self.response_commit_state, ResponseCommitState::Immediate) {
            self.response_commit_state = ResponseCommitState::Armed;
        }
    }

    /// Queue `task` for the downstream, or hold it if it is the final header.
    pub(crate) async fn push_gated_task(
        &mut self,
        task: HttpTask,
        tasks: &mut Vec<HttpTask>,
    ) -> Result<()> {
        let armed = matches!(self.response_commit_state, ResponseCommitState::Armed);
        match task {
            HttpTask::Failed(_) => tasks.push(task),
            HttpTask::Header(header, end) if armed => {
                if end
                    || !response_header_commits(&header)
                    || header.status == http::StatusCode::SWITCHING_PROTOCOLS
                {
                    if response_header_commits(&header) {
                        self.response_commit_state = ResponseCommitState::Committed;
                    }
                    tasks.push(HttpTask::Header(header, end));
                    return Ok(());
                }
                if self.response_written().is_some_and(response_header_commits) {
                    return Error::e_explain(
                        InternalError,
                        "a final response was written before the commit gate could hold the \
                         upstream header",
                    );
                }
                self.response_commit_state = ResponseCommitState::Held(header);
                // Informational responses go out before the held hooks run.
                if !tasks.is_empty() && self.write_response_tasks(std::mem::take(tasks)).await? {
                    return Error::e_explain(
                        InternalError,
                        "response ended before its held final header was committed",
                    );
                }
            }
            HttpTask::Body(None, false) if !armed => {}
            _ if armed => {
                return Error::e_explain(
                    InternalError,
                    "response body or trailers arrived before a final response header",
                );
            }
            _ => {
                return Error::e_explain(
                    InternalError,
                    "response reached a terminal task before its held header was committed",
                );
            }
        }
        Ok(())
    }

    async fn commit_held_response_header(&mut self) -> Result<()> {
        if self.response_written().is_some_and(response_header_commits)
            || self.has_pending_downstream_tasks()
        {
            return Error::e_explain(
                InternalError,
                "a response was written or queued while the final upstream header was held",
            );
        }

        let state = std::mem::replace(
            &mut self.response_commit_state,
            ResponseCommitState::Immediate,
        );
        let ResponseCommitState::Held(header) = state else {
            self.response_commit_state = state;
            return Error::e_explain(
                InternalError,
                "commit_held_response_header() called without a held response header",
            );
        };
        if header.status.is_informational() {
            return Error::e_explain(
                InternalError,
                "a committed held response header must be a final, non-1xx response",
            );
        }
        if header.headers.contains_key(http::header::CONTENT_LENGTH)
            && header.headers.contains_key(http::header::TRANSFER_ENCODING)
        {
            return Error::e_explain(
                InternalError,
                "a committed held response cannot have both Content-Length and Transfer-Encoding",
            );
        }

        self.write_response_header(header, false).await?;
        self.response_commit_state = ResponseCommitState::Committed;
        Ok(())
    }
}

impl<SV, C, DS> HttpProxy<SV, C, DS>
where
    C: custom::Connector,
    DS: DownstreamSession,
{
    pub(crate) async fn held_body_filter(
        &self,
        session: &mut Session<DS>,
        body: &mut Option<Bytes>,
        end_of_stream: bool,
        ctx: &mut <SV as ProxyHttp<DS>>::CTX,
    ) -> Result<Option<Duration>>
    where
        SV: ProxyHttp<DS> + Send + Sync,
        <SV as ProxyHttp<DS>>::CTX: Send + Sync,
    {
        let response = HeldResponseBody {
            response: HeldResponse::new(session),
            body: &mut *body,
            end_of_stream,
        };
        let duration = self
            .inner
            .held_upstream_response_body_filter(response, ctx)
            .await?;
        if session.response_header_held() && (body.is_some() || end_of_stream) {
            return Error::e_explain(
                InternalError,
                "response body reached downstream before its held header was committed",
            );
        }
        Ok(duration)
    }

    pub(crate) async fn held_trailer_filter(
        &self,
        session: &mut Session<DS>,
        trailers: &mut header::HeaderMap,
        ctx: &mut <SV as ProxyHttp<DS>>::CTX,
    ) -> Result<Option<Duration>>
    where
        SV: ProxyHttp<DS> + Send + Sync,
        <SV as ProxyHttp<DS>>::CTX: Send + Sync,
    {
        self.inner
            .held_upstream_response_trailer_filter(HeldResponse::new(session), trailers, ctx)
            .await?;
        if session.response_header_held() {
            return Error::e_explain(
                InternalError,
                "response trailers returned without committing the held header",
            );
        }
        Ok(None)
    }
}
