//! Pipelined `RaftNetworkV2::stream_append` over the UC inter-node transports.
//!
//! # Why
//!
//! openraft's `ReplicationCore` calls [`RaftNetworkV2::stream_append`] exactly
//! once per peer replication session, feeding it an *optimistic* stream of
//! `AppendEntriesRequest`s — the request stream produces the next request from
//! leader-log state *without* awaiting the previous response (see
//! `replication::next_append_request`, which pushes `req.last_log_id()` onto an
//! inflight queue and yields immediately). The default `RaftNetworkV2`
//! implementation (`stream_append_sequential`) nevertheless processes them
//! strictly one-at-a-time: send, await, send the next. On a fat/long pipe that
//! serialises the leader on a single in-flight RPC per peer.
//!
//! UC's transports correlate responses by request-id (UDP `mux.rpc(rid)`, QUIC
//! `conn.request`), and Task 6 made the per-peer networks cheaply `Clone`
//! (sharing the request-id counter via `Arc`s) — so launching several
//! `append_entries` from clones concurrently is wire-safe. This wrapper keeps
//! up to `depth` AppendEntries RPCs in flight to a single peer, yields their
//! results **in input order**, and refills as results drain.
//!
//! # Correctness: ordered + early-stop
//!
//! The output stream of `stream_append` must be byte-for-byte equivalent to the
//! sequential default *modulo* concurrency. Two invariants matter:
//!
//! 1. **Order.** Results are yielded in the order the requests were produced.
//!    A `FuturesOrdered` look-ahead window gives us exactly that.
//! 2. **Early-stop.** The sequential default terminates the output stream after
//!    the first item that is any of: (a) the unary RPC returned `Err`; (b) the
//!    converted [`StreamAppendResult`] is `Err` (Conflict / HigherVote); or
//!    (c) the response is a `PartialSuccess(p)` with `p != req.last` — i.e. the
//!    follower accepted only a prefix, so the leader must recompute the next
//!    `prev_log_id` and any already-buffered later requests are now stale. We
//!    may have *already* launched those later requests concurrently; on hitting
//!    (a)/(b)/(c) we yield that one item and then end the stream, dropping the
//!    other in-flight results. Skipping this would be a Raft safety bug.
//!
//! This mirrors `openraft::network::stream_append_sequential`
//! (network/stream_append_trait.rs) one-to-one; only the look-ahead is added.

use std::future::Future;
use std::marker::PhantomData;

use futures::StreamExt;
use futures::stream::{FuturesOrdered, Stream};
use openraft::OptionalSend;
use openraft::base::{BoxFuture, BoxStream};
use openraft::error::RPCError;
use openraft::error::ReplicationClosed;
use openraft::error::StreamingError;
use openraft::network::RPCOption;
use openraft::network::v2::RaftNetworkV2;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, StreamAppendResult, VoteRequest,
    VoteResponse,
};
use openraft::raft::TransferLeaderRequest;
use openraft::raft::TransferLeaderResponse;
use openraft::type_config::alias::{LogIdOf, SnapshotOf, VoteOf};
use openraft_legacy::network_v1::Adapter;
use openraft_legacy::network_v1::RaftNetwork as RaftNetworkV1;

use crate::raft::TypeConfig;

/// `RaftNetworkV2` wrapper that overrides `stream_append` with a bounded,
/// in-order pipeline while delegating every unary RPC unchanged.
///
/// `inner` is the openraft-legacy [`Adapter`] that already lifts the V1
/// `RaftNetwork` (UDP/QUIC) into `RaftNetworkV2`; we forward
/// `append_entries`/`vote`/`full_snapshot`/`transfer_leader`/`backoff` straight
/// to it. `raw` is a `Clone` of the underlying V1 network used to fan out
/// concurrent `append_entries` inside the pipelined `stream_append` — cloning is
/// cheap (`Arc` handles) and shares the request-id counter, so each concurrent
/// RPC still gets a globally-unique correlation id.
pub struct PipelinedNet<N>
where
    N: RaftNetworkV1<TypeConfig> + Clone,
{
    inner: Adapter<TypeConfig, N>,
    raw: N,
    depth: usize,
    _phantom: PhantomData<TypeConfig>,
}

impl<N> PipelinedNet<N>
where
    N: RaftNetworkV1<TypeConfig> + Clone,
{
    /// Wrap a V1 per-peer network with a pipeline of look-ahead `depth`.
    ///
    /// `depth` must be `>= 1`; a `0` is treated as `1` (no pipelining) to stay
    /// safe rather than busy-spin on an empty `FuturesOrdered`.
    pub fn new(net: N, depth: usize) -> Self {
        Self {
            inner: Adapter::new(net.clone()),
            raw: net,
            depth: depth.max(1),
            _phantom: PhantomData,
        }
    }
}

/// Extract the `(prev, last]` log-id range of a request from its *public*
/// fields. openraft's own `AppendEntriesRequest::log_id_range()` is
/// `pub(crate)`, so we reconstruct it here exactly as openraft does:
/// `prev = prev_log_id`, `last = entries.last().log_id() | prev_log_id`.
fn log_id_range(
    req: &AppendEntriesRequest<TypeConfig>,
) -> (Option<LogIdOf<TypeConfig>>, Option<LogIdOf<TypeConfig>>) {
    use openraft::entry::RaftEntry;
    let prev = req.prev_log_id;
    let last = req.entries.last().map(|e| e.log_id()).or(prev);
    (prev, last)
}

/// One pipeline slot: the in-flight unary RPC paired with the `(prev, last]`
/// range captured *before* the request was consumed, needed to convert the
/// response into a [`StreamAppendResult`] and to evaluate the partial-success
/// early-stop condition.
type SlotOutput = (
    Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>>,
    Option<LogIdOf<TypeConfig>>, // prev
    Option<LogIdOf<TypeConfig>>, // last
);

impl<N> RaftNetworkV2<TypeConfig> for PipelinedNet<N>
where
    N: RaftNetworkV1<TypeConfig> + Clone,
{
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.inner.append_entries(rpc, option).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.inner.vote(rpc, option).await
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<TypeConfig>,
        snapshot: SnapshotOf<TypeConfig>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        self.inner.full_snapshot(vote, snapshot, cancel, option).await
    }

    async fn transfer_leader(
        &mut self,
        req: TransferLeaderRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<TransferLeaderResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.inner.transfer_leader(req, option).await
    }

    fn backoff(&self) -> Option<openraft::network::Backoff> {
        // Mirror the `Adapter`, which wraps the legacy (non-Option) backoff in
        // `Some(..)`.
        Some(RaftNetworkV1::<TypeConfig>::backoff(&self.raw))
    }

    fn stream_append<'s, S>(
        &'s mut self,
        input: S,
        option: RPCOption,
    ) -> BoxFuture<
        's,
        Result<
            BoxStream<'s, Result<StreamAppendResult<TypeConfig>, RPCError<TypeConfig>>>,
            RPCError<TypeConfig>,
        >,
    >
    where
        S: Stream<Item = AppendEntriesRequest<TypeConfig>> + OptionalSend + Unpin + 'static,
    {
        let raw = self.raw.clone();
        let depth = self.depth;

        Box::pin(async move {
            // State threaded through `stream::unfold`:
            //   input        — remaining optimistic requests from the leader
            //   in_flight     — ordered window of ≤depth pending RPCs
            //   stopped       — true once an early-stop item has been yielded
            //   input_done    — true once `input` has yielded `None`
            // `raw`/`option`/`depth` are captured immutably.
            //
            // `input_done` is load-bearing: openraft's request stream is itself
            // a `futures::stream::unfold`, which PANICS if polled again after it
            // returns `None`. We may top up the window across several outer
            // iterations, so we must never re-poll an exhausted `input`.
            struct St<S> {
                input: S,
                in_flight: FuturesOrdered<BoxFuture<'static, SlotOutput>>,
                stopped: bool,
                input_done: bool,
            }

            let state = St {
                input,
                in_flight: FuturesOrdered::new(),
                stopped: false,
                input_done: false,
            };

            // Helper: turn one request into an in-flight unary RPC future,
            // capturing its `(prev, last]` range for response conversion.
            fn spawn_rpc<N>(
                raw: &N,
                option: &RPCOption,
                req: AppendEntriesRequest<TypeConfig>,
            ) -> BoxFuture<'static, SlotOutput>
            where
                N: RaftNetworkV1<TypeConfig> + Clone,
            {
                let (prev, last) = log_id_range(&req);
                let mut net = raw.clone();
                let opt = option.clone();
                Box::pin(async move {
                    let res = RaftNetworkV1::<TypeConfig>::append_entries(&mut net, req, opt)
                        .await
                        .map_err(rpc_err_from_legacy);
                    (res, prev, last)
                })
            }

            let strm = futures::stream::unfold(state, move |mut st| {
                let raw = raw.clone();
                let option = option.clone();
                async move {
                    // Once we early-stopped, the stream is finished.
                    if st.stopped {
                        return None;
                    }

                    // Acquire the next completed RPC to yield — IN ORDER — while
                    // keeping ≤depth requests in flight. Critically, we must not
                    // BLOCK pulling a new request while an in-flight response is
                    // already available: openraft's heartbeat request stream
                    // only produces the next request when the next heartbeat is
                    // due, so blocking on `input` would stall response delivery
                    // and break follower liveness. So:
                    //   - window empty & input live  -> await a new request
                    //   - window has room & input live -> race input vs. head
                    //   - window full or input done   -> await the head
                    let (result, prev, last) = loop {
                        let has_room = st.in_flight.len() < depth;

                        if st.in_flight.is_empty() {
                            if st.input_done {
                                // Nothing in flight and no more input: done.
                                return None;
                            }
                            // Nothing to drain yet; block for the next request.
                            match st.input.next().await {
                                Some(req) => {
                                    let fut = spawn_rpc(&raw, &option, req);
                                    st.in_flight.push_back(fut);
                                }
                                // Nothing in flight and input just ended: the
                                // whole stream is finished. We return `None`, so
                                // `st` is dropped — no need to record exhaustion.
                                None => return None,
                            }
                            continue;
                        }

                        if has_room && !st.input_done {
                            // Race a fresh request against the head completing.
                            // If a new request arrives first, enqueue it and keep
                            // looping (it may complete out of *arrival* order but
                            // FuturesOrdered still yields by submission order). If
                            // the head completes first, yield it.
                            use futures::future::Either;
                            let next_req = st.input.next();
                            let head = st.in_flight.next();
                            futures::pin_mut!(next_req);
                            futures::pin_mut!(head);
                            match futures::future::select(next_req, head).await {
                                Either::Left((Some(req), _head)) => {
                                    let fut = spawn_rpc(&raw, &option, req);
                                    st.in_flight.push_back(fut);
                                    // loop: re-evaluate (head future was dropped
                                    // un-polled-to-completion but FuturesOrdered
                                    // is restartable — it is re-acquired next
                                    // iteration without losing the pending RPC).
                                    continue;
                                }
                                Either::Left((None, _head)) => {
                                    // Input exhausted mid-race; mark it and drain
                                    // the head we were racing against.
                                    st.input_done = true;
                                    match st.in_flight.next().await {
                                        Some(out) => break out,
                                        None => return None,
                                    }
                                }
                                Either::Right((Some(out), _next_req)) => {
                                    // Head completed; the un-consumed `next_req`
                                    // is dropped. The next outer iteration will
                                    // poll `input` again — safe because we only
                                    // reach here when input was NOT exhausted.
                                    break out;
                                }
                                Either::Right((None, _)) => {
                                    // FuturesOrdered empty mid-race shouldn't
                                    // happen (we checked non-empty), but be safe.
                                    continue;
                                }
                            }
                        }

                        // Window full or input exhausted: just drain the head.
                        match st.in_flight.next().await {
                            Some(out) => break out,
                            None => return None,
                        }
                    };

                    let item = match result {
                        Ok(resp) => {
                            // Evaluate partial-success BEFORE consuming `resp`.
                            // `get_partial_success` is pub(crate), so match the
                            // public `PartialSuccess` variant directly.
                            let partial = match &resp {
                                AppendEntriesResponse::PartialSuccess(p) => Some(*p),
                                _ => None,
                            };
                            let stream_result = resp.into_stream_result(prev, last);

                            // Early-stop conditions (b) and (c).
                            if stream_result.is_err() {
                                // Conflict / HigherVote: leader must recompute.
                                st.stopped = true;
                            } else if matches!(partial, Some(p) if p != last) {
                                // Partial accept: the follower took only a prefix,
                                // so the leader must recompute the next
                                // prev_log_id; stop after yielding this item.
                                st.stopped = true;
                            }
                            Ok(stream_result)
                        }
                        // Early-stop condition (a): transport-level error.
                        Err(e) => {
                            st.stopped = true;
                            Err(e)
                        }
                    };

                    Some((item, st))
                }
            });

            let strm: BoxStream<'s, _> = Box::pin(strm);
            Ok(strm)
        })
    }
}

/// Lift the legacy `RaftNetwork::append_entries` error
/// (`RPCError<C, RaftError<C>>`) into the V2 `RPCError<C>` shape, matching what
/// the `Adapter` does via `decompose_infallible()`. `append_entries` carries an
/// *infallible* inner `RaftError` (the response models conflicts/higher-vote as
/// `Ok` variants), so decomposition never produces a real inner error.
fn rpc_err_from_legacy(
    e: openraft::error::RPCError<TypeConfig, openraft::error::RaftError<TypeConfig>>,
) -> RPCError<TypeConfig> {
    use openraft::error::decompose::DecomposeResult;
    // Wrap in Err and decompose: Result<_, RPCError<C, RaftError<C>>> ->
    // Result<Result<_, Infallible>, RPCError<C>>; the inner error is infallible.
    let r: Result<(), _> = Err(e);
    match r.decompose_infallible() {
        Ok(()) => unreachable!("Err input cannot decompose to Ok"),
        Err(rpc) => rpc,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use openraft::Vote;
    use openraft::vote::RaftLeaderId as _;
    use openraft::error::{InstallSnapshotError, RaftError};
    use openraft::raft::{InstallSnapshotRequest, InstallSnapshotResponse};

    use crate::raft::{LeaderId, RaftLogId};

    type LegacyRpcErr<E> = RPCError<TypeConfig, RaftError<TypeConfig, E>>;

    fn vote() -> Vote<LeaderId> {
        Vote::new_committed(1, 1)
    }

    fn log_id(index: u64) -> RaftLogId {
        RaftLogId::new(LeaderId::new(1, 1), index)
    }

    /// A heartbeat-shaped request with `prev_log_id = Some(log_id(index))` and
    /// no entries, so `(prev, last]` collapses to `(Some(index), Some(index)]`
    /// — enough to drive ordering, concurrency, and partial-success early-stop
    /// without constructing real entries.
    fn req(index: u64) -> AppendEntriesRequest<TypeConfig> {
        AppendEntriesRequest {
            vote: vote(),
            prev_log_id: Some(log_id(index)),
            entries: vec![],
            leader_commit: None,
        }
    }

    /// Programmable mock V1 network. Each `append_entries` sleeps `delay`,
    /// tracking max observed concurrency, and returns the response chosen by
    /// `responder` keyed on the request's `prev_log_id` index.
    #[derive(Clone)]
    struct MockNet {
        delay: Duration,
        in_flight: Arc<AtomicUsize>,
        max_in_flight: Arc<AtomicUsize>,
        /// Map request index -> response to return.
        responder: Arc<dyn Fn(u64) -> AppendEntriesResponse<TypeConfig> + Send + Sync>,
        /// Records the order in which requests were observed (by index).
        seen: Arc<std::sync::Mutex<Vec<u64>>>,
    }

    impl MockNet {
        fn new(
            delay: Duration,
            responder: impl Fn(u64) -> AppendEntriesResponse<TypeConfig> + Send + Sync + 'static,
        ) -> Self {
            Self {
                delay,
                in_flight: Arc::new(AtomicUsize::new(0)),
                max_in_flight: Arc::new(AtomicUsize::new(0)),
                responder: Arc::new(responder),
                seen: Arc::new(std::sync::Mutex::new(Vec::new())),
            }
        }
    }

    impl RaftNetworkV1<TypeConfig> for MockNet {
        async fn append_entries(
            &mut self,
            rpc: AppendEntriesRequest<TypeConfig>,
            _option: RPCOption,
        ) -> Result<AppendEntriesResponse<TypeConfig>, LegacyRpcErr<openraft::error::Infallible>>
        {
            let idx = rpc.prev_log_id.as_ref().map(|l| l.index).unwrap_or(0);
            let cur = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(cur, Ordering::SeqCst);
            self.seen.lock().unwrap().push(idx);
            tokio::time::sleep(self.delay).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            Ok((self.responder)(idx))
        }

        async fn vote(
            &mut self,
            _rpc: VoteRequest<TypeConfig>,
            _option: RPCOption,
        ) -> Result<VoteResponse<TypeConfig>, LegacyRpcErr<openraft::error::Infallible>> {
            unreachable!("vote not exercised")
        }

        async fn install_snapshot(
            &mut self,
            _rpc: InstallSnapshotRequest<TypeConfig>,
            _option: RPCOption,
        ) -> Result<InstallSnapshotResponse<TypeConfig>, LegacyRpcErr<InstallSnapshotError>>
        {
            unreachable!("install_snapshot not exercised")
        }
    }

    /// Ordered + bounded: 8 requests through depth=4 must arrive in input order
    /// and observe a max concurrency of exactly 4 (not 1, not 8).
    #[tokio::test]
    async fn stream_append_pipelines_in_order_bounded() {
        let mock = MockNet::new(Duration::from_millis(20), |_idx| {
            AppendEntriesResponse::Success
        });
        let max_in_flight = mock.max_in_flight.clone();

        let mut net = PipelinedNet::new(mock, 4);

        let reqs: Vec<_> = (1..=8).map(req).collect();
        let input = futures::stream::iter(reqs);
        let option = RPCOption::new(Duration::from_secs(5));

        let mut out = net.stream_append(input, option).await.expect("stream_append start");

        let mut got = 0usize;
        while let Some(item) = out.next().await {
            let res = item.expect("transport ok");
            let sa = res.expect("stream append ok");
            // Each is a full Success => Ok(Some(last)) with last = log_id(index).
            assert_eq!(sa, Some(log_id((got + 1) as u64)), "out-of-order at {got}");
            got += 1;
        }

        assert_eq!(got, 8, "all 8 results yielded in order");
        assert_eq!(
            max_in_flight.load(Ordering::SeqCst),
            4,
            "pipeline must keep exactly depth=4 in flight"
        );
    }

    /// Early-stop: request #3 returns a partial-success that does NOT reach its
    /// `last`, so the stream must yield items #1, #2, #3 and then END — it must
    /// not yield #4+ even though they may have been launched concurrently.
    #[tokio::test]
    async fn stream_append_early_stops_on_partial_success() {
        // For req(idx), last = Some(log_id(idx)). Make idx==3 return
        // PartialSuccess(Some(log_id(0))) which != Some(log_id(3)) -> stop.
        let mock = MockNet::new(Duration::from_millis(10), |idx| {
            if idx == 3 {
                AppendEntriesResponse::PartialSuccess(Some(log_id(0)))
            } else {
                AppendEntriesResponse::Success
            }
        });

        let mut net = PipelinedNet::new(mock, 4);

        let reqs: Vec<_> = (1..=8).map(req).collect();
        let input = futures::stream::iter(reqs);
        let option = RPCOption::new(Duration::from_secs(5));

        let mut out = net.stream_append(input, option).await.expect("stream_append start");

        let mut yielded = Vec::new();
        while let Some(item) = out.next().await {
            let res = item.expect("transport ok");
            let sa = res.expect("stream append ok");
            yielded.push(sa);
        }

        // #1,#2 full Success -> Some(log_id(1)), Some(log_id(2)); #3 partial ->
        // Some(log_id(0)); then END. No #4.
        assert_eq!(
            yielded,
            vec![Some(log_id(1)), Some(log_id(2)), Some(log_id(0))],
            "must early-stop after the partial-success item"
        );
    }

    /// Early-stop on a Conflict (StreamAppendResult Err): yields the error item
    /// then ends.
    #[tokio::test]
    async fn stream_append_early_stops_on_conflict() {
        let mock = MockNet::new(Duration::from_millis(10), |idx| {
            if idx == 2 {
                AppendEntriesResponse::Conflict
            } else {
                AppendEntriesResponse::Success
            }
        });

        let mut net = PipelinedNet::new(mock, 4);
        let reqs: Vec<_> = (1..=8).map(req).collect();
        let input = futures::stream::iter(reqs);
        let option = RPCOption::new(Duration::from_secs(5));

        let mut out = net.stream_append(input, option).await.expect("stream_append start");

        let mut count = 0;
        let mut last_was_err = false;
        while let Some(item) = out.next().await {
            let res = item.expect("transport ok");
            last_was_err = res.is_err();
            count += 1;
        }
        // #1 Success, #2 Conflict (Err) -> stop. Exactly 2 items, last is Err.
        assert_eq!(count, 2, "stop right after the conflict item");
        assert!(last_was_err, "final item is the conflict error");
    }
}
