//! A CPU-busy-spin executor for openraft's `AsyncRuntime`.
//!
//! Worker threads never park: each polls its assigned futures in a tight loop
//! with a no-op waker. This eliminates the cross-thread futex park/unpark that a
//! parking scheduler (tokio multi-thread) incurs on every openraft *internal*
//! task hop (RaftCore -> sm_worker, replication acks, io-completion forwarder).
//!
//! Channels stay waker-correct (the runtime reuses tokio's runtime-agnostic
//! `sync` types) so the openraft <-> tokio API boundary still works — e.g.
//! `client_write().await` called on a tokio thread is woken when RaftCore (on a
//! busy-spin worker) completes the response oneshot. Those wakers are simply
//! *redundant* for intra-executor hops because the worker re-polls every task
//! unconditionally; that redundancy is exactly where the futex cost disappears.
//!
//! SKELETON STATUS — correctness over speed, just enough to compile + boot:
//!  * Tasks are distributed round-robin to a fixed worker pool via `std::mpsc`;
//!    no work-stealing.
//!  * No CPU-affinity pinning yet (see the TODO in `start`); the real build
//!    pins one worker per reserved core.
//!  * Task panics are not caught (a panic takes down its worker thread, loudly).
//!  * It passes openraft's `AsyncRuntime` conformance `Suite` (see lib.rs).

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, Sender, TryRecvError, channel};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

// --- no-op waker: the busy-poll loop re-polls regardless of wakeups ---------
const NOOP_VTABLE: RawWakerVTable = RawWakerVTable::new(noop_clone, noop, noop, noop);
fn noop_clone(_: *const ()) -> RawWaker {
    RawWaker::new(std::ptr::null(), &NOOP_VTABLE)
}
fn noop(_: *const ()) {}
fn noop_waker() -> Waker {
    // SAFETY: the vtable functions are all no-ops over a null data pointer.
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &NOOP_VTABLE)) }
}

// --- the executor -----------------------------------------------------------
struct Executor {
    // One inbox per worker thread. `Mutex<Sender<..>>` so the whole `Executor`
    // is `Sync` and can live in a `'static` `OnceLock`.
    workers: Vec<Mutex<Sender<BoxFuture>>>,
    next: AtomicUsize,
}

static EXEC: OnceLock<Executor> = OnceLock::new();

// The application's ambient tokio runtime, captured when openraft spawns from
// within it (e.g. inside `Raft::new`). Busy-spin workers enter it so that
// reactor-bound I/O futures openraft awaits on a worker (quinn sockets, quinn's
// internal `tokio::spawn`/timers) find a driver. Entering the app's own runtime
// — rather than a separate reactor — keeps quinn's socket ownership consistent
// (the endpoint is created on that same runtime). Absent any ambient runtime
// (e.g. the conformance Suite), this stays `None` and workers never enter.
//
// It is *refreshable*, not write-once: it is cleared when the pool drains to
// zero (all nodes stopped) and re-captured on the next spawn, so a process that
// builds more than one tokio runtime over its lifetime (notably each
// `#[tokio::test]`) does not leave workers entered into a dropped runtime.
// `REACTOR_GEN` bumps on every change; workers re-clone the handle when it does.
static REACTOR_HANDLE: Mutex<Option<tokio::runtime::Handle>> = Mutex::new(None);
static REACTOR_GEN: AtomicU64 = AtomicU64::new(0);

/// Capture the ambient tokio runtime handle if we are inside one and none is
/// currently held. No-op without an ambient runtime or once captured.
fn capture_reactor() {
    let mut g = REACTOR_HANDLE.lock().unwrap();
    if g.is_none()
        && let Ok(h) = tokio::runtime::Handle::try_current()
    {
        *g = Some(h);
        REACTOR_GEN.fetch_add(1, Ordering::Release);
    }
}

/// Clear the captured handle (called when the pool drains to zero) so the next
/// spawn re-captures whatever runtime is then current.
fn clear_reactor() {
    let mut g = REACTOR_HANDLE.lock().unwrap();
    if g.take().is_some() {
        REACTOR_GEN.fetch_add(1, Ordering::Release);
    }
}

fn default_workers() -> usize {
    std::env::var("UC_BUSYSPIN_WORKERS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n >= 1)
        // Default to a SINGLE never-park worker: it already collapses the futex
        // for a node's openraft tasks, and one spinning core coexists with the
        // app's tokio runtime. Going multi-worker pegs N cores at 100%, so it is
        // opt-in (UC_BUSYSPIN_WORKERS) and wants dedicated/pinned cores — note
        // the in-process multi-node tests share ONE process-global pool, so more
        // workers there starve tokio's I/O thread rather than help.
        .unwrap_or(1)
}

/// Ensure the global busy-spin executor is running (idempotent).
pub fn ensure_started(threads: usize) {
    EXEC.get_or_init(|| start(threads.max(1)));
}

fn global() -> &'static Executor {
    EXEC.get_or_init(|| start(default_workers()))
}

fn start(n: usize) -> Executor {
    let mut workers = Vec::with_capacity(n);
    for id in 0..n {
        let (tx, rx): (Sender<BoxFuture>, Receiver<BoxFuture>) = channel();
        std::thread::Builder::new()
            .name(format!("uc-busyspin-{id}"))
            .spawn(move || {
                // TODO(pinning): core_affinity::set_for_current(cores[id]) here
                // for true CPU-pinned operation. Omitted in the skeleton to keep
                // the dependency set empty; mechanically a one-line add.
                worker_loop(rx)
            })
            .expect("spawn busyspin worker");
        workers.push(Mutex::new(tx));
    }
    Executor { workers, next: AtomicUsize::new(0) }
}

fn worker_loop(rx: Receiver<BoxFuture>) {
    let waker = noop_waker();
    let mut tasks: Vec<BoxFuture> = Vec::new();
    // The app-runtime handle is owned locally and re-cloned whenever the global
    // generation changes, so we always enter the *current* runtime (not a
    // dropped one). It is entered per poll pass — cheap thread-local context
    // set/restore — which also makes a mid-pass refresh take effect next pass.
    let mut local_gen = u64::MAX;
    let mut local_handle: Option<tokio::runtime::Handle> = None;
    loop {
        let cur_gen = REACTOR_GEN.load(Ordering::Acquire);
        if cur_gen != local_gen {
            local_handle = REACTOR_HANDLE.lock().unwrap().clone();
            local_gen = cur_gen;
        }
        // Absorb newly spawned tasks.
        loop {
            match rx.try_recv() {
                Ok(t) => tasks.push(t),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return, // never happens: senders are 'static
            }
        }
        // Busy-poll every live task within the app-runtime context (if any) so
        // reactor-bound I/O futures find a driver; drop the ones that finish.
        {
            let _enter = local_handle.as_ref().map(|h| h.enter());
            let mut i = 0;
            while i < tasks.len() {
                let mut cx = Context::from_waker(&waker);
                match tasks[i].as_mut().poll(&mut cx) {
                    Poll::Ready(()) => {
                        // Task finished; drop the completed future.
                        drop(tasks.swap_remove(i));
                        let n = LIVE_TASKS.fetch_sub(1, Ordering::Relaxed) - 1;
                        if debug_on() {
                            eprintln!("[busyspin] done  -> live={n}");
                        }
                        // Pool drained: release the captured runtime so the next
                        // spawn re-captures whatever runtime is then current.
                        if n == 0 {
                            clear_reactor();
                        }
                    }
                    Poll::Pending => i += 1,
                }
            }
        }
        // Idle pacing between passes. Default `yield_now()` so the worker
        // coexists with the app's tokio I/O thread (and other nodes) under CPU
        // oversubscription — on a dedicated core this is ~a no-op so latency is
        // preserved. `UC_BUSYSPIN_PURE=1` forces a pure `spin_loop()` hint for
        // dedicated/pinned-core deployments that want the absolute minimum
        // wakeup latency and never want to cede the core.
        if pure_spin() {
            std::hint::spin_loop();
        } else {
            std::thread::yield_now();
        }
    }
}

fn pure_spin() -> bool {
    static PURE: OnceLock<bool> = OnceLock::new();
    *PURE.get_or_init(|| {
        std::env::var("UC_BUSYSPIN_PURE").map(|v| v == "1" || v == "true").unwrap_or(false)
    })
}

static LIVE_TASKS: AtomicUsize = AtomicUsize::new(0);

fn debug_on() -> bool {
    static D: OnceLock<bool> = OnceLock::new();
    *D.get_or_init(|| std::env::var("UC_BUSYSPIN_DEBUG").is_ok())
}

fn submit(task: BoxFuture) {
    let ex = global();
    let n = LIVE_TASKS.fetch_add(1, Ordering::Relaxed) + 1;
    if debug_on() {
        eprintln!("[busyspin] spawn -> live={n}");
    }
    let idx = ex.next.fetch_add(1, Ordering::Relaxed) % ex.workers.len();
    let _ = ex.workers[idx].lock().unwrap().send(task);
}

// --- spawn + JoinHandle -----------------------------------------------------

/// Join error type. The skeleton does not catch task panics, so this is never
/// produced; it exists to satisfy the `AsyncRuntime` associated-type bounds.
#[derive(Debug)]
pub struct JoinError;

impl JoinError {
    pub fn is_panic(&self) -> bool {
        false
    }
}

impl std::fmt::Display for JoinError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("busyspin join error")
    }
}

struct JoinSlot<T> {
    inner: Mutex<(Option<T>, Option<Waker>)>,
}

/// Future that resolves when the spawned task completes. Waker-correct, so it
/// can be awaited from any executor (tokio API callers included).
pub struct JoinHandle<T> {
    slot: Arc<JoinSlot<T>>,
}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T, JoinError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut g = self.slot.inner.lock().unwrap();
        match g.0.take() {
            Some(v) => Poll::Ready(Ok(v)),
            None => {
                g.1 = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    // First spawn from openraft happens inside the app's runtime (Raft::new);
    // grab that runtime handle so workers can enter it for reactor-bound I/O.
    capture_reactor();
    let slot = Arc::new(JoinSlot { inner: Mutex::new((None, None)) });
    let slot2 = slot.clone();
    let wrapped: BoxFuture = Box::pin(async move {
        let out = future.await;
        let mut g = slot2.inner.lock().unwrap();
        g.0 = Some(out);
        if let Some(w) = g.1.take() {
            w.wake();
        }
    });
    submit(wrapped);
    JoinHandle { slot }
}

/// Drive a future to completion on the *calling* thread by busy-polling. Tasks
/// spawned within run on the global worker pool (which `global()` guarantees is
/// up). Used by `AsyncRuntime::block_on` (tests/bootstrap).
pub fn block_on<F: Future>(future: F) -> F::Output {
    let _ = global(); // ensure workers exist for any spawns inside `future`
    let mut fut = Box::pin(future);
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    loop {
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
        std::hint::spin_loop();
    }
}
