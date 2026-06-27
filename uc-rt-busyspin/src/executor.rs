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
use std::sync::atomic::{AtomicUsize, Ordering};
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

// The application's ambient tokio runtime, captured the first time openraft
// spawns from within it (e.g. inside `Raft::new`). Busy-spin workers enter it so
// that reactor-bound I/O futures openraft awaits on a worker (quinn sockets,
// quinn's internal `tokio::spawn`/timers) find a driver. Entering the app's own
// runtime — rather than a separate reactor — keeps quinn's socket ownership
// consistent (the endpoint is created on that same runtime). Absent any ambient
// runtime (e.g. the conformance Suite), this stays empty and workers never enter
// — single-node needs no reactor.
static REACTOR_HANDLE: OnceLock<tokio::runtime::Handle> = OnceLock::new();

/// Capture the ambient tokio runtime handle if we are called from within one.
/// Idempotent; a no-op once captured or when there is no ambient runtime.
fn capture_reactor() {
    if REACTOR_HANDLE.get().is_none()
        && let Ok(h) = tokio::runtime::Handle::try_current()
    {
        let _ = REACTOR_HANDLE.set(h);
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
    // Lazily enter the app's tokio runtime once it has been captured (handles
    // the case where workers start before the first `spawn` captures it). The
    // guard is held for the worker's lifetime.
    let mut _rt_guard: Option<tokio::runtime::EnterGuard<'static>> = None;
    loop {
        if _rt_guard.is_none()
            && let Some(h) = REACTOR_HANDLE.get()
        {
            _rt_guard = Some(h.enter());
        }
        // Absorb newly spawned tasks.
        loop {
            match rx.try_recv() {
                Ok(t) => tasks.push(t),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return, // never happens: senders are 'static
            }
        }
        // Busy-poll every live task; drop the ones that finish.
        let mut i = 0;
        while i < tasks.len() {
            let mut cx = Context::from_waker(&waker);
            match tasks[i].as_mut().poll(&mut cx) {
                Poll::Ready(()) => {
                    // Task finished; drop the completed future.
                    drop(tasks.swap_remove(i));
                }
                Poll::Pending => i += 1,
            }
        }
        std::hint::spin_loop();
    }
}

fn submit(task: BoxFuture) {
    let ex = global();
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
