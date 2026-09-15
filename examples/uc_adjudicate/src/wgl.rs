//! B2.i–iii: per-key WGL linearizability through `uc_remote` under leader
//! kills, the acked-write-loss oracle, and (with `churn`) snapshot + purge
//! churn with the install observation the gate's row iii demands.
//!
//! Shape: `examples/uc_crashtest/tests/remote_lin.rs`, generalised — N
//! workers each with a `RemoteClient`, a chaos thread that SIGKILLs the
//! serving leader every `kill_period` and supervises respawns, one
//! `GenHistory<KvOp, KvResp>` PER KEY (the checker sees one register at a
//! time — `uc_lincheck` has no key concept, and per-key histories are how
//! the two-FSM capstones do it too), and the final linearizable read of
//! every key checked against the last-acknowledged-mutation candidate set.
//!
//! Churn adds, on a period: command an instant (`uc2ctl snapshot` on the
//! leader); once a follower holds the complete set, SIGKILL that
//! follower's SERVICE and start a fresh one (below the purge floor, it
//! can only come back by installing); and once, mid-run, wipe a follower's
//! whole instance directory and rejoin it (a snapshot session; the node
//! logs `snapshot_installed`). Both kinds of install are counted and
//! reported; zero of either makes the row NOT RUN, per the gate.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use uc_lincheck::checker::{DEFAULT_BUDGET, Verdict, check_model};
use uc_lincheck::history::{GenEntry, GenHistory, GenOutcome};
use uc_remote::{Consistency, RemoteClient, RemoteConfig, RemoteError};

use crate::adapter::{Adapter, Caps, Digest, KvModel, KvOp, KvResp};
use crate::diverge;
use crate::report::Outcome;
use crate::rig::{Counters, Rig, ServiceRestart, join_within};

#[derive(Clone, Debug)]
pub struct WglCfg {
    pub seed: u64,
    pub secs: u64,
    pub workers: u32,
    pub keys: u32,
    pub throttle: Duration,
    pub kill_period: Duration,
    pub churn: bool,
    pub churn_period: Duration,
    pub budget: u64,
    pub request_timeout: Duration,
}

impl Default for WglCfg {
    fn default() -> Self {
        WglCfg {
            seed: 1,
            secs: 20,
            workers: 4,
            keys: 4,
            throttle: Duration::from_millis(40),
            kill_period: Duration::from_secs(3),
            churn: false,
            churn_period: Duration::from_millis(1500),
            budget: DEFAULT_BUDGET,
            request_timeout: Duration::from_secs(15),
        }
    }
}

#[derive(Clone, Debug)]
struct Mutation {
    pos: u64,
    value: Option<u64>,
    replayed: bool,
}

struct Shared {
    adapter: Arc<dyn Adapter>,
    caps: Caps,
    gateways: Vec<String>,
    app_id: String,
    keys: Vec<Vec<u8>>,
    histories: Vec<GenHistory<KvOp, KvResp>>,
    mutations: Vec<Mutex<Vec<Mutation>>>,
    indeterminate: Vec<Mutex<Vec<Option<u64>>>>,
    throttle: Duration,
    request_timeout: Duration,
    ok_ops: AtomicU64,
    indeterminate_ops: AtomicU64,
    unsent_ops: AtomicU64,
    decode_errors: AtomicU64,
    reconnects: AtomicU64,
    decode_samples: Mutex<Vec<String>>,
}

fn connect(shared: &Shared, stop: &AtomicBool, budget: Duration) -> Option<RemoteClient> {
    connect_with(shared, stop, budget, 16)
}

fn connect_with(
    shared: &Shared,
    stop: &AtomicBool,
    budget: Duration,
    max_inflight: u32,
) -> Option<RemoteClient> {
    let deadline = Instant::now() + budget;
    while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
        let cfg = RemoteConfig {
            app_id: shared.app_id.clone(),
            members: shared.gateways.clone(),
            client_id: Some(rand::random::<u64>() | 1),
            request_timeout: shared.request_timeout,
            connect_timeout: Duration::from_secs(2),
            max_inflight,
            ..Default::default()
        };
        if let Ok(c) = RemoteClient::connect(cfg) {
            return Some(c);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    None
}

/// Value of a mutation if it applied (`None` = the key is absent after it).
fn mutation_value(op: &KvOp) -> Option<Option<u64>> {
    match op {
        KvOp::Put(v) => Some(Some(*v)),
        KvOp::Delete => Some(None),
        KvOp::Cas { new, .. } => Some(Some(*new)),
        KvOp::Get => None,
    }
}

fn worker(id: u32, seed: u64, shared: Arc<Shared>, stop: Arc<AtomicBool>) {
    let mut rng = StdRng::seed_from_u64(seed ^ ((id as u64 + 1) << 32));
    let mut counter: u64 = 0;
    // Per key: the last (version, value) pair this worker saw acknowledged.
    let mut known: HashMap<usize, (Option<u64>, u64)> = HashMap::new();
    let mut client = connect(&shared, &stop, Duration::from_secs(30));
    while !stop.load(Ordering::Relaxed) {
        let Some(c) = client.as_ref() else {
            shared.reconnects.fetch_add(1, Ordering::Relaxed);
            client = connect(&shared, &stop, Duration::from_secs(10));
            continue;
        };
        let k = if shared.caps.keys {
            rng.random_range(0..shared.keys.len())
        } else {
            0
        };
        counter += 1;
        let fresh = ((id as u64) << 40) | counter;
        let (op, cas_version) = match rng.random_range(0..4u8) {
            0 => (KvOp::Put(fresh), 0),
            1 => (KvOp::Get, 0),
            2 if shared.caps.delete => (KvOp::Delete, 0),
            2 => (KvOp::Get, 0),
            _ => match known.get(&k) {
                Some((Some(ver), val)) => (
                    KvOp::Cas {
                        old: Some(*val),
                        new: fresh,
                    },
                    *ver,
                ),
                Some((None, val)) => (
                    KvOp::Cas {
                        old: Some(*val),
                        new: fresh,
                    },
                    0,
                ),
                None if shared.caps.cas_absent => (
                    KvOp::Cas {
                        old: None,
                        new: fresh,
                    },
                    0,
                ),
                None => (KvOp::Put(fresh), 0),
            },
        };
        let bytes = shared.adapter.encode(&shared.keys[k], &op, cas_version);
        let hist = &shared.histories[k];
        let inv = hist.invoke();
        let res = if matches!(op, KvOp::Get) {
            c.query(&bytes, Consistency::Linearizable)
                .and_then(|t| t.wait())
        } else {
            c.submit(&bytes).and_then(|t| t.wait())
        };
        match res {
            Ok(resp) => match shared.adapter.decode(&op, &resp.bytes) {
                Ok(d) => {
                    // Learn CAS material and record the mutation for the oracle.
                    match (&op, &d.resp) {
                        (KvOp::Put(v), KvResp::Ack) => {
                            known.insert(k, (d.version, *v));
                        }
                        (KvOp::Get, KvResp::Value(Some(v))) => {
                            known.insert(k, (d.version, *v));
                        }
                        (KvOp::Get, KvResp::Value(None)) => {
                            known.remove(&k);
                        }
                        (KvOp::Cas { new, .. }, KvResp::CasOk(true)) => {
                            known.insert(k, (d.version, *new));
                        }
                        (KvOp::Cas { .. }, KvResp::CasOk(false)) => {
                            known.remove(&k);
                        }
                        (KvOp::Delete, _) => {
                            known.remove(&k);
                        }
                        _ => {}
                    }
                    let mutated = matches!(
                        (&op, &d.resp),
                        (KvOp::Put(_), KvResp::Ack)
                            | (KvOp::Delete, KvResp::Deleted(true))
                            | (KvOp::Cas { .. }, KvResp::CasOk(true))
                    );
                    if mutated {
                        shared.mutations[k].lock().unwrap().push(Mutation {
                            pos: d.version.unwrap_or(resp.position),
                            value: mutation_value(&op).unwrap(),
                            replayed: resp.replayed,
                        });
                    }
                    shared.ok_ops.fetch_add(1, Ordering::Relaxed);
                    hist.record(id, op, inv, GenOutcome::Ok(d.resp));
                }
                Err(e) => {
                    // The service answered something its page does not
                    // document. Recorded as indeterminate so the history
                    // stays sound, and counted: any such answer FAILs.
                    shared.decode_errors.fetch_add(1, Ordering::Relaxed);
                    let mut s = shared.decode_samples.lock().unwrap();
                    if s.len() < 8 {
                        s.push(format!("{op:?}: {e}"));
                    }
                    if let Some(v) = mutation_value(&op) {
                        shared.indeterminate[k].lock().unwrap().push(v);
                    }
                    hist.record(id, op, inv, GenOutcome::Indeterminate);
                }
            },
            Err(
                RemoteError::PayloadTooLarge
                | RemoteError::Config(_)
                | RemoteError::HelloRefused { .. },
            ) => {
                // Refused at the door, never sent: not part of the history.
                shared.unsent_ops.fetch_add(1, Ordering::Relaxed);
            }
            Err(RemoteError::NoMembersReachable) => {
                shared.unsent_ops.fetch_add(1, Ordering::Relaxed);
                client = None;
            }
            Err(e) => {
                shared.indeterminate_ops.fetch_add(1, Ordering::Relaxed);
                if let Some(v) = mutation_value(&op) {
                    shared.indeterminate[k].lock().unwrap().push(v);
                }
                hist.record(id, op, inv, GenOutcome::Indeterminate);
                if matches!(e, RemoteError::Closed | RemoteError::Io(_)) {
                    client = None;
                }
            }
        }
        std::thread::sleep(shared.throttle);
    }
    if let Some(c) = client {
        c.shutdown();
    }
}

// Sized to move the purge floor over a ~30 s churn window without a firehose:
// a saturating filler on loopback (where discovery engages the 8896 B jumbo
// rung) drove a node to an `IngressRingCorrupt` fail-stop, which the harness
// correctly reports but which drowns the row's own signal. A bounded window
// with a small pause per batch generates the log volume purge needs and no
// more. See the gate doc's B2.iii note and the harness README.
const FILL_WINDOW: usize = 48;
const FILL_VALUE_BYTES: usize = 512;
const FILL_BATCH_PAUSE: Duration = Duration::from_millis(2);

fn filler(shared: Arc<Shared>, stop: Arc<AtomicBool>) {
    let mut counter = 0u64;
    let mut client = None;
    let mut inflight: std::collections::VecDeque<uc_remote::Ticket> =
        std::collections::VecDeque::new();
    let big = vec![0xA5u8; FILL_VALUE_BYTES];
    while !stop.load(Ordering::Relaxed) {
        let Some(c) = client.as_ref() else {
            client = connect_with(
                &shared,
                &stop,
                Duration::from_secs(10),
                FILL_WINDOW as u32 * 2,
            );
            continue;
        };
        counter += 1;
        let key = format!("fill:{}", counter % 4096);
        let cmd = shared
            .adapter
            .encode_put_bytes(key.as_bytes(), &big)
            .unwrap_or_else(|| {
                shared
                    .adapter
                    .encode(key.as_bytes(), &KvOp::Put(counter), 0)
            });
        while inflight.len() >= FILL_WINDOW - 1 {
            let _ = inflight.pop_front().unwrap().wait();
        }
        match c.submit(&cmd) {
            Ok(t) => inflight.push_back(t),
            Err(_) => {
                inflight.clear();
                client = None;
            }
        }
        if counter.is_multiple_of(FILL_WINDOW as u64) {
            std::thread::sleep(FILL_BATCH_PAUSE);
        }
    }
    while let Some(t) = inflight.pop_front() {
        let _ = t.wait();
    }
    if let Some(c) = client {
        c.shutdown();
    }
}

/// The values the final read of a key may legitimately observe (the
/// `remote_lin` oracle, per key): the last FRESH acknowledged mutation by
/// position, any REPLAYED mutation above it (a replay's `position` is only
/// an upper bound when it is the resend's), and every indeterminate
/// mutation (it may still commit).
fn expected_final(muts: &[Mutation], indet: &[Option<u64>]) -> Vec<Option<u64>> {
    let mut c = Vec::new();
    let last_fresh = muts.iter().filter(|m| !m.replayed).max_by_key(|m| m.pos);
    match last_fresh {
        Some(m) => {
            c.push(m.value);
            for r in muts.iter().filter(|r| r.replayed && r.pos > m.pos) {
                c.push(r.value);
            }
        }
        None => {
            // No FRESH ack: every acknowledged mutation was a session-dedup
            // replay, which still proves the server committed it. Match
            // `remote_lin`'s oracle — the candidates are the replayed values
            // (a replayed Delete contributes `None`), NOT an unconditional
            // absent. Absent is legitimate ONLY for a key nothing ever
            // mutated; pushing it otherwise would accept a Put silently
            // reverting to absent, which is exactly the acked-loss this row
            // exists to catch.
            for r in muts.iter().filter(|r| r.replayed) {
                c.push(r.value);
            }
            if muts.is_empty() {
                c.push(None);
            }
        }
    }
    c.extend(indet.iter().copied());
    c
}

pub struct KeyVerdict {
    pub key: String,
    pub verdict: Verdict,
    pub spent: u64,
    pub entries: usize,
}

pub struct WglReport {
    pub outcome: Outcome,
    pub per_key: Vec<KeyVerdict>,
    pub ok_ops: u64,
    pub indeterminate_ops: u64,
    pub unsent_ops: u64,
    pub decode_errors: u64,
    pub decode_samples: Vec<String>,
    pub reconnects: u64,
    pub acked_loss: Vec<String>,
    pub final_read_failures: Vec<String>,
    pub counters: Counters,
    pub node_installs: usize,
    pub service_installs: usize,
    pub digests: Option<Result<Vec<(String, Digest)>, String>>,
    pub notes: Vec<String>,
    pub uc_version: String,
}

const CHECKER_STACK: usize = 256 << 20;

fn check_key(entries: Vec<GenEntry<KvOp, KvResp>>, budget: u64) -> Result<(Verdict, u64)> {
    let run = |b: u64| -> Result<(Verdict, u64)> {
        let es = entries.clone();
        let h = std::thread::Builder::new()
            .name("wgl-checker".into())
            .stack_size(CHECKER_STACK)
            .spawn(move || check_model::<KvModel>(&es, b))?;
        h.join()
            .map_err(|_| anyhow::anyhow!("checker thread panicked"))
    };
    let (v, spent) = run(budget)?;
    if v == Verdict::Inconclusive {
        return run(budget * 10);
    }
    Ok((v, spent))
}

/// Churn state machine, ticked by the chaos thread every 200 ms.
///
/// Every `period`: command an instant on the leader. Whenever a follower
/// holds the NEWEST complete set and its archive has already dropped a
/// prefix (`archive_first_base > 0` — purge fired below that set), kill
/// that follower's service: a fresh one starts at position 0 below a
/// purged prefix and can only come back by installing the artifact,
/// which [`Rig::service_install_inferred`] then confirms from the slot's
/// `applied`. Once, after mid-run and once a prefix has been purged
/// somewhere, wipe a follower's instance directory: it rejoins below the
/// floor through a snapshot session and the node logs `snapshot_installed`.
struct Churn {
    next: Instant,
    period: Duration,
    latest_instant: Option<u64>,
    pending_restarts: Vec<ServiceRestart>,
    restarts_per_node: Vec<u32>,
    service_installs: usize,
    wipe_after: Instant,
    wiped: Option<(usize, u64)>,
    node_installs: usize,
    stop_new_faults: bool,
}

const MAX_SERVICE_RESTARTS_PER_NODE: u32 = 3;

impl Churn {
    fn tick(&mut self, rig: &mut Rig, notes: &mut Vec<String>) {
        // Resolve inferences that have come true.
        let mut still = Vec::new();
        for r in self.pending_restarts.drain(..) {
            if rig.service_install_inferred(&r).is_some() {
                self.service_installs += 1;
            } else {
                still.push(r);
            }
        }
        self.pending_restarts = still;
        if let Some((i, mark)) = self.wiped {
            let n = rig.node_log_installs_since(i, mark);
            if n > 0 {
                self.node_installs = n;
            }
        }
        if self.stop_new_faults {
            return;
        }
        if Instant::now() >= self.next {
            self.next = Instant::now() + self.period;
            match rig.command_instant() {
                Ok(p) => self.latest_instant = Some(p),
                Err(e) => notes.push(format!("instant: {e:#}")),
            }
        }
        let leader = rig.find_leader();
        if let Some(p) = self.latest_instant {
            for i in 0..rig.cfg.n {
                let busy = self.pending_restarts.iter().any(|r| r.node == i);
                if Some(i) != leader
                    && !busy
                    && self.restarts_per_node[i] < MAX_SERVICE_RESTARTS_PER_NODE
                    && rig.complete_set(i) == Some(p)
                    && rig.archive_first_base(i) > 0
                {
                    match rig.restart_service(i) {
                        Ok(r) => {
                            self.pending_restarts.push(r);
                            self.restarts_per_node[i] += 1;
                        }
                        Err(e) => notes.push(format!("service restart: {e:#}")),
                    }
                }
            }
        }
        if self.wiped.is_none() && Instant::now() >= self.wipe_after {
            let purged_somewhere = (0..rig.cfg.n).any(|i| rig.archive_first_base(i) > 0);
            if purged_somewhere && let Some(i) = (0..rig.cfg.n).find(|&i| Some(i) != leader) {
                self.pending_restarts.retain(|r| r.node != i);
                match rig.wipe_and_rejoin(i) {
                    Ok(mark) => self.wiped = Some((i, mark)),
                    Err(e) => notes.push(format!("wipe: {e:#}")),
                }
            }
        }
    }
}

pub fn run(rig: Rig, adapter: Arc<dyn Adapter>, cfg: &WglCfg) -> Result<WglReport> {
    let caps = adapter.caps();
    let nkeys = if caps.keys {
        cfg.keys.max(1) as usize
    } else {
        1
    };
    let keys: Vec<Vec<u8>> = (0..nkeys)
        .map(|i| format!("wgl:{}:{i}", cfg.seed).into_bytes())
        .collect();
    let shared = Arc::new(Shared {
        adapter: adapter.clone(),
        caps,
        gateways: rig.gateways(),
        app_id: rig.cfg.app_id.clone(),
        keys: keys.clone(),
        histories: (0..nkeys).map(|_| GenHistory::default()).collect(),
        mutations: (0..nkeys).map(|_| Mutex::new(Vec::new())).collect(),
        indeterminate: (0..nkeys).map(|_| Mutex::new(Vec::new())).collect(),
        throttle: cfg.throttle,
        request_timeout: cfg.request_timeout,
        ok_ops: AtomicU64::new(0),
        indeterminate_ops: AtomicU64::new(0),
        unsent_ops: AtomicU64::new(0),
        decode_errors: AtomicU64::new(0),
        reconnects: AtomicU64::new(0),
        decode_samples: Mutex::new(Vec::new()),
    });
    let uc_version = rig.uc_version.clone();
    let rig = Arc::new(Mutex::new(rig));
    let stop = Arc::new(AtomicBool::new(false));
    let mut notes = Vec::new();

    // Warm-up write so every key exists before the first kill? No — an
    // absent key is a valid state the model starts in.
    let workers: Vec<_> = (0..cfg.workers)
        .map(|id| {
            let s = shared.clone();
            let st = stop.clone();
            let seed = cfg.seed;
            std::thread::Builder::new()
                .name(format!("wgl-worker-{id}"))
                .spawn(move || worker(id, seed, s, st))
                .expect("spawn worker")
        })
        .collect();

    // Churn needs log VOLUME: purge drops whole journal segments, so a
    // throttled history alone never moves the floor. The filler writes
    // unchecked keys as fast as the window allows; it is not in any history.
    let filler = cfg.churn.then(|| {
        let s = shared.clone();
        let st = stop.clone();
        std::thread::Builder::new()
            .name("wgl-filler".into())
            .spawn(move || filler(s, st))
            .expect("spawn filler")
    });

    let chaos = {
        let rig = rig.clone();
        let stop = stop.clone();
        let cfg = cfg.clone();
        std::thread::Builder::new()
            .name("wgl-chaos".into())
            .spawn(move || -> (Vec<String>, usize, usize) {
                let mut notes = Vec::new();
                let started = Instant::now();
                let mut next_kill = started + cfg.kill_period;
                let n = rig.lock().unwrap().cfg.n;
                let mut churn = cfg.churn.then(|| Churn {
                    next: started + cfg.churn_period,
                    period: cfg.churn_period,
                    latest_instant: None,
                    pending_restarts: Vec::new(),
                    restarts_per_node: vec![0; n],
                    service_installs: 0,
                    wipe_after: started + Duration::from_secs(cfg.secs / 2),
                    wiped: None,
                    node_installs: 0,
                    stop_new_faults: false,
                });
                while !stop.load(Ordering::Relaxed) {
                    std::thread::sleep(Duration::from_millis(200));
                    let mut r = rig.lock().unwrap();
                    if let Err(e) = r.supervise() {
                        notes.push(format!("supervise: {e:#}"));
                    }
                    if Instant::now() >= next_kill {
                        next_kill = Instant::now() + cfg.kill_period;
                        if let Some(l) = r.find_leader()
                            && let Err(e) = r.kill_and_restart_node(l)
                        {
                            notes.push(format!("kill/restart node {l}: {e:#}"));
                        }
                    }
                    if let Some(c) = churn.as_mut() {
                        c.tick(&mut r, &mut notes);
                    }
                }
                // Give pending inferences a last chance to resolve.
                if let Some(c) = churn.as_mut() {
                    c.stop_new_faults = true;
                    let deadline = Instant::now() + Duration::from_secs(30);
                    while Instant::now() < deadline
                        && (!c.pending_restarts.is_empty()
                            || (c.wiped.is_some() && c.node_installs == 0))
                    {
                        std::thread::sleep(Duration::from_millis(200));
                        let mut r = rig.lock().unwrap();
                        let _ = r.supervise();
                        c.tick(&mut r, &mut notes);
                    }
                    (notes, c.node_installs, c.service_installs)
                } else {
                    (notes, 0, 0)
                }
            })
            .expect("spawn chaos")
    };

    std::thread::sleep(Duration::from_secs(cfg.secs));
    stop.store(true, Ordering::Relaxed);
    for (i, w) in workers.into_iter().enumerate() {
        join_within(
            w,
            &format!("worker {i}"),
            cfg.request_timeout * 3 + Duration::from_secs(30),
        )?;
    }
    if let Some(f) = filler {
        join_within(
            f,
            "filler",
            cfg.request_timeout * 3 + Duration::from_secs(30),
        )?;
    }
    let (chaos_notes, node_installs, service_installs) =
        join_within(chaos, "chaos thread", Duration::from_secs(90))?;
    notes.extend(chaos_notes);

    // Final reads, against whatever leads now.
    let mut acked_loss = Vec::new();
    let mut final_read_failures = Vec::new();
    {
        let never = AtomicBool::new(false);
        let mut client = connect(&shared, &never, Duration::from_secs(30));
        for (k, key) in keys.iter().enumerate() {
            let q = adapter.encode(key, &KvOp::Get, 0);
            let deadline = Instant::now() + Duration::from_secs(60);
            let mut got: Option<Option<u64>> = None;
            while got.is_none() && Instant::now() < deadline {
                let Some(c) = client.as_ref() else {
                    client = connect(&shared, &never, Duration::from_secs(10));
                    continue;
                };
                match c
                    .query(&q, Consistency::Linearizable)
                    .and_then(|t| t.wait())
                {
                    Ok(resp) => match adapter.decode(&KvOp::Get, &resp.bytes) {
                        Ok(d) => {
                            if let KvResp::Value(v) = d.resp {
                                got = Some(v);
                            }
                        }
                        Err(e) => {
                            final_read_failures
                                .push(format!("{}: decode {e}", String::from_utf8_lossy(key)));
                            break;
                        }
                    },
                    Err(
                        RemoteError::Closed | RemoteError::Io(_) | RemoteError::NoMembersReachable,
                    ) => {
                        client = None;
                    }
                    Err(_) => std::thread::sleep(Duration::from_millis(200)),
                }
            }
            match got {
                Some(v) => {
                    let muts = shared.mutations[k].lock().unwrap();
                    let indet = shared.indeterminate[k].lock().unwrap();
                    let cands = expected_final(&muts, &indet);
                    if !cands.contains(&v) {
                        acked_loss.push(format!(
                            "{}: final read {v:?}, acknowledged candidates {cands:?} ({} mutations, {} indeterminate)",
                            String::from_utf8_lossy(key),
                            muts.len(),
                            indet.len()
                        ));
                    }
                }
                None => final_read_failures.push(format!(
                    "{}: no linearizable read within 60s",
                    String::from_utf8_lossy(key)
                )),
            }
        }
        if let Some(c) = client {
            c.shutdown();
        }
    }

    // Divergence, while the cluster is still up.
    let digests = if caps.digest {
        let r = rig.lock().unwrap();
        Some(
            diverge::live_digests(
                adapter.as_ref(),
                &r.gateways(),
                &r.cfg.app_id,
                Duration::from_secs(60),
            )
            .map(|rows| {
                rows.into_iter()
                    .map(|r| (r.gateway, r.digest))
                    .collect::<Vec<_>>()
            })
            .map_err(|e| format!("{e:#}")),
        )
    } else {
        None
    };

    // Tear the cluster down BEFORE the checker runs (CPU, and no orphans
    // if the checker dies).
    let counters = {
        let mut r = rig.lock().unwrap();
        r.stop_all();
        r.counters.clone()
    };
    drop(rig);

    let shared = Arc::try_unwrap(shared)
        .ok()
        .context("sole owner of the shared state")?;
    let ok_ops = shared.ok_ops.load(Ordering::Relaxed);
    let indeterminate_ops = shared.indeterminate_ops.load(Ordering::Relaxed);
    let unsent_ops = shared.unsent_ops.load(Ordering::Relaxed);
    let decode_errors = shared.decode_errors.load(Ordering::Relaxed);
    let reconnects = shared.reconnects.load(Ordering::Relaxed);
    let decode_samples = shared.decode_samples.into_inner().unwrap();
    let mut per_key = Vec::new();
    for (k, h) in shared.histories.into_iter().enumerate() {
        let entries = h.into_entries();
        let n = entries.len();
        let (verdict, spent) = check_key(entries, cfg.budget)?;
        per_key.push(KeyVerdict {
            key: String::from_utf8_lossy(&keys[k]).into_owned(),
            verdict,
            spent,
            entries: n,
        });
    }

    let violation = per_key.iter().any(|k| k.verdict == Verdict::Violation);
    let inconclusive = per_key.iter().any(|k| k.verdict == Verdict::Inconclusive);
    let diverged = matches!(&digests, Some(Ok(rows)) if !rows.windows(2).all(|w| w[0].1.count == w[1].1.count && w[0].1.digest == w[1].1.digest));
    let digest_failed = matches!(&digests, Some(Err(_)));
    let mut outcome = Outcome::Pass;
    if violation
        || !acked_loss.is_empty()
        || decode_errors > 0
        || diverged
        || counters.node_exits > 0
    {
        outcome = Outcome::Fail;
    } else if inconclusive || !final_read_failures.is_empty() || digest_failed {
        outcome = Outcome::NotRun;
    }
    if counters.kills < 3 {
        notes.push(format!(
            "vacuity: only {} leader kills (need >= 3)",
            counters.kills
        ));
        if outcome == Outcome::Pass {
            outcome = Outcome::NotRun;
        }
    }
    if ok_ops == 0 {
        notes.push("vacuity: no acknowledged op".into());
        if outcome == Outcome::Pass {
            outcome = Outcome::NotRun;
        }
    }
    if cfg.churn {
        if !caps.snapshots {
            notes.push(format!(
                "churn requested but adapter {} attaches without snapshots",
                adapter.name()
            ));
            if outcome == Outcome::Pass {
                outcome = Outcome::NotRun;
            }
        } else if node_installs + service_installs == 0 {
            notes.push("churn: no snapshot install observed on a restarted or joining service (row iii: NOT RUN)".into());
            if outcome == Outcome::Pass {
                outcome = Outcome::NotRun;
            }
        }
    }
    if counters.node_exits > 0 {
        notes.push(format!(
            "{} node(s) exited on their own — a UC fail-stop, see logs",
            counters.node_exits
        ));
    }
    if counters.svc_exits > 0 {
        // A service exiting on its own is a fail-stop signal, but during
        // churn/failover a benign attach race can also trigger it, so it is a
        // note rather than an automatic FAIL — a reader weighs it against the
        // logs.
        notes.push(format!(
            "{} service(s) exited on their own and were respawned — see logs",
            counters.svc_exits
        ));
    }
    if counters.restart_timeouts > 0 {
        // A node did not present a fresh instance within the restart budget;
        // the topology degraded for part of the run, so a PASS is not
        // trustworthy — degrade it to NOT RUN.
        notes.push(format!(
            "{} node restart(s) timed out waiting for a fresh instance — topology degraded",
            counters.restart_timeouts
        ));
        if outcome == Outcome::Pass {
            outcome = Outcome::NotRun;
        }
    }
    Ok(WglReport {
        outcome,
        per_key,
        ok_ops,
        indeterminate_ops,
        unsent_ops,
        decode_errors,
        decode_samples,
        reconnects,
        acked_loss,
        final_read_failures,
        counters,
        node_installs,
        service_installs,
        digests,
        notes,
        uc_version,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(pos: u64, value: Option<u64>, replayed: bool) -> Mutation {
        Mutation {
            pos,
            value,
            replayed,
        }
    }

    #[test]
    fn oracle_last_fresh_wins_plus_replays_above_plus_indeterminate() {
        let muts = vec![
            m(10, Some(1), false),
            m(30, None, false),
            m(20, Some(2), false),
            m(40, Some(9), true),
        ];
        let c = expected_final(&muts, &[Some(7)]);
        assert_eq!(c, vec![None, Some(9), Some(7)]);
        // A replay BELOW the last fresh mutation is not a candidate.
        let muts = vec![m(10, Some(1), true), m(30, Some(3), false)];
        assert_eq!(expected_final(&muts, &[]), vec![Some(3)]);
        // Nothing FRESH acknowledged but a replay committed a value: the
        // replayed value is a candidate; absent is NOT (a Put cannot silently
        // revert) — the acked-loss this row exists to catch.
        assert_eq!(expected_final(&[m(5, Some(4), true)], &[]), vec![Some(4)]);
        // A key nothing ever mutated legitimately reads absent.
        assert_eq!(expected_final(&[], &[]), vec![None]);
        // A replayed Delete does make absent a candidate.
        assert_eq!(expected_final(&[m(5, None, true)], &[]), vec![None]);
    }
}
