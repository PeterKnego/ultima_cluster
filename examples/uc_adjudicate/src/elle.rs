//! B2-v2.iv — Elle list-append driven through the remote path. Generates
//! a Jepsen-style singleton-transaction history (`uc_lincheck::edn`, the
//! same recorder `uc_node/tests/elle_v2.rs` uses over shmem) from N
//! workers appending unique values to a few keys and reading the lists
//! back, under leader kills, and writes `history.edn` for
//! `scripts/dogfood_elle.sh` to adjudicate under both `serializable` and
//! `strong-serializable` with the vendored elle-cli.
//!
//! Runnable only with an adapter that has Append + a list read (v2); with
//! v1 it reports NOT RUN without touching a cluster.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use uc_lincheck::edn::{EdnOp, EdnRecorder, EdnType};
use uc_remote::{Consistency, RemoteClient, RemoteConfig, RemoteError};

use crate::adapter::Adapter;
use crate::report::Outcome;
use crate::rig::{Counters, Rig, join_within};

#[derive(Clone, Debug)]
pub struct ElleCfg {
    pub seed: u64,
    pub secs: u64,
    pub workers: u32,
    pub keys: u32,
    pub throttle: Duration,
    pub kill_period: Option<Duration>,
    pub out: PathBuf,
    pub request_timeout: Duration,
}

pub struct ElleReport {
    pub outcome: Outcome,
    pub history: Option<PathBuf>,
    pub ok: u64,
    pub completed: u64,
    pub counters: Counters,
    pub notes: Vec<String>,
}

struct Shared {
    adapter: Arc<dyn Adapter>,
    gateways: Vec<String>,
    app_id: String,
    keys: Vec<Vec<u8>>,
    rec: EdnRecorder,
    throttle: Duration,
    request_timeout: Duration,
    unsent: AtomicU64,
}

fn connect(shared: &Shared, stop: &AtomicBool, budget: Duration) -> Option<RemoteClient> {
    let deadline = Instant::now() + budget;
    while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
        let cfg = RemoteConfig {
            app_id: shared.app_id.clone(),
            members: shared.gateways.clone(),
            client_id: Some(rand::random::<u64>() | 1),
            request_timeout: shared.request_timeout,
            connect_timeout: Duration::from_secs(2),
            max_inflight: 16,
            ..Default::default()
        };
        if let Ok(c) = RemoteClient::connect(cfg) {
            return Some(c);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    None
}

fn worker(id: u32, seed: u64, shared: Arc<Shared>, stop: Arc<AtomicBool>) {
    let mut rng = StdRng::seed_from_u64(seed ^ ((id as u64 + 1) << 32));
    let mut process = id as u64;
    let mut counter: u64 = 0;
    let mut client = connect(&shared, &stop, Duration::from_secs(30));
    while !stop.load(Ordering::Relaxed) {
        let Some(c) = client.as_ref() else {
            client = connect(&shared, &stop, Duration::from_secs(10));
            continue;
        };
        let k = rng.random_range(0..shared.keys.len());
        counter += 1;
        let val = ((id as u64) << 40) | counter;
        let is_read = rng.random_bool(0.5);
        let op = if is_read {
            EdnOp::Read {
                key: k as u32,
                result: None,
            }
        } else {
            EdnOp::Append { key: k as u32, val }
        };
        shared.rec.record(EdnType::Invoke, process, &op);
        let res = if is_read {
            let q = shared
                .adapter
                .encode_list_read(&shared.keys[k])
                .expect("caps.append");
            c.query(&q, Consistency::Linearizable)
                .and_then(|t| t.wait())
        } else {
            let cmd = shared
                .adapter
                .encode_append(&shared.keys[k], val)
                .expect("caps.append");
            c.submit(&cmd).and_then(|t| t.wait())
        };
        match res {
            Ok(resp) => {
                if is_read {
                    match shared.adapter.decode_list(&resp.bytes) {
                        Ok(list) => shared.rec.record(
                            EdnType::Ok,
                            process,
                            &EdnOp::Read {
                                key: k as u32,
                                result: Some(list),
                            },
                        ),
                        Err(_) => {
                            shared.rec.record(EdnType::Info, process, &op);
                            process = shared.rec.retire();
                        }
                    }
                } else {
                    shared.rec.record(EdnType::Ok, process, &op);
                }
            }
            Err(
                RemoteError::PayloadTooLarge
                | RemoteError::Config(_)
                | RemoteError::HelloRefused { .. },
            ) => {
                shared.rec.record(EdnType::Fail, process, &op);
                shared.unsent.fetch_add(1, Ordering::Relaxed);
            }
            Err(RemoteError::NoMembersReachable) => {
                shared.rec.record(EdnType::Fail, process, &op);
                client = None;
            }
            Err(e) => {
                // Maybe committed: `:info` retires the process id (Jepsen
                // semantics — a process with an unresolved op never issues
                // another).
                shared.rec.record(EdnType::Info, process, &op);
                process = shared.rec.retire();
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

pub fn run(rig: Option<Rig>, adapter: Arc<dyn Adapter>, cfg: &ElleCfg) -> Result<ElleReport> {
    if !adapter.caps().append {
        return Ok(ElleReport {
            outcome: Outcome::NotRun,
            history: None,
            ok: 0,
            completed: 0,
            counters: Counters::default(),
            notes: vec![format!(
                "adapter {} has no Append/list read; the Elle row is a v2 row",
                adapter.name()
            )],
        });
    }
    let rig = rig.context("a rig is required once the adapter supports Append")?;
    let keys: Vec<Vec<u8>> = (0..cfg.keys.max(1))
        .map(|i| format!("elle:{}:{i}", cfg.seed).into_bytes())
        .collect();
    let shared = Arc::new(Shared {
        adapter: adapter.clone(),
        gateways: rig.gateways(),
        app_id: rig.cfg.app_id.clone(),
        keys,
        rec: EdnRecorder::new(cfg.workers as u64),
        throttle: cfg.throttle,
        request_timeout: cfg.request_timeout,
        unsent: AtomicU64::new(0),
    });
    let rig = Arc::new(std::sync::Mutex::new(rig));
    let stop = Arc::new(AtomicBool::new(false));
    let workers: Vec<_> = (0..cfg.workers)
        .map(|id| {
            let s = shared.clone();
            let st = stop.clone();
            let seed = cfg.seed;
            std::thread::spawn(move || worker(id, seed, s, st))
        })
        .collect();
    let chaos = {
        let rig = rig.clone();
        let stop = stop.clone();
        let kill_period = cfg.kill_period;
        std::thread::spawn(move || -> Vec<String> {
            let mut notes = Vec::new();
            let mut next_kill = kill_period.map(|p| Instant::now() + p);
            while !stop.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(200));
                let mut r = rig.lock().unwrap();
                let _ = r.supervise();
                if let (Some(p), Some(t)) = (kill_period, next_kill)
                    && Instant::now() >= t
                {
                    next_kill = Some(Instant::now() + p);
                    if let Some(l) = r.find_leader()
                        && let Err(e) = r.kill_and_restart_node(l)
                    {
                        notes.push(format!("kill/restart node {l}: {e:#}"));
                    }
                }
            }
            notes
        })
    };
    std::thread::sleep(Duration::from_secs(cfg.secs));
    stop.store(true, Ordering::Relaxed);
    for (i, w) in workers.into_iter().enumerate() {
        join_within(
            w,
            &format!("elle worker {i}"),
            cfg.request_timeout * 3 + Duration::from_secs(30),
        )?;
    }
    let mut notes = join_within(chaos, "elle chaos", Duration::from_secs(60))?;
    let counters = {
        let mut r = rig.lock().unwrap();
        r.stop_all();
        r.counters.clone()
    };
    std::fs::create_dir_all(&cfg.out)?;
    let path = cfg.out.join("history.edn");
    shared.rec.write_to(&path)?;
    std::fs::write(cfg.out.join("seed"), format!("{}\n", cfg.seed))?;
    let ok = shared.rec.ok_count();
    let completed = shared.rec.completed_count();
    let unsent = shared.unsent.load(Ordering::Relaxed);
    if unsent > 0 {
        notes.push(format!("{unsent} ops refused at the door (recorded :fail)"));
    }
    let mut outcome = Outcome::Pass; // "history written"; the verdict is elle-cli's
    if ok == 0 {
        notes.push("vacuity: no :ok op".into());
        outcome = Outcome::NotRun;
    }
    if cfg.kill_period.is_some() && counters.kills < 3 {
        notes.push(format!(
            "vacuity: only {} leader kills (need >= 3)",
            counters.kills
        ));
        outcome = Outcome::NotRun;
    }
    Ok(ElleReport {
        outcome,
        history: Some(path),
        ok,
        completed,
        counters,
        notes,
    })
}
