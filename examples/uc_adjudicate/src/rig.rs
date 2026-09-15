//! The process rig: N voters, each a release `uc2-node` + the service
//! binary under test + a release `uc2-gateway`, on loopback, under one
//! root directory, every child SIGKILLed and reaped on drop.
//!
//! Why release binaries and TOML files rather than `uc_crashtest-node`'s
//! flags: the platform under test is FROZEN at a release (the charter's
//! decision 4), the builder linked its service against that release's
//! crates, and the same rig has to run unchanged against the next
//! tarball for the platform-upgrade scenario. `--uc-bin-dir` names the
//! tarball's `bin/`; the rig records what `uc2-node --version` printed.
//!
//! Observation is maintainer-side and black-box: the node's control page
//! (`cnc2.dat`, the file `uc2ctl status` reads) for the leader flags, the
//! service slot's `applied`/incarnation words and the archive's first
//! base; the node's stderr log for `snapshot_installed`; the instance
//! directory listing for a complete snapshot set.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::net::{SocketAddr, TcpListener, UdpSocket};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use uc_log::cnc::{CncPage, unpack_service_status};
use uc_protocol::v2::cnc::{NODE_FLAG_CAN_SERVE, NODE_FLAG_LEADER};

use crate::adapter::Adapter;

/// A spawned child, SIGKILLed and reaped on drop (the `uc_crashtest`
/// `Reap` pattern: reassigning one IS kill + reap + respawn).
pub struct Reap(pub Child);

pub const REAP_TIMEOUT: Duration = Duration::from_secs(30);

impl Drop for Reap {
    fn drop(&mut self) {
        let _ = self.0.kill();
        if poll_exit(&mut self.0, REAP_TIMEOUT).is_none() {
            use std::io::Write;
            let _ = writeln!(
                std::io::stderr(),
                "[rig] child pid {} did not become reapable within {:?} after SIGKILL — abandoning it",
                self.0.id(),
                REAP_TIMEOUT
            );
        }
    }
}

pub fn poll_exit(child: &mut Child, timeout: Duration) -> Option<std::process::ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(Some(status)) = child.try_wait() {
            return Some(status);
        }
        if Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// `JoinHandle::join` with a deadline and a label (from `uc_crashtest`).
pub fn join_within<T>(
    handle: std::thread::JoinHandle<T>,
    label: &str,
    timeout: Duration,
) -> Result<T> {
    let started = Instant::now();
    while !handle.is_finished() {
        if started.elapsed() >= timeout {
            bail!("{label} did not finish within {timeout:?}");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    handle
        .join()
        .map_err(|e| anyhow!("{label} panicked: {}", panic_msg(&e)))
}

fn panic_msg(e: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = e.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = e.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic".into()
    }
}

#[derive(Clone)]
pub struct RigCfg {
    pub uc_bin_dir: PathBuf,
    pub service_bin: PathBuf,
    pub adapter: Arc<dyn Adapter>,
    pub root: PathBuf,
    pub n: usize,
    pub app_id: String,
    /// `[purge] below_snapshot_slack_bytes = 0` and a small journal segment,
    /// so an instant purges everything below it.
    pub purge: bool,
    pub buffer_bytes: Option<u64>,
    pub journal_segment_bytes: Option<u64>,
    /// The gateway's `request_timeout_ms`: how long a client can be pinned
    /// to a dead node before UNKNOWN frees it to re-send.
    pub gateway_request_timeout_ms: u64,
}

#[derive(Default, Debug, Clone)]
pub struct Counters {
    pub kills: u64,
    pub node_restarts: u64,
    pub restart_timeouts: u64,
    pub svc_restarts: u64,
    pub svc_exits: u64,
    pub gw_respawns: u64,
    pub node_exits: u64,
    pub wipes: u64,
    pub instants: u64,
    pub instant_failures: u64,
}

/// What `restart_service` observed at the moment of the restart, for the
/// install inference (see [`Rig::service_install_inferred`]).
#[derive(Debug, Clone, Copy)]
pub struct ServiceRestart {
    pub node: usize,
    pub first_base_at_restart: u64,
    pub incarnation_before: u32,
}

pub struct Rig {
    pub cfg: RigCfg,
    pub dirs: Vec<PathBuf>,
    pub node_addrs: Vec<SocketAddr>,
    pub gw_addrs: Vec<SocketAddr>,
    pub metrics_addrs: Vec<SocketAddr>,
    nodes: Vec<Option<Reap>>,
    svcs: Vec<Option<Reap>>,
    gws: Vec<Option<Reap>>,
    admin_key: PathBuf,
    logs: PathBuf,
    pub counters: Counters,
    pub uc_version: String,
}

fn free_udp_addr() -> Result<SocketAddr> {
    Ok(UdpSocket::bind("127.0.0.1:0")?.local_addr()?)
}

fn free_tcp_addr() -> Result<SocketAddr> {
    Ok(TcpListener::bind("127.0.0.1:0")?.local_addr()?)
}

fn log_file(path: &Path) -> Result<File> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("open log {}", path.display()))
}

impl Rig {
    pub fn start(cfg: RigCfg) -> Result<Rig> {
        let n = cfg.n;
        if n < 3 {
            bail!("a rig needs at least 3 voters (got {n})");
        }
        for b in ["uc2-node", "uc2-gateway", "uc2ctl"] {
            let p = cfg.uc_bin_dir.join(b);
            if !p.is_file() {
                bail!(
                    "{} is missing — --uc-bin-dir must be a release tarball's bin/",
                    p.display()
                );
            }
        }
        if !cfg.service_bin.is_file() {
            bail!("service binary {} is missing", cfg.service_bin.display());
        }
        let uc_version = {
            let out = Command::new(cfg.uc_bin_dir.join("uc2-node"))
                .arg("--version")
                .output()?;
            String::from_utf8_lossy(&out.stdout).trim().to_string()
        };
        fs::create_dir_all(&cfg.root)?;
        let logs = cfg.root.join("logs");
        fs::create_dir_all(&logs)?;
        let admin_key = cfg.root.join("adjudicate.key");
        if !admin_key.exists() {
            let out = Command::new(cfg.uc_bin_dir.join("uc2ctl"))
                .arg("gen-admin-key")
                .arg(&admin_key)
                .output()?;
            if !out.status.success() {
                bail!(
                    "gen-admin-key failed: {}",
                    String::from_utf8_lossy(&out.stderr)
                );
            }
        }
        let mut dirs = Vec::new();
        let mut node_addrs = Vec::new();
        let mut gw_addrs = Vec::new();
        let mut metrics_addrs = Vec::new();
        for i in 0..n {
            dirs.push(cfg.root.join(format!("n{i}")));
            node_addrs.push(free_udp_addr()?);
            gw_addrs.push(free_tcp_addr()?);
            metrics_addrs.push(free_tcp_addr()?);
        }
        let mut rig = Rig {
            cfg,
            dirs,
            node_addrs,
            gw_addrs,
            metrics_addrs,
            nodes: (0..n).map(|_| None).collect(),
            svcs: (0..n).map(|_| None).collect(),
            gws: (0..n).map(|_| None).collect(),
            admin_key,
            logs,
            counters: Counters::default(),
            uc_version,
        };
        for i in 0..n {
            fs::create_dir_all(&rig.dirs[i])?;
            rig.write_configs(i)?;
        }
        for i in 0..n {
            rig.spawn_node(i)?;
        }
        for i in 0..n {
            rig.await_cnc(i, Duration::from_secs(20))?;
        }
        rig.await_leader(Duration::from_secs(30))?;
        for i in 0..n {
            rig.spawn_service(i)?;
        }
        for i in 0..n {
            rig.await_service_attached(i, Duration::from_secs(30))?;
        }
        for i in 0..n {
            rig.spawn_gateway(i)?;
        }
        Ok(rig)
    }

    pub fn gateways(&self) -> Vec<String> {
        self.gw_addrs.iter().map(|a| a.to_string()).collect()
    }

    pub fn admin_key(&self) -> &Path {
        &self.admin_key
    }

    fn write_configs(&self, i: usize) -> Result<()> {
        let c = &self.cfg;
        let mut node = String::new();
        node.push_str(&format!(
            "id = {i}\nbind = \"{}\"\ninstance_dir = \"{}\"\napp_id = \"{}\"\n",
            self.node_addrs[i],
            self.dirs[i].display(),
            c.app_id
        ));
        // Top-level keys BEFORE any table: a key after `[[members]]` would
        // parse as that member's field and be refused by name.
        if let Some(b) = c.buffer_bytes {
            node.push_str(&format!("buffer_bytes = {b}\n"));
        }
        if let Some(b) = c.journal_segment_bytes {
            node.push_str(&format!("journal_segment_bytes = {b}\n"));
        }
        for (j, a) in self.node_addrs.iter().enumerate() {
            node.push_str(&format!("[[members]]\nid = {j}\naddr = \"{a}\"\n"));
        }
        if c.purge {
            node.push_str("[purge]\nbelow_snapshot_slack_bytes = 0\n");
        }
        node.push_str("[crypto]\nenabled = false\n[log]\nlevel = \"info\"\n");
        node.push_str(&format!(
            "[metrics]\nbind = \"{}\"\n",
            self.metrics_addrs[i]
        ));
        node.push_str(&format!(
            "[services]\nnames = [\"{}\"]\n",
            c.adapter.fsm_name()
        ));
        node.push_str(&format!(
            "[admin]\nauth = \"hmac\"\nkeys = [{{ name = \"adjudicate\", key_path = \"{}\" }}]\n",
            self.admin_key.display()
        ));
        fs::write(self.dirs[i].join("node.toml"), node)?;

        let mut gw = String::new();
        gw.push_str(&format!(
            "[local]\ninstance_dir = \"{}\"\napp_id = \"{}\"\nlisten = \"{}\"\n",
            self.dirs[i].display(),
            c.app_id,
            self.gw_addrs[i]
        ));
        for (j, a) in self.gw_addrs.iter().enumerate() {
            gw.push_str(&format!("[[members]]\nnode_id = {j}\ngateway = \"{a}\"\n"));
        }
        gw.push_str(&format!(
            "[limits]\nrequest_timeout_ms = {}\n",
            c.gateway_request_timeout_ms
        ));
        gw.push_str(&format!(
            "[session]\nenvelope = {}\n",
            c.adapter.sessioned()
        ));
        fs::write(self.dirs[i].join("gateway.toml"), gw)?;
        Ok(())
    }

    pub fn node_log(&self, i: usize) -> PathBuf {
        self.logs.join(format!("node{i}.log"))
    }
    pub fn service_log(&self, i: usize) -> PathBuf {
        self.logs.join(format!("service{i}.log"))
    }
    pub fn gateway_log(&self, i: usize) -> PathBuf {
        self.logs.join(format!("gateway{i}.log"))
    }

    fn spawn_node(&mut self, i: usize) -> Result<()> {
        let log = log_file(&self.node_log(i))?;
        let child = Command::new(self.cfg.uc_bin_dir.join("uc2-node"))
            .arg("--config")
            .arg(self.dirs[i].join("node.toml"))
            .stdin(Stdio::null())
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .spawn()
            .context("spawn uc2-node")?;
        self.nodes[i] = Some(Reap(child));
        Ok(())
    }

    fn spawn_service(&mut self, i: usize) -> Result<()> {
        let log = log_file(&self.service_log(i))?;
        let child = Command::new(&self.cfg.service_bin)
            .arg("--instance-dir")
            .arg(&self.dirs[i])
            .arg("--app-id")
            .arg(&self.cfg.app_id)
            .args(self.cfg.adapter.service_args())
            .stdin(Stdio::null())
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .spawn()
            .with_context(|| format!("spawn service {}", self.cfg.service_bin.display()))?;
        self.svcs[i] = Some(Reap(child));
        Ok(())
    }

    fn spawn_gateway(&mut self, i: usize) -> Result<()> {
        let log = log_file(&self.gateway_log(i))?;
        let child = Command::new(self.cfg.uc_bin_dir.join("uc2-gateway"))
            .arg("--config")
            .arg(self.dirs[i].join("gateway.toml"))
            .stdin(Stdio::null())
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .spawn()
            .context("spawn uc2-gateway")?;
        self.gws[i] = Some(Reap(child));
        Ok(())
    }

    // ---------------------------------------------------------- observation

    pub fn cnc(&self, i: usize) -> Option<Arc<CncPage>> {
        CncPage::open_file(&self.dirs[i].join("cnc2.dat"), &self.cfg.app_id).ok()
    }

    fn await_cnc(&self, i: usize, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        while self.cnc(i).is_none() {
            if Instant::now() > deadline {
                bail!(
                    "node {i}: no control page within {timeout:?} (see {})",
                    self.node_log(i).display()
                );
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        Ok(())
    }

    pub fn is_serving_leader(&self, i: usize) -> bool {
        self.cnc(i)
            .map(|c| {
                c.status().flags.load_acquire() & (NODE_FLAG_LEADER | NODE_FLAG_CAN_SERVE)
                    == (NODE_FLAG_LEADER | NODE_FLAG_CAN_SERVE)
            })
            .unwrap_or(false)
    }

    pub fn find_leader(&self) -> Option<usize> {
        (0..self.cfg.n).find(|&i| self.nodes[i].is_some() && self.is_serving_leader(i))
    }

    pub fn await_leader(&self, timeout: Duration) -> Result<usize> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(l) = self.find_leader() {
                return Ok(l);
            }
            if Instant::now() > deadline {
                bail!("no serving leader within {timeout:?}");
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    pub fn instance_id(&self, i: usize) -> Option<u128> {
        self.cnc(i).and_then(|c| c.try_instance_id())
    }

    /// Service row 0's slot: (attached, incarnation, applied).
    pub fn service_slot(&self, i: usize) -> Option<(bool, u32, u64)> {
        let c = self.cnc(i)?;
        let s = c.service_slot(0);
        let (_, attached, inc) = unpack_service_status(s.status.load_acquire());
        Some((attached, inc, s.applied.load_acquire()))
    }

    pub fn service_applied(&self, i: usize) -> u64 {
        self.service_slot(i).map(|s| s.2).unwrap_or(0)
    }

    fn await_service_attached(&self, i: usize, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if matches!(self.service_slot(i), Some((true, _, _))) {
                return Ok(());
            }
            if Instant::now() > deadline {
                bail!(
                    "node {i}: the service did not attach within {timeout:?} (see {})",
                    self.service_log(i).display()
                );
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    pub fn archive_first_base(&self, i: usize) -> u64 {
        self.cnc(i)
            .map(|c| c.archive_first_base().load_acquire())
            .unwrap_or(0)
    }

    /// The newest position P at which BOTH `snapshots/0/snap-P.ultsnap` and
    /// `snapshots/cluster/snap-P.ultcluster` exist on node `i` — the
    /// runbook's definition of a complete set.
    pub fn complete_set(&self, i: usize) -> Option<u64> {
        let row = list_positions(&self.dirs[i].join("snapshots").join("0"), ".ultsnap");
        let cluster = list_positions(
            &self.dirs[i].join("snapshots").join("cluster"),
            ".ultcluster",
        );
        row.into_iter().filter(|p| cluster.contains(p)).max()
    }

    /// Path of node `i`'s row-0 artifact at `p`.
    pub fn artifact_path(&self, i: usize, p: u64) -> PathBuf {
        self.dirs[i]
            .join("snapshots")
            .join("0")
            .join(format!("snap-{p}.ultsnap"))
    }

    pub fn node_log_len(&self, i: usize) -> u64 {
        fs::metadata(self.node_log(i)).map(|m| m.len()).unwrap_or(0)
    }

    /// Count of `snapshot_installed` records in node `i`'s log after byte
    /// `since` — the node-side install (a wiped-and-rejoined node).
    pub fn node_log_installs_since(&self, i: usize, since: u64) -> usize {
        let Ok(mut f) = File::open(self.node_log(i)) else {
            return 0;
        };
        let _ = f.seek(SeekFrom::Start(since));
        let mut s = String::new();
        let _ = f.read_to_string(&mut s);
        s.matches("\"snapshot_installed\"").count() + s.matches("snapshot_installed ").count()
    }

    // ---------------------------------------------------------------- faults

    /// SIGKILL node `i` (then its service, then its gateway), restart the
    /// node on the same directory, wait for a FRESH instance id, then
    /// restart the service and the gateway.
    pub fn kill_and_restart_node(&mut self, i: usize) -> Result<()> {
        let old = self.instance_id(i);
        self.nodes[i] = None;
        self.svcs[i] = None;
        self.gws[i] = None;
        self.counters.kills += 1;
        self.spawn_node(i)?;
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut fresh = false;
        while Instant::now() <= deadline {
            if matches!(self.instance_id(i), Some(id) if Some(id) != old) {
                fresh = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        if fresh {
            self.counters.node_restarts += 1;
        } else {
            // The fresh instance did not appear in time. Do NOT abandon the
            // slot — spawn the service and gateway anyway (the node may still
            // be coming up) so `supervise` keeps managing them, and record the
            // timeout so the outcome logic can see the topology degraded.
            self.counters.restart_timeouts += 1;
        }
        self.spawn_service(i)?;
        self.spawn_gateway(i)?;
        Ok(())
    }

    /// SIGKILL the service on node `i` and start a fresh one against the
    /// same (untouched) node; returns what the inference needs.
    pub fn restart_service(&mut self, i: usize) -> Result<ServiceRestart> {
        let (_, inc, _) = self.service_slot(i).unwrap_or((false, 0, 0));
        let first_base = self.archive_first_base(i);
        self.svcs[i] = None;
        self.counters.svc_restarts += 1;
        self.spawn_service(i)?;
        Ok(ServiceRestart {
            node: i,
            first_base_at_restart: first_base,
            incarnation_before: inc,
        })
    }

    /// The service-side install, INFERRED: the journal on node `i` began
    /// at `first_base > 0` when a fresh (empty, position-0) service was
    /// started, and that service has since applied to at or beyond it.
    /// The purged prefix is gone, so the only way there is an artifact.
    /// Returns `Some(applied)` once the inference holds, `None` while it
    /// does not (yet).
    pub fn service_install_inferred(&self, r: &ServiceRestart) -> Option<u64> {
        if r.first_base_at_restart == 0 {
            return None;
        }
        let (attached, inc, applied) = self.service_slot(r.node)?;
        if attached && inc != r.incarnation_before && applied >= r.first_base_at_restart {
            Some(applied)
        } else {
            None
        }
    }

    /// Stop node `i` and everything on it, delete its instance directory,
    /// and start it again empty: it must rejoin through a snapshot session.
    /// Returns the node-log offset to count `snapshot_installed` from.
    pub fn wipe_and_rejoin(&mut self, i: usize) -> Result<u64> {
        self.gws[i] = None;
        self.svcs[i] = None;
        self.nodes[i] = None;
        let mark = self.node_log_len(i);
        fs::remove_dir_all(&self.dirs[i])
            .with_context(|| format!("wipe {}", self.dirs[i].display()))?;
        fs::create_dir_all(&self.dirs[i])?;
        self.write_configs(i)?;
        self.counters.wipes += 1;
        self.spawn_node(i)?;
        self.await_cnc(i, Duration::from_secs(20))?;
        self.spawn_service(i)?;
        self.spawn_gateway(i)?;
        Ok(mark)
    }

    /// `uc2ctl snapshot` against the current leader. Returns the instant's
    /// position as printed.
    pub fn command_instant(&mut self) -> Result<u64> {
        let leader = self
            .find_leader()
            .ok_or_else(|| anyhow!("no leader to command an instant on"))?;
        let out = Command::new(self.cfg.uc_bin_dir.join("uc2ctl"))
            .arg("snapshot")
            .arg("--instance-dir")
            .arg(&self.dirs[leader])
            .arg("--app-id")
            .arg(&self.cfg.app_id)
            .arg("--admin-key")
            .arg(&self.admin_key)
            .output()?;
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        if !out.status.success() {
            self.counters.instant_failures += 1;
            bail!("uc2ctl snapshot on node {leader} failed: {}", text.trim());
        }
        match parse_instant(&text) {
            Some(p) => {
                self.counters.instants += 1;
                Ok(p)
            }
            None => {
                self.counters.instant_failures += 1;
                bail!(
                    "uc2ctl snapshot printed no instant position: {}",
                    text.trim()
                )
            }
        }
    }

    /// Respawn any gateway or service that exited on its own (a gateway
    /// exits when its node's instance restarts under it; a service exits
    /// 1 on a fail-stop). A node that exits is counted, not respawned —
    /// that is a finding.
    pub fn supervise(&mut self) -> Result<()> {
        for i in 0..self.cfg.n {
            if let Some(r) = self.gws[i].as_mut()
                && let Ok(Some(_)) = r.0.try_wait()
            {
                self.gws[i] = None;
                self.counters.gw_respawns += 1;
                if self.nodes[i].is_some() {
                    self.spawn_gateway(i)?;
                }
            }
            if let Some(r) = self.svcs[i].as_mut()
                && let Ok(Some(_)) = r.0.try_wait()
            {
                self.svcs[i] = None;
                self.counters.svc_exits += 1;
                if self.nodes[i].is_some() {
                    self.spawn_service(i)?;
                }
            }
            if let Some(r) = self.nodes[i].as_mut()
                && let Ok(Some(st)) = r.0.try_wait()
            {
                self.counters.node_exits += 1;
                eprintln!(
                    "[rig] node {i} exited on its own: {st} (see {})",
                    self.node_log(i).display()
                );
                self.nodes[i] = None;
            }
        }
        Ok(())
    }

    pub fn live_nodes(&self) -> usize {
        self.nodes.iter().filter(|n| n.is_some()).count()
    }

    /// Kill everything, gateways first.
    pub fn stop_all(&mut self) {
        for i in 0..self.cfg.n {
            self.gws[i] = None;
        }
        for i in 0..self.cfg.n {
            self.svcs[i] = None;
        }
        for i in 0..self.cfg.n {
            self.nodes[i] = None;
        }
    }
}

impl Drop for Rig {
    fn drop(&mut self) {
        self.stop_all();
    }
}

fn list_positions(dir: &Path, suffix: &str) -> Vec<u64> {
    let Ok(rd) = fs::read_dir(dir) else {
        return Vec::new();
    };
    rd.filter_map(|e| e.ok())
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            let stem = name.strip_prefix("snap-")?.strip_suffix(suffix)?;
            stem.parse::<u64>().ok()
        })
        .collect()
}

/// `uc2ctl snapshot` prints the instant it commanded; accept `instant=<P>`
/// or, failing that, the first `position=<P>`/`instant <P>` token.
pub fn parse_instant(text: &str) -> Option<u64> {
    for key in ["instant=", "instant ", "position=", "position "] {
        if let Some(i) = text.find(key) {
            let rest = &text[i + key.len()..];
            let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
            if let Ok(p) = digits.parse::<u64>() {
                return Some(p);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn instant_parsing() {
        assert_eq!(
            parse_instant("snapshot: instant=4096 target=all\n"),
            Some(4096)
        );
        assert_eq!(parse_instant("ok position=77"), Some(77));
        assert_eq!(parse_instant("refused 48 snapshot_unsupported"), None);
    }
}
