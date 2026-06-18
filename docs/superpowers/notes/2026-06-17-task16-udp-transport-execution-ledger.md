# task16 inter-node UDP transport — progress ledger

Branch: feat/inter-node-udp-transport
Plan: docs/superpowers/plans/2026-06-17-inter-node-udp-transport.md
Base commit: dc75ff3

## Tasks
(none complete yet)

Task 1: complete (commits dc75ff3..ec096b7; refactor edits in ec096b7, renames swept into 59deef5 by concurrent external journal commit; review clean, spec ✅)
  - Minor (no action): commit history split across 59deef5/ec096b7 (bisect cosmetic).
  - FINISHING-BRANCH NOTE: 59deef5 carries UNRELATED task13 journal-fdatasync docs that landed on this feature branch via a concurrent external process. Decide at merge whether to keep/separate.
  - WATCH: an external process commits to this branch concurrently — re-check HEAD before each task; next base = ec096b7.

Task 2: complete (commit ec096b7..18cd061; review clean, spec ✅)
  - Minor (ledger): udp_tuning_defaults test asserts 2/6 fields (brief listed all 6 exact) — config.rs:189-190.
  - Minor (ledger, ACT-IF-NEEDED): UdpTuning + Transport lack PartialEq. A later task comparing Transport values (e.g. assert_eq!(cfg.transport, Transport::Udp(...))) will fail to compile — add `PartialEq` to the derive then. network/udp/mod.rs:9 + config.rs Transport.
  - next base = 18cd061.

Task 3: CODE COMPLETE (commit 85eaa87, builds clean both feature configs, suite green) — but BRANCH TOPOLOGY BROKEN, paused for user decision.
  - Reflog HEAD@{5}: external process did `checkout feat -> main`. Everything from plan commit onward landed on MAIN, not feat.
  - main (local) = origin/main(6934f88) + 7 commits, INTERLEAVED:
      4142c02 journal-fdatasync spec      [USER]
      c9f2e8d journal-fdatasync plan      [USER]
      dc75ff3 task16 UDP plan             [MINE]
      59deef5 journal-fdatasync result + Task1 RENAMES entangled [USER+MINE]
      ec096b7 Task1 edits                 [MINE]
      18cd061 Task2                       [MINE]
      85eaa87 Task3                       [MINE]
  - feat/inter-node-udp-transport = b55ae86 (SPEC ONLY, orphaned; spec FILE absent from main tree + working tree).
  - Task1 renames live in journal commit 59deef5 -> cannot cleanly cherry-pick MINE without USER's journal commit.
  - ACTIVE external writer: working tree dirty with 9 unrelated modified files (counter_loop, uc-crashtest, uc-lincheck, commit-path-load, uc_protocol/probes). Do NOT reset main / clobber tree while it runs.
  - All work safe in commits+reflog. Awaiting user choice on recovery.

=== TOPOLOGY DISENTANGLED (user chose "full disentangle now") ===
Done via plumbing (temp index) + cherry-pick; tree-verified: feat tip diff vs old 85eaa87 == ONLY the spec file (321 lines). Code state of Tasks 1-3 preserved byte-for-byte. Both builds pass.
- main (reset) = origin/main(6934f88) + 4142c02 + c9f2e8d + 55f6662  [journal-fdatasync only, NO UDP]
- feat/inter-node-udp-transport (rebuilt, branches off main tip 55f6662):
    8e335d7 spec   (was b55ae86)
    bcb48e7 plan   (was dc75ff3)
    1dc7884 Task1 renames (split out of old journal commit 59deef5)
    bc55ff2 Task1 edits  (was ec096b7)
    c40b2e6 Task2        (was 18cd061)
    514c6bd Task3        (was 85eaa87)
- merge-base(feat,main)=55f6662 → clean descendant, no journal dup at merge.
- SAFETY/RECOVERY: backup branch `backup/pre-disentangle-main-85eaa87` = full pre-surgery main; tag `backup/feat-spec-b55ae86`. Concurrent-process WIP (9 files) preserved in `git stash` ("concurrent-process WIP (based on 85eaa87)"). To restore old world: checkout backup branch + stash pop.
- NEW BASES for review/next tasks: Task3 review = c40b2e6..514c6bd. Task4 base = 514c6bd (feat tip).

Task 1: complete (now bc55ff2 on feat; review was clean spec ✅).
Task 2: complete (now c40b2e6 on feat; review clean spec ✅; Minors in ledger above).
Task 3: CODE DONE (now 514c6bd on feat; builds clean both configs) — REVIEW PENDING (interrupted by topology fix).

Task 3: complete (now 514c6bd on feat; review clean spec ✅; full uc_node suite green post-surgery).
  - Minor (ledger): builder.rs Quic arm remaps construction-failure NetworkError -> ClusterError::Config (was ClusterError::Network via #[from]). Success path identical; no test asserts on it. Consider mapping to ClusterError::Network(e) in final polish.
  - next base (Task 4) = 514c6bd (feat tip).

Task 4: complete (547ef6b + fix 1d567e6; review spec ✅; 4/4 wire tests pass; clippy clean).
  - Important FIXED: CRC-range doc comment header[..24]->full 28-byte header (multi-lang interop). Minor FIXED: redundant test import. Verified doc/import-only, no logic change.
  - next base (Task 5) = 1d567e6 (feat tip).

Task 5: complete (a4bbf74; review spec ✅; 2/2 fragment tests pass; clippy clean).
  - Minor (ledger): test doesn't assert middle-fragment flags==0 / last-no-BEGIN; redundant test import. Logic correct by analysis (boundary, degenerate-mtu .max(1), empty payload all sound).
  - next base (Task 6) = a4bbf74.

Task 6: complete (38b9ad4 + fix 95492ee; review spec ✅; 6/6 reassembly tests pass; clippy clean).
  - Important (DOCUMENTED, unreachable): malformed-BEGIN drop advances highest_contiguous past undelivered slot — only reachable for malformed sender; our fragmenter always BEGINs. Added clarifying comment per reviewer.
  - Minors FIXED: #[derive(Debug)] added; +2 tests (multi-message-drain, disjoint-gaps) — both pass, validating the trickiest logic.
  - next base (Task 7) = 95492ee.

Task 7: complete (aa7fad2 + fix 0834c1b; review spec ✅; 5/5 send_window tests pass; clippy clean).
  - Important FIXED: push no longer leaks in_flight on duplicate seq (subtract replaced entry); regression test added+passing. Minors FIXED: on_ack saturating_add(1); #[derive(Debug)].
  - PHASE B pure units (Tasks 4-7) ALL DONE. next base (Task 8) = 0834c1b.

Task 8: complete (d55c5fe; review spec ✅; 2/2 session tests incl loss-recovery-via-NAK; clippy clean).
  - Canonical surface: new/send_message/process/recv_message/SessionTx. Dropped deliver/ingress/window_cap/last_acked_to_peer.
  - Minors (ledger, later-task scope): SM arg(advertised window) ignored by sender (both default 128KiB) — true flow-control fidelity = later; NAK re-emitted per Data receipt (chatty) — suppression belongs in tick(); send_message holds state lock across tx.send_to().await (await-safe, no cross-deadlock; revisit w/ real socket Task 9).
  - PHASE B COMPLETE (Tasks 4-8). next base (Task 9) = d55c5fe.

Task 9 (mux CORE): complete (563132d + fix 816f67e; review spec ✅; 20/20 udp tests incl real-loopback rpc round-trip; clippy clean).
  - Resolved: session-id stamped-by-initiator/routed-by-wire; additive try_recv_message on session; SYNC parking_lot handler (pulled Task12 fix fwd).
  - Important FIXED: rpc removes pending entry if send_message errs (latent leak).
  - Minors (ledger): single recv-loop head-of-line across peers (v1 ok); cross-session drain only on that session's next inbound (self-corrects w/ traffic+ticker); crc32 sid unauthenticated (handshake=future); app_id accepted-not-validated (handshake=future).
  - Ticker (brief Step 6) SPLIT OUT -> Task 9b (next): tick() heartbeat/SM/NAK-retry + per-session periodic spawn. Needed for Phase D tail-loss tests.
  - next base = 816f67e.

Task 9b (ticker): complete (9b1e80b + fix 55a6617; review spec ✅; 21/21 udp tests incl tick_re_naks_open_gap; clippy clean).
  - tick(): re-NAK open gaps + re-advertise SM + heartbeat(next_send_seq); snapshot-under-lock then send. Spawned per-session ONLY on insert path.
  - Important FIXED: MissedTickBehavior::Delay (no NAK burst under starvation). Minors (ledger): heartbeat arg=0 reserved comment; detached-ticker lifetime (no eviction in v1, documented).
  - *** PHASE B COMPLETE (Tasks 4,5,6,7,8,9,9b). UDP channel fully built+tested. ***
  - PHASE C next: Task10 UdpRaftNetwork, Task11 UdpRaftNetworkFactory, Task12 spawn_udp_server+UdpTransport+wire builder Udp arm, Task13 3-node smoke+lincheck over UDP.
  - next base (Task 10) = 55a6617.

Task 10 (UdpRaftNetwork): complete (f6d8880; review spec ✅; builds both configs; clippy clean; 21 udp tests pass).
  - Mirrors quic/instance.rs exactly (signatures, rpc_err, fault gating); do_rpc mints request_id via fetch_add + checks resp msg_type.
  - Minors (ledger, no fix): request_id ownership doc-gap (correct either way — mux session demux guarantees uniqueness); source=0 default same as quic. #[allow(dead_code)] on target/new/with_fault is TEMP -> Task 11 factory wires new/with_fault; `target` only read under fault-injection (keep a targeted allow there).
  - next base (Task 11) = f6d8880.

Task 11 (UdpRaftNetworkFactory): complete (330e577; review spec ✅; builds+clippy clean BOTH configs; 21 udp tests pass).
  - Shared node-wide request_id Arc; net.into_v2() w/ RaftNetwork-as-_ import; removed dead_code from new/with_fault; target field precise cfg_attr allow.
  - Minors (no action): source=0 default = same as quic.
  - next base (Task 12) = 330e577.

Task 12 (spawn_udp_server + UdpTransport + builder Udp arm): complete (a8b66ca; review spec ✅; full uc_node suite GREEN exit 0; clippy+build clean BOTH configs).
  - ClusterTransport trait -> async fn (#[allow(async_fn_in_trait)] internal); QuicTransport async (no body change); builder BOTH arms .await; QUIC path behaviorally unchanged.
  - UdpTransport shares ONE UdpMux (mux_or_bind check-drop-await-store, no lock-across-await); spawn_udp_server real (sync handler set, dispatch mirrors quic, sentinel HandshakeAck fail-fast); server_stub.rs DELETED; mux stores recv JoinHandle + shutdown aborts (idempotent).
  - *** Transport::Udp NOW SELECTABLE END-TO-END. PHASE C glue (Tasks 10-12) done. ***
  - Minors (ledger, no action): sentinel-type comment; BoxFuture vs direct-await (design-driven).
  - next base (Task 13) = a8b66ca.

Task 13 (3-node UDP + lincheck): PARTIAL (d1d7092; Part A PASS, Part B BLOCKED — committed; build clean; working tree clean; mux.rs:135 diag was STALE from reverted prototype).
  - Part A udp_three_node: PASS — real 3-node UDP leader election + multi-MTU AppendEntries fragment/reassemble + convergence. *** STEADY-STATE UDP RAFT PROVEN. ***
  - Part B lincheck UDP: no-fault smoke PASS; leader-KILL+RESTART tests FAIL (NOT a safety violation — channel fails closed). Default QUIC lin_register 3/3 still PASS (env switch additive).
  - ROOT CAUSE (real plan gap — UDP node-restart): cluster.rs reuses SAME addr on restart →
      (1) UdpMux::shutdown aborts recv loop but socket fd NOT released (ticker tasks + factory hold Arc<UdpSocket>) → EADDRINUSE on rebind;
      (2) NO session epoch: same addr → same session_id, but restarted node resets to seq 0 → survivors' stale Reassembler drops all (reassembly.rs `if seq<next return`). Wedged.
  - => INSERT Task 13b (UDP session epoch + clean socket release), then RE-RUN Part B. Fix design:
      (a) sid = session_id_for(local,peer) XOR process_epoch (random u32 at UdpMux::bind) — initiator stamps, receiver adopts from wire; restart=new epoch=new sid=fresh session. No wire-format change.
      (b) shutdown releases socket: store per-session ticker JoinHandles in registry value; shutdown aborts recv loop + all tickers + clears registry; factory mux Arc drops when Raft drops on node.shutdown → fd freed before rebind.
  - Task 13 will be marked complete once 13b lands and Part B passes. next base = d1d7092.

Task 13b (UDP session epoch + socket release): complete (4ac5f8c; review spec ✅; INDEPENDENT UDP capstone re-run exit 0).
  - Fix A: sid = session_id_for(local,peer) ^ per-process epoch (rand at bind, initiator-only XOR, receiver adopts wire id). Restart->new epoch->fresh session.
  - Fix B: mux holds SOLE strong Arc<UdpSocket>; sessions+recv-loop use Weak (graceful None drop); shutdown take()s socket + abort+join recv loop/tickers/route tasks + clears handler/registry. fd freed before rebind; post-shutdown straggler RPCs get dead Weak (can't re-pin fd).
  - Reviewer: no Critical/Important; Minors (ledger): overstated "immediate" comment (fd release depends on recv-loop join); benign post-drain ticker leak (idle, dead Weak, until process exit).
  - Gates: udp units 21/21; udp_three_node 1/1; UDP capstone 3/3 incl leader kill+restart linearizable_under_failover (58.5s) + INDEPENDENT re-run; QUIC capstone 3/3 unaffected; clippy clean both configs.

Task 13: COMPLETE (Part A d1d7092 + Part B unblocked by 13b 4ac5f8c). UDP transport PROVEN linearizable under failover incl node restart.
  - *** PHASE C COMPLETE (Tasks 10,11,12,13,13b). UDP transport fully functional + correctness-proven. ***
  - next base (Task 14) = 4ac5f8c.

Task 14 (fault drop/delay): complete (1d05fc4 + fix 3b7bc19; review spec ✅; fault tests 7/7; builds+clippy clean BOTH configs).
  - fault.rs additive: set_loss/loss/set_delay/delay/should_drop(roll<loss); heal() now clears ALL (blocked+loss+delay). mux recv-loop drops/delays inbound segs (cfg-gated) via source+fault_table+peer_nodes(addr->node) threaded from factory set_fault_injection/new_client.
  - Minors (ledger): drop hits control segs too (realistic, commented); peer_nodes never pruned (bounded in tests).
  - next base (Task 15) = 3b7bc19.

Task 15 (partition + loss over UDP): complete (8fbb2fd; review spec ✅).
  - Partition suite UDP 3/3 (minority/leader-iso/quorum-loss) + QUIC 3/3. Lossy UDP: 629 ops all-Ok WGL-Linearizable under real 10% segment loss (NAK retransmit proven). Lossy QUIC no-op 708 ops Linearizable.
  - set_loss_all(prob) on LinCluster (every ordered pair via shared FaultTable). assert_linearizable always runs before Ok → Violation panic propagates past 3-attempt timing-retry (never masks safety). Progress + 30-op vacuity guards real.
  - *** UDP LINEARIZABLE UNDER PARTITION + PACKET LOSS. ***
  - Minors (no action): redundant wait_for_stable_leader; theoretical QUIC warmup edge (backstopped by 30-op guard).
  - next base (Task 16) = 8fbb2fd.

Task 16 (hard-crash over UDP): complete (a16d429; self-verified trivial env-match diff + build clean; UDP hard-crash PASS single-node harness; QUIC PASS). No separate reviewer (4-line pattern-mirror, test-validated).
  - *** PHASE D COMPLETE (14,15,16). UDP correctness: failover + partition + 10% loss + hard-crash all linearizable. ***
  - PHASE E next: Task17 UC_TRANSPORT in uc-node-launch + arbitrary-N script; Task18 bench-infra ansible transport+netem.
  - next base (Task 17) = a16d429.

Task 17 (UC_TRANSPORT + N-node script): complete (a053039; review spec ✅; live N=3 UDP smoke passed: leader+CSV+clean teardown). Minors (ledger): "QUIC server" doc comment now inaccurate (one-word fix, pre-existing copy); probe_csv tempfile not in cleanup (pre-existing); ${N}node_loopback config-name string (pre-existing commit-path-load).
  - next base (Task 18) = a053039.

Task 18 (bench-infra knobs): complete (d4318c9; self-verified YAML parses + var consistency + always-teardown). transport + netem_loss_pct/delay_ms/iface in group_vars; UC_TRANSPORT export + netem apply/remove(block/always) in roles/run; all 4 labeled in collect manifest. No reviewer dispatch (unexecutable infra config, established-pattern mirror, self-verified).
  - Minors (ledger): netem apply just OUTSIDE block (near-impossible leak window, del idempotent); tc qdisc add vs replace on stale qdisc; eth0 may need per-host override.
  - *** PHASE E COMPLETE (17,18). *** next base (Task 19) = d4318c9.

Task 19 (internode-rpc-bench): complete (9940336; review spec ✅; verified builds+runs both transports, bytes-dep diagnostic was STALE). bench_support shim (udp_echo_pair/quic_echo_pair → uniform EchoClient::rpc) in uc_node; open-loop CO-free driver, exact 13-col CSV.
  - A/B datapoint (loopback 64B if=8 5000/s 3s): udp-rpc p50 807µs p99 2.5ms; quic-rpc p50 634µs p99 1.36ms. UDP slower on loopback = mux in-band ticker+flow-control overhead (real transport, fair measure). Reviewer: apples-to-apples, CO-free faithful, inflight genuine.
  - Minors (no action): saturation one-slot-per-loop (same as commit-path-load, harmless if≥8); QUIC echo no finish() (matches prod); could cfg-gate bench_support.
  - *** PHASE F: Task 19 done. Only Task 20 (consolidate doc + A/B writeup) remains. ***
  - next base (Task 20) = 9940336.

Task 20 (consolidate doc + A/B): complete (8619457). docs/tasks/task16_inter_node_udp_transport.md written (stands alone). Regression: cargo test -p uc_node all green; clippy --workspace clean (our crates). Extra A/B: 1024B UDP marginally ahead (p50 0.70 vs 0.80ms) — payload-dependent; QUIC stays default; cross-host A/B deferred to operator.
  *** ALL 20 PLAN TASKS COMPLETE (1-20 + inserted 9b, 13b). FEATURE BUILT + CORRECTNESS-PROVEN + A/B-INSTRUMENTED. ***
  Next: FINAL whole-branch review, then finishing-a-development-branch.

=== FINAL WHOLE-BRANCH REVIEW (opus) ===
Verdict: Ready after fixes. QUIC-default-unchanged PASS; UDP end-to-end coherence PASS; all per-task Minors triaged DEFER (documented v1 tradeoffs).
One Important (cross-cutting, per-task reviews structurally couldn't see): send_message flow-control park was OUTSIDE the RPC timeout → silent peer w/ full window could wedge that peer's replication w/ no timeout escape. Per-peer, no safety/quorum impact.
FINAL FIX (4b463df): rpc() now wraps send_message INSIDE tokio::time::timeout (wedge → Timeout → openraft retries); pending cleaned on all paths; epoch .max(1) (never 0); node.rs "QUIC server"→"transport server" comment. 21/21 udp units + udp_three_node pass; clippy clean both configs.

*** FEATURE COMPLETE. All 22 task-units (1-20 + 9b + 13b) + final-review fix done, reviewed, green. Branch feat/inter-node-udp-transport ready for finishing. ***
Safety backups still present: backup/pre-disentangle-main-85eaa87, tag backup/feat-spec-b55ae86. Concurrent-process WIP in git stash (+ a later cargo-fmt stash). main = journal-only (origin/main + 3 journal commits, NOT pushed).

FINAL GATE: cargo test -p uc_node = 111 passed, 0 failed. Feature complete + green. Awaiting user direction on finishing (merge/push) given disentangled main + unpushed journal commits.

=== FOLLOW-ON: cross-host net-ping harness (user-requested 2026-06-18) ===
Step 1 DONE: split-role ping bin (0f72001; reviewed spec ✅). bench_support split into {udp,quic}_echo_server(listen)/_client(connect) + EchoServer::local_addr; internode-rpc-bench gains --role server|client|both (both=loopback unchanged), --listen/--connect, --mode ladder|ping. server=stderr-logs+run-until-SIGINT+stdout-CSV-clean (harness-capturable); ping=sequential single-inflight RTT. Cross-host correct (wildcard binds, host-agnostic session_id, accept-any cert). Verified 2-proc over 127.0.0.1 both transports.
  - Finding: ping mode (single-inflight) UDP FASTER than QUIC (p50 ~35 vs 49µs release) — opposite of inflight-8 ladder (QUIC faster). Latency-bound vs throughput-bound.
  - Minor (no action): ping row target_rate column meaningless (schema preserved).
REMAINING (pending user forks + provisioning go-ahead): host harness lifecycle (reuse bench-infra up/iterate/destroy + persistent UC echo responders), uc_autobench netping driver (control-side SSH, experiment matrix, no re-provision), Aeron ping baseline, the actual cross-host run.

NETPING harness forks LOCKED (2026-06-18): Aeron baseline = aeron-io/benchmarks echo (LoadTestRig/echo, continuity w/ task13); fleet = cheap 2-host (instance_type knob, DEFAULT ccx13 = 2 dedicated vCPU so Aeron busy-spin is fair-ish, not shared cx; Aeron-on-2-cores documented as indicative); build ALL host wiring now (scaffolding, validated structurally — yaml/terraform-validate/shellcheck — until a provisioning run).
Design: harness = bench-infra fleet (up-ping → experiments → destroy). NEW netping.yml playbook + netping_serve role: deploy internode-rpc-bench + start PERSISTENT responders on node0 (UC udp echo, UC quic echo, Aeron echo) until teardown. uc_autobench netping driver (control-side): reads inventory, runs matrix (transport{udp,quic,aeron} × payload × mode{ping,ladder} × netem) by SSHing node1 client → node0 server, netem per-experiment, RTT → tasks/netping/results.tsv. No re-provision between experiments. Build in 2 subagents: (A) bench-infra host wiring, (B) uc_autobench driver.

NETPING HARNESS COMPLETE (built, reviewed, structurally validated — pending a provisioning run):
  - Step1 ping bin: 0f72001 (reviewed ✅).
  - Step2 host wiring: 3f3f769 (netping.yml + netping_serve role: persistent UC udp/quic + Aeron echo responders on node0; up-ping = 2× ccx13; group_vars ports 9100/9101). Cross-piece review ✅.
  - Step3 driver: a13f5c5 (netping-sweep.sh: matrix transport×payload×netem, SSH node1 client→node0, RTT→tasks/netping/results.tsv, DRY_RUN). 
  - Fix: 1da3433 (symmetric netem both hosts; interpretable RTT).
  - Verified: --config IS a valid bin flag (driver↔bin consistent); ports/paths/node-roles consistent; dry-run expands all 36 experiments; shellcheck/yaml/terraform-validate clean.
  - ONE verify-on-provision caveat: Aeron echo launcher name + RTT output parser are PARAMETERIZED (AERON_PING_CMD/aeron_echo_launcher) + fail-fast, must be confirmed against the live aeron-io/benchmarks dist on first run.
  - Lifecycle: `make up-ping` (bench-infra, 2×ccx13) → `bash uc_autobench/scripts/netping-sweep.sh` (many experiments, no re-provision) → `make destroy`.
  - NOT YET PROVISIONED/RUN (billable, awaiting user go-ahead).

=== CROSS-HOST RUN EXECUTED (2026-06-18) — fleet provisioned, run, DESTROYED ===
2x Hetzner ccx13 over private net (enp7s0), split-role ping (node1 client -> node0 server). Commit cf670e1.
  - up-ping: terraform OK (2 nodes), ansible toolchains+build_uc+build_aeron OK; netping_serve FAILED only on Aeron echo launcher 'echo' not found -> real name is echo-server/echo-client. UC udp(9100)/quic(9101) responders came up fine before that.
  - CLEAN LAN RTT (64B single-inflight): UDP p50 305us/p99 413us vs QUIC p50 339us/p99 467us -> UDP WINS (inverts loopback small-payload result). +1ms/+5ms delay: ~even (link dominates). 1% loss: UDP ping NO ROW (single-inflight ping has no retry layer -> aborts on first loss; HARNESS gap not transport defect — transport is linearizable under 10% loss in-cluster); QUIC survives w/ severe p99 tail (28-59ms).
  - Aeron baseline DEFERRED: remote-echo-benchmarks needs >=4 isolated busy-spin cores/host; infeasible on ccx13. Launchers verified (echo-server/echo-client); group_vars aeron_echo_launcher fixed echo->echo-server. Future: ccx33.
  - Fleet DESTROYED (6 resources, billing stopped). Stale known_hosts for reused Hetzner IPs cleared. Results in bench-out/netping-crosshost.tsv (gitignored) + task16 doc §6.4.
  - HARNESS FOLLOW-UPS (not done): (1) ping mode needs per-RPC timeout+skip-and-count to measure UDP-under-loss; (2) driver connects over public ansible_host + shapes eth0 — for private-net realism use private_ip + enp7s0 (I ran manual sweep over private net instead); (3) Aeron run needs >=4-core fleet.
  GOTCHA: `cmd | tee log | tail -1` makes the bg-task exit code = tail's (0) and hides output in the task file (real log in the tee target) AND starves the Monitor watching the task file. Don't wrap long bg cmds in tail.

=== FAN-OUT (Raft-replication) MODE — built + validated (2026-06-18) ===
User asked: one node concurrent-pings 2 others, measure both, simulate Raft leader→2-follower replicate+commit.
  - Harness fixes first: c2d253c (loss-tolerant ping count-and-continue + driver private-net connect/enp7s0).
  - Bin fan-out: c5b7c80 + e503972 (all-acks max fix). --mode fanout, multi --connect (comma list), --quorum K (default floor((N+1)/2)=Raft majority: N=2→1). Sequential rounds; fire N concurrent rpcs; record K-th SUCCESS = quorum latency (PRIMARY); all-acks=slowest success (SECONDARY); per-target ok/fail; loss-tolerant (quorum-miss rounds counted, never abort; always emit row). system={transport}-fanout. Reviewed ✅ (quorum timing correct; concurrent fires; K-th-success stamp from shared t0).
  - 3-node orchestration: 4d6ec15. netping_serve responders on ALL nodes; Makefile up-fanout (node_count=3 ccx13); netping-sweep.sh EXPERIMENT=fanout (node0 leader → node1+node2 follower private IPs, --connect comma list, --quorum, netem on all 3, DRY_RUN validated). program.md updated. Aeron-fanout skipped gracefully (deferred).
  - LOCAL demo (loopback, 2 servers + 1 fanout client): K=1 p50 55us (Raft commit=faster follower), K=2 p50 59us (all-acks, ≥K=1 ✓). Correct quorum timing.
  - NOT run cross-host yet (3-node fleet, billable, awaiting go-ahead). `make up-fanout` → `EXPERIMENT=fanout QUORUM=1 bash uc_autobench/scripts/netping-sweep.sh` → `make destroy`.
  - All on feat/inter-node-udp-transport, NOT pushed.

=== CCX33 FAN-OUT CROSS-HOST RUN (2026-06-18) — provisioned, run, DESTROYED (commit a6e6c01) ===
3x ccx33 (8 dedicated vCPU). GOTCHA: terraform.tfvars was externally changed to cloud=aws (AWS provider→IMDS 404 blocked apply); worked around with dedicated fanout-hetzner.tfvars (user's tfvars untouched). Also: reused-IP stale known_hosts (ANSIBLE_HOST_KEY_CHECKING=False insufficient for CHANGED keys → ssh-keygen -R the 3 IPs + re-run ansible). Makefile: up-fanout now FANOUT_INSTANCE_TYPE-configurable.
  - FAN-OUT (node0 leader → node1+node2): CLEAN udp-fanout K=1 (commit) p50 257us BEATS quic 319us (~20%); K=2 ~even/slightly higher. +1ms/+5ms delay: ~even (link dominates). 1% loss: UDP tighter tail (p99 755us) but FEW rounds (105) = slow per-round NAK+1s-timeout recovery; QUIC many rounds (2213) but p99 28.6ms tail. (loss-tolerant ping fix worked — UDP produced rows.)
  - AERON per-link baseline: ATTEMPTED, NOT completed. Got: channels→private IPs, ALL LoadTestRig params resolved (message.rate/length/iterations/warmup/batch.size/output.file), echo-server(EchoNode) alive on follower — but cross-host awaitConnected 60s TIMEOUT. Blocker = Aeron cross-host channel/driver wiring; needs canonical remote-echo-benchmarks orchestrator (~20-var env: per-thread CPU pinning + *_DESTINATION/SOURCE_CHANNEL both ends). True Aeron QUORUM-fanout would also need custom Java (echo is point-to-point). Deferred.
  - Aeron launchers: echo-server (responder) / echo-client (LoadTestRig); channel props io.aeron.benchmarks.aeron.{destination,source}.channel (default localhost:13333/13334).
  - Results: task16 §6.5 + bench-out/netping-fanout-crosshost.tsv. Fleet DESTROYED. Verdict: keep QUIC default.
  - GOTCHA (tooling): `cmd | tee log | tail` AND `ssh '...&' run_in_background` both break (exit code / detach). Run bg cmds plainly; for held-open remote procs the tool reaps the inner & on return.

=== AERON remote-echo-benchmarks WIRED (a54b56f, DONE_WITH_CONCERNS — structural, not run) ===
uc_autobench/scripts/aeron-echo-baseline.sh: control-side driver, reads inventory (node0 client / node1 server), sets all 26 required_vars (20 CLIENT_*/SERVER_* + 6 SSH_*), ccx33 core map (1/2/3=driver conductor/sender/receiver, 4=app, 0,5,6,7 non-isolated, CPU_NODE=0), private-IP channels (dest node1:13333, source node0:13334, mtu=1408), LoadTestRig knobs (rate10k/len64/iters5/warmup2/batch1), results scp'd HDR tarball→bench-out/. program.md documents it. shellcheck clean; DRY_RUN shows all 26 vars + channels. Turnkey: `make up-fanout FANOUT_INSTANCE_TYPE=ccx33` → `bash uc_autobench/scripts/aeron-echo-baseline.sh`.
  - Upstream contract (fetched from aeron-io/benchmarks): orchestrator runs from CONTROL box, SSHes BOTH hosts via remote-benchmarks-runner; no client↔server SSH. Per-link floor only (echo is point-to-point; true quorum-fanout needs custom Java).
  - VERIFY-ON-PROVISION: (1) control↔hosts (or node0↔node1) SSH per the runner's exec path; (2) JAVA_HOME_REMOTE; (3) java drivers in dist; (4) aeron_hdr_to_csv regex vs LoadTestRig HDR; (5) UDP 13333/13334 open on private link (firewall).
