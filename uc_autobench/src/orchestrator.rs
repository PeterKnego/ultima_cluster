//! The orchestrator state machine. See spec §4 for the lifecycle.

use crate::leaderboard::{Entry, Leaderboard, diversity_hash};
use crate::llm::LlmClient;
use crate::outcome::{LoopEvent, Outcome};
use crate::persist::EventLog;
use crate::prompt::{PromptContext, render_system_prompt, render_user_message};
use crate::proposal::{
    StaticCheckResult, VariantProposal, apply_patch, restore_snapshot, snapshot_files,
    static_checks,
};
use crate::sandbox::{SandboxOutcome, run_subprocess};
use crate::task::{BenchResult, OptimizationTask};
use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant};

pub struct OrchestratorConfig {
    pub repo_root: PathBuf,
    pub runs_dir: PathBuf,
    pub run_id: String,
    pub git_head: String,
}

pub struct RunOutcome {
    pub iterations_run: u32,
    pub best_variant_id: Option<String>,
}

pub fn run_loop(
    task: Box<dyn OptimizationTask>,
    llm: Box<dyn LlmClient>,
    cfg: OrchestratorConfig,
) -> anyhow::Result<RunOutcome> {
    let spec = task.spec().clone();
    let run_dir = cfg.runs_dir.join(&spec.task.id).join(&cfg.run_id);
    fs::create_dir_all(&run_dir)?;
    let variants_dir = run_dir.join("variants");
    fs::create_dir_all(&variants_dir)?;

    // Run-dir metadata (spec §7.1): snapshot the task spec + git HEAD for
    // reproducibility. Written once at run start.
    fs::write(run_dir.join("task.toml.snapshot"), toml::to_string(&spec)?)?;
    fs::write(run_dir.join("git.head"), &cfg.git_head)?;

    let mut log = EventLog::open(run_dir.join("events.jsonl"))?;
    log.append(&LoopEvent::RunStarted {
        t: now_iso(),
        run_id: cfg.run_id.clone(),
        task: spec.task.id.clone(),
        git_head: cfg.git_head.clone(),
    })?;

    let mut leaderboard = Leaderboard::new(20, spec.microbench.primary_dir);
    let mut current_best: Option<Entry> = None;
    let mut recent_rejections: Vec<String> = Vec::new();
    let mut temperature: f32 = 0.4;
    let started = Instant::now();
    let wall_budget = Duration::from_secs((spec.budget.wall_clock_hours * 3600.0) as u64);
    let mut iter: u32 = 0;
    let mut plateau_count: u32 = 0;

    let sys_prompt = render_system_prompt(&spec, task.extra_prompt_context());

    while iter < spec.budget.max_iterations && started.elapsed() < wall_budget {
        // Build context. For iter 0 with no best yet, we proceed with empty current_best;
        // the stub loop test uses this path.
        let current_files = if current_best.is_some() {
            task.read_state(&cfg.repo_root).unwrap_or_default()
        } else {
            std::collections::HashMap::new()
        };
        let current_files_btree: std::collections::BTreeMap<_, _> =
            current_files.into_iter().collect();
        let dummy_metrics = BenchResult::default();
        let leaders = current_best
            .as_ref()
            .map(|b| leaderboard.diverse_pick(&b.variant_id, 2))
            .unwrap_or_default();
        let ctx = PromptContext {
            spec: &spec,
            current_best_id: current_best
                .as_ref()
                .map(|e| e.variant_id.as_str())
                .unwrap_or("(none)"),
            current_best_files: &current_files_btree,
            current_best_metrics: &dummy_metrics,
            diverse_leaders: leaders,
            recent_rejections: recent_rejections.iter().rev().take(5).cloned().collect(),
            temperature,
            temperature_explanation: if plateau_count > 0 {
                "plateau"
            } else {
                "default"
            },
        };
        let user_msg = render_user_message(&ctx);

        let proposal = match llm.propose(&sys_prompt, &user_msg, temperature) {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(error=%e, "LLM error, ending run");
                break;
            }
        };

        let variant_id = format!("{:04}-{}", iter, slugify(&proposal.hypothesis));
        let variant_dir = variants_dir.join(&variant_id);
        fs::create_dir_all(&variant_dir)?;
        fs::create_dir_all(variant_dir.join("logs"))?;
        let proposal_path = variant_dir.join("proposal.json");
        fs::write(&proposal_path, serde_json::to_string_pretty(&proposal)?)?;

        log.append(&LoopEvent::VariantProposed {
            t: now_iso(),
            variant: variant_id.clone(),
            proposal_path: proposal_path.clone(),
        })?;

        let outcome = run_one_variant(
            &proposal,
            &variant_id,
            &variant_dir,
            &spec,
            &cfg,
            &mut log,
            &*task,
        )?;

        // Capture the prior best *before* we mutate it so the OutcomeRecorded
        // event records the predecessor, not the freshly-promoted variant.
        let prev_best = current_best.as_ref().map(|e| e.variant_id.clone());

        // Promotion / leaderboard update.
        if let Outcome::Promoted { microbench, .. } = &outcome {
            let primary = microbench
                .primary(&spec.microbench.primary)
                .unwrap_or(f64::NAN);
            let src = proposal.files.values().next().cloned().unwrap_or_default();
            let entry = Entry {
                variant_id: variant_id.clone(),
                primary_metric: primary,
                diversity_tag: diversity_hash(&src),
                hypothesis: proposal.hypothesis.clone(),
            };
            leaderboard.insert(entry.clone());
            current_best = Some(entry);
            plateau_count = 0;
        } else {
            plateau_count += 1;
            if let Outcome::BenchRegression { metrics } = &outcome {
                let primary = metrics
                    .primary(&spec.microbench.primary)
                    .unwrap_or(f64::NAN);
                let src = proposal.files.values().next().cloned().unwrap_or_default();
                leaderboard.insert(Entry {
                    variant_id: variant_id.clone(),
                    primary_metric: primary,
                    diversity_tag: diversity_hash(&src),
                    hypothesis: proposal.hypothesis.clone(),
                });
            }
            let tag = match &outcome {
                Outcome::StaticReject { reason } => {
                    format!("#{variant_id} STATIC_REJECT: {reason}")
                }
                Outcome::TestFail { reason } => format!("#{variant_id} TEST_FAIL: {reason}"),
                Outcome::BenchRegression { .. } => format!("#{variant_id} BENCH_REGRESSION"),
                Outcome::GoodhartReject { regress_pct, .. } => {
                    format!("#{variant_id} GOODHART: e2e regressed {regress_pct}%")
                }
                _ => format!("#{variant_id} other"),
            };
            recent_rejections.push(tag);
        }

        log.append(&LoopEvent::OutcomeRecorded {
            t: now_iso(),
            variant: variant_id.clone(),
            outcome: outcome.clone(),
            prev_best,
        })?;
        fs::write(
            variant_dir.join("outcome.json"),
            serde_json::to_string_pretty(&outcome)?,
        )?;

        // Rewrite summary.md every iteration (spec §7.4). Best-effort: a write
        // error here doesn't abort the loop, just logs.
        if let Err(e) = write_summary(
            &run_dir,
            &spec,
            iter + 1,
            &current_best,
            &recent_rejections,
            &leaderboard,
        ) {
            tracing::warn!(error=%e, "failed to write summary.md");
        }

        // Temperature schedule (spec §4.3): 0.4 default; 0.7 after
        // `plateau_window` stale iters; 0.9 after `2 * plateau_window`. Use u64
        // for the doubled threshold so a large `plateau_window` (u32) can't
        // overflow.
        let window = spec.budget.plateau_window as u64;
        let stale = plateau_count as u64;
        let new_temp = if stale >= window.saturating_mul(2) && temperature < 0.9 {
            Some(0.9_f32)
        } else if stale >= window && temperature < 0.7 {
            Some(0.7_f32)
        } else {
            None
        };
        if let Some(new_temp) = new_temp {
            temperature = new_temp;
            log.append(&LoopEvent::PlateauTemperature {
                t: now_iso(),
                new_temp,
                reason: format!("{plateau_count} iters without improvement"),
            })?;
        }

        iter += 1;
    }

    log.append(&LoopEvent::RunEnded {
        t: now_iso(),
        reason: if iter >= spec.budget.max_iterations {
            "max_iterations".into()
        } else {
            "wall_clock_or_error".into()
        },
        best: current_best.as_ref().map(|e| e.variant_id.clone()),
    })?;

    Ok(RunOutcome {
        iterations_run: iter,
        best_variant_id: current_best.map(|e| e.variant_id),
    })
}

#[allow(clippy::too_many_arguments)]
fn run_one_variant(
    proposal: &VariantProposal,
    variant_id: &str,
    variant_dir: &std::path::Path,
    spec: &crate::task::TaskSpec,
    cfg: &OrchestratorConfig,
    log: &mut EventLog,
    task: &dyn OptimizationTask,
) -> anyhow::Result<Outcome> {
    // 1. Static checks.
    let sc = static_checks(
        proposal,
        &spec.contract.mutable_paths,
        &spec.contract.frozen_paths,
    );
    if let StaticCheckResult::Reject { reason } = sc {
        log.append(&LoopEvent::StaticCheck {
            t: now_iso(),
            variant: variant_id.into(),
            ok: false,
        })?;
        return Ok(Outcome::StaticReject { reason });
    }
    log.append(&LoopEvent::StaticCheck {
        t: now_iso(),
        variant: variant_id.into(),
        ok: true,
    })?;

    // 2. Snapshot + apply.
    //
    // Hardening (T9 review): `restore_snapshot` only reverts paths captured in
    // the snapshot, but `apply_patch` writes every path in `proposal.files`. To
    // guarantee the ALWAYS-restore contract reverts files the proposal *creates*
    // (not just declared mutable paths), snapshot the UNION of
    // `spec.contract.mutable_paths` and `proposal.files.keys()`.
    let mut snap_paths = spec.contract.mutable_paths.clone();
    let already: HashSet<&std::path::Path> = spec
        .contract
        .mutable_paths
        .iter()
        .map(|p| p.as_path())
        .collect();
    for p in proposal.files.keys() {
        if !already.contains(p.as_path()) {
            snap_paths.push(p.clone());
        }
    }
    let snap = snapshot_files(&cfg.repo_root, &snap_paths)?;

    // Best-effort: persist each subprocess's output to `logs/<name>.log` for
    // debugging the real shmem run (spec §7.1 directory layout). Failures here
    // are non-fatal, like the summary.md write.
    let logs_dir = variant_dir.join("logs");
    let write_log = |name: &str, outcome: &SandboxOutcome| {
        let body = match outcome {
            SandboxOutcome::Completed { stdout, stderr, .. } => {
                format!("{stdout}\n--- stderr ---\n{stderr}")
            }
            SandboxOutcome::TimedOut { duration } => {
                format!("timed out after {duration:?}")
            }
        };
        if let Err(e) = fs::write(logs_dir.join(format!("{name}.log")), body) {
            tracing::warn!(error=%e, name, "failed to persist subprocess log");
        }
    };

    let result = (|| -> anyhow::Result<Outcome> {
        // Apply the patch INSIDE the restore-bearing closure: if apply_patch
        // fails partway (some files written, then an fs error), the `?` here
        // still leaves the unconditional `restore_snapshot` below to revert the
        // partial application.
        apply_patch(&cfg.repo_root, proposal)?;

        // 3. Correctness gate.
        let gate_start = Instant::now();
        for (name, gate_cmd) in [
            ("cargo-test", &spec.gates.test_cmd),
            ("ring-torture", &spec.gates.torture_cmd),
        ] {
            let r = run_subprocess(gate_cmd, Duration::from_secs(spec.gates.test_timeout_s))?;
            write_log(name, &r);
            let ok = matches!(&r, SandboxOutcome::Completed { exit_code: 0, .. });
            if !ok {
                let reason = match r {
                    SandboxOutcome::Completed {
                        exit_code, stderr, ..
                    } => format!(
                        "`{gate_cmd}` exit {exit_code}: {}",
                        stderr.chars().take(500).collect::<String>()
                    ),
                    SandboxOutcome::TimedOut { duration } => {
                        format!("`{gate_cmd}` timed out after {duration:?}")
                    }
                };
                log.append(&LoopEvent::CorrectnessGate {
                    t: now_iso(),
                    variant: variant_id.into(),
                    ok: false,
                    duration_ms: gate_start.elapsed().as_millis() as u64,
                })?;
                return Ok(Outcome::TestFail { reason });
            }
        }
        log.append(&LoopEvent::CorrectnessGate {
            t: now_iso(),
            variant: variant_id.into(),
            ok: true,
            duration_ms: gate_start.elapsed().as_millis() as u64,
        })?;

        // 4. Microbench.
        let bench = run_subprocess(
            &spec.microbench.cmd,
            Duration::from_secs(spec.gates.test_timeout_s),
        )?;
        write_log("microbench", &bench);
        let (stdout, _) = match bench {
            SandboxOutcome::Completed {
                exit_code: 0,
                stdout,
                stderr,
                ..
            } => (stdout, stderr),
            SandboxOutcome::Completed {
                exit_code, stderr, ..
            } => {
                log.append(&LoopEvent::Microbench {
                    t: now_iso(),
                    variant: variant_id.into(),
                    ok: false,
                    metrics: None,
                })?;
                return Ok(Outcome::TestFail {
                    reason: format!("microbench exit {exit_code}: {stderr}"),
                });
            }
            SandboxOutcome::TimedOut { duration } => {
                log.append(&LoopEvent::Microbench {
                    t: now_iso(),
                    variant: variant_id.into(),
                    ok: false,
                    metrics: None,
                })?;
                return Ok(Outcome::TestFail {
                    reason: format!("microbench timeout after {duration:?}"),
                });
            }
        };
        // Pick the JSON line (last non-empty).
        let json_line = stdout
            .lines()
            .rev()
            .find(|l| l.trim().starts_with('{'))
            .unwrap_or("{}");
        let microbench = task.parse_microbench(json_line)?;
        log.append(&LoopEvent::Microbench {
            t: now_iso(),
            variant: variant_id.into(),
            ok: true,
            metrics: Some(microbench.clone()),
        })?;

        // 5. Goodhart gate + promotion.
        // For v1 stub purposes: any successful microbench is promoted (the stub
        // task has no e2e gate and no current-best comparison). Real promotion
        // logic per spec §4 will be filled in when the shmem task wires up.
        Ok(Outcome::Promoted {
            microbench,
            e2e: None,
        })
    })();

    // Always restore.
    restore_snapshot(&cfg.repo_root, &snap)?;

    result
}

fn slugify(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .chars()
        .take(40)
        .collect()
}

fn now_iso() -> String {
    jiff::Timestamp::now().to_string()
}

fn write_summary(
    run_dir: &std::path::Path,
    spec: &crate::task::TaskSpec,
    iters_completed: u32,
    current_best: &Option<Entry>,
    recent_rejections: &[String],
    leaderboard: &Leaderboard,
) -> anyhow::Result<()> {
    use std::fmt::Write;
    let mut s = String::new();
    let _ = writeln!(s, "# {} — run summary", spec.task.id);
    let _ = writeln!(
        s,
        "\n**Iterations completed:** {iters_completed} / {}",
        spec.budget.max_iterations
    );
    match current_best {
        Some(b) => {
            let _ = writeln!(
                s,
                "\n**Current best:** `{}` — {} = {}",
                b.variant_id, spec.microbench.primary, b.primary_metric
            );
            let _ = writeln!(s, "\n**Best hypothesis:** {}", b.hypothesis);
        }
        None => {
            let _ = writeln!(s, "\n**Current best:** (none yet)");
        }
    }
    let _ = writeln!(s, "\n## Top {} leaderboard", leaderboard.entries().len());
    let _ = writeln!(
        s,
        "\n| Variant | {} | Hypothesis |",
        spec.microbench.primary
    );
    let _ = writeln!(s, "|---|---|---|");
    for e in leaderboard.entries() {
        let h: String = e.hypothesis.chars().take(80).collect();
        let _ = writeln!(s, "| `{}` | {} | {} |", e.variant_id, e.primary_metric, h);
    }
    let _ = writeln!(s, "\n## Recent rejections (last 10)");
    for r in recent_rejections.iter().rev().take(10) {
        let _ = writeln!(s, "- {r}");
    }
    std::fs::write(run_dir.join("summary.md"), s)?;
    Ok(())
}
