//! Prompt rendering: stable system prompt (cacheable) + per-iteration user message.

use crate::leaderboard::Entry;
use crate::task::{BenchResult, TaskSpec};
use std::collections::BTreeMap;
use std::fmt::Write;
use std::path::PathBuf;

pub struct PromptContext<'a> {
    pub spec: &'a TaskSpec,
    pub current_best_id: &'a str,
    pub current_best_files: &'a BTreeMap<PathBuf, String>,
    pub current_best_metrics: &'a BenchResult,
    pub diverse_leaders: Vec<Entry>,
    pub recent_rejections: Vec<String>,
    pub temperature: f32,
    pub temperature_explanation: &'a str,
}

pub fn render_system_prompt(spec: &TaskSpec, extra_context: &str) -> String {
    let mut s = String::new();
    let _ = writeln!(
        s,
        "You are a Rust systems engineer optimizing the `{}` codebase.",
        spec.task.id
    );
    let _ = writeln!(s, "\n## Contract\n");
    let _ = writeln!(s, "**Mutable paths (you may rewrite these):**");
    for p in &spec.contract.mutable_paths {
        let _ = writeln!(s, "- `{}`", p.display());
    }
    let _ = writeln!(
        s,
        "\n**Frozen paths (DO NOT touch — proposal rejected if changed):**"
    );
    for p in &spec.contract.frozen_paths {
        let _ = writeln!(s, "- `{}`", p.display());
    }
    let _ = writeln!(s, "\n## Fitness function\n");
    let _ = writeln!(
        s,
        "Primary metric: `{}` ({:?}).",
        spec.microbench.primary, spec.microbench.primary_dir
    );
    if let Some(e2e) = &spec.e2e_gate {
        let _ = writeln!(
            s,
            "End-to-end Goodhart gate: `{}` ({:?}). Variant rejected if it regresses > {}% vs current best.",
            e2e.primary,
            e2e.primary_dir,
            e2e.regress_pct.unwrap_or(0.0)
        );
    }
    let _ = writeln!(s, "\n## Task-specific context\n\n{extra_context}");
    let _ = writeln!(s, "\n## Output protocol\n");
    let _ = writeln!(
        s,
        "You will respond by calling the `propose_variant` tool exactly once with a JSON object matching its schema. Provide full file contents (not diffs) for every file you modify."
    );
    s
}

pub fn render_user_message(ctx: &PromptContext) -> String {
    let mut s = String::new();
    let primary = &ctx.spec.microbench.primary;
    let val = ctx
        .current_best_metrics
        .primary(primary)
        .unwrap_or(f64::NAN);
    let _ = writeln!(
        s,
        "## Current best — variant `{}` ({primary}={val})",
        ctx.current_best_id
    );
    for (path, content) in ctx.current_best_files {
        let _ = writeln!(s, "\n### `{}`\n```rust\n{}\n```", path.display(), content);
    }

    let _ = writeln!(s, "\n## Diverse leaders");
    for l in &ctx.diverse_leaders {
        let _ = writeln!(
            s,
            "- `{}` ({primary}={}): {}",
            l.variant_id, l.primary_metric, l.hypothesis
        );
    }

    let _ = writeln!(s, "\n## Recent rejections");
    for r in &ctx.recent_rejections {
        let _ = writeln!(s, "- {r}");
    }

    let _ = writeln!(
        s,
        "\n## Search temperature: {} — {}",
        ctx.temperature, ctx.temperature_explanation
    );
    let _ = writeln!(
        s,
        "\nPropose ONE variant. Respond by calling `propose_variant`."
    );
    s
}
