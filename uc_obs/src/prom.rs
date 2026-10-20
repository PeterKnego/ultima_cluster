// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The Prometheus text-format (0.0.4) encoder helpers both daemons render
//! `/metrics` with (#173): one `# HELP`/`# TYPE` pair and one unlabeled
//! sample per family. Moved here from `uc_node::obs::metrics` so
//! `uc2-gateway` shares the encoder instead of copying it; the labeled and
//! per-service helpers stay in `uc_node`, which is their only user.

/// The `# HELP` and `# TYPE` lines that open a family.
pub fn push_family_header(out: &mut String, name: &str, help: &str, ty: &str) {
    out.push_str("# HELP ");
    out.push_str(name);
    out.push(' ');
    out.push_str(help);
    out.push('\n');
    out.push_str("# TYPE ");
    out.push_str(name);
    out.push(' ');
    out.push_str(ty);
    out.push('\n');
}

fn push_sample(out: &mut String, name: &str, value: &str) {
    out.push_str(name);
    out.push(' ');
    out.push_str(value);
    out.push('\n');
}

/// An unlabeled integer gauge.
pub fn push_gauge(out: &mut String, name: &str, help: &str, value: u64) {
    push_family_header(out, name, help, "gauge");
    push_sample(out, name, &value.to_string());
}

/// An unlabeled float gauge.
pub fn push_gauge_f64(out: &mut String, name: &str, help: &str, value: f64) {
    push_family_header(out, name, help, "gauge");
    push_sample(out, name, &value.to_string());
}

/// An unlabeled counter.
pub fn push_counter(out: &mut String, name: &str, help: &str, value: u64) {
    push_family_header(out, name, help, "counter");
    push_sample(out, name, &value.to_string());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_counter_and_a_gauge_render_their_header_and_one_sample() {
        let mut out = String::new();
        push_counter(&mut out, "x_total", "Things.", 3);
        push_gauge(&mut out, "y", "Level.", 7);
        assert_eq!(
            out,
            "# HELP x_total Things.\n# TYPE x_total counter\nx_total 3\n\
             # HELP y Level.\n# TYPE y gauge\ny 7\n"
        );
    }
}
