//! Investigation-only: dump raw per-sample latencies so a percentile tail can
//! be correlated against a `perf` trace by ordinal/timestamp. Not used by the
//! autoresearch loop; gated behind an env var at the call site.

use std::io::Write;
use std::path::{Path, PathBuf};

pub fn dump_samples(path: &Path, samples: &[f64]) -> std::io::Result<()> {
    let mut f = std::io::BufWriter::new(std::fs::File::create(path)?);
    for s in samples {
        writeln!(f, "{s}")?;
    }
    f.flush()
}

pub fn dump_path_from_env(key: &str) -> Option<PathBuf> {
    match std::env::var(key) {
        Ok(v) if !v.is_empty() => Some(PathBuf::from(v)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dump_writes_one_line_per_sample_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("s.txt");
        dump_samples(&p, &[10.0, 20.5, 30.0]).unwrap();
        let body = std::fs::read_to_string(&p).unwrap();
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines, ["10", "20.5", "30"]);
    }

    #[test]
    fn env_path_none_when_unset_or_empty() {
        // A key that is not set returns None.
        assert!(dump_path_from_env("UC_DUMP_DEFINITELY_UNSET_KEY").is_none());
    }
}
