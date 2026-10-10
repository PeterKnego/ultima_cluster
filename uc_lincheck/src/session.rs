// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Session-guarantee checker for a MONOTONE state (a counter that only grows):
//! per session, every read must be at least the session's last acknowledged
//! write (read-your-writes) and at least its previous read (monotonic reads).
//! Linearizability is the wrong test for a deliberately weaker guarantee.

use std::collections::HashMap;

#[derive(Default)]
pub struct SessionChecker {
    floor: HashMap<u64, u64>,
    violations: Vec<String>,
}

impl SessionChecker {
    pub fn new() -> SessionChecker {
        SessionChecker::default()
    }

    /// A write this session had acknowledged left the state at `value`.
    pub fn record_write_ack(&mut self, session: u64, value: u64) {
        let f = self.floor.entry(session).or_insert(0);
        *f = (*f).max(value);
    }

    /// A read this session got returned `value`.
    pub fn record_read(&mut self, session: u64, value: u64) -> Result<(), String> {
        let f = self.floor.entry(session).or_insert(0);
        if value < *f {
            let v = format!("session {session}: read {value} below its floor {}", *f);
            self.violations.push(v.clone());
            return Err(v);
        }
        *f = value;
        Ok(())
    }

    pub fn violations(&self) -> &[String] {
        &self.violations
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_read_below_the_sessions_own_write_is_a_violation() {
        let mut c = SessionChecker::new();
        c.record_write_ack(1, 10);
        assert!(c.record_read(1, 9).is_err());
        assert!(c.record_read(1, 10).is_ok());
        assert_eq!(c.violations().len(), 1);
    }

    #[test]
    fn reads_must_not_go_backwards() {
        let mut c = SessionChecker::new();
        assert!(c.record_read(1, 7).is_ok());
        assert!(c.record_read(1, 6).is_err());
    }

    #[test]
    fn sessions_are_independent() {
        let mut c = SessionChecker::new();
        c.record_write_ack(1, 100);
        assert!(
            c.record_read(2, 5).is_ok(),
            "another session's write is not owed"
        );
    }
}
