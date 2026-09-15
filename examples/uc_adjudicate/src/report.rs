//! A tiny JSON-object writer for the harness's machine-readable result
//! lines (no serde_json in the tree; the lines are flat and short).

use std::fmt::{Display, Write};

pub struct Json {
    buf: String,
    first: bool,
}

impl Default for Json {
    fn default() -> Self {
        Self::new()
    }
}

impl Json {
    pub fn new() -> Self {
        Json {
            buf: String::from("{"),
            first: true,
        }
    }
    fn sep(&mut self) {
        if !self.first {
            self.buf.push(',');
        }
        self.first = false;
    }
    /// A number or bool (anything whose `Display` is valid JSON).
    pub fn num(mut self, k: &str, v: impl Display) -> Self {
        self.sep();
        let _ = write!(self.buf, "\"{k}\":{v}");
        self
    }
    pub fn str(mut self, k: &str, v: impl AsRef<str>) -> Self {
        self.sep();
        let _ = write!(self.buf, "\"{k}\":\"{}\"", escape(v.as_ref()));
        self
    }
    /// A pre-rendered JSON value (an array or object).
    pub fn raw(mut self, k: &str, v: &str) -> Self {
        self.sep();
        let _ = write!(self.buf, "\"{k}\":{v}");
        self
    }
    pub fn finish(mut self) -> String {
        self.buf.push('}');
        self.buf
    }
}

pub fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out
}

pub fn str_array(items: &[String]) -> String {
    let inner: Vec<String> = items.iter().map(|s| format!("\"{}\"", escape(s))).collect();
    format!("[{}]", inner.join(","))
}

/// The gate's four correctness outcomes (convention 4). The process exit
/// code follows: PASS 0, FAIL 1, NOT RUN 3 (2 is a usage or setup error).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Pass,
    Fail,
    NotRun,
}

impl Outcome {
    pub fn label(self) -> &'static str {
        match self {
            Outcome::Pass => "PASS",
            Outcome::Fail => "FAIL",
            Outcome::NotRun => "NOT RUN",
        }
    }
    pub fn exit_code(self) -> i32 {
        match self {
            Outcome::Pass => 0,
            Outcome::Fail => 1,
            Outcome::NotRun => 3,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn renders_flat_object() {
        let j = Json::new()
            .num("a", 1)
            .str("b", "x\"y")
            .raw("c", "[1,2]")
            .finish();
        assert_eq!(j, "{\"a\":1,\"b\":\"x\\\"y\",\"c\":[1,2]}");
    }
}
