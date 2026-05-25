use crate::task::{BenchResult, OptimizationTask, TaskSpec};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

const EXTRA_CONTEXT: &str = r#"You are optimizing shared-memory lock-free ring buffers in `uc_protocol::ring::{spsc, mpsc, broadcast}`.

Invariants you MUST preserve:
- FIFO ordering for SPSC and MPSC; per-producer FIFO for MPSC.
- No loss for SPSC and MPSC; all subscribers see every message for Broadcast.
- No torn reads: producers publish length AFTER the payload is fully written; readers see len=0 → spin/yield.
- On-disk byte layout MAY be repacked, but the header still encodes:
  magic, version, capacity, producer_seq, consumer_seq (semantics preserved).

Considerations relevant to perf on x86_64 and aarch64:
- Cache line is 64B on most x86 and 128B on Apple silicon. False sharing on producer/consumer indices is a common killer.
- Memory ordering: `Acquire`/`Release` are usually sufficient on the hot path; `SeqCst` is rarely needed.
- Polling reduces latency at the cost of CPU; batching head updates reduces atomic contention at the cost of perceived latency.

Public API you MAY NOT change (verified by `ring_torture` conformance suite and by `frozen_paths`):
- `pub use ring::{SpscRing, SpscProducer, SpscConsumer, MpscRing, MpscProducer, MpscConsumer, BroadcastRing, BroadcastProducer, BroadcastConsumer}` and their constructors / send / recv methods.
- The four `pub use common::{FrameHeader, RecordHeader, RingError, RingHeader}` symbols.

You MAY rewrite the internals of these files freely:
- `uc_protocol/src/ring/spsc.rs`
- `uc_protocol/src/ring/mpsc.rs`
- `uc_protocol/src/ring/broadcast.rs`
- `uc_protocol/src/ring/common.rs`
"#;

pub struct ShmemTask {
    spec: TaskSpec,
}

impl ShmemTask {
    pub fn load() -> anyhow::Result<Self> {
        let toml = std::fs::read_to_string("uc_autobench/tasks/shmem/task.toml")?;
        Ok(Self {
            spec: TaskSpec::from_toml_str(&toml)?,
        })
    }
}

impl OptimizationTask for ShmemTask {
    fn id(&self) -> &str {
        &self.spec.task.id
    }
    fn spec(&self) -> &TaskSpec {
        &self.spec
    }
    fn read_state(&self, root: &Path) -> anyhow::Result<HashMap<PathBuf, String>> {
        let mut out = HashMap::new();
        for rel in &self.spec.contract.mutable_paths {
            out.insert(rel.clone(), std::fs::read_to_string(root.join(rel))?);
        }
        Ok(out)
    }
    fn parse_microbench(&self, stdout: &str) -> anyhow::Result<BenchResult> {
        BenchResult::from_json_line(stdout)
    }
    fn extra_prompt_context(&self) -> &str {
        EXTRA_CONTEXT
    }
}
