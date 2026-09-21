mod common;
use uc_diffreplay::corpus::Corpus;
use uc_diffreplay::drive::{DriveOptions, Origin, drive, drive_with};
use uc_diffreplay::trace::EntryKind;
use uc_lincheck::register::RegisterSm;
use uc_service::{
    ApplyCtx, OutputError, RawOutputHandler, RawStateMachine, SnapshotError, SnapshotStateMachine,
};

#[test]
fn driver_replays_the_span_above_the_origin_and_projects_both_ends() {
    let inst = common::tempdir();
    let out = common::tempdir();
    let (p, _) = common::build_register_history(inst.path(), "drv", 5, 3);
    let corpus = Corpus::export(inst.path(), "drv", 0, p, u64::MAX, 0, out.path()).unwrap();

    let t = drive(RegisterSm::default(), &corpus, Origin::Artifact).unwrap();

    // Origin projection: the artifact at P holds writes 0..5 → value=Some(4).
    // (`last_applied` is a position this test does not know, so only the first
    // line is asserted.)
    assert!(
        t.projection_at_origin
            .as_deref()
            .unwrap()
            .starts_with("value=Some(4)\n")
    );
    // Exactly the 3 writes above P were applied, in order, each acked.
    let msgs: Vec<_> = t
        .entries
        .iter()
        .filter(|e| matches!(e.kind, EntryKind::Message))
        .collect();
    assert_eq!(msgs.len(), 3, "{:?}", t.entries);
    assert!(msgs.windows(2).all(|w| w[0].pos < w[1].pos));
    assert!(
        t.projection_at_end
            .as_deref()
            .unwrap()
            .starts_with("value=Some(7)\n")
    );
    assert_eq!(t.origin, p);
}

#[test]
fn genesis_origin_replays_everything_from_zero() {
    let inst = common::tempdir();
    let out = common::tempdir();
    let (p, _) = common::build_register_history(inst.path(), "gen", 5, 3);
    let corpus = Corpus::export(inst.path(), "gen", 0, p, u64::MAX, 0, out.path()).unwrap();

    let t = drive(RegisterSm::default(), &corpus, Origin::Genesis).unwrap();
    assert_eq!(
        t.projection_at_origin, None,
        "genesis has nothing installed to project"
    );
    let msgs = t
        .entries
        .iter()
        .filter(|e| matches!(e.kind, EntryKind::Message))
        .count();
    assert_eq!(msgs, 8);
    assert!(
        t.projection_at_end
            .as_deref()
            .unwrap()
            .starts_with("value=Some(7)\n")
    );
}

/// A minimal SM used only to exercise the `ids_calls`/`output` carries
/// (plan B2 T6): `RegisterSm` never calls `ctx.ids()` and has no
/// `on_committed` handler wired to it, so it cannot drive these tests. One
/// `ids()` call per apply, cursor tracked from `ctx.position` — the corpus's
/// MESSAGE frames are dispatched positionally (`walk_block` never checks a
/// MESSAGE's identity), so replaying a `RegisterSm` corpus through this SM
/// from genesis is exactly as legitimate as replaying it through `RegisterSm`
/// itself.
#[derive(Default)]
struct IdMinter {
    last: Option<u64>,
}

impl RawStateMachine for IdMinter {
    const NAME: &'static str = "id_minter";

    fn apply(&mut self, ctx: &mut ApplyCtx, _cmd: &[u8], out: &mut Vec<u8>) {
        let mut ids = ctx.ids();
        let _ = ids.next();
        self.last = Some(ctx.position);
        out.clear();
    }

    fn query(&self, _q: &[u8], _out: &mut Vec<u8>) {}

    fn last_applied(&self) -> Option<u64> {
        self.last
    }
}

impl SnapshotStateMachine for IdMinter {
    type SnapshotHandle = ();

    fn freeze(&self) -> Result<((), u64), SnapshotError> {
        Ok(((), self.last.unwrap_or(0)))
    }

    fn stream_snapshot(_h: (), _dst: &mut dyn std::io::Write) -> Result<(), SnapshotError> {
        Ok(())
    }

    fn install_snapshot(
        &mut self,
        position: u64,
        src: &mut dyn std::io::Read,
    ) -> Result<u64, SnapshotError> {
        let mut buf = Vec::new();
        std::io::Read::read_to_end(src, &mut buf)?;
        self.last = Some(position.saturating_sub(1));
        Ok(position)
    }

    fn project(&self, out: &mut dyn std::io::Write) -> Result<(), SnapshotError> {
        writeln!(out, "last={:?}", self.last)?;
        Ok(())
    }
}

#[test]
fn ids_calls_is_captured_once_per_apply() {
    let inst = common::tempdir();
    let out = common::tempdir();
    let (p, _) = common::build_register_history(inst.path(), "ids", 5, 3);
    let corpus = Corpus::export(inst.path(), "ids", 0, p, u64::MAX, 0, out.path()).unwrap();

    let t = drive(IdMinter::default(), &corpus, Origin::Genesis).unwrap();
    let msgs: Vec<_> = t
        .entries
        .iter()
        .filter(|e| matches!(e.kind, EntryKind::Message))
        .collect();
    assert!(!msgs.is_empty(), "{:?}", t.entries);
    assert!(
        msgs.iter().all(|e| e.ids_calls == 1),
        "every Write calls ctx.ids() exactly once: {msgs:?}"
    );
}

/// Records call order and alternates Ok/Permanent by it — deterministic and
/// independent of the (32-byte-aligned, always-even) frame position.
#[derive(Default)]
struct AlternatingOutput {
    calls: std::sync::atomic::AtomicU32,
}

impl RawOutputHandler<IdMinter> for AlternatingOutput {
    async fn on_committed(
        &self,
        _position: u64,
        _cmd: &[u8],
        _state: &IdMinter,
    ) -> Result<(), OutputError> {
        let n = self
            .calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if n.is_multiple_of(2) {
            Ok(())
        } else {
            Err(OutputError::Permanent("boom".into()))
        }
    }
}

#[test]
fn a_recording_output_handler_is_captured_per_message_entry() {
    let inst = common::tempdir();
    let out = common::tempdir();
    let (p, _) = common::build_register_history(inst.path(), "output", 5, 3);
    let corpus = Corpus::export(inst.path(), "output", 0, p, u64::MAX, 0, out.path()).unwrap();

    let t = drive_with(
        IdMinter::default(),
        &corpus,
        Origin::Genesis,
        DriveOptions {
            output: Some(AlternatingOutput::default()),
        },
    )
    .unwrap();
    let msgs: Vec<_> = t
        .entries
        .iter()
        .filter(|e| matches!(e.kind, EntryKind::Message))
        .collect();
    assert!(!msgs.is_empty(), "{:?}", t.entries);
    let expected: Vec<String> = (0..msgs.len())
        .map(|i| {
            if i.is_multiple_of(2) {
                "ok".to_string()
            } else {
                "permanent: boom".to_string()
            }
        })
        .collect();
    let got: Vec<String> = msgs.iter().map(|e| e.output.clone().unwrap()).collect();
    assert_eq!(got, expected);
}

#[test]
fn without_a_handler_output_is_none() {
    let inst = common::tempdir();
    let out = common::tempdir();
    let (p, _) = common::build_register_history(inst.path(), "nohandler", 5, 3);
    let corpus = Corpus::export(inst.path(), "nohandler", 0, p, u64::MAX, 0, out.path()).unwrap();

    let t = drive(IdMinter::default(), &corpus, Origin::Genesis).unwrap();
    assert!(
        t.entries
            .iter()
            .filter(|e| matches!(e.kind, EntryKind::Message))
            .all(|e| e.output.is_none()),
        "{:?}",
        t.entries
    );
}
