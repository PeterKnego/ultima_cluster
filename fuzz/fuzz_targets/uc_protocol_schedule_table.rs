// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
#![no_main]

use libfuzzer_sys::fuzz_target;
use uc_protocol::v2::schedule::*;

// The replicated schedule table (time-and-timers plan 2): the wire body of a
// `FRAME_TYPE_SCHEDULE_TABLE` frame, which every node decodes off the log, and
// which an operator stages as a FILE in the instance directory
// (`schedules.pending`) for the leader to read back. Both are bytes the node
// did not write.
//
// Two properties:
//
//  * the codec is TOTAL and idempotent — `decode` returns `None` or a table,
//    and re-encoding a decoded table decodes back to the same table. (Byte
//    equality is deliberately NOT asserted: the two reserved header bytes are
//    ignored by the decoder, so a buffer with garbage there decodes fine and
//    re-encodes with zeros.)
//  * the recurrence arithmetic is TOTAL on every decoded rule for any `t` —
//    `next_after`, `latest_at_or_before` and `arm` must not panic, overflow or
//    divide by zero whatever the operator put in the table. It runs on the
//    consensus agent (`fire_due_timers`, boot arming), so a panic there is a
//    node fail-stop.
//
// Input layout: the first 8 bytes (zero-padded if short) are the fuzzed `t`,
// the rest is the encoded table.
fuzz_target!(|data: &[u8]| {
    let split = data.len().min(8);
    let mut t = [0u8; 8];
    t[..split].copy_from_slice(&data[..split]);
    let t_ns = u64::from_le_bytes(t);
    let buf = &data[split..];

    let Some(table) = decode_schedule_table(buf) else {
        return;
    };
    assert!(table.entries.len() <= MAX_SCHEDULE_ENTRIES);
    let mut re = Vec::new();
    encode_schedule_table(&table, &mut re);
    assert_eq!(
        re.len(),
        SCHEDULE_HEADER_LEN + table.entries.len() * SCHEDULE_ENTRY_LEN
    );
    assert_eq!(decode_schedule_table(&re), Some(table.clone()));

    for e in &table.entries {
        let next = e.rule.next_after(t_ns);
        if let Some(n) = next {
            // Strictly after, except at the very top of the range where the
            // documented saturation pins the answer to `u64::MAX`.
            assert!(
                n > t_ns || n == u64::MAX,
                "next_after must be STRICTLY after {t_ns}: {n}"
            );
        }
        let latest = e.rule.latest_at_or_before(t_ns);
        if let Some(l) = latest {
            assert!(l <= t_ns, "latest_at_or_before must be <= {t_ns}: {l}");
        }
        // Both `arm` shapes: nothing delivered yet (boot / first adoption) and
        // an already-delivered occurrence (the one-tick catch-up rule).
        let _ = e.rule.arm(None, t_ns);
        let _ = e.rule.arm(Some(t_ns), t_ns);
        let _ = e.rule.arm(latest, t_ns);
        let _ = e.rule.arm(next, t_ns);
    }
});
