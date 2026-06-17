//! Sender-side retain buffer: keeps encoded DATA datagrams until the receiver
//! acks them, enforces the flow-control byte budget, and answers retransmit
//! (NAK) lookups. This is both the retransmit store and the flow-control
//! accounting — they share one bound (Aeron's receiver window).
use std::collections::BTreeMap;

use bytes::Bytes;

pub struct SendWindow {
    capacity_bytes: u64,
    in_flight: u64,
    /// seq -> full encoded datagram (header+payload+crc), ready to re-send.
    retained: BTreeMap<u64, Bytes>,
}

impl SendWindow {
    pub fn new(capacity_bytes: u64) -> Self {
        Self {
            capacity_bytes,
            in_flight: 0,
            retained: BTreeMap::new(),
        }
    }

    pub fn in_flight_bytes(&self) -> u64 {
        self.in_flight
    }

    pub fn can_admit(&self, bytes: usize) -> bool {
        self.in_flight + bytes as u64 <= self.capacity_bytes
    }

    pub fn push(&mut self, seq: u64, encoded: Bytes) {
        self.in_flight += encoded.len() as u64;
        self.retained.insert(seq, encoded);
    }

    pub fn on_ack(&mut self, highest_contiguous: u64) {
        // Drop all seq <= highest_contiguous.
        let keep = self.retained.split_off(&(highest_contiguous + 1));
        for (_, v) in std::mem::replace(&mut self.retained, keep) {
            self.in_flight -= v.len() as u64;
        }
    }

    pub fn resend(&self, start: u64, count: u64) -> Vec<Bytes> {
        (start..start + count)
            .filter_map(|s| self.retained.get(&s).cloned())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn d(n: usize) -> Bytes {
        Bytes::from(vec![0u8; n])
    }

    #[test]
    fn admits_until_capacity() {
        let mut w = SendWindow::new(100);
        assert!(w.can_admit(60));
        w.push(0, d(60));
        assert_eq!(w.in_flight_bytes(), 60);
        assert!(w.can_admit(40));
        assert!(!w.can_admit(41));
    }

    #[test]
    fn ack_frees_capacity() {
        let mut w = SendWindow::new(100);
        w.push(0, d(40));
        w.push(1, d(40));
        assert_eq!(w.in_flight_bytes(), 80);
        w.on_ack(0); // drops seq 0
        assert_eq!(w.in_flight_bytes(), 40);
        assert!(w.can_admit(60));
    }

    #[test]
    fn resend_returns_retained_range() {
        let mut w = SendWindow::new(1000);
        w.push(5, Bytes::from_static(b"five"));
        w.push(6, Bytes::from_static(b"six"));
        w.push(7, Bytes::from_static(b"seven"));
        let r = w.resend(6, 2);
        assert_eq!(r.len(), 2);
        assert_eq!(&r[0][..], b"six");
        assert_eq!(&r[1][..], b"seven");
    }

    #[test]
    fn resend_skips_already_acked() {
        let mut w = SendWindow::new(1000);
        w.push(0, d(10));
        w.push(1, d(10));
        w.on_ack(0);
        let r = w.resend(0, 2); // 0 is gone, 1 remains
        assert_eq!(r.len(), 1);
    }
}
