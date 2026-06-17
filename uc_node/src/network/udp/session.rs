//! One reliable, ordered, fragmenting message channel to a single peer over a
//! shared UDP socket. Flow control + NAK retransmit per the Aeron-derived subset.
//!
//! The single inbound entry point is `process(seg)` (driven by the mux's
//! per-session task in a later task); `send_message` is the outbound entry
//! point; `recv_message` yields reassembled inbound messages.
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{Mutex, mpsc};

use super::reassembly::Reassembler;
use super::send_window::SendWindow;
use super::wire::{SegType, Segment, WIRE_VERSION};
use super::{UdpTuning, fragment};
use crate::network::NetworkError;

/// Outbound datagram primitive. Implemented over the shared socket in a later
/// task; a test double in unit tests.
#[async_trait::async_trait]
pub trait SessionTx: Send + Sync + 'static {
    async fn send_to(&self, datagram: Bytes);
}

struct SessionState {
    next_send_seq: u64,
    window: SendWindow,
    reasm: Reassembler,
}

pub struct UdpSession {
    session_id: u32,
    tx: Arc<dyn SessionTx>,
    tuning: UdpTuning,
    state: Mutex<SessionState>,
    inbound_tx: mpsc::UnboundedSender<Bytes>,
    inbound_rx: Mutex<mpsc::UnboundedReceiver<Bytes>>,
}

impl UdpSession {
    pub fn new(session_id: u32, tx: Arc<dyn SessionTx>, tuning: UdpTuning) -> Arc<Self> {
        let (itx, irx) = mpsc::unbounded_channel();
        Arc::new(Self {
            session_id,
            tx,
            state: Mutex::new(SessionState {
                next_send_seq: 0,
                window: SendWindow::new(tuning.flow_window_bytes),
                reasm: Reassembler::new(),
            }),
            tuning,
            inbound_tx: itx,
            inbound_rx: Mutex::new(irx),
        })
    }

    fn mk(&self, seg_type: SegType, flags: u8, seq: u64, arg: u64, payload: Bytes) -> Bytes {
        Segment {
            version: WIRE_VERSION,
            seg_type,
            flags,
            session_id: self.session_id,
            seq,
            arg,
            payload,
        }
        .encode()
        .freeze()
    }

    /// Fragment + transmit a logical message, retaining segments for retransmit.
    /// Applies flow-control back-pressure: yields until the window admits.
    pub async fn send_message(&self, payload: Bytes) -> Result<(), NetworkError> {
        let mut st = self.state.lock().await;
        let first = st.next_send_seq;
        let frags = fragment::fragment(&payload, first, self.tuning.mtu);
        for (seq, flags, chunk) in frags {
            let dg = self.mk(SegType::Data, flags, seq, 0, chunk);
            // Flow-control: wait until the window can admit this datagram.
            while !st.window.can_admit(dg.len()) {
                drop(st);
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                st = self.state.lock().await;
            }
            st.window.push(seq, dg.clone());
            st.next_send_seq = seq + 1;
            self.tx.send_to(dg).await;
        }
        Ok(())
    }

    /// Feed one inbound segment. Drives reassembly + control responses. This is
    /// the canonical inbound entry point (called by the mux's per-session task).
    pub async fn process(&self, seg: Segment) {
        match seg.seg_type {
            SegType::Data | SegType::Hello => {
                let mut st = self.state.lock().await;
                let msgs = st.reasm.accept(seg.seq, seg.flags, seg.payload);
                for m in msgs {
                    let _ = self.inbound_tx.send(m);
                }
                // Reactive NAK for any gap.
                let gaps = st.reasm.gaps();
                let hc = st.reasm.highest_contiguous();
                drop(st);
                for (start, count) in gaps {
                    let nak = self.mk(SegType::Nak, 0, start, count, Bytes::new());
                    self.tx.send_to(nak).await;
                }
                if let Some(hc) = hc {
                    let sm = self.mk(
                        SegType::Sm,
                        0,
                        hc,
                        self.tuning.flow_window_bytes,
                        Bytes::new(),
                    );
                    self.tx.send_to(sm).await;
                }
            }
            SegType::Sm => {
                let mut st = self.state.lock().await;
                st.window.on_ack(seg.seq); // seg.seq = peer highest_contiguous
            }
            SegType::Nak => {
                let st = self.state.lock().await;
                let dgs = st.window.resend(seg.seq, seg.arg);
                drop(st);
                for dg in dgs {
                    self.tx.send_to(dg).await;
                }
            }
            SegType::Heartbeat | SegType::HelloAck => { /* liveness only */ }
        }
    }

    pub async fn recv_message(&self) -> Option<Bytes> {
        self.inbound_rx.lock().await.recv().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    /// Test double: a SessionTx that pushes datagrams into a shared queue,
    /// optionally dropping a scheduled DATA seq to exercise NAK recovery.
    #[derive(Clone)]
    struct LinkTx {
        peer_inbox: Arc<Mutex<Vec<Bytes>>>,
        drop_seqs: Arc<Mutex<Vec<u64>>>,
    }

    #[async_trait::async_trait]
    impl SessionTx for LinkTx {
        async fn send_to(&self, datagram: Bytes) {
            // Inspect the seq; drop if scheduled (simulate loss).
            if let Ok(seg) = crate::network::udp::wire::Segment::decode(&datagram) {
                let mut drops = self.drop_seqs.lock().await;
                if let Some(pos) = drops.iter().position(|s| {
                    *s == seg.seq && seg.seg_type == crate::network::udp::wire::SegType::Data
                }) {
                    drops.remove(pos);
                    return; // dropped
                }
            }
            self.peer_inbox.lock().await.push(datagram);
        }
    }

    struct NullTx;
    #[async_trait::async_trait]
    impl SessionTx for NullTx {
        async fn send_to(&self, _d: Bytes) {}
    }

    fn small_mtu_tuning() -> UdpTuning {
        UdpTuning {
            mtu: super::super::wire::SEG_HEADER_LEN + 4 + 5,
            ..UdpTuning::default()
        }
    }

    #[tokio::test]
    async fn message_round_trip_no_loss() {
        let inbox = Arc::new(Mutex::new(Vec::new()));
        let tx = LinkTx {
            peer_inbox: inbox.clone(),
            drop_seqs: Arc::new(Mutex::new(vec![])),
        };
        let a = UdpSession::new(1, Arc::new(tx), UdpTuning::default());
        let b = UdpSession::new(1, Arc::new(NullTx), UdpTuning::default());

        a.send_message(Bytes::from_static(b"hello world"))
            .await
            .unwrap();
        // Deliver everything A transmitted into B.
        let dgs: Vec<Bytes> = inbox.lock().await.drain(..).collect();
        for dg in dgs {
            b.process(crate::network::udp::wire::Segment::decode(&dg).unwrap())
                .await;
        }
        let msg = b.recv_message().await.unwrap();
        assert_eq!(&msg[..], b"hello world");
    }

    #[tokio::test]
    async fn recovers_dropped_fragment_via_nak() {
        // A sends a 3-fragment message; the middle fragment (seq 1) is dropped
        // on first transmit. B NAKs; A resends; B reassembles.
        let a_inbox = Arc::new(Mutex::new(Vec::new())); // datagrams A->B
        let b_inbox = Arc::new(Mutex::new(Vec::new())); // datagrams B->A
        let a_tx = LinkTx {
            peer_inbox: a_inbox.clone(),
            drop_seqs: Arc::new(Mutex::new(vec![1])),
        };
        let b_tx = LinkTx {
            peer_inbox: b_inbox.clone(),
            drop_seqs: Arc::new(Mutex::new(vec![])),
        };
        let a = UdpSession::new(1, Arc::new(a_tx), small_mtu_tuning());
        let b = UdpSession::new(1, Arc::new(b_tx), small_mtu_tuning());

        a.send_message(Bytes::from(vec![7u8; 12])).await.unwrap(); // forces >1 fragment
        // Deliver A->B (seq 1 missing).
        let dgs: Vec<Bytes> = a_inbox.lock().await.drain(..).collect();
        for dg in dgs {
            b.process(Segment::decode(&dg).unwrap()).await;
        }
        // B's control (NAK + SM) went to b_inbox. Feed to A.
        let dgs: Vec<Bytes> = b_inbox.lock().await.drain(..).collect();
        for dg in dgs {
            a.process(Segment::decode(&dg).unwrap()).await;
        }
        // A's resend now sits in a_inbox; deliver to B.
        let dgs: Vec<Bytes> = a_inbox.lock().await.drain(..).collect();
        for dg in dgs {
            b.process(Segment::decode(&dg).unwrap()).await;
        }
        let msg = b.recv_message().await.unwrap();
        assert_eq!(msg.len(), 12);
    }
}
