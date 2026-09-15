//! A pinned snapshot-read client: one HELLO and one QUERY on the edge it
//! dialed, never hopping to the leader. `uc_remote::RemoteClient` follows a
//! `HELLO_OK`'s leader hint on connect, which is right for a client and
//! wrong for the divergence check — a snapshot read is served by the
//! replica the edge sits on, and the check needs EACH replica's answer.
//! Speaks the framed remote protocol directly (`uc_remote::frame`/`conn`).

use std::net::{TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use uc_remote::FramedConn;
use uc_remote::frame::{
    FrameType, Header, Hello, HelloOk, HelloRefused, PROTOCOL_VERSION, ResponseMeta,
};

/// Send `payload` as a SNAPSHOT-consistency QUERY to the gateway at `addr`
/// and return the response meta plus the reply bytes.
pub fn snapshot_query(
    addr: &str,
    app_id: &str,
    payload: &[u8],
    timeout: Duration,
) -> Result<(ResponseMeta, Bytes)> {
    let deadline = Instant::now() + timeout;
    let sock = addr
        .to_socket_addrs()
        .with_context(|| format!("resolve {addr}"))?
        .next()
        .ok_or_else(|| anyhow!("{addr}: no address"))?;
    let stream =
        TcpStream::connect_timeout(&sock, timeout).with_context(|| format!("connect {addr}"))?;
    stream.set_read_timeout(Some(Duration::from_millis(100)))?;
    stream.set_nodelay(true)?;
    let mut conn = FramedConn::new(stream)?;
    let client_id = rand::random::<u64>() | 1;
    let mut hello = Vec::new();
    Hello { app_id }.encode(&mut hello);
    conn.write_frame(
        Header {
            ty: FrameType::Hello,
            flags: 0,
            version: PROTOCOL_VERSION,
            client_id,
            seq: 0,
        },
        &hello,
    )?;
    loop {
        if Instant::now() > deadline {
            bail!("{addr}: no HELLO_OK within {timeout:?}");
        }
        match conn.read_frame(timeout)? {
            None => continue,
            Some((h, body)) => match h.ty {
                FrameType::HelloOk => {
                    let _ok = HelloOk::decode(&body)?;
                    break;
                }
                FrameType::HelloRefused => {
                    let r = HelloRefused::decode(&body)?;
                    bail!("{addr}: HELLO_REFUSED reason {} {:?}", r.reason, r.detail);
                }
                other => bail!("{addr}: unexpected {other:?} before HELLO_OK"),
            },
        }
    }
    // flags 0 = snapshot consistency (FLAG_LINEARIZABLE clear).
    conn.write_frame(
        Header {
            ty: FrameType::Query,
            flags: 0,
            version: PROTOCOL_VERSION,
            client_id,
            seq: 1,
        },
        payload,
    )?;
    loop {
        if Instant::now() > deadline {
            bail!("{addr}: no RESPONSE within {timeout:?}");
        }
        match conn.read_frame(timeout)? {
            None => continue,
            Some((h, body)) => match h.ty {
                FrameType::Response if h.seq == 1 => {
                    let meta = ResponseMeta::decode(&body)?;
                    return Ok((meta, body.slice(ResponseMeta::LEN..)));
                }
                // LeaderChanged is UNSOLICITED (seq 0, pushed to every ready
                // connection on any transition) — it is not a redirect of THIS
                // query, which the edge never redirects, so absorb it.
                FrameType::Status
                | FrameType::Ping
                | FrameType::Pong
                | FrameType::LeaderChanged => continue,
                FrameType::Retry => bail!(
                    "{addr}: RETRY reason {:?} for a snapshot query",
                    body.first()
                ),
                FrameType::Unknown => bail!("{addr}: UNKNOWN for a snapshot query"),
                FrameType::Redirect => {
                    bail!(
                        "{addr}: the edge redirected a snapshot query; a query is never redirected, so this is unexpected"
                    )
                }
                other => bail!("{addr}: unexpected {other:?} while waiting for the reply"),
            },
        }
    }
}
