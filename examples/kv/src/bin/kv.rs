//! `kv` — the command-line client. Talks to the cluster from outside, over
//! TCP, through any `uc2-gateway` (`uc_remote::RemoteClient` follows
//! redirects and re-sends across failover on its own).
//!
//! Exit codes: 0 success; 1 the request failed (no gateway, timeout, expired,
//! unknown outcome); 2 bad arguments; 3 a negative answer from the store
//! (key not found, CAS version mismatch, bad request).

use std::process::ExitCode;
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};
use kv_store::wire::{self, AppendReply, GetReply, ListReply, WriteReply};
use uc_remote::{Consistency, RemoteClient, RemoteConfig, RemoteError, RemoteResponse};

#[derive(Parser)]
#[command(
    name = "kv",
    about = "Key-value operations against a kv cluster, through its gateways"
)]
struct Args {
    /// Every gateway's address, comma-separated: `host:port[,host:port…]`.
    /// List them all; the client dials in order and follows redirects.
    #[arg(long, value_delimiter = ',', required = true)]
    gateways: Vec<String>,
    /// Application identity. Must match the gateway's and the node's.
    #[arg(long, default_value = "kv")]
    app_id: String,
    /// Budget for the request, across re-sends and reconnects (applied to
    /// the connect loop and again to the wait, so the worst case is ~2x).
    #[arg(long, default_value_t = 10)]
    timeout_secs: u64,
    /// Session identity (u64). Default: a fresh random one per process.
    ///
    /// Reusing an id across processes RE-SENDS: the new process starts at
    /// seq 1 again, so its first command is answered with the cached reply
    /// of the previous process's first command (`replayed=true`) and is NOT
    /// applied. Use it only to retry exactly one command whose answer was
    /// lost. See README § Retries.
    #[arg(long)]
    client_id: Option<u64>,
    /// Read KEY and VALUE arguments as hex, and print values as hex.
    #[arg(long)]
    hex: bool,
    #[command(subcommand)]
    cmd: Sub,
}

#[derive(Subcommand)]
enum Sub {
    /// Set KEY to VALUE. Prints the new version.
    Put { key: String, value: String },
    /// Read KEY. Prints the value and its version.
    Get {
        key: String,
        /// Go through the cluster's read barrier: the answer reflects every
        /// write acknowledged before this call. Without it the read is
        /// served from whichever replica's gateway answered, and may lag.
        #[arg(long)]
        linearizable: bool,
    },
    /// Remove KEY. Prints the version that was removed.
    Delete { key: String },
    /// Set KEY to VALUE only if its current version is --version (0 = absent).
    Cas {
        key: String,
        value: String,
        #[arg(long)]
        version: u64,
    },
    /// The replica's entry count, state digest and applied position — for
    /// comparing replicas (snapshot read, so ask each gateway in turn).
    Digest {
        #[arg(long)]
        linearizable: bool,
    },
    /// v2: append VALUE to KEY's list (creating it). Prints the list's new version and length.
    Append { key: String, value: String },
    /// v2: read KEY's whole list, oldest first, one element per line.
    List {
        key: String,
        #[arg(long)]
        linearizable: bool,
    },
}

enum Fail {
    Args(String),
    Run(String),
    /// A well-formed negative answer (exit 3).
    Negative(String),
}

fn parse_bytes(s: &str, hex: bool, what: &str) -> Result<Vec<u8>, Fail> {
    if !hex {
        return Ok(s.as_bytes().to_vec());
    }
    let s = s.trim();
    if !s.is_ascii() {
        return Err(Fail::Args(format!("{what}: non-hex characters")));
    }
    if !s.len().is_multiple_of(2) {
        return Err(Fail::Args(format!("{what}: odd-length hex")));
    }
    (0..s.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&s[i..i + 2], 16)
                .map_err(|e| Fail::Args(format!("{what}: bad hex: {e}")))
        })
        .collect()
}

fn show(bytes: &[u8], hex: bool) -> String {
    if hex {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    } else {
        String::from_utf8_lossy(bytes).into_owned()
    }
}

/// Connect, retrying while the deadline holds: a cluster still electing, or
/// gateways not yet listening, is a timing condition. A `Config` refusal is
/// not retried.
fn connect(args: &Args, deadline: Instant) -> Result<RemoteClient, Fail> {
    let cfg = RemoteConfig {
        app_id: args.app_id.clone(),
        members: args.gateways.clone(),
        client_id: args.client_id,
        request_timeout: Duration::from_secs(args.timeout_secs),
        // The service runs Sessioned and the gateway's envelope is on, so an
        // UNKNOWN outcome is safe to re-send: it comes back fresh or replayed.
        resend_on_unknown: true,
        ..Default::default()
    };
    loop {
        match RemoteClient::connect(cfg.clone()) {
            Ok(c) => return Ok(c),
            Err(RemoteError::Config(m)) => return Err(Fail::Args(m)),
            Err(e) => {
                if Instant::now() >= deadline {
                    return Err(Fail::Run(format!("cannot reach any gateway: {e}")));
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

fn remaining(deadline: Instant) -> Duration {
    deadline
        .saturating_duration_since(Instant::now())
        .max(Duration::from_secs(1))
}

fn submit(client: &RemoteClient, cmd: &[u8], deadline: Instant) -> Result<RemoteResponse, Fail> {
    let ticket = client.submit(cmd).map_err(|e| Fail::Run(e.to_string()))?;
    ticket
        .wait_timeout(remaining(deadline))
        .map_err(|e| Fail::Run(e.to_string()))
}

fn query(
    client: &RemoteClient,
    q: &[u8],
    linearizable: bool,
    deadline: Instant,
) -> Result<RemoteResponse, Fail> {
    let c = if linearizable {
        Consistency::Linearizable
    } else {
        Consistency::Snapshot
    };
    let ticket = client.query(q, c).map_err(|e| Fail::Run(e.to_string()))?;
    ticket
        .wait_timeout(remaining(deadline))
        .map_err(|e| Fail::Run(e.to_string()))
}

fn bad(reason: u8) -> Fail {
    let why = match reason {
        wire::BAD_TRUNCATED => "truncated frame",
        wire::BAD_FORMAT_VERSION => "unsupported format version",
        wire::BAD_UNKNOWN_OP => "unknown op",
        wire::BAD_KEY_LEN => "key length out of range",
        wire::BAD_VALUE_LEN => "value too long",
        wire::BAD_TRAILING => "trailing bytes",
        _ => "unknown reason",
    };
    Fail::Negative(format!("bad_request reason={reason} ({why})"))
}

fn write_outcome(resp: &RemoteResponse, what: &str) -> Result<(), Fail> {
    match wire::decode_write_reply(&resp.bytes).map_err(|e| Fail::Run(e.to_string()))? {
        WriteReply::Ok { version } => {
            println!(
                "ok {what}version={version} position={} replayed={}",
                resp.position, resp.replayed
            );
            Ok(())
        }
        WriteReply::NotFound => Err(Fail::Negative(format!(
            "not_found position={} replayed={}",
            resp.position, resp.replayed
        ))),
        WriteReply::VersionMismatch { current } => Err(Fail::Negative(format!(
            "version_mismatch current={current} position={} replayed={}",
            resp.position, resp.replayed
        ))),
        WriteReply::BadRequest(r) => Err(bad(r)),
        WriteReply::WrongShape => Err(Fail::Negative(format!(
            "wrong_shape (the key is a list) position={} replayed={}",
            resp.position, resp.replayed
        ))),
    }
}

fn run(args: &Args) -> Result<(), Fail> {
    for g in &args.gateways {
        if g.trim().is_empty() || !g.contains(':') {
            return Err(Fail::Args(format!(
                "--gateways entry {g:?} is not a host:port address"
            )));
        }
    }
    if args.timeout_secs == 0 {
        return Err(Fail::Args(
            "--timeout-secs must be greater than zero".into(),
        ));
    }
    let wire_err = |e: wire::WireError| Fail::Args(e.to_string());

    // Encode (and size-check) before dialling, so a bad argument never costs
    // a connection.
    enum Req {
        Write(Vec<u8>, &'static str),
        Get(Vec<u8>, bool),
        Digest(bool),
        Append(Vec<u8>),
        List(Vec<u8>, bool),
    }
    let req = match &args.cmd {
        Sub::Put { key, value } => {
            let k = parse_bytes(key, args.hex, "KEY")?;
            let v = parse_bytes(value, args.hex, "VALUE")?;
            Req::Write(wire::try_encode_put(&k, &v).map_err(wire_err)?, "")
        }
        Sub::Delete { key } => {
            let k = parse_bytes(key, args.hex, "KEY")?;
            Req::Write(wire::try_encode_delete(&k).map_err(wire_err)?, "deleted_")
        }
        Sub::Cas {
            key,
            value,
            version,
        } => {
            let k = parse_bytes(key, args.hex, "KEY")?;
            let v = parse_bytes(value, args.hex, "VALUE")?;
            Req::Write(
                wire::try_encode_cas(&k, *version, &v).map_err(wire_err)?,
                "",
            )
        }
        Sub::Get { key, linearizable } => {
            let k = parse_bytes(key, args.hex, "KEY")?;
            Req::Get(wire::try_encode_get(&k).map_err(wire_err)?, *linearizable)
        }
        Sub::Digest { linearizable } => Req::Digest(*linearizable),
        Sub::Append { key, value } => {
            let k = parse_bytes(key, args.hex, "KEY")?;
            let v = parse_bytes(value, args.hex, "VALUE")?;
            Req::Append(wire::try_encode_append(&k, &v).map_err(wire_err)?)
        }
        Sub::List { key, linearizable } => {
            let k = parse_bytes(key, args.hex, "KEY")?;
            Req::List(wire::try_encode_list(&k).map_err(wire_err)?, *linearizable)
        }
    };

    let deadline = Instant::now() + Duration::from_secs(args.timeout_secs);
    let client = connect(args, deadline)?;
    let result = (|| match req {
        Req::Write(frame, what) => {
            let resp = submit(&client, &frame, deadline)?;
            write_outcome(&resp, what)
        }
        Req::Get(frame, lin) => {
            let resp = query(&client, &frame, lin, deadline)?;
            match wire::decode_get_reply(&resp.bytes).map_err(|e| Fail::Run(e.to_string()))? {
                GetReply::Found { version, value } => {
                    println!("version={version} value={}", show(&value, args.hex));
                    Ok(())
                }
                GetReply::NotFound => Err(Fail::Negative("not_found".into())),
                GetReply::BadRequest(r) => Err(bad(r)),
                GetReply::WrongShape => Err(Fail::Negative(
                    "wrong_shape (the key is a list; use `list`)".into(),
                )),
            }
        }
        Req::Append(frame) => {
            let resp = submit(&client, &frame, deadline)?;
            match wire::decode_append_reply(&resp.bytes).map_err(|e| Fail::Run(e.to_string()))? {
                AppendReply::Ok { version, len } => {
                    println!(
                        "ok version={version} len={len} position={} replayed={}",
                        resp.position, resp.replayed
                    );
                    Ok(())
                }
                AppendReply::WrongShape => Err(Fail::Negative(format!(
                    "wrong_shape (the key is a value) position={} replayed={}",
                    resp.position, resp.replayed
                ))),
                AppendReply::ListFull { len } => Err(Fail::Negative(format!(
                    "list_full len={len} position={} replayed={}",
                    resp.position, resp.replayed
                ))),
                AppendReply::BadRequest(r) => Err(bad(r)),
            }
        }
        Req::List(frame, lin) => {
            let resp = query(&client, &frame, lin, deadline)?;
            match wire::decode_list_reply(&resp.bytes).map_err(|e| Fail::Run(e.to_string()))? {
                ListReply::Found { version, items } => {
                    println!("version={version} len={}", items.len());
                    for (i, it) in items.iter().enumerate() {
                        println!("[{i}] {}", show(it, args.hex));
                    }
                    Ok(())
                }
                ListReply::NotFound => Err(Fail::Negative("not_found".into())),
                ListReply::WrongShape => Err(Fail::Negative(
                    "wrong_shape (the key is a value; use `get`)".into(),
                )),
                ListReply::BadRequest(r) => Err(bad(r)),
            }
        }
        Req::Digest(lin) => {
            let resp = query(&client, &wire::encode_digest(), lin, deadline)?;
            let d = wire::decode_digest_reply(&resp.bytes).map_err(|e| Fail::Run(e.to_string()))?;
            println!(
                "count={} digest={:#018x} last_applied={} via={}",
                d.count,
                d.digest,
                d.last_applied,
                client.connected_addr().unwrap_or_default()
            );
            Ok(())
        }
    })();
    // Explicit close: the process is exiting anyway, but a reference client
    // should show the shutdown rather than rely on Drop.
    client.shutdown();
    result
}

fn main() -> ExitCode {
    let args = Args::parse();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(Fail::Run(m)) => {
            eprintln!("kv: {m}");
            ExitCode::from(1)
        }
        Err(Fail::Args(m)) => {
            eprintln!("kv: {m}");
            ExitCode::from(2)
        }
        Err(Fail::Negative(m)) => {
            println!("{m}");
            ExitCode::from(3)
        }
    }
}
