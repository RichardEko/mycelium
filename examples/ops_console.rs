//! **Ops Console** — a generic, read-only dashboard over a Mycelium node's operational endpoints.
//!
//! Point it at ANY gateway-enabled node (`http_port` set) and it surfaces the substrate's own
//! operational views in one place: `/stats` (node runtime + tripwires), `/gateway/fleet` (the
//! cluster-wide snapshot), `/gateway/diagnose` (the Legible-Emergence *fleet narrative* — "why is
//! the fleet in this state", in plain English), `/gateway/kv/keys` (the KV namespace map), and
//! `/metrics` (Prometheus). Read-only, auto-refreshing, one console for any cluster.
//!
//! This is a **dev / reference** tool, not a shipped control plane — Mycelium is a *library, not a
//! platform*. It builds nothing new; it just renders the endpoints every node already exposes. A
//! customer would fork it or point Grafana at `/metrics`.
//!
//! It runs its own tiny HTTP server (default `:8099`) that also **proxies** `/api?host=..&path=..`
//! to the target node server-side — so the browser never has to worry about CORS on `/stats`/`/fleet`.
//!
//! Run:
//!   cargo run --example ops_console                    # target defaults to 127.0.0.1:9050
//!   cargo run --example ops_console -- 127.0.0.1:8091  # or any host:port
//! Then open http://127.0.0.1:8099/  (the host box in the UI switches targets live).

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const CONSOLE_PORT: u16 = 8099;

/// Percent-decode a query-param value (the browser `encodeURIComponent`s the host, so `:` arrives
/// as `%3A`). Without this, reqwest gets an invalid URL and returns a "builder error".
fn pct_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%'
            && i + 2 < b.len()
            && let (Some(h), Some(l)) = (hex(b[i + 1]), hex(b[i + 2]))
        {
            out.push(h * 16 + l);
            i += 3;
            continue;
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}
fn hex(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

#[tokio::main]
async fn main() {
    // Default target gateway (host:port, no scheme). Override with a positional arg or the UI box.
    let default_target = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:9050".to_string());
    let client = Arc::new(
        reqwest::Client::builder()
            .timeout(Duration::from_secs(4))
            .build()
            .expect("reqwest client"),
    );

    let listener = match TcpListener::bind(("127.0.0.1", CONSOLE_PORT)).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("ops-console: cannot bind 127.0.0.1:{CONSOLE_PORT} — {e}");
            std::process::exit(1);
        }
    };
    println!("Ops Console  →  http://127.0.0.1:{CONSOLE_PORT}/");
    println!("  default target: {default_target}   (switch any time via the host box in the UI)");
    println!("  surfaces /stats · /gateway/fleet · /gateway/diagnose · /gateway/kv/keys · /metrics");

    loop {
        let (mut sock, _) = match listener.accept().await {
            Ok(x) => x,
            Err(_) => continue,
        };
        let client = Arc::clone(&client);
        let default_target = default_target.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 8192];
            let n = match sock.read(&mut buf).await {
                Ok(n) if n > 0 => n,
                _ => return,
            };
            let req = String::from_utf8_lossy(&buf[..n]);
            let line = req.lines().next().unwrap_or("");
            let target = line.split_whitespace().nth(1).unwrap_or("/");
            let (path, query) = target.split_once('?').unwrap_or((target, ""));

            let (ctype, body): (String, Vec<u8>) = if path == "/" {
                // Inject the launch target so the host box defaults to whatever we were pointed at.
                (
                    "text/html; charset=utf-8".into(),
                    include_str!("ops_console.html")
                        .replace("__DEFAULT_TARGET__", &default_target)
                        .into_bytes(),
                )
            } else if path == "/api" {
                // /api?host=<host:port>&path=<gateway-path>  (values carry no & or =)
                let mut host = default_target.clone();
                let mut gpath = "/stats".to_string();
                for kv in query.split('&') {
                    if let Some(v) = kv.strip_prefix("host=") {
                        let v = pct_decode(v);
                        if !v.is_empty() {
                            host = v;
                        }
                    } else if let Some(v) = kv.strip_prefix("path=") {
                        gpath = pct_decode(v);
                    }
                }
                let host = host
                    .trim_start_matches("http://")
                    .trim_start_matches("https://")
                    .trim_end_matches('/');
                let url = format!("http://{host}{gpath}");
                match client.get(&url).send().await {
                    Ok(r) => {
                        let ct = r
                            .headers()
                            .get(reqwest::header::CONTENT_TYPE)
                            .and_then(|h| h.to_str().ok())
                            .unwrap_or("text/plain")
                            .to_string();
                        (ct, r.bytes().await.unwrap_or_default().to_vec())
                    }
                    Err(e) => (
                        "application/json".into(),
                        format!("{{\"__ops_error__\":\"{}\"}}", e.to_string().replace('"', "'"))
                            .into_bytes(),
                    ),
                }
            } else {
                ("text/plain".into(), b"not found".to_vec())
            };

            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = sock.write_all(header.as_bytes()).await;
            let _ = sock.write_all(&body).await;
            let _ = sock.flush().await;
        });
    }
}
