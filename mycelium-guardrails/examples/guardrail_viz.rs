//! # `guardrail_viz` — watch an agent get structurally stopped, with cryptographic proof.
//!
//! The browser version of [`guardrail_wedge`]. A small microgrid co-op fleet: a **provider** node
//! serves a governed tool (`agent.tool.invoke`) behind a **Tier-C `authorized_callers`** gate; an
//! **authorized** agent is on the allowlist, an **unauthorized** one is not; a neutral **observer**
//! holds no special role. Fire invocations from the dashboard and watch, live:
//! - the **authorized** call admitted (the tool runs), the **unauthorized** call *structurally
//!   stopped at the gate* — refused, not merely failed;
//! - each denial **tamper-evidently sealed** into the provider's Ed25519-signed audit chain, and the
//!   **observer** — any node, holding no role — reconstructing that chain and proving the stop.
//!
//! Honest framing (binding #3): the proof attests the provider *sealed stopping* the caller —
//! *provable-stopping*, not a global "could not have done Y anywhere" claim (the chain is per-node).
//! tls is required so the sealed principal is the signature-verified caller. Because the nodes run
//! `compliance`, the provider's gateway also exposes `/gateway/audit` — so the **Ops Console Audit
//! tab** (point it at `:9096`) shows the very same denial seals.
//!
//! ```text
//! cargo run -p mycelium-guardrails --example guardrail_viz --features compliance,gateway,metrics-export  # → :8096
//! ```
#![allow(clippy::field_reassign_with_default)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use mycelium::{GossipAgent, GossipConfig, NodeId, TlsConfig};
use mycelium_guardrails::{apply, guarded_rpc_serve, prove_denials, Policy};

const KIND: &str = "agent.tool.invoke";
const HTTP_PORT: u16 = 8096;
/// The Mycelium gateway port on the provider node — target it with the Ops Console.
const OPS_PORT: u16 = 9096;
/// The Mycelium concepts + services this demo exercises — injected into the dashboard's "what you're
/// seeing" box (the UI-example contract; see docs/wiki/dev/ui-example-contract.md). `tag` is a
/// layer/service key the shared panel colour-codes (I·II·III·IV · companion · gateway · audit).
const CONCEPTS: &str = r#"[
  {"tag":"IV","name":"capability gate","gloss":"the governed tool served behind a Tier-C authorized_callers gate"},
  {"tag":"security","name":"guardrails","gloss":"three policy tiers — soft-warn → hard-prevent (provider-rejected)"},
  {"tag":"audit","name":"audit chain","gloss":"Ed25519-signed, hash-linked denial seals — /gateway/audit"},
  {"tag":"security","name":"TLS identity","gloss":"the sealed principal is the signature-verified caller"},
  {"tag":"gateway","name":"gateway + metrics","gloss":"/stats · /gateway/audit · /metrics — this Ops Console"}
]"#;

// ── the tls mesh (from guardrail_wedge) ─────────────────────────────────────────

fn free_port() -> u16 {
    mycelium::test_util::alloc_port()
}

async fn try_start(
    port: u16,
    boot: Vec<u16>,
    cert_dir: &std::path::Path,
    http_port: Option<u16>,
) -> Option<Arc<GossipAgent>> {
    let mut cfg = GossipConfig::default();
    cfg.cluster_name = Some(std::env::var("GOSSIP_CLUSTER_NAME").unwrap_or_else(|_| "guardrails".to_string()));
    cfg.bind_port = port;
    cfg.http_port = http_port;
    cfg.bootstrap_peers = boot.into_iter().map(|p| NodeId::new("127.0.0.1", p).unwrap()).collect();
    cfg.reconnect_backoff_secs = 1;
    cfg.health_check_interval_secs = 1;
    cfg.tls = Some(TlsConfig { auto_cert_dir: cert_dir.to_path_buf(), ..TlsConfig::default() });
    let agent = Arc::new(GossipAgent::new(NodeId::new("127.0.0.1", port).unwrap(), cfg));
    agent.start().await.ok().map(|_| agent)
}

/// `n` mutually-bootstrapped tls agents (shared CA). Node 0 is the provider and carries the gateway.
async fn start_mesh(n: usize, cert_dir: &std::path::Path) -> Vec<Arc<GossipAgent>> {
    for _ in 0..16 {
        let ports: Vec<u16> = (0..n).map(|_| free_port()).collect();
        let mut agents = Vec::with_capacity(n);
        let mut ok = true;
        for (i, &p) in ports.iter().enumerate() {
            let boot = if i == 0 { vec![] } else { vec![ports[0]] };
            let hp = if i == 0 { Some(OPS_PORT) } else { None };
            match try_start(p, boot, cert_dir, hp).await {
                Some(a) => agents.push(a),
                None => {
                    ok = false;
                    break;
                }
            }
        }
        if ok {
            return agents;
        }
        for a in agents {
            a.shutdown_with_timeout(Duration::from_secs(5)).await;
        }
    }
    panic!("could not bind a {n}-agent tls mesh after 16 attempts");
}

async fn poll_until(mut cond: impl FnMut() -> bool, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    cond()
}

// ── the guardrail fleet + dashboard state ───────────────────────────────────────

struct Ctx {
    authorized:   Arc<GossipAgent>,
    unauthorized: Arc<GossipAgent>,
    observer:     Arc<GossipAgent>,
    provider_id:  NodeId, // the provider agent stays alive in `agents`; we only need its id here
    policy:       Vec<(String, String, String)>, // (clause name, tier label, detail)
}

struct Action {
    id:       u64,
    role:     &'static str,
    emoji:    &'static str,
    caller:   String,
    admitted: bool,
    detail:   String,
}

#[derive(Default)]
struct VizState {
    log:  Vec<Action>,
    next: u64,
}

fn hex12(b: &[u8; 32]) -> String {
    b.iter().take(6).map(|x| format!("{x:02x}")).collect()
}

fn esc(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"").replace(['\n', '\t'], " ")
}

fn short(id: &str) -> String {
    id.rsplit(':').next().map(|p| format!(":{p}")).unwrap_or_else(|| id.to_string())
}

fn pct_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' if i + 2 < b.len() => {
                if let Some(v) = std::str::from_utf8(&b[i + 1..i + 3])
                    .ok()
                    .and_then(|h| u8::from_str_radix(h, 16).ok())
                {
                    out.push(v);
                    i += 3;
                    continue;
                }
                out.push(b[i]);
                i += 1;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).to_string()
}

/// The live proof is *reconstructed on every read* by the neutral observer — so the panel always
/// reflects the provider's current sealed chain (and grows as more denials are sealed).
fn state_json(ctx: &Ctx, st: &VizState) -> String {
    let fleet = format!(
        r##"{{"role":"Provider","emoji":"🛡","color":"#a78bfa","id":"{}","note":"serves the governed tool behind the gate"}},"##,
        esc(&short(&ctx.provider_id.to_string()))
    ) + &format!(
        r##"{{"role":"Authorized","emoji":"✅","color":"#3fb950","id":"{}","note":"on the allowlist"}},"##,
        esc(&short(&ctx.authorized.node_id().to_string()))
    ) + &format!(
        r##"{{"role":"Unauthorized","emoji":"⛔","color":"#f85149","id":"{}","note":"not on the allowlist"}},"##,
        esc(&short(&ctx.unauthorized.node_id().to_string()))
    ) + &format!(
        r##"{{"role":"Observer","emoji":"👁","color":"#58a6ff","id":"{}","note":"no role — reconstructs the proof"}}"##,
        esc(&short(&ctx.observer.node_id().to_string()))
    );

    let policy = ctx
        .policy
        .iter()
        .map(|(n, t, d)| format!(r#"{{"name":"{}","tier":"{}","detail":"{}"}}"#, esc(n), esc(t), esc(d)))
        .collect::<Vec<_>>()
        .join(",");

    let log = st
        .log
        .iter()
        .map(|a| {
            format!(
                r#"{{"id":{},"role":"{}","emoji":"{}","caller":"{}","admitted":{},"detail":"{}"}}"#,
                a.id, a.role, a.emoji, esc(&short(&a.caller)), a.admitted, esc(&a.detail)
            )
        })
        .collect::<Vec<_>>()
        .join(",");

    let proof = prove_denials(&ctx.observer, &ctx.provider_id, None);
    let denials = proof
        .denials
        .iter()
        .map(|d| {
            format!(
                r#"{{"seq":{},"caller":"{}","target":"{}","hash":"{}"}}"#,
                d.seq, esc(&short(&d.caller)), esc(&d.target), hex12(&d.content_hash)
            )
        })
        .collect::<Vec<_>>()
        .join(",");

    format!(
        r#"{{"fleet":[{fleet}],"policy":[{policy}],"log":[{log}],"proof":{{"verified":{},"observer":"{}","denials":[{denials}]}}}}"#,
        proof.chain_verified,
        esc(&short(&ctx.observer.node_id().to_string()))
    )
}

// ── HTTP dashboard ─────────────────────────────────────────────────────────────

async fn serve_http(ctx: Arc<Ctx>, state: Arc<Mutex<VizState>>) {
    let listener = match TcpListener::bind(format!("127.0.0.1:{HTTP_PORT}")).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("HTTP server failed to bind :{HTTP_PORT} — {e}");
            return;
        }
    };
    loop {
        let Ok((mut stream, _)) = listener.accept().await else { continue };
        let ctx = Arc::clone(&ctx);
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            let mut buf = vec![0u8; 8192];
            let n = match stream.read(&mut buf).await {
                Ok(n) if n > 0 => n,
                _ => return,
            };
            let req = String::from_utf8_lossy(&buf[..n]);
            let target = req.lines().next().unwrap_or("").split_whitespace().nth(1).unwrap_or("/");

            let (ctype, body): (&str, String) = if target.starts_with("/invoke") {
                let who = target.split_once("?as=").map(|(_, v)| pct_decode(v)).unwrap_or_default();
                let (caller, role, emoji) = match who.as_str() {
                    "authorized" => (&ctx.authorized, "Authorized", "✅"),
                    _ => (&ctx.unauthorized, "Unauthorized", "⛔"),
                };
                let reply = caller
                    .service()
                    .rpc_call(ctx.provider_id.clone(), KIND, b"do-something".to_vec(), Duration::from_secs(10))
                    .await;
                let (admitted, detail) = match reply {
                    Ok(r) if &r[..] == b"tool-ack" => {
                        (true, "Tier C — on the allowlist: admitted, the governed tool ran.".to_string())
                    }
                    Ok(_) => (
                        false,
                        "Tier C — not on the allowlist: structurally stopped at the provider's gate, \
                         the denial sealed into its audit chain."
                            .to_string(),
                    ),
                    Err(e) => (false, format!("stopped ({e})")),
                };
                {
                    let mut st = state.lock().unwrap();
                    let id = st.next;
                    st.next += 1;
                    st.log.push(Action { id, role, emoji, caller: caller.node_id().to_string(), admitted, detail });
                }
                // On a denial, let the seal gossip to the observer so the proof reflects it.
                if !admitted {
                    let cid = caller.node_id().to_string();
                    let c = Arc::clone(&ctx);
                    poll_until(
                        move || {
                            let p = prove_denials(&c.observer, &c.provider_id, Some(&cid));
                            p.chain_verified && !p.denials.is_empty()
                        },
                        Duration::from_secs(8),
                    )
                    .await;
                }
                ("application/json", state_json(&ctx, &state.lock().unwrap()))
            } else if target.starts_with("/state") {
                ("application/json", state_json(&ctx, &state.lock().unwrap()))
            } else {
                let console = if cfg!(feature = "gateway") {
                    format!(
                        "<a class=\"opsbtn\" href=\"http://127.0.0.1:8099/?target=127.0.0.1:{OPS_PORT}\" \
                         title=\"Open in the Ops Console — its Audit tab shows these very denial seals\">⚙ Ops Console</a>"
                    )
                } else {
                    String::new()
                };
                ("text/html; charset=utf-8",
                 include_str!("guardrail_viz.html")
                     .replace("__OPS_CONSOLE_LINK__", &console)
                     .replace("__CONCEPTS__", CONCEPTS))
            };

            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {ctype}\r\nAccess-Control-Allow-Origin: *\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(resp.as_bytes()).await;
        });
    }
}

#[tokio::main]
async fn main() {
    let dir = std::env::temp_dir().join(format!("myc-guardrail-viz-{}", free_port()));
    let _ = std::fs::remove_dir_all(&dir);
    let agents = start_mesh(4, &dir).await;
    let (provider, authorized, unauthorized, observer) = (
        Arc::clone(&agents[0]),
        Arc::clone(&agents[1]),
        Arc::clone(&agents[2]),
        Arc::clone(&agents[3]),
    );
    assert!(
        poll_until(|| agents.iter().all(|a| a.peers().len() >= 3), Duration::from_secs(30)).await,
        "the 4-node mesh forms"
    );

    // The provider declares its 3-tier policy; only the authorized caller is on the Tier-C allowlist.
    let policy = Policy::new()
        .act_within_groups(["microgrid-ops"])
        .deny_tools(["shell"])
        .authorized_callers([authorized.node_id().to_string()]);
    let applied = apply(policy.clone(), &provider).await;
    let policy_clauses: Vec<(String, String, String)> = policy
        .strength_report()
        .into_iter()
        .map(|c| (c.name.to_string(), c.tier.label().to_string(), c.detail.to_string()))
        .collect();

    // The Tier-C gate in front of the governed tool: authorized callers reach the handler,
    // unauthorized callers are dropped with a sealed `Invoke`/`Denied` and an error reply.
    let _guard = guarded_rpc_serve(&applied, KIND, move |agent, req| async move {
        agent.service().rpc_respond(&req, b"tool-ack".to_vec());
    });

    #[cfg(feature = "gateway")]
    {
        let _ = provider.kv().set("ui/viz", format!("http://127.0.0.1:{HTTP_PORT}/"));
        let _ = provider.kv().set("ui/label", "Guardrail wedge".to_string());
    }

    let ctx = Arc::new(Ctx {
        provider_id: provider.node_id().clone(),
        authorized,
        unauthorized,
        observer,
        policy: policy_clauses,
    });
    let state = Arc::new(Mutex::new(VizState::default()));
    tokio::spawn(serve_http(Arc::clone(&ctx), Arc::clone(&state)));

    println!("╔══════════════════════════════════════════════════════╗");
    println!("║  Guardrail wedge — structural stop + cryptographic proof ║");
    println!("║  Open in browser → http://127.0.0.1:{HTTP_PORT}/         ║");
    #[cfg(feature = "gateway")]
    println!("║  Ops Console     → point it at 127.0.0.1:{OPS_PORT} (Audit tab = the seals) ║");
    println!("╚══════════════════════════════════════════════════════╝");

    tokio::signal::ctrl_c().await.ok();
    for a in agents {
        a.shutdown_with_timeout(Duration::from_secs(5)).await;
    }
    let _ = std::fs::remove_dir_all(&dir);
}
