mod audit;
mod config;
mod llm;
mod runner;

use axum::{Router, routing::get, response::Html};
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;

use mycelium::{
    Capability, CapValue, GossipAgent, GossipConfig, GossipError, NodeId,
    PersistenceConfig, SyncMode, TlsConfig,
};

use config::SkillFile;
use runner::SkillRunner;

#[tokio::main]
async fn main() {
    // Initialise tracing
    #[cfg(feature = "cli")]
    {
        use tracing_subscriber::{EnvFilter, fmt};
        fmt()
            .with_env_filter(EnvFilter::from_default_env()
                .add_directive("skillrunner=info".parse().unwrap())
                .add_directive("mycelium=warn".parse().unwrap()))
            .init();
    }

    let path = parse_skill_arg();

    let sf = match SkillFile::load(&path) {
        Ok(sf) => Arc::new(sf),
        Err(e) => {
            eprintln!("error: failed to load {path}: {e}");
            std::process::exit(1);
        }
    };

    if let Err(e) = run(sf).await {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

async fn run(sf: Arc<SkillFile>) -> Result<(), Box<dyn std::error::Error>> {
    let node_id = NodeId::new(&sf.node.bind_address, sf.node.bind_port)?;
    let config  = build_gossip_config(&sf)?;
    let builder = GossipAgent::new(node_id, config);
    #[cfg(feature = "a2a")]
    let builder = if sf.node.http_port.is_some() { builder.with_a2a() } else { builder };
    let agent   = Arc::new(builder);

    // Management dashboard — served from the same HTTP port as the embedded gateway
    // so the page can query /gateway/kv/* without any CORS restrictions.
    if sf.node.http_port.is_some() {
        agent.with_http_routes(mgmt_routes());
    }

    // Optional OTEL tracer
    #[cfg(feature = "otel")]
    let otel_provider = sf.skill.otel.as_ref()
        .map(|cfg| audit::otel::init_tracer(cfg))
        .transpose()?;

    agent.start().await?;
    tracing::info!(
        "skillrunner: node {} started ({}:{})",
        agent.node_id(),
        sf.node.bind_address,
        sf.node.bind_port,
    );

    // Push input/output schemas to KV for tool discovery by peer skills
    let node_id_str = agent.node_id().to_string();
    let ns   = &sf.capability.ns;
    let name = &sf.capability.name;

    if let Some(ref schema) = sf.capability.input {
        let key = format!("skills/{ns}/{name}/{node_id_str}/input");
        let _ = agent.kv().set(key, Bytes::from(serde_json::to_vec(schema)?));
    }
    if let Some(ref schema) = sf.capability.output {
        let key = format!("skills/{ns}/{name}/{node_id_str}/output");
        let _ = agent.kv().set(key, Bytes::from(serde_json::to_vec(schema)?));
    }

    // Advertise capability on the mesh
    let cap = build_capability(&sf);
    let refresh = Duration::from_secs(sf.capability.ttl_secs);
    let _cap_handle = agent.capabilities().advertise_capability(cap, refresh);

    tracing::info!(
        "skillrunner: advertising {ns}/{name} (refresh {}s, max_concurrent {:?})",
        sf.capability.ttl_secs,
        sf.capability.policy.as_ref().and_then(|p| p.max_concurrent),
    );

    // Build HTTP client for LLM calls
    let http_client = Arc::new(
        reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .build()?,
    );

    // Graceful shutdown on ctrl-c / SIGTERM
    let agent_shutdown = Arc::clone(&agent);
    tokio::spawn(async move {
        shutdown_signal().await;
        tracing::info!("skillrunner: shutting down");
        agent_shutdown.shutdown().await;
    });

    // Run the skill invocation loop (blocks until shutdown)
    let skill_runner = SkillRunner {
        agent:  Arc::clone(&agent),
        skill:  Arc::clone(&sf),
        client: http_client,
        #[cfg(feature = "otel")]
        otel:   otel_provider,
    };
    skill_runner.run().await;

    Ok(())
}

#[allow(clippy::field_reassign_with_default)]
fn build_gossip_config(sf: &SkillFile) -> Result<GossipConfig, GossipError> {
    let mut cfg = GossipConfig::default();
    cfg.bind_address = sf.node.bind_address.clone();
    cfg.bind_port    = sf.node.bind_port;
    cfg.http_port    = sf.node.http_port;

    cfg.bootstrap_peers = sf.node.bootstrap_peers.iter()
        .filter_map(|addr| {
            let (ip, port_str) = addr.rsplit_once(':')?;
            let port: u16 = port_str.parse().ok()?;
            NodeId::new(ip, port).ok()
        })
        .collect();

    if let Some(ref p) = sf.node.persistence {
        cfg.persistence = Some(PersistenceConfig {
            base_path:               p.base_path.clone().into(),
            sync_mode:               if p.sync_flush { SyncMode::Flush } else { SyncMode::Async },
            snapshot_wal_threshold:  10_000,
            snapshot_interval_secs:  300,
        });
    }

    if let Some(ref t) = sf.node.tls {
        cfg.tls = Some(TlsConfig {
            cert_pem:     t.cert_pem.as_ref().map(Into::into),
            key_pem:      t.key_pem.as_ref().map(Into::into),
            ca_cert_pem:  t.ca_cert_pem.as_ref().map(Into::into),
            auto_cert_dir: t.auto_cert_dir.as_ref()
                .map(Into::into)
                .unwrap_or_else(|| "./mycelium-tls/".into()),
        });
    }

    Ok(cfg)
}

fn build_capability(sf: &SkillFile) -> Capability {
    let mut cap = Capability::new(sf.capability.ns.as_str(), sf.capability.name.as_str());

    if let Some(ref desc) = sf.capability.description {
        cap = cap.with("description", CapValue::Text(desc.as_str().into()));
    }

    if let Some(ref policy) = sf.capability.policy {
        if !policy.authorized_callers.is_empty() {
            cap = cap.with_authorized_callers(policy.authorized_callers.iter().map(String::as_str));
        }
    }

    // Advertise platform requirements as capability attributes so capability
    // resolution can filter on them (e.g. CapConstraint::Eq "gpu")
    if let Some(ref platform) = sf.capability.platform {
        for req in &platform.requires {
            cap = cap.with(format!("requires.{req}"), CapValue::Bool(true));
        }
    }

    // Embed input/output schemas into the gossip-propagated Capability so peer
    // nodes can inspect the contract from resolve() results without a separate
    // KV lookup. The `skills/{ns}/{name}/{node}/input` KV keys written above
    // are kept for backward compatibility with pre-schema-field peers.
    if let Some(ref schema) = sf.capability.input {
        if let Ok(json_str) = serde_json::to_string(schema) {
            cap = cap.with_input_schema(json_str.as_str());
        }
    }
    if let Some(ref schema) = sf.capability.output {
        if let Ok(json_str) = serde_json::to_string(schema) {
            cap = cap.with_output_schema(json_str.as_str());
        }
    }

    cap
}

// ── Management dashboard ──────────────────────────────────────────────────────

fn mgmt_routes() -> Router {
    Router::new().route("/mgmt", get(mgmt_handle_root))
}

async fn mgmt_handle_root() -> Html<&'static str> {
    Html(MGMT_HTML)
}

static MGMT_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Mycelium — Skills Dashboard</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Segoe UI',system-ui,sans-serif;background:#0f0f1a;color:#e2e8f0;min-height:100vh}
header{background:#1a1a2e;border-bottom:1px solid #2d2d4e;padding:14px 24px;display:flex;align-items:center;gap:12px}
header h1{font-size:1.05rem;font-weight:700;color:#a78bfa}
#status{font-size:0.75rem;color:#64748b;margin-left:auto}
main{max-width:960px;margin:0 auto;padding:24px 20px}
h2{font-size:0.78rem;font-weight:600;color:#475569;text-transform:uppercase;letter-spacing:.08em;margin-bottom:12px}
#summary{background:#1e293b;border:1px solid #2d2d4e;border-radius:10px;padding:14px 18px;display:flex;gap:28px;margin-bottom:28px;flex-wrap:wrap}
.stat{display:flex;flex-direction:column;gap:2px}
.stat-val{font-size:1.4rem;font-weight:700;color:#a78bfa;line-height:1}
.stat-label{font-size:0.72rem;color:#64748b}
#skills{display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:14px;margin-bottom:28px}
.skill-card{background:#1e293b;border:1px solid #2d2d4e;border-radius:12px;padding:16px}
.skill-badge{display:inline-block;font-size:0.7rem;font-weight:700;padding:2px 9px;border-radius:99px;margin-bottom:10px;text-transform:uppercase;letter-spacing:.06em;background:#3b0764;color:#c084fc}
.skill-name{font-size:1rem;font-weight:600;color:#e2e8f0;margin-bottom:4px}
.skill-ns{font-size:0.72rem;color:#64748b;margin-bottom:10px}
.providers{display:flex;flex-direction:column;gap:4px;margin-top:8px}
.provider{font-family:monospace;font-size:0.72rem;color:#475569;display:flex;align-items:center;gap:6px}
.dot{width:7px;height:7px;border-radius:50%;background:#56d364;flex-shrink:0}
#audit{background:#0d1117;border:1px solid #21262d;border-radius:10px;padding:14px 18px;margin-bottom:24px}
.audit-row{font-family:monospace;font-size:0.76rem;color:#adbac7;padding:5px 0;border-bottom:1px solid #21262d;display:flex;gap:12px}
.audit-row:last-child{border-bottom:none}
.audit-skill{color:#79c0ff;flex:0 0 180px}
.audit-ok{color:#56d364}.audit-err{color:#f85149}
.audit-dur{color:#64748b;flex:0 0 70px;text-align:right}
.audit-ts{color:#374151;flex:0 0 90px;text-align:right}
.no-audit{color:#334155;font-size:0.8rem;font-style:italic}
::-webkit-scrollbar{width:5px}::-webkit-scrollbar-track{background:#0f0f1a}::-webkit-scrollbar-thumb{background:#2d2d4e;border-radius:3px}
</style>
</head>
<body>
<header>
  <h1>&#127812; Mycelium Skills Dashboard</h1>
  <div id="status">connecting…</div>
</header>
<main>
  <div id="summary">
    <div class="stat"><div class="stat-val" id="s-skills">—</div><div class="stat-label">Active skills</div></div>
    <div class="stat"><div class="stat-val" id="s-providers">—</div><div class="stat-label">Nodes (providers)</div></div>
    <div class="stat"><div class="stat-val" id="s-invocations">—</div><div class="stat-label">Invocations (recent)</div></div>
    <div class="stat"><div class="stat-val" id="s-refresh">—</div><div class="stat-label">Last refresh</div></div>
  </div>

  <h2>Skills on Mesh</h2>
  <div id="skills"><div style="color:#475569;font-size:0.85rem">Loading…</div></div>

  <h2 style="margin-top:8px">Recent Invocations (audit/)</h2>
  <div id="audit"><div class="no-audit">Loading…</div></div>
</main>
<script>
(function(){
function pad2(n){return n<10?'0'+n:String(n);}
function fmtTime(d){return pad2(d.getHours())+':'+pad2(d.getMinutes())+':'+pad2(d.getSeconds());}
function esc(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}

async function refresh(){
  var now=new Date();
  try{
    // Load capability keys
    var r=await fetch('/gateway/kv/keys?prefix=cap/');
    if(!r.ok) throw new Error('kv/keys status '+r.status);
    var d=await r.json();
    var keys=d.keys||[];

    // Parse cap/{node_id}/{ns}/{name}
    var skillMap={};
    var providerSet=new Set();
    for(var k of keys){
      var p=k.split('/');
      if(p.length<4||p[0]!=='cap') continue;
      // p = ["cap", "127.0.0.1:7950", "llm", "orchestrator"]
      var nodeId=p[1], ns=p[2], name=p[3];
      var sk=ns+'/'+name;
      if(!skillMap[sk]) skillMap[sk]={ns,name,providers:[]};
      skillMap[sk].providers.push(nodeId);
      providerSet.add(nodeId);
    }

    // Render skills grid
    var skillKeys=Object.keys(skillMap).sort();
    document.getElementById('s-skills').textContent=skillKeys.length;
    document.getElementById('s-providers').textContent=providerSet.size;

    var grid=document.getElementById('skills');
    if(skillKeys.length===0){
      grid.innerHTML='<div style="color:#475569;font-size:0.85rem">No skills visible yet — waiting for gossip convergence…</div>';
    } else {
      grid.innerHTML=skillKeys.map(function(sk){
        var s=skillMap[sk];
        var pCount=s.providers.length;
        var badge=pCount===1?'skill-badge':'skill-badge" style="background:#0f3460;color:#60a5fa';
        var providerHtml=s.providers.map(function(pid){
          return '<div class="provider"><span class="dot"></span>'+esc(pid)+'</div>';
        }).join('');
        return '<div class="skill-card">'
          +'<span class="'+badge+'">'+esc(s.ns)+'</span>'
          +'<div class="skill-name">'+esc(s.name)+'</div>'
          +'<div class="skill-ns">'+pCount+' provider'+(pCount!==1?'s':'')+'</div>'
          +'<div class="providers">'+providerHtml+'</div>'
          +'</div>';
      }).join('');
    }

    // Load audit entries
    var ar=await fetch('/gateway/kv/keys?prefix=audit/');
    var auditEntries=[];
    if(ar.ok){
      var ad=await ar.json();
      var auditKeys=(ad.keys||[]).sort().reverse().slice(0,10);
      document.getElementById('s-invocations').textContent=(ad.keys||[]).length;
      for(var ak of auditKeys){
        try{
          var vr=await fetch('/gateway/kv?key='+encodeURIComponent(ak));
          if(vr.ok){
            var v=await vr.json();
            var raw=v.value;
            // value is base64-encoded JSON bytes
            var json=JSON.parse(atob(raw));
            auditEntries.push(json);
          }
        }catch(e){}
      }
    } else {
      document.getElementById('s-invocations').textContent='—';
    }

    var auditDiv=document.getElementById('audit');
    if(auditEntries.length===0){
      auditDiv.innerHTML='<div class="no-audit">No invocations recorded yet — invoke a skill to see audit entries</div>';
    } else {
      auditDiv.innerHTML=auditEntries.map(function(e){
        var ts=e.ts_unix_nanos?new Date(Math.floor(e.ts_unix_nanos/1e6)):null;
        var timeStr=ts?pad2(ts.getHours())+':'+pad2(ts.getMinutes())+':'+pad2(ts.getSeconds()):'?';
        return '<div class="audit-row">'
          +'<span class="audit-skill">'+esc(e.skill_ns+'/'+e.skill_name)+'</span>'
          +'<span class="'+(e.success?'audit-ok':'audit-err')+'">'+(e.success?'✓ ok':'✗ err')+'</span>'
          +'<span class="audit-dur">'+(e.duration_ms||'?')+'ms</span>'
          +'<span class="audit-ts">'+esc(timeStr)+'</span>'
          +'</div>';
      }).join('');
    }

    document.getElementById('s-refresh').textContent=fmtTime(now);
    document.getElementById('status').textContent=
      '&#10003; '+skillKeys.length+' skill'+(skillKeys.length!==1?'s':'')+' · refreshes every 4s';
  }catch(e){
    document.getElementById('status').textContent='&#9888; offline — '+e.message;
  }
}

refresh();
setInterval(refresh,4000);
})();
</script>
</body>
</html>"##;

fn parse_skill_arg() -> String {
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        if (args[i] == "--skill" || args[i] == "-s") && i + 1 < args.len() {
            return args[i + 1].clone();
        }
        if let Some(val) = args[i].strip_prefix("--skill=") {
            return val.to_string();
        }
        i += 1;
    }
    eprintln!("usage: skillrunner --skill <path/to/skill.toml>");
    std::process::exit(1);
}

async fn shutdown_signal() {
    use tokio::signal;
    let ctrl_c = async { signal::ctrl_c().await.ok(); };

    #[cfg(unix)]
    {
        let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    ctrl_c.await;
}
