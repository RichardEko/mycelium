//! The wiki as **MCP tools** (Phase 4) — `wiki.read` / `wiki.propose` / `wiki.query`, so a fleet agent
//! reaches the group wiki the way it reaches any other tool, over Mycelium's existing MCP invoke path.
//! No bespoke transport: [`register_mcp_tools`](Wiki::register_mcp_tools) writes each tool's schema to
//! `tools/{name}/{node}` in KV for cluster-wide discovery, and mycelium routes `mcp.invoke` signals to
//! the handlers. Dropping the returned [`WikiMcpTools`] guard tombstones the schemas and stops the
//! handler tasks.
//!
//! `read`/`query` are served **directly from the store on the calling node** (the data-plane
//! parallel-read property — any node can host them, not just the curator); `propose` enqueues to the
//! curator's evaporating queue. So the same three tools work identically on every group node.

use std::collections::BTreeMap;
use std::sync::Arc;

use mycelium::McpToolHandle;
use serde_json::{json, Value};

use crate::agent::Wiki;
use crate::model::{Predicate, SectionId};
use crate::store::WikiStore;

/// Lifetime guard for the registered wiki MCP tools — hold it while the tools should be discoverable;
/// drop it to tombstone their KV schema entries and stop the handler tasks.
pub struct WikiMcpTools {
    _handles: Vec<McpToolHandle>,
}

/// Coerce a JSON object of string values into an attribute map (non-string values are skipped —
/// attributes are join-keys / scope tags, always strings).
fn attrs_from_json(v: &Value) -> BTreeMap<String, String> {
    v.as_object()
        .map(|o| o.iter().filter_map(|(k, val)| val.as_str().map(|s| (k.clone(), s.to_string()))).collect())
        .unwrap_or_default()
}

/// `wiki.read` — direct store read on the calling node. Returns the page JSON, or `null` if absent.
pub(crate) fn handle_read<S: WikiStore + 'static>(w: &Wiki<S>, args: Value) -> Result<Value, String> {
    let page = args["page"].as_str().ok_or("missing string field 'page'")?;
    match w.read(page).map_err(|e| e.to_string())? {
        Some(p) => serde_json::to_value(&p).map_err(|e| e.to_string()),
        None    => Ok(Value::Null),
    }
}

/// `wiki.query` — structured attribute filter across the corpus (not similarity search). Returns the
/// matching section refs.
pub(crate) fn handle_query<S: WikiStore + 'static>(w: &Wiki<S>, args: Value) -> Result<Value, String> {
    let mut pred = Predicate::new();
    if let Some(obj) = args["equals"].as_object() {
        for (k, v) in obj {
            if let Some(s) = v.as_str() { pred = pred.with(k.clone(), s); }
        }
    }
    let hits = w.query(&pred).map_err(|e| e.to_string())?;
    serde_json::to_value(&hits).map_err(|e| e.to_string())
}

/// `wiki.propose` — enqueue an edit to the curator. `section` omitted → a fresh section id is minted
/// (new section); provided → an edit of that section. Returns the proposal key.
pub(crate) fn handle_propose<S: WikiStore + 'static>(w: &Arc<Wiki<S>>, args: Value) -> Result<Value, String> {
    let page = args["page"].as_str().ok_or("missing string field 'page'")?;
    let heading = args["heading"].as_str().unwrap_or("");
    let body = args["body"].as_str().ok_or("missing string field 'body'")?;
    let section: SectionId = match args["section"].as_str() {
        Some(s) => Arc::from(s),
        None    => w.new_section_id(page),
    };
    let key = w.propose(page, section.clone(), heading, body, attrs_from_json(&args["attributes"]));
    Ok(json!({ "proposal": key, "section": section.as_ref() }))
}

impl<S: WikiStore + 'static> Wiki<S> {
    /// Register `wiki.read` / `wiki.propose` / `wiki.query` as MCP tools on this node. Hold the returned
    /// [`WikiMcpTools`] for as long as the tools should be discoverable and invocable.
    pub fn register_mcp_tools(self: &Arc<Self>) -> WikiMcpTools {
        let mcp = self.agent().mcp();

        let wr = Arc::clone(self);
        let read = mcp.register_mcp_tool(
            "wiki.read",
            json!({
                "type": "object",
                "properties": { "page": { "type": "string", "description": "page path" } },
                "required": ["page"],
            }),
            move |args| {
                let wr = Arc::clone(&wr);
                async move { handle_read(&wr, args) }
            },
        );

        let wq = Arc::clone(self);
        let query = mcp.register_mcp_tool(
            "wiki.query",
            json!({
                "type": "object",
                "properties": { "equals": { "type": "object", "description": "attribute equals-filter (all-of)" } },
            }),
            move |args| {
                let wq = Arc::clone(&wq);
                async move { handle_query(&wq, args) }
            },
        );

        let wp = Arc::clone(self);
        let propose = mcp.register_mcp_tool(
            "wiki.propose",
            json!({
                "type": "object",
                "properties": {
                    "page":       { "type": "string" },
                    "section":    { "type": "string", "description": "omit to mint a new section" },
                    "heading":    { "type": "string" },
                    "body":       { "type": "string" },
                    "attributes": { "type": "object", "description": "join-keys / scope tags (string values)" },
                },
                "required": ["page", "body"],
            }),
            move |args| {
                let wp = Arc::clone(&wp);
                async move { handle_propose(&wp, args) }
            },
        );

        WikiMcpTools { _handles: vec![read, query, propose] }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::field_reassign_with_default)] // GossipConfig is built the way mycelium's own tests do
    use super::*;
    use std::time::Duration;
    use mycelium::{GossipAgent, GossipConfig, NodeId};
    use crate::agent::{WikiConfig, WikiRole};
    use crate::fs::FsStore;

    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
    }

    async fn single_node_wiki(dir: &std::path::Path) -> (Arc<GossipAgent>, Arc<Wiki<FsStore>>) {
        let port = free_port();
        let mut cfg = GossipConfig::default();
        cfg.bind_port = port;
        let agent = Arc::new(GossipAgent::new(NodeId::new("127.0.0.1", port).unwrap(), cfg));
        agent.start().await.unwrap();
        let store = Arc::new(FsStore::open(dir, "ops").unwrap());
        let wcfg = WikiConfig {
            group: "ops".into(), role: WikiRole::Reader,
            cap_refresh: Duration::from_secs(2), drain_interval: Duration::from_millis(200),
            lint_interval: Duration::from_secs(30),
        };
        let wiki = Wiki::new(Arc::clone(&agent), wcfg, store).await;
        (agent, wiki)
    }

    #[tokio::test]
    async fn read_and_query_handlers_serve_the_store() {
        let dir = tempfile::tempdir().unwrap();
        let (agent, wiki) = single_node_wiki(dir.path()).await;

        // Seed a page directly through the store, then serve it through the MCP handlers.
        let sec = crate::model::Section {
            id: "s1".into(), heading: "Symptoms".into(), body: "gateway 503s".into(),
            attributes: BTreeMap::from([("node".to_string(), "e_rl_rk".to_string())]),
        };
        wiki.store().write_page("incidents/x", std::slice::from_ref(&sec), &BTreeMap::new()).unwrap();

        let page = handle_read(&wiki, json!({ "page": "incidents/x" })).unwrap();
        assert_eq!(page["path"], "incidents/x");
        assert_eq!(page["sections"][0]["body"], "gateway 503s");

        let missing = handle_read(&wiki, json!({ "page": "nope" })).unwrap();
        assert!(missing.is_null());

        let hits = handle_query(&wiki, json!({ "equals": { "node": "e_rl_rk" } })).unwrap();
        assert_eq!(hits.as_array().unwrap().len(), 1);
        assert_eq!(hits[0]["page"], "incidents/x");

        agent.shutdown_with_timeout(Duration::from_secs(5)).await;
    }

    #[tokio::test]
    async fn propose_handler_enqueues_and_registration_publishes_schemas() {
        let dir = tempfile::tempdir().unwrap();
        let (agent, wiki) = single_node_wiki(dir.path()).await;

        // A new-section propose (no `section`) mints an id and writes an evaporating KV proposal.
        let out = handle_propose(&wiki, json!({
            "page": "incidents/x", "heading": "Fix", "body": "rolled back",
            "attributes": { "node": "e_rl_rk" },
        })).unwrap();
        assert!(out["section"].as_str().is_some(), "a section id was minted");
        let key = out["proposal"].as_str().unwrap();
        assert!(agent.kv().get(key).is_some(), "the proposal is on the queue");

        // Registration publishes the three tool schemas for cluster-wide discovery.
        let _guard = wiki.register_mcp_tools();
        let tools: Vec<String> = agent.kv().scan_prefix("tools/wiki.")
            .into_iter().map(|(k, _)| k.to_string()).collect();
        for name in ["wiki.read", "wiki.query", "wiki.propose"] {
            assert!(tools.iter().any(|k| k.contains(name)), "tool {name} is discoverable: {tools:?}");
        }

        agent.shutdown_with_timeout(Duration::from_secs(5)).await;
    }
}
