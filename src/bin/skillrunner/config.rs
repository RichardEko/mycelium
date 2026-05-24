use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub(crate) struct SkillFile {
    pub node:       NodeSection,
    pub capability: CapabilitySection,
    pub skill:      SkillSection,
}

#[derive(Debug, Deserialize)]
pub(crate) struct NodeSection {
    #[serde(default = "default_bind_address")]
    pub bind_address:    String,
    pub bind_port:       u16,
    #[serde(default)]
    pub bootstrap_peers: Vec<String>,
    pub http_port:       Option<u16>,
    pub persistence:     Option<PersistenceSection>,
    pub tls:             Option<TlsSection>,
}

fn default_bind_address() -> String { "127.0.0.1".into() }

#[derive(Debug, Deserialize)]
pub(crate) struct PersistenceSection {
    pub base_path:   String,
    #[serde(default)]
    pub sync_flush:  bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TlsSection {
    pub auto_cert_dir: Option<String>,
    pub cert_pem:      Option<String>,
    pub key_pem:       Option<String>,
    pub ca_cert_pem:   Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct CapabilitySection {
    pub ns:          String,
    pub name:        String,
    pub description: Option<String>,
    /// Capability advertisement refresh interval. Entries age out after 3× this value.
    #[serde(default = "default_ttl_secs")]
    pub ttl_secs:    u64,
    pub input:       Option<serde_json::Value>,
    pub output:      Option<serde_json::Value>,
    pub policy:      Option<PolicySection>,
    pub platform:    Option<PlatformSection>,
}

fn default_ttl_secs() -> u64 { 60 }

#[derive(Debug, Deserialize)]
pub(crate) struct PolicySection {
    pub max_concurrent:     Option<usize>,
    #[serde(default)]
    pub authorized_callers: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PlatformSection {
    #[serde(default)]
    pub requires: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SkillSection {
    pub prompt: String,
    #[serde(default)]
    pub tools:  Vec<String>,
    pub llm:    LlmSection,
    #[cfg_attr(not(feature = "otel"), allow(dead_code))]
    pub otel:   Option<OtelSection>,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct LlmSection {
    pub endpoint:    String,
    pub model:       String,
    pub api_key:     Option<String>,
    pub max_tokens:  Option<u32>,
    #[serde(default = "default_temperature")]
    pub temperature: f32,
}

fn default_temperature() -> f32 { 0.7 }

#[cfg_attr(not(feature = "otel"), allow(dead_code))]
#[derive(Debug, Deserialize, Clone)]
pub(crate) struct OtelSection {
    pub endpoint:     String,
    pub service_name: String,
}

impl SkillFile {
    pub(crate) fn load(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let text = std::fs::read_to_string(path)?;
        let sf: SkillFile = toml::from_str(&text)?;
        sf.validate()?;
        Ok(sf)
    }

    fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.node.bind_port == 0 {
            return Err("node.bind_port must be non-zero".into());
        }
        if self.capability.ns.is_empty() || self.capability.name.is_empty() {
            return Err("capability.ns and capability.name must not be empty".into());
        }
        if self.skill.llm.endpoint.is_empty() {
            return Err("skill.llm.endpoint must not be empty".into());
        }
        if self.skill.llm.model.is_empty() {
            return Err("skill.llm.model must not be empty".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str = r#"
[node]
bind_port = 7947
bootstrap_peers = ["127.0.0.1:7946"]

[capability]
ns   = "llm"
name = "chat"

[capability.input]
type = "object"
[capability.input.properties]
message = { type = "string" }

[capability.output]
type = "object"
[capability.output.properties]
reply = { type = "string" }

[skill]
prompt = "You are a helpful assistant."

[skill.llm]
endpoint = "http://localhost:11434/v1"
model    = "llama3.2"
"#;

    #[test]
    fn parse_minimal() {
        let sf: SkillFile = toml::from_str(MINIMAL).expect("parse failed");
        assert_eq!(sf.capability.ns, "llm");
        assert_eq!(sf.capability.name, "chat");
        assert_eq!(sf.node.bind_port, 7947);
        assert_eq!(sf.skill.llm.model, "llama3.2");
        assert!(sf.capability.policy.is_none());
        assert!(sf.skill.otel.is_none());
    }

    const FULL: &str = r#"
[node]
bind_address    = "0.0.0.0"
bind_port       = 7948
bootstrap_peers = ["10.0.0.1:7946", "10.0.0.2:7946"]
http_port       = 9000

[node.persistence]
base_path  = "/tmp/skill-data"
sync_flush = true

[capability]
ns          = "dev"
name        = "code-review"
description = "Reviews PR diffs"
ttl_secs    = 300

[capability.input]
type = "object"
[capability.input.properties]
pr_number = { type = "integer" }

[capability.output]
type = "object"
[capability.output.properties]
summary = { type = "string" }

[capability.policy]
max_concurrent     = 2
authorized_callers = ["orchestrator", "planner"]

[capability.platform]
requires = ["gpu"]

[skill]
prompt = "Review the PR."
tools  = ["gh", "read_file"]

[skill.llm]
endpoint    = "http://localhost:11434/v1"
model       = "llama3.2"
api_key     = "sk-test"
max_tokens  = 4096
temperature = 0.3

[skill.otel]
endpoint     = "http://localhost:4317"
service_name = "code-review-skill"
"#;

    #[test]
    fn parse_full() {
        let sf: SkillFile = toml::from_str(FULL).expect("parse failed");
        assert_eq!(sf.node.bind_address, "0.0.0.0");
        assert_eq!(sf.node.http_port, Some(9000));
        assert!(sf.node.persistence.is_some());

        let policy = sf.capability.policy.as_ref().unwrap();
        assert_eq!(policy.max_concurrent, Some(2));
        assert_eq!(policy.authorized_callers, vec!["orchestrator", "planner"]);

        let platform = sf.capability.platform.as_ref().unwrap();
        assert_eq!(platform.requires, vec!["gpu"]);

        assert_eq!(sf.skill.tools, vec!["gh", "read_file"]);
        assert_eq!(sf.skill.llm.temperature, 0.3);
        assert_eq!(sf.skill.llm.max_tokens, Some(4096));
        assert!(sf.skill.otel.is_some());
    }

    #[test]
    fn validate_rejects_zero_port() {
        let mut sf: SkillFile = toml::from_str(MINIMAL).unwrap();
        sf.node.bind_port = 0;
        assert!(sf.validate().is_err());
    }

    #[test]
    fn default_temperature_applied() {
        let sf: SkillFile = toml::from_str(MINIMAL).unwrap();
        assert!((sf.skill.llm.temperature - 0.7).abs() < f32::EPSILON);
    }

    #[test]
    fn default_ttl_applied() {
        let sf: SkillFile = toml::from_str(MINIMAL).unwrap();
        assert_eq!(sf.capability.ttl_secs, 60);
    }
}
