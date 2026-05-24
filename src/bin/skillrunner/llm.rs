// OpenAI-compatible LLM HTTP client.
//
// `call_openai_compatible` is the single function that will become the first
// impl of a `LlmBackend` trait when pluggability is needed (Phase 3 → trait).

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::config::LlmSection;

// ── Public types ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ToolSchema {
    pub name:        String,
    pub description: String,
    pub input:       Value,   // JSON Schema for parameters
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct ToolCall {
    pub id:        String,
    pub name:      String,
    pub arguments: Value,
}

pub(crate) struct LlmRequest {
    pub system_prompt: String,
    pub user_input:    Value,
    pub tools:         Vec<ToolSchema>,
    pub model:         String,
    pub max_tokens:    Option<u32>,
    pub temperature:   f32,
}

#[allow(dead_code)]
pub(crate) struct LlmResponse {
    pub output:        Value,
    pub tool_calls:    Vec<ToolCall>,
    pub finish_reason: String,
    pub tokens_used:   u32,
}

#[derive(Debug)]
pub(crate) enum LlmError {
    Http(reqwest::Error),
    Parse(String),
    NoChoices,
}

impl std::fmt::Display for LlmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LlmError::Http(e)   => write!(f, "HTTP error: {e}"),
            LlmError::Parse(s)  => write!(f, "parse error: {s}"),
            LlmError::NoChoices => write!(f, "no choices in LLM response"),
        }
    }
}

impl From<reqwest::Error> for LlmError {
    fn from(e: reqwest::Error) -> Self { LlmError::Http(e) }
}

// ── OpenAI wire types ─────────────────────────────────────────────────────────

#[derive(Serialize)]
struct ChatRequest<'a> {
    model:       &'a str,
    messages:    Vec<Message>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tools:       Option<Vec<OaiTool<'a>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_choice: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens:  Option<u32>,
    temperature: f32,
    stream:      bool,
}

#[derive(Serialize, Clone)]
struct Message {
    role:    String,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_calls: Option<Vec<OaiToolCallOut>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_call_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name:         Option<String>,
}

#[derive(Serialize)]
struct OaiTool<'a> {
    r#type:   &'static str,
    function: OaiToolFn<'a>,
}

#[derive(Serialize)]
struct OaiToolFn<'a> {
    name:        &'a str,
    description: &'a str,
    parameters:  &'a Value,
}

#[derive(Serialize, Deserialize, Clone)]
struct OaiToolCallOut {
    id:       String,
    r#type:   String,
    function: OaiToolCallFn,
}

#[derive(Serialize, Deserialize, Clone)]
struct OaiToolCallFn {
    name:      String,
    arguments: String,
}

#[derive(Deserialize)]
struct ChatResponse {
    choices: Vec<Choice>,
    usage:   Option<Usage>,
}

#[derive(Deserialize)]
struct Choice {
    message:       ChoiceMessage,
    finish_reason: Option<String>,
}

#[derive(Deserialize)]
struct ChoiceMessage {
    content:    Option<String>,
    tool_calls: Option<Vec<OaiToolCallOut>>,
}

#[derive(Deserialize)]
struct Usage {
    total_tokens: u32,
}

// ── Main entry point ──────────────────────────────────────────────────────────

/// Drive one complete LLM turn, including a tool-call loop (max 5 rounds).
///
/// `tool_dispatch` is called for each tool invocation the model requests; it
/// should return the serialised result or an error string.
pub(crate) async fn call_openai_compatible<F, Fut>(
    client:        &reqwest::Client,
    config:        &LlmSection,
    req:           LlmRequest,
    tool_dispatch: F,
) -> Result<LlmResponse, LlmError>
where
    F:   Fn(String, Value) -> Fut + Send + Sync,
    Fut: std::future::Future<Output = Value> + Send,
{
    let url = format!("{}/chat/completions", config.endpoint.trim_end_matches('/'));

    let auth_header: Option<String> = config.api_key.as_ref()
        .filter(|k| !k.is_empty())
        .map(|k| format!("Bearer {k}"));

    let oai_tools: Option<Vec<OaiTool<'_>>> = if req.tools.is_empty() {
        None
    } else {
        Some(req.tools.iter().map(|t| OaiTool {
            r#type:   "function",
            function: OaiToolFn {
                name:        &t.name,
                description: &t.description,
                parameters:  &t.input,
            },
        }).collect())
    };

    let tool_choice = if oai_tools.is_some() { Some("auto") } else { None };

    let mut messages = vec![
        Message {
            role:         "system".into(),
            content:      Some(Value::String(req.system_prompt.clone())),
            tool_calls:   None,
            tool_call_id: None,
            name:         None,
        },
        Message {
            role:         "user".into(),
            // OpenAI API requires string content; coerce object/array inputs to JSON text
            content:      Some(Value::String(match &req.user_input {
                Value::String(s) => s.clone(),
                other => serde_json::to_string_pretty(other).unwrap_or_default(),
            })),
            tool_calls:   None,
            tool_call_id: None,
            name:         None,
        },
    ];

    let mut tokens_used = 0u32;
    let mut all_tool_calls: Vec<ToolCall> = Vec::new();

    for _round in 0..5 {
        let body = ChatRequest {
            model:       &req.model,
            messages:    messages.clone(),
            tools:       oai_tools.as_ref().map(|t| {
                t.iter().map(|oai| OaiTool {
                    r#type:   "function",
                    function: OaiToolFn {
                        name:        oai.function.name,
                        description: oai.function.description,
                        parameters:  oai.function.parameters,
                    },
                }).collect()
            }),
            tool_choice,
            max_tokens:  req.max_tokens,
            temperature: req.temperature,
            stream:      false,
        };

        let mut request_builder = client.post(&url).json(&body);
        if let Some(ref auth) = auth_header {
            request_builder = request_builder.header("Authorization", auth.as_str());
        }

        let http_resp = request_builder.send().await?;
        let status = http_resp.status();
        let body = http_resp.bytes().await?;
        let resp: ChatResponse = serde_json::from_slice(&body).map_err(|e| {
            // Surface the raw API error if the server returned one
            let api_err = serde_json::from_slice::<Value>(&body)
                .ok()
                .and_then(|v| v.get("error")?.get("message")?.as_str().map(String::from));
            LlmError::Parse(api_err.unwrap_or_else(|| format!("HTTP {status}: {e}")))
        })?;

        if let Some(u) = resp.usage { tokens_used = u.total_tokens; }

        let choice = resp.choices.into_iter().next().ok_or(LlmError::NoChoices)?;
        let finish = choice.finish_reason.as_deref().unwrap_or("stop").to_string();

        if let Some(tcs) = &choice.message.tool_calls {
            // Append assistant message with the tool call requests
            messages.push(Message {
                role:         "assistant".into(),
                content:      None,
                tool_calls:   Some(tcs.clone()),
                tool_call_id: None,
                name:         None,
            });

            for tc in tcs {
                let args: Value = serde_json::from_str(&tc.function.arguments)
                    .unwrap_or(Value::Null);
                let result = tool_dispatch(tc.function.name.clone(), args.clone()).await;

                all_tool_calls.push(ToolCall {
                    id:        tc.id.clone(),
                    name:      tc.function.name.clone(),
                    arguments: args,
                });

                messages.push(Message {
                    role:         "tool".into(),
                    content:      Some(result),
                    tool_calls:   None,
                    tool_call_id: Some(tc.id.clone()),
                    name:         Some(tc.function.name.clone()),
                });
            }
            // Continue the loop to get the model's next response
            continue;
        }

        // Model produced a final text response
        let text = choice.message.content
            .unwrap_or_default();
        let output = serde_json::from_str::<Value>(&text)
            .unwrap_or(Value::String(text));

        return Ok(LlmResponse {
            output,
            tool_calls: all_tool_calls,
            finish_reason: finish,
            tokens_used,
        });
    }

    // Exceeded tool-call rounds — return whatever content we have
    Err(LlmError::Parse("exceeded maximum tool-call rounds".into()))
}

/// Convenience wrapper: single LLM turn with no tool calls.
#[allow(dead_code)]
pub(crate) async fn call_simple(
    client: &reqwest::Client,
    config: &LlmSection,
    prompt: &str,
    input:  Value,
) -> Result<LlmResponse, LlmError> {
    call_openai_compatible(
        client,
        config,
        LlmRequest {
            system_prompt: prompt.to_string(),
            user_input:    input,
            tools:         Vec::new(),
            model:         config.model.clone(),
            max_tokens:    config.max_tokens,
            temperature:   config.temperature,
        },
        |_name, _args| async { Value::String("(no tool dispatch configured)".into()) },
    ).await
}

