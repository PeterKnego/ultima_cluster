//! LLM client abstraction. Two impls: `StubClient` (deterministic, for tests)
//! and `AnthropicClient` (real API via tool use).

use crate::proposal::VariantProposal;
use std::sync::Mutex;

pub trait LlmClient: Send + Sync {
    fn propose(
        &self,
        system_prompt: &str,
        user_message: &str,
        temperature: f32,
    ) -> anyhow::Result<VariantProposal>;
}

/// Pops canned proposals in order. Returns an error if exhausted.
pub struct StubClient {
    canned: Mutex<Vec<VariantProposal>>,
}

impl StubClient {
    pub fn with_canned(items: Vec<VariantProposal>) -> Self {
        // Reverse so we can `pop()` in O(1) and consume in declared order.
        let mut items = items;
        items.reverse();
        Self {
            canned: Mutex::new(items),
        }
    }
}

impl LlmClient for StubClient {
    fn propose(&self, _system: &str, _user: &str, _temp: f32) -> anyhow::Result<VariantProposal> {
        self.canned
            .lock()
            .unwrap()
            .pop()
            .ok_or_else(|| anyhow::anyhow!("StubClient exhausted"))
    }
}

/// Real Anthropic API client. Uses tool use to enforce structured output.
/// See https://docs.anthropic.com/en/docs/build-with-claude/tool-use
pub struct AnthropicClient {
    http: reqwest::blocking::Client,
    api_key: String,
    model: String,
}

impl AnthropicClient {
    pub fn from_env() -> anyhow::Result<Self> {
        let api_key = std::env::var("ANTHROPIC_API_KEY")
            .map_err(|_| anyhow::anyhow!("ANTHROPIC_API_KEY not set"))?;
        let model =
            std::env::var("UC_AUTOBENCH_MODEL").unwrap_or_else(|_| "claude-opus-4-7".to_string());
        Ok(Self {
            http: reqwest::blocking::Client::builder()
                .timeout(std::time::Duration::from_secs(300))
                .build()?,
            api_key,
            model,
        })
    }
}

impl LlmClient for AnthropicClient {
    fn propose(
        &self,
        system: &str,
        user: &str,
        temperature: f32,
    ) -> anyhow::Result<VariantProposal> {
        // Tool schema = the `VariantProposal` JSON shape.
        let tool = serde_json::json!({
            "name": "propose_variant",
            "description": "Propose one variant for the optimization loop.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "hypothesis": {"type": "string"},
                    "rationale": {"type": "string"},
                    "expected_outcome": {"type": "object"},
                    "risk_notes": {"type": "string"},
                    "files": {
                        "type": "object",
                        "description": "Map of repo-relative path to full new file content",
                        "additionalProperties": {"type": "string"}
                    }
                },
                "required": ["hypothesis", "rationale", "risk_notes", "files"]
            }
        });

        let body = serde_json::json!({
            "model": self.model,
            "max_tokens": 16384,
            "temperature": temperature,
            "system": [
                {
                    "type": "text",
                    "text": system,
                    "cache_control": {"type": "ephemeral"}
                }
            ],
            "messages": [{"role": "user", "content": user}],
            "tools": [tool],
            "tool_choice": {"type": "tool", "name": "propose_variant"}
        });

        let resp = self
            .http
            .post("https://api.anthropic.com/v1/messages")
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json")
            .json(&body)
            .send()?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().unwrap_or_default();
            anyhow::bail!("Anthropic API error: {status}: {text}");
        }

        let payload: serde_json::Value = resp.json()?;
        // The response `content` array contains a `tool_use` block whose `input`
        // is our `VariantProposal` shape.
        let input = payload["content"]
            .as_array()
            .and_then(|arr| arr.iter().find(|b| b["type"] == "tool_use"))
            .and_then(|b| b.get("input"))
            .ok_or_else(|| anyhow::anyhow!("no tool_use block in response: {payload}"))?;

        let proposal: VariantProposal = serde_json::from_value(input.clone())?;
        Ok(proposal)
    }
}
