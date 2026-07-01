use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::{event::EventId, session::SessionId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct LogContext {
    pub session_id: Option<SessionId>,
    pub event_id: Option<EventId>,
    pub run_id: Option<String>,
    pub fields: BTreeMap<String, String>,
}

impl LogContext {
    pub fn with_field(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.fields.insert(key.into(), value.into());
        self
    }

    fn redacted(&self, redactor: &Redactor) -> Self {
        let fields = self
            .fields
            .iter()
            .map(|(key, value)| (key.clone(), redactor.redact_field(key, value)))
            .collect();

        Self {
            session_id: self.session_id.clone(),
            event_id: self.event_id.clone(),
            run_id: self.run_id.clone(),
            fields,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructuredLogEvent {
    pub level: LogLevel,
    pub message: String,
    pub context: LogContext,
}

impl StructuredLogEvent {
    pub fn new(level: LogLevel, message: impl Into<String>, context: LogContext) -> Self {
        Self {
            level,
            message: message.into(),
            context,
        }
    }

    pub fn to_line(&self, redactor: &Redactor) -> String {
        let event = Self {
            level: self.level,
            message: redactor.redact_value(&self.message),
            context: self.context.redacted(redactor),
        };

        serde_json::to_string(&event).expect("structured log event should serialize")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Redactor {
    secret_keys: Vec<String>,
}

impl Redactor {
    pub fn new(secret_keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            secret_keys: secret_keys
                .into_iter()
                .map(|value| value.into().to_ascii_lowercase())
                .collect(),
        }
    }

    pub fn redact_field(&self, key: &str, value: &str) -> String {
        let key = key.to_ascii_lowercase();

        if self.secret_keys.iter().any(|secret| key.contains(secret)) {
            return "[REDACTED]".to_owned();
        }

        self.redact_value(value)
    }

    pub fn redact_value(&self, value: &str) -> String {
        let mut redacted = value.to_owned();

        for key in &self.secret_keys {
            for marker in [format!("{key}="), format!("{key}:")] {
                redacted = redact_after_marker(&redacted, &marker);
            }
        }

        redacted
    }
}

impl Default for Redactor {
    fn default() -> Self {
        Self::new(["secret", "token", "password", "authorization"])
    }
}

fn redact_after_marker(input: &str, marker: &str) -> String {
    let lower = input.to_ascii_lowercase();
    let Some(index) = lower.find(marker) else {
        return input.to_owned();
    };
    let value_start = index + marker.len();
    let value_end = input[value_start..]
        .find(char::is_whitespace)
        .map(|offset| value_start + offset)
        .unwrap_or(input.len());

    format!("{}[REDACTED]{}", &input[..value_start], &input[value_end..])
}

#[cfg(test)]
mod tests {
    use super::{LogContext, LogLevel, Redactor, StructuredLogEvent};

    #[test]
    fn redacts_secret_fields_and_inline_values() {
        let event = StructuredLogEvent::new(
            LogLevel::Info,
            "received token=abc123 for request",
            LogContext::default()
                .with_field("app_secret", "plain-secret")
                .with_field("chat_id", "oc_123"),
        );

        let line = event.to_line(&Redactor::default());

        assert!(line.contains("[REDACTED]"));
        assert!(line.contains("oc_123"));
        assert!(!line.contains("abc123"));
        assert!(!line.contains("plain-secret"));
    }
}
