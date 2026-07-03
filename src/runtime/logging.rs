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
                .map(|value| value.into().trim().to_ascii_lowercase())
                .filter(|value| !value.is_empty())
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
            redacted = redact_inline_key_values(&redacted, key);
        }

        redacted
    }
}

impl Default for Redactor {
    fn default() -> Self {
        Self::new(["secret", "token", "password", "authorization"])
    }
}

fn redact_inline_key_values(input: &str, key: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut cursor = 0;

    while let Some(match_start) = find_next_marker(input, key, cursor) {
        let delimiter_index = match_start + key.len();
        let mut value_start = delimiter_index + 1;

        while let Some(char) = input[value_start..].chars().next() {
            if !char.is_whitespace() {
                break;
            }

            value_start += char.len_utf8();
        }

        let value_end = secret_value_end(input, value_start, key);
        output.push_str(&input[cursor..value_start]);
        output.push_str("[REDACTED]");
        cursor = value_end;
    }

    output.push_str(&input[cursor..]);
    output
}

fn find_next_marker(input: &str, key: &str, start: usize) -> Option<usize> {
    let lower = input.to_ascii_lowercase();
    let mut cursor = start;

    while cursor < input.len() {
        let relative_index = lower[cursor..].find(key)?;
        let match_start = cursor + relative_index;
        let delimiter_index = match_start + key.len();

        if input[delimiter_index..]
            .chars()
            .next()
            .is_some_and(|char| matches!(char, '=' | ':'))
        {
            return Some(match_start);
        }

        cursor = delimiter_index;
    }

    None
}

fn secret_value_end(input: &str, value_start: usize, key: &str) -> usize {
    let first_token_end = next_token_end(input, value_start);

    if key == "authorization"
        && input[value_start..first_token_end].eq_ignore_ascii_case("bearer")
        && first_token_end < input.len()
    {
        let second_token_start = skip_whitespace(input, first_token_end);
        return next_token_end(input, second_token_start);
    }

    first_token_end
}

fn next_token_end(input: &str, start: usize) -> usize {
    input[start..]
        .find(char::is_whitespace)
        .map(|offset| start + offset)
        .unwrap_or(input.len())
}

fn skip_whitespace(input: &str, start: usize) -> usize {
    let mut cursor = start;

    while let Some(char) = input[cursor..].chars().next() {
        if !char.is_whitespace() {
            break;
        }

        cursor += char.len_utf8();
    }

    cursor
}

#[cfg(test)]
mod tests {
    use super::{LogContext, LogLevel, Redactor, StructuredLogEvent};

    #[test]
    fn redacts_secret_fields_and_inline_values() {
        let event = StructuredLogEvent::new(
            LogLevel::Info,
            "received token=abc123 for request with token=def456",
            LogContext::default()
                .with_field("app_secret", "plain-secret")
                .with_field("chat_id", "oc_123"),
        );

        let line = event.to_line(&Redactor::default());

        assert!(line.contains("[REDACTED]"));
        assert!(line.contains("oc_123"));
        assert!(!line.contains("abc123"));
        assert!(!line.contains("def456"));
        assert!(!line.contains("plain-secret"));
    }

    #[test]
    fn redacts_authorization_bearer_values() {
        let line = Redactor::default()
            .redact_value("request failed Authorization: Bearer abc123 for chat");

        assert!(line.contains("Authorization: [REDACTED] for chat"));
        assert!(!line.contains("Bearer"));
        assert!(!line.contains("abc123"));
    }

    #[test]
    fn ignores_empty_secret_keys() {
        let line = Redactor::new(["", "  ", "token"]).redact_value("token=abc visible=value");

        assert!(line.contains("token=[REDACTED]"));
        assert!(line.contains("visible=value"));
        assert!(!line.contains("abc"));
    }
}
