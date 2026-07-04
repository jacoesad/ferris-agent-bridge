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

    while let Some(marker) = find_next_marker(input, key, cursor) {
        let (value_start, value_end) = secret_value_span(input, marker.value_start, key);
        output.push_str(&input[cursor..value_start]);
        output.push_str("[REDACTED]");
        cursor = value_end;
    }

    output.push_str(&input[cursor..]);
    output
}

struct InlineSecretMarker {
    value_start: usize,
}

fn find_next_marker(input: &str, key: &str, start: usize) -> Option<InlineSecretMarker> {
    let lower = input.to_ascii_lowercase();
    let mut cursor = start;

    while cursor < input.len() {
        let relative_index = lower[cursor..].find(key)?;
        let match_start = cursor + relative_index;
        let mut delimiter_index = match_start + key.len();

        let Some(suffix_end) = skip_inline_key_suffix(input, delimiter_index) else {
            cursor = delimiter_index;
            continue;
        };
        delimiter_index = suffix_end;
        delimiter_index = skip_optional_quote(input, delimiter_index);
        delimiter_index = skip_whitespace(input, delimiter_index);

        if input[delimiter_index..]
            .chars()
            .next()
            .is_some_and(|char| matches!(char, '=' | ':'))
        {
            let value_start = skip_whitespace(input, delimiter_index + 1);
            return Some(InlineSecretMarker { value_start });
        }

        cursor = delimiter_index;
    }

    None
}

fn skip_inline_key_suffix(input: &str, start: usize) -> Option<usize> {
    let mut cursor = start;

    while let Some(char) = input[cursor..].chars().next() {
        if char.is_whitespace() {
            return None;
        }

        if matches!(char, '=' | ':') || char == '"' || char == '\'' {
            break;
        }

        if input[cursor..].starts_with("\\\"") || input[cursor..].starts_with("\\'") {
            break;
        }

        cursor += char.len_utf8();
    }

    Some(cursor)
}

fn skip_optional_quote(input: &str, start: usize) -> usize {
    if input[start..].starts_with("\\\"") || input[start..].starts_with("\\'") {
        start + 2
    } else if input[start..]
        .chars()
        .next()
        .is_some_and(|char| matches!(char, '"' | '\''))
    {
        start + 1
    } else {
        start
    }
}

fn secret_value_span(input: &str, value_start: usize, key: &str) -> (usize, usize) {
    if let Some((inner_start, inner_end)) = quoted_value_span(input, value_start) {
        return (inner_start, inner_end);
    }

    let first_token_end = next_unquoted_value_end(input, value_start);

    if key == "authorization" && first_token_end < input.len() {
        let second_token_start = skip_whitespace(input, first_token_end);
        if second_token_start > first_token_end {
            return (
                value_start,
                next_unquoted_value_end(input, second_token_start),
            );
        }
    }

    (value_start, first_token_end)
}

fn quoted_value_span(input: &str, value_start: usize) -> Option<(usize, usize)> {
    if let Some(span) = escaped_quoted_value_span(input, value_start, "\\\"") {
        return Some(span);
    }

    if let Some(span) = escaped_quoted_value_span(input, value_start, "\\'") {
        return Some(span);
    }

    let quote = input[value_start..].chars().next()?;
    if !matches!(quote, '"' | '\'') {
        return None;
    }

    let inner_start = value_start + quote.len_utf8();
    let mut cursor = inner_start;

    while let Some(char) = input[cursor..].chars().next() {
        if char == quote {
            return Some((inner_start, cursor));
        }

        cursor += char.len_utf8();

        if char == '\\' {
            if let Some(escaped) = input[cursor..].chars().next() {
                cursor += escaped.len_utf8();
            }
        }
    }

    Some((inner_start, input.len()))
}

fn escaped_quoted_value_span(
    input: &str,
    value_start: usize,
    escaped_quote: &str,
) -> Option<(usize, usize)> {
    if !input[value_start..].starts_with(escaped_quote) {
        return None;
    }

    let inner_start = value_start + escaped_quote.len();
    let mut cursor = inner_start;

    while let Some(offset) = input[cursor..].find(escaped_quote) {
        let candidate = cursor + offset;
        if escaped_quote_is_closing(input, candidate, escaped_quote) {
            return Some((inner_start, candidate));
        }

        cursor = candidate + escaped_quote.len();
    }

    Some((inner_start, input.len()))
}

fn escaped_quote_is_closing(input: &str, candidate: usize, escaped_quote: &str) -> bool {
    input[candidate..].starts_with(escaped_quote)
        && input[..candidate]
            .chars()
            .next_back()
            .is_none_or(|char| char != '\\')
}

fn next_unquoted_value_end(input: &str, start: usize) -> usize {
    input[start..]
        .char_indices()
        .find(|(_, char)| is_unquoted_value_delimiter(*char))
        .map(|(offset, _)| start + offset)
        .unwrap_or(input.len())
}

fn is_unquoted_value_delimiter(char: char) -> bool {
    char.is_whitespace() || matches!(char, ',' | ';' | '}' | ']')
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
    fn redacts_authorization_basic_values() {
        let line =
            Redactor::default().redact_value("request failed Authorization: Basic abc123 for chat");

        assert!(line.contains("Authorization: [REDACTED] for chat"));
        assert!(!line.contains("Basic"));
        assert!(!line.contains("abc123"));
    }

    #[test]
    fn redacts_authorization_unknown_two_part_values() {
        let line = Redactor::default()
            .redact_value("request failed Authorization: ApiKey abc123 for chat");

        assert!(line.contains("Authorization: [REDACTED] for chat"));
        assert!(!line.contains("ApiKey"));
        assert!(!line.contains("abc123"));
    }

    #[test]
    fn redacts_json_style_secret_values() {
        let line = Redactor::default()
            .redact_value(r#"payload {"token":"abc123","visible":"ok","password": "pw456"}"#);

        assert!(line.contains(r#""token":"[REDACTED]""#));
        assert!(line.contains(r#""visible":"ok""#));
        assert!(line.contains(r#""password": "[REDACTED]""#));
        assert!(!line.contains("abc123"));
        assert!(!line.contains("pw456"));
    }

    #[test]
    fn redacts_json_secret_values_next_to_other_fields() {
        let line = Redactor::default().redact_value(r#"{"token":"abc","chat":"1"}"#);

        assert_eq!(line, r#"{"token":"[REDACTED]","chat":"1"}"#);
        assert!(!line.contains("abc"));
    }

    #[test]
    fn redacts_secret_like_inline_key_names() {
        let line = Redactor::default().redact_value(
            "loaded app_secret=abc123 accessToken=tok456 appSecret=sec789 secret_key=key123 token_id=tid456 passwordHash=hash789 tokenValue=tval012",
        );

        assert!(line.contains("app_secret=[REDACTED]"));
        assert!(line.contains("accessToken=[REDACTED]"));
        assert!(line.contains("appSecret=[REDACTED]"));
        assert!(line.contains("secret_key=[REDACTED]"));
        assert!(line.contains("token_id=[REDACTED]"));
        assert!(line.contains("passwordHash=[REDACTED]"));
        assert!(line.contains("tokenValue=[REDACTED]"));
        assert!(!line.contains("abc123"));
        assert!(!line.contains("tok456"));
        assert!(!line.contains("sec789"));
        assert!(!line.contains("key123"));
        assert!(!line.contains("tid456"));
        assert!(!line.contains("hash789"));
        assert!(!line.contains("tval012"));
    }

    #[test]
    fn redacts_secret_like_inline_key_names_with_unicode_suffixes() {
        let line = Redactor::default().redact_value("token名=abc123 visible=value");

        assert!(line.contains("token名=[REDACTED]"));
        assert!(line.contains("visible=value"));
        assert!(!line.contains("abc123"));
    }

    #[test]
    fn redacts_secret_like_inline_key_names_with_symbol_suffixes() {
        let line = Redactor::default()
            .redact_value("token[]=abc123 token/type=tok456 password[hash]=pw789");

        assert!(line.contains("token[]=[REDACTED]"));
        assert!(line.contains("token/type=[REDACTED]"));
        assert!(line.contains("password[hash]=[REDACTED]"));
        assert!(!line.contains("abc123"));
        assert!(!line.contains("tok456"));
        assert!(!line.contains("pw789"));
    }

    #[test]
    fn redacts_authorization_like_inline_key_names() {
        let line =
            Redactor::default().redact_value("authorizationHeader=ApiKey abc123 visible=value");

        assert!(line.contains("authorizationHeader=[REDACTED] visible=value"));
        assert!(!line.contains("ApiKey"));
        assert!(!line.contains("abc123"));
    }

    #[test]
    fn redacts_unquoted_inline_secret_values_until_field_delimiters() {
        let line = Redactor::default()
            .redact_value("token=abc,chat=1 password=pw;visible=value secret=hidden]tail");

        assert!(line.contains("token=[REDACTED],chat=1"));
        assert!(line.contains("password=[REDACTED];visible=value"));
        assert!(line.contains("secret=[REDACTED]]tail"));
        assert!(line.contains("visible=value"));
        assert!(!line.contains("abc"));
        assert!(!line.contains("pw"));
        assert!(!line.contains("hidden"));
    }

    #[test]
    fn redacts_comma_separated_inline_secret_without_swallowing_next_field() {
        let line = Redactor::default().redact_value("token=abc,chat=1");

        assert_eq!(line, "token=[REDACTED],chat=1");
        assert!(!line.contains("abc"));
    }

    #[test]
    fn does_not_cross_whitespace_after_inline_secret_marker() {
        let line = Redactor::default().redact_value("token label token=abc123 visible=value");

        assert!(line.contains("token label token=[REDACTED]"));
        assert!(line.contains("visible=value"));
        assert!(!line.contains("abc123"));
    }

    #[test]
    fn redacts_escaped_json_style_secret_values() {
        let line = Redactor::default().redact_value(
            r#"payload {\"token\":\"abc123\",\"apiToken\":\"tok456\",\"token/type\":\"tok789\",\"visible\":\"ok\"}"#,
        );

        assert!(line.contains(r#"\"token\":\"[REDACTED]\""#));
        assert!(line.contains(r#"\"apiToken\":\"[REDACTED]\""#));
        assert!(line.contains(r#"\"token/type\":\"[REDACTED]\""#));
        assert!(line.contains(r#"\"visible\":\"ok\""#));
        assert!(!line.contains("abc123"));
        assert!(!line.contains("tok456"));
        assert!(!line.contains("tok789"));
    }

    #[test]
    fn redacts_escaped_json_secret_values_with_escaped_quotes() {
        let line = Redactor::default()
            .redact_value(r#"payload {\"password\":\"one\\\"two\",\"visible\":\"ok\"}"#);

        assert!(line.contains(r#"\"password\":\"[REDACTED]\""#));
        assert!(line.contains(r#"\"visible\":\"ok\""#));
        assert!(!line.contains("one"));
        assert!(!line.contains("two"));
    }

    #[test]
    fn redacts_escaped_json_secret_values_with_escaped_quotes_before_commas() {
        let line = Redactor::default()
            .redact_value(r#"payload {\"password\":\"one\\\",two\",\"visible\":\"ok\"}"#);

        assert!(line.contains(r#"\"password\":\"[REDACTED]\""#));
        assert!(line.contains(r#"\"visible\":\"ok\""#));
        assert!(!line.contains("one"));
        assert!(!line.contains("two"));
    }

    #[test]
    fn redacts_escaped_json_secret_values_with_escaped_quotes_before_closing_quotes() {
        let line = Redactor::default()
            .redact_value(r#"payload {\"password\":\"one\\\",\",\"visible\":\"ok\"}"#);

        assert!(line.contains(r#"\"password\":\"[REDACTED]\""#));
        assert!(line.contains(r#"\"visible\":\"ok\""#));
        assert!(!line.contains("one"));
    }

    #[test]
    fn redacts_escaped_json_secret_values_with_escaped_quotes_before_structural_chars() {
        let line = Redactor::default().redact_value(
            r#"payload {\"password\":\"one\\\"}two\",\"token\":\"needle\\\",]more\",\"secret\":\"hidden\\\",}tail\",\"visible\":\"ok\"}"#,
        );

        assert!(line.contains(r#"\"password\":\"[REDACTED]\""#));
        assert!(line.contains(r#"\"token\":\"[REDACTED]\""#));
        assert!(line.contains(r#"\"secret\":\"[REDACTED]\""#));
        assert!(line.contains(r#"\"visible\":\"ok\""#));
        assert!(!line.contains("one"));
        assert!(!line.contains("two"));
        assert!(!line.contains("needle"));
        assert!(!line.contains("more"));
        assert!(!line.contains("hidden"));
        assert!(!line.contains("tail"));
    }

    #[test]
    fn redacts_escaped_json_secret_values_ending_with_backslash() {
        let line = Redactor::default()
            .redact_value(r#"payload {\"password\":\"one\\\",\"visible\":\"ok\"}"#);

        assert!(line.contains(r#"\"password\":\"[REDACTED]"#));
        assert!(line.contains("visible"));
        assert!(!line.contains("one"));
    }

    #[test]
    fn ignores_empty_secret_keys() {
        let line = Redactor::new(["", "  ", "token"]).redact_value("token=abc visible=value");

        assert!(line.contains("token=[REDACTED]"));
        assert!(line.contains("visible=value"));
        assert!(!line.contains("abc"));
    }
}
