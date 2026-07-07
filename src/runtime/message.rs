use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

use super::session::SessionId;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct MessageId(String);

impl MessageId {
    pub fn new(value: impl Into<String>) -> Result<Self, String> {
        let value = value.into();

        if value.trim().is_empty() {
            return Err("message id must not be empty".to_owned());
        }

        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for MessageId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageAuthor {
    User,
    Agent,
    System,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageContent(MessageContentKind);

#[derive(Debug, Clone, PartialEq, Eq)]
enum MessageContentKind {
    Text { text: String },
    Markdown { markdown: String },
}

impl Serialize for MessageContent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        #[serde(tag = "type", rename_all = "snake_case")]
        enum MessageContentWire<'a> {
            Text { text: &'a str },
            Markdown { markdown: &'a str },
        }

        match &self.0 {
            MessageContentKind::Text { text } => MessageContentWire::Text { text },
            MessageContentKind::Markdown { markdown } => MessageContentWire::Markdown { markdown },
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for MessageContent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
        enum MessageContentWire {
            Text { text: String },
            Markdown { markdown: String },
        }

        match MessageContentWire::deserialize(deserializer)? {
            MessageContentWire::Text { text } => Self::text(text),
            MessageContentWire::Markdown { markdown } => Self::markdown(markdown),
        }
        .map_err(de::Error::custom)
    }
}

impl MessageContent {
    pub fn text(value: impl Into<String>) -> Result<Self, String> {
        let text = value.into();

        if text.trim().is_empty() {
            return Err("message text must not be empty".to_owned());
        }

        Ok(Self(MessageContentKind::Text { text }))
    }

    pub fn markdown(value: impl Into<String>) -> Result<Self, String> {
        let markdown = value.into();

        if markdown.trim().is_empty() {
            return Err("message markdown must not be empty".to_owned());
        }

        Ok(Self(MessageContentKind::Markdown { markdown }))
    }

    pub fn as_text(&self) -> Option<&str> {
        match &self.0 {
            MessageContentKind::Text { text } => Some(text),
            MessageContentKind::Markdown { .. } => None,
        }
    }

    pub fn as_markdown(&self) -> Option<&str> {
        match &self.0 {
            MessageContentKind::Text { .. } => None,
            MessageContentKind::Markdown { markdown } => Some(markdown),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Message {
    pub id: MessageId,
    pub session_id: Option<SessionId>,
    pub author: MessageAuthor,
    pub content: MessageContent,
    pub created_at_unix: u64,
}

impl Message {
    pub fn new(
        id: MessageId,
        session_id: Option<SessionId>,
        author: MessageAuthor,
        content: MessageContent,
        created_at_unix: u64,
    ) -> Self {
        Self {
            id,
            session_id,
            author,
            content,
            created_at_unix,
        }
    }

    pub fn user_text(
        id: impl Into<String>,
        session_id: Option<SessionId>,
        text: impl Into<String>,
        created_at_unix: u64,
    ) -> Result<Self, String> {
        Ok(Self::new(
            MessageId::new(id)?,
            session_id,
            MessageAuthor::User,
            MessageContent::text(text)?,
            created_at_unix,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{Message, MessageContent, MessageId};

    #[test]
    fn message_round_trips_as_json() {
        let message = Message::user_text("msg_1", None, "hello", 1).expect("valid message");
        let encoded = serde_json::to_string(&message).expect("message should serialize");
        let decoded: Message = serde_json::from_str(&encoded).expect("message should decode");

        assert_eq!(decoded, message);
    }

    #[test]
    fn rejects_invalid_message_ids_from_json() {
        let err = serde_json::from_str::<MessageId>("\"\"")
            .expect_err("empty message id json should fail");

        assert!(err.to_string().contains("message id must not be empty"));
    }

    #[test]
    fn rejects_empty_message_content_from_json() {
        let err = serde_json::from_str::<MessageContent>(r#"{"type":"text","text":""}"#)
            .expect_err("empty message text json should fail");

        assert!(err.to_string().contains("message text must not be empty"));

        let err = serde_json::from_str::<MessageContent>(r#"{"type":"markdown","markdown":"  "}"#)
            .expect_err("empty message markdown json should fail");

        assert!(
            err.to_string()
                .contains("message markdown must not be empty")
        );
    }

    #[test]
    fn rejects_unknown_message_fields_from_json() {
        let err = serde_json::from_str::<Message>(
            r#"{
            "id": "msg_1",
            "session_id": null,
            "author": "agent",
            "content": {"type": "text", "text": "hello"},
            "created_at_unix": 1,
            "future_field": true
        }"#,
        )
        .expect_err("unknown message fields must not be dropped");

        assert!(err.to_string().contains("unknown field `future_field`"));
    }

    #[test]
    fn rejects_unknown_message_content_fields_from_json() {
        let err = serde_json::from_str::<MessageContent>(
            r#"{"type":"text","text":"hello","future_field":true}"#,
        )
        .expect_err("unknown text content fields must not be dropped");

        assert!(err.to_string().contains("unknown field `future_field`"));

        let err = serde_json::from_str::<MessageContent>(
            r#"{"type":"markdown","markdown":"**hello**","future_field":true}"#,
        )
        .expect_err("unknown markdown content fields must not be dropped");

        assert!(err.to_string().contains("unknown field `future_field`"));
    }

    #[test]
    fn message_content_exposes_valid_payloads_without_public_variants() {
        let text = MessageContent::text("hello").expect("valid text");
        let markdown = MessageContent::markdown("**hello**").expect("valid markdown");

        assert_eq!(text.as_text(), Some("hello"));
        assert_eq!(text.as_markdown(), None);
        assert_eq!(markdown.as_text(), None);
        assert_eq!(markdown.as_markdown(), Some("**hello**"));
    }
}
