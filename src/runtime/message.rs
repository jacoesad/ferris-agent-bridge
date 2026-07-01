use std::fmt;

use serde::{Deserialize, Serialize};

use super::session::SessionId;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageAuthor {
    User,
    Agent,
    System,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MessageContent {
    Text { text: String },
    Markdown { markdown: String },
}

impl MessageContent {
    pub fn text(value: impl Into<String>) -> Result<Self, String> {
        let text = value.into();

        if text.trim().is_empty() {
            return Err("message text must not be empty".to_owned());
        }

        Ok(Self::Text { text })
    }

    pub fn markdown(value: impl Into<String>) -> Result<Self, String> {
        let markdown = value.into();

        if markdown.trim().is_empty() {
            return Err("message markdown must not be empty".to_owned());
        }

        Ok(Self::Markdown { markdown })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    use super::Message;

    #[test]
    fn message_round_trips_as_json() {
        let message = Message::user_text("msg_1", None, "hello", 1).expect("valid message");
        let encoded = serde_json::to_string(&message).expect("message should serialize");
        let decoded: Message = serde_json::from_str(&encoded).expect("message should decode");

        assert_eq!(decoded, message);
    }
}
