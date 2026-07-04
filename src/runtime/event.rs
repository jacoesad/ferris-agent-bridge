use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, de};

use super::{message::Message, session::SessionId};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct EventId(String);

impl EventId {
    pub fn new(value: impl Into<String>) -> Result<Self, String> {
        let value = value.into();

        if value.trim().is_empty() {
            return Err("event id must not be empty".to_owned());
        }

        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for EventId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventSource {
    Platform,
    Agent,
    Runtime,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EventKind {
    MessageReceived { message: Message },
    SessionStarted { session_id: SessionId },
    RuntimeNotice { message: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    pub id: EventId,
    pub source: EventSource,
    pub kind: EventKind,
    pub received_at_unix: u64,
}

impl Event {
    pub fn new(id: EventId, source: EventSource, kind: EventKind, received_at_unix: u64) -> Self {
        Self {
            id,
            source,
            kind,
            received_at_unix,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Event, EventId, EventKind, EventSource};
    use crate::runtime::message::Message;

    #[test]
    fn event_round_trips_as_json() {
        let message = Message::user_text("msg_1", None, "hello", 1).expect("valid message");
        let event = Event::new(
            EventId::new("evt_1").expect("valid id"),
            EventSource::Platform,
            EventKind::MessageReceived { message },
            2,
        );

        let encoded = serde_json::to_string(&event).expect("event should serialize");
        let decoded: Event = serde_json::from_str(&encoded).expect("event should decode");

        assert_eq!(decoded, event);
    }

    #[test]
    fn rejects_invalid_event_ids_from_json() {
        let err =
            serde_json::from_str::<EventId>("\"\"").expect_err("empty event id json should fail");

        assert!(err.to_string().contains("event id must not be empty"));
    }
}
