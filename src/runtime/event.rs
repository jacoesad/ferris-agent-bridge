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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InboundEventRecord {
    id: EventId,
    received_at_unix: u64,
    recorded_at_unix: u64,
}

impl InboundEventRecord {
    pub fn new(id: EventId, received_at_unix: u64, recorded_at_unix: u64) -> Result<Self, String> {
        let record = Self {
            id,
            received_at_unix,
            recorded_at_unix,
        };
        record.validate()?;
        Ok(record)
    }

    pub fn from_event(event: &Event, recorded_at_unix: u64) -> Result<Self, String> {
        Self::new(
            event.id.clone(),
            event.received_at_unix,
            recorded_at_unix.max(event.received_at_unix),
        )
    }

    pub fn id(&self) -> &EventId {
        &self.id
    }

    pub fn received_at_unix(&self) -> u64 {
        self.received_at_unix
    }

    pub fn recorded_at_unix(&self) -> u64 {
        self.recorded_at_unix
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.recorded_at_unix < self.received_at_unix {
            return Err(format!(
                "inbound event {} has recorded_at_unix before received_at_unix",
                self.id
            ));
        }

        Ok(())
    }
}

impl<'de> Deserialize<'de> for InboundEventRecord {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct InboundEventRecordWire {
            id: EventId,
            received_at_unix: u64,
            recorded_at_unix: u64,
        }

        let wire = InboundEventRecordWire::deserialize(deserializer)?;
        Self::new(wire.id, wire.received_at_unix, wire.recorded_at_unix).map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboundEventRecordStatus {
    Recorded,
    Duplicate,
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
    use super::{Event, EventId, EventKind, EventSource, InboundEventRecord};
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

    #[test]
    fn inbound_event_record_round_trips_as_json() {
        let event = event_fixture("evt_1", 10);
        let record = InboundEventRecord::from_event(&event, 12).expect("record should be valid");

        let encoded = serde_json::to_string(&record).expect("record should serialize");
        let decoded: InboundEventRecord =
            serde_json::from_str(&encoded).expect("record should decode");

        assert_eq!(decoded, record);
        assert_eq!(decoded.id().as_str(), "evt_1");
        assert_eq!(decoded.received_at_unix(), 10);
        assert_eq!(decoded.recorded_at_unix(), 12);
    }

    #[test]
    fn inbound_event_record_does_not_record_before_received_at() {
        let event = event_fixture("evt_1", 10);
        let record = InboundEventRecord::from_event(&event, 8)
            .expect("recorded time should be clamped to event receive time");

        assert_eq!(record.recorded_at_unix(), 10);
    }

    #[test]
    fn rejects_invalid_inbound_event_record_time_order_from_json() {
        let err = serde_json::from_str::<InboundEventRecord>(
            r#"{
                "id": "evt_1",
                "received_at_unix": 10,
                "recorded_at_unix": 9
            }"#,
        )
        .expect_err("recorded before received should fail");

        assert!(
            err.to_string()
                .contains("recorded_at_unix before received_at_unix")
        );
    }

    fn event_fixture(id: &str, received_at_unix: u64) -> Event {
        let message = Message::user_text("msg_1", None, "hello", 1).expect("valid message");
        Event::new(
            EventId::new(id).expect("valid id"),
            EventSource::Platform,
            EventKind::MessageReceived { message },
            received_at_unix,
        )
    }
}
