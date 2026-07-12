use serde::{Deserialize, Deserializer, Serialize, de};

use super::{
    event::{Event, EventId, EventKind, EventSource},
    message::{Message, MessageAuthor},
    session::SessionId,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QueuedMessage {
    event_id: EventId,
    session_id: SessionId,
    message: Message,
    received_at_unix: u64,
    enqueued_at_unix: u64,
}

impl QueuedMessage {
    pub(crate) fn from_event(event: &Event, enqueued_at_unix: u64) -> Result<Self, String> {
        if event.source != EventSource::Platform {
            return Err(format!(
                "inbound message event {} must come from a platform",
                event.id
            ));
        }

        let EventKind::MessageReceived { message } = &event.kind else {
            return Err(format!(
                "inbound event {} does not contain a received message",
                event.id
            ));
        };
        let session_id = message.session_id.clone().ok_or_else(|| {
            format!(
                "inbound message event {} must reference a session before queueing",
                event.id
            )
        })?;
        let queued = Self {
            event_id: event.id.clone(),
            session_id,
            message: message.clone(),
            received_at_unix: event.received_at_unix,
            enqueued_at_unix: enqueued_at_unix.max(event.received_at_unix),
        };
        queued.validate()?;
        Ok(queued)
    }

    pub fn event_id(&self) -> &EventId {
        &self.event_id
    }

    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    pub fn message(&self) -> &Message {
        &self.message
    }

    pub fn received_at_unix(&self) -> u64 {
        self.received_at_unix
    }

    pub fn enqueued_at_unix(&self) -> u64 {
        self.enqueued_at_unix
    }

    pub(crate) fn rebase_enqueued_at_unix(&mut self, minimum: u64) {
        self.enqueued_at_unix = self.enqueued_at_unix.max(minimum);
    }

    pub(crate) fn has_same_identity(&self, other: &Self) -> bool {
        self.event_id == other.event_id
            && self.session_id == other.session_id
            && self.message == other.message
            && self.received_at_unix == other.received_at_unix
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.message.author != MessageAuthor::User {
            return Err(format!(
                "queued message event {} must contain a user-authored message",
                self.event_id
            ));
        }

        if self.message.session_id.as_ref() != Some(&self.session_id) {
            return Err(format!(
                "queued message event {} does not match session {}",
                self.event_id, self.session_id
            ));
        }

        if self.enqueued_at_unix < self.received_at_unix {
            return Err(format!(
                "queued message event {} has enqueued_at_unix before received_at_unix",
                self.event_id
            ));
        }

        Ok(())
    }
}

impl<'de> Deserialize<'de> for QueuedMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct QueuedMessageWire {
            event_id: EventId,
            session_id: SessionId,
            message: Message,
            received_at_unix: u64,
            enqueued_at_unix: u64,
        }

        let wire = QueuedMessageWire::deserialize(deserializer)?;
        let queued = Self {
            event_id: wire.event_id,
            session_id: wire.session_id,
            message: wire.message,
            received_at_unix: wire.received_at_unix,
            enqueued_at_unix: wire.enqueued_at_unix,
        };
        queued.validate().map_err(de::Error::custom)?;
        Ok(queued)
    }
}
