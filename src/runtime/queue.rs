use std::collections::BTreeMap;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageBatch {
    session_id: SessionId,
    messages: Vec<QueuedMessage>,
    ready_at_unix: u64,
}

impl MessageBatch {
    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    pub fn messages(&self) -> &[QueuedMessage] {
        &self.messages
    }

    pub fn ready_at_unix(&self) -> u64 {
        self.ready_at_unix
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageQueuePoll {
    Ready(MessageBatch),
    Waiting { next_ready_at_unix: Option<u64> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageQueuePolicy {
    debounce_secs: u64,
    max_batch_size: usize,
}

impl MessageQueuePolicy {
    pub fn new(debounce_secs: u64, max_batch_size: usize) -> Result<Self, String> {
        if max_batch_size == 0 {
            return Err("message queue max_batch_size must be greater than zero".to_owned());
        }

        Ok(Self {
            debounce_secs,
            max_batch_size,
        })
    }

    pub fn debounce_secs(&self) -> u64 {
        self.debounce_secs
    }

    pub fn max_batch_size(&self) -> usize {
        self.max_batch_size
    }

    pub(crate) fn poll(
        &self,
        queued_messages: &[QueuedMessage],
        now_unix: u64,
    ) -> MessageQueuePoll {
        let mut by_session: BTreeMap<&SessionId, Vec<&QueuedMessage>> = BTreeMap::new();
        for queued in queued_messages {
            by_session
                .entry(queued.session_id())
                .or_default()
                .push(queued);
        }

        let mut ready = Vec::new();
        let mut next_ready_at_unix = None;
        for (session_id, messages) in by_session {
            let ready_at_unix = if messages.len() >= self.max_batch_size {
                messages[self.max_batch_size - 1].enqueued_at_unix()
            } else {
                messages
                    .last()
                    .expect("session group must contain a message")
                    .enqueued_at_unix()
                    .saturating_add(self.debounce_secs)
            };

            if ready_at_unix <= now_unix {
                ready.push((ready_at_unix, session_id, messages));
            } else {
                next_ready_at_unix = Some(
                    next_ready_at_unix
                        .map_or(ready_at_unix, |current: u64| current.min(ready_at_unix)),
                );
            }
        }

        let Some((ready_at_unix, session_id, messages)) = ready
            .into_iter()
            .min_by(|left, right| (left.0, left.1).cmp(&(right.0, right.1)))
        else {
            return MessageQueuePoll::Waiting { next_ready_at_unix };
        };
        let messages = messages
            .into_iter()
            .take(self.max_batch_size)
            .cloned()
            .collect();

        MessageQueuePoll::Ready(MessageBatch {
            session_id: session_id.clone(),
            messages,
            ready_at_unix,
        })
    }
}

impl Default for MessageQueuePolicy {
    fn default() -> Self {
        Self {
            debounce_secs: 2,
            max_batch_size: 20,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::MessageQueuePolicy;

    #[test]
    fn message_queue_policy_rejects_empty_batches() {
        assert!(MessageQueuePolicy::new(1, 0).is_err());
    }

    #[test]
    fn message_queue_policy_exposes_bounds() {
        let policy = MessageQueuePolicy::new(3, 8).expect("valid policy");

        assert_eq!(policy.debounce_secs(), 3);
        assert_eq!(policy.max_batch_size(), 8);
    }
}
