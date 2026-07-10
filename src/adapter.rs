use std::{error::Error, fmt};

use crate::runtime::{
    event::{Event, EventId, InboundEventRecordStatus},
    outbox::OutboundDeliveryAttempt,
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InboundDeliveryAckToken(String);

impl InboundDeliveryAckToken {
    pub fn new(value: impl Into<String>) -> Result<Self, String> {
        let value = value.into();

        if value.trim().is_empty() {
            return Err("inbound delivery acknowledgement token must not be empty".to_owned());
        }

        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for InboundDeliveryAckToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundDelivery {
    event: Event,
    ack_token: InboundDeliveryAckToken,
}

impl InboundDelivery {
    pub fn new(event: Event, ack_token: InboundDeliveryAckToken) -> Self {
        Self { event, ack_token }
    }

    pub fn event(&self) -> &Event {
        &self.event
    }

    pub fn ack_token(&self) -> &InboundDeliveryAckToken {
        &self.ack_token
    }

    pub fn into_event(self) -> Event {
        self.event
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundDeliveryAcknowledgement {
    event_id: EventId,
    ack_token: InboundDeliveryAckToken,
    record_status: InboundEventRecordStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboundDeliveryFailureKind {
    Retryable,
    Uncertain,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundDeliveryFailure {
    kind: OutboundDeliveryFailureKind,
    message: String,
}

impl OutboundDeliveryFailure {
    pub fn retryable(message: impl Into<String>) -> Self {
        Self::new(OutboundDeliveryFailureKind::Retryable, message)
    }

    pub fn uncertain(message: impl Into<String>) -> Self {
        Self::new(OutboundDeliveryFailureKind::Uncertain, message)
    }

    pub fn kind(&self) -> OutboundDeliveryFailureKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    fn new(kind: OutboundDeliveryFailureKind, message: impl Into<String>) -> Self {
        let message = message.into();
        let message = if message.trim().is_empty() {
            match kind {
                OutboundDeliveryFailureKind::Retryable => {
                    "outbound adapter reported a retryable failure without an error message"
                        .to_owned()
                }
                OutboundDeliveryFailureKind::Uncertain => {
                    "outbound adapter reported an uncertain outcome without an error message"
                        .to_owned()
                }
            }
        } else {
            message
        };

        Self { kind, message }
    }
}

impl fmt::Display for OutboundDeliveryFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

impl Error for OutboundDeliveryFailure {}

impl InboundDeliveryAcknowledgement {
    pub(crate) fn new(
        event_id: EventId,
        ack_token: InboundDeliveryAckToken,
        record_status: InboundEventRecordStatus,
    ) -> Self {
        Self {
            event_id,
            ack_token,
            record_status,
        }
    }

    pub fn event_id(&self) -> &EventId {
        &self.event_id
    }

    pub fn ack_token(&self) -> &InboundDeliveryAckToken {
        &self.ack_token
    }

    pub fn record_status(&self) -> InboundEventRecordStatus {
        self.record_status
    }
}

pub trait ImAdapter {
    fn acknowledge_inbound_delivery(
        &mut self,
        acknowledgement: &InboundDeliveryAcknowledgement,
    ) -> Result<(), String>;

    fn deliver_outbound_message(
        &mut self,
        attempt: &OutboundDeliveryAttempt,
    ) -> Result<(), OutboundDeliveryFailure>;
}

#[cfg(test)]
mod tests {
    use super::{
        InboundDelivery, InboundDeliveryAckToken, InboundDeliveryAcknowledgement,
        InboundEventRecordStatus, OutboundDeliveryFailure, OutboundDeliveryFailureKind,
    };
    use crate::runtime::{
        event::{Event, EventId, EventKind, EventSource},
        message::Message,
    };

    #[test]
    fn rejects_empty_inbound_delivery_ack_tokens() {
        assert!(InboundDeliveryAckToken::new("").is_err());
        assert!(InboundDeliveryAckToken::new("  ").is_err());
        assert_eq!(
            InboundDeliveryAckToken::new("ack_1")
                .expect("valid ack token")
                .as_str(),
            "ack_1"
        );
    }

    #[test]
    fn inbound_delivery_exposes_event_and_ack_token() {
        let event = event_fixture("evt_1");
        let ack_token = InboundDeliveryAckToken::new("ack_1").expect("valid ack token");
        let delivery = InboundDelivery::new(event.clone(), ack_token.clone());

        assert_eq!(delivery.event(), &event);
        assert_eq!(delivery.ack_token(), &ack_token);
        assert_eq!(delivery.into_event(), event);
    }

    #[test]
    fn acknowledgement_carries_event_token_and_record_status() {
        let event_id = EventId::new("evt_1").expect("valid event id");
        let ack_token = InboundDeliveryAckToken::new("ack_1").expect("valid ack token");
        let acknowledgement = InboundDeliveryAcknowledgement::new(
            event_id.clone(),
            ack_token.clone(),
            InboundEventRecordStatus::Duplicate,
        );

        assert_eq!(acknowledgement.event_id(), &event_id);
        assert_eq!(acknowledgement.ack_token(), &ack_token);
        assert_eq!(
            acknowledgement.record_status(),
            InboundEventRecordStatus::Duplicate
        );
    }

    #[test]
    fn outbound_delivery_failures_preserve_retry_safety_classification() {
        let retryable = OutboundDeliveryFailure::retryable("provider rejected the request");
        assert_eq!(retryable.kind(), OutboundDeliveryFailureKind::Retryable);
        assert_eq!(retryable.message(), "provider rejected the request");

        let uncertain = OutboundDeliveryFailure::uncertain("provider response timed out");
        assert_eq!(uncertain.kind(), OutboundDeliveryFailureKind::Uncertain);
        assert_eq!(uncertain.message(), "provider response timed out");
    }

    #[test]
    fn outbound_delivery_failures_normalize_empty_messages() {
        let retryable = OutboundDeliveryFailure::retryable("  ");
        assert!(retryable.message().contains("retryable failure"));

        let uncertain = OutboundDeliveryFailure::uncertain("");
        assert!(uncertain.message().contains("uncertain outcome"));
    }

    fn event_fixture(id: &str) -> Event {
        let message = Message::user_text("msg_1", None, "hello", 1).expect("valid message");
        Event::new(
            EventId::new(id).expect("valid event id"),
            EventSource::Platform,
            EventKind::MessageReceived { message },
            2,
        )
    }
}
