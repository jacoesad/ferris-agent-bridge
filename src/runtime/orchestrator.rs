use crate::adapter::{ImAdapter, InboundDelivery, InboundDeliveryAcknowledgement};

use super::{
    error::RuntimeError,
    event::{EventId, InboundEventRecordStatus},
    state::StateStore,
};

#[derive(Debug, Clone)]
pub struct RuntimeOrchestrator {
    state_store: StateStore,
}

impl RuntimeOrchestrator {
    pub fn new(state_store: StateStore) -> Self {
        Self { state_store }
    }

    pub fn state_store(&self) -> &StateStore {
        &self.state_store
    }

    pub fn accept_inbound_delivery<A>(
        &self,
        im_adapter: &mut A,
        delivery: InboundDelivery,
    ) -> Result<InboundDeliveryOutcome, RuntimeError>
    where
        A: ImAdapter,
    {
        let event_id = delivery.event().id.clone();
        let record_status = self
            .state_store
            .persist_inbound_event(delivery.event())
            .map_err(|err| {
                RuntimeError::recoverable(format!(
                    "failed to persist inbound event {event_id} before acknowledgement: {err}"
                ))
            })?;

        let acknowledgement = InboundDeliveryAcknowledgement::new(
            event_id.clone(),
            delivery.ack_token().clone(),
            record_status,
        );
        im_adapter
            .acknowledge_inbound_delivery(&acknowledgement)
            .map_err(|err| {
                RuntimeError::recoverable(format!(
                    "failed to acknowledge inbound event {event_id} after persistence: {err}"
                ))
            })?;

        Ok(InboundDeliveryOutcome::new(event_id, record_status))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundDeliveryOutcome {
    event_id: EventId,
    record_status: InboundEventRecordStatus,
}

impl InboundDeliveryOutcome {
    pub(crate) fn new(event_id: EventId, record_status: InboundEventRecordStatus) -> Self {
        Self {
            event_id,
            record_status,
        }
    }

    pub fn event_id(&self) -> &EventId {
        &self.event_id
    }

    pub fn record_status(&self) -> InboundEventRecordStatus {
        self.record_status
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    use crate::{
        adapter::{
            ImAdapter, InboundDelivery, InboundDeliveryAckToken, InboundDeliveryAcknowledgement,
            OutboundDeliveryFailure,
        },
        runtime::{
            error::ErrorClass,
            event::{Event, EventId, EventKind, EventSource, InboundEventRecordStatus},
            message::Message,
            outbox::OutboundDeliveryAttempt,
            state::StateStore,
        },
    };

    use super::RuntimeOrchestrator;

    static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn accept_inbound_delivery_persists_before_acknowledging() {
        let store =
            StateStore::new(test_path("accept-inbound-delivery").join("runtime.state.json"));
        let event = event_fixture("evt_1");
        let mut adapter = RecordingImAdapter::with_state_check(store.clone());
        let orchestrator = RuntimeOrchestrator::new(store.clone());

        let outcome = orchestrator
            .accept_inbound_delivery(&mut adapter, delivery_fixture(event.clone(), "ack_1"))
            .expect("persisted delivery should be acknowledged");

        assert_eq!(outcome.event_id(), &event.id);
        assert_eq!(outcome.record_status(), InboundEventRecordStatus::Recorded);
        assert_eq!(adapter.acknowledgements.len(), 1);
        assert_eq!(adapter.acknowledgements[0].event_id(), &event.id);
        assert_eq!(
            adapter.acknowledgements[0].record_status(),
            InboundEventRecordStatus::Recorded
        );

        let state = store.load().expect("state should load");
        assert!(state.has_inbound_event(&event.id));
    }

    #[test]
    fn accept_inbound_delivery_acknowledges_duplicate_events() {
        let store =
            StateStore::new(test_path("accept-duplicate-delivery").join("runtime.state.json"));
        let event = event_fixture("evt_1");
        store
            .persist_inbound_event(&event)
            .expect("first event should persist");
        let mut adapter = RecordingImAdapter::default();
        let orchestrator = RuntimeOrchestrator::new(store.clone());

        let outcome = orchestrator
            .accept_inbound_delivery(&mut adapter, delivery_fixture(event.clone(), "ack_retry"))
            .expect("duplicate delivery should still be acknowledged");

        assert_eq!(outcome.record_status(), InboundEventRecordStatus::Duplicate);
        assert_eq!(adapter.acknowledgements.len(), 1);
        assert_eq!(
            adapter.acknowledgements[0].record_status(),
            InboundEventRecordStatus::Duplicate
        );

        let state = store.load().expect("state should load");
        assert_eq!(state.inbound_events().len(), 1);
    }

    #[test]
    fn accept_inbound_delivery_does_not_acknowledge_when_persistence_fails() {
        let parent_file = test_path("persistence-failure").join("state-parent");
        fs::create_dir_all(parent_file.parent().expect("test path should have parent"))
            .expect("test parent should be created");
        fs::write(&parent_file, "not a directory").expect("test parent file should be created");
        let store = StateStore::new(parent_file.join("runtime.state.json"));
        let mut adapter = RecordingImAdapter::default();
        let orchestrator = RuntimeOrchestrator::new(store);

        let err = orchestrator
            .accept_inbound_delivery(
                &mut adapter,
                delivery_fixture(event_fixture("evt_1"), "ack_1"),
            )
            .expect_err("persistence failure must not be acknowledged");

        assert_eq!(err.class(), ErrorClass::Recoverable);
        assert!(err.message().contains("before acknowledgement"));
        assert!(adapter.acknowledgements.is_empty());
    }

    #[test]
    fn accept_inbound_delivery_reports_acknowledgement_failure_after_persistence() {
        let store = StateStore::new(test_path("ack-failure").join("runtime.state.json"));
        let event = event_fixture("evt_1");
        let mut adapter = RecordingImAdapter {
            fail_ack: true,
            ..RecordingImAdapter::default()
        };
        let orchestrator = RuntimeOrchestrator::new(store.clone());

        let err = orchestrator
            .accept_inbound_delivery(&mut adapter, delivery_fixture(event.clone(), "ack_1"))
            .expect_err("acknowledgement failure should be reported");

        assert_eq!(err.class(), ErrorClass::Recoverable);
        assert!(err.message().contains("after persistence"));
        assert_eq!(adapter.acknowledgements.len(), 1);

        let state = store.load().expect("state should load");
        assert!(state.has_inbound_event(&event.id));
    }

    #[derive(Default)]
    struct RecordingImAdapter {
        acknowledgements: Vec<InboundDeliveryAcknowledgement>,
        fail_ack: bool,
        state_check: Option<StateStore>,
    }

    impl RecordingImAdapter {
        fn with_state_check(state_store: StateStore) -> Self {
            Self {
                state_check: Some(state_store),
                ..Self::default()
            }
        }
    }

    impl ImAdapter for RecordingImAdapter {
        fn acknowledge_inbound_delivery(
            &mut self,
            acknowledgement: &InboundDeliveryAcknowledgement,
        ) -> Result<(), String> {
            if let Some(state_store) = &self.state_check {
                let state = state_store.load()?;
                if !state.has_inbound_event(acknowledgement.event_id()) {
                    return Err(format!(
                        "acknowledged inbound event {} before persistence",
                        acknowledgement.event_id()
                    ));
                }
            }

            self.acknowledgements.push(acknowledgement.clone());

            if self.fail_ack {
                return Err("transport acknowledgement failed".to_owned());
            }

            Ok(())
        }

        fn deliver_outbound_message(
            &mut self,
            _attempt: &OutboundDeliveryAttempt,
        ) -> Result<(), OutboundDeliveryFailure> {
            Ok(())
        }
    }

    fn delivery_fixture(event: Event, ack_token: &str) -> InboundDelivery {
        InboundDelivery::new(
            event,
            InboundDeliveryAckToken::new(ack_token).expect("valid ack token"),
        )
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

    fn test_path(label: &str) -> PathBuf {
        let sequence = NEXT_DIR.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "ferris-agent-bridge-runtime-orchestrator-test-{}-{label}-{sequence}",
            std::process::id()
        ))
    }
}
