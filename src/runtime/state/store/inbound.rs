use crate::runtime::event::{Event, InboundEventRecordStatus};

use super::StateStore;

impl StateStore {
    pub fn persist_inbound_event(&self, event: &Event) -> Result<InboundEventRecordStatus, String> {
        let _guard = self.lock_write()?;
        let mut state = self.load()?;
        let record_status = state.record_inbound_event(event)?;

        if record_status == InboundEventRecordStatus::Recorded {
            self.save_unlocked(&state)?;
        }

        Ok(record_status)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::{
            Arc, Barrier,
            atomic::{AtomicU64, Ordering},
        },
        thread,
    };

    use super::super::StateStore;
    use crate::runtime::{
        event::{Event, EventId, EventKind, EventSource, InboundEventRecordStatus},
        message::Message,
        persistence::fail_next_write_before_replace,
        state::RuntimeState,
    };

    static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn state_store_round_trips_inbound_event_ledger() {
        let path = test_path("state-inbound-event-round-trip").join("runtime.state.json");
        let store = StateStore::new(&path);
        let event = event_fixture("evt_1", 10);
        let mut state = RuntimeState::new();

        assert_eq!(
            state
                .record_inbound_event(&event)
                .expect("event should record"),
            InboundEventRecordStatus::Recorded
        );

        store.save(&state).expect("state should save");
        let loaded = store.load().expect("state should load");

        let record = loaded
            .inbound_event(&event.id)
            .expect("inbound event record should exist");
        assert_eq!(record.received_at_unix(), 10);
        assert!(record.recorded_at_unix() >= 10);
        assert_eq!(loaded, state);
    }
    #[test]
    fn state_store_persists_inbound_event_before_returning_status() {
        let path = test_path("state-inbound-event-ack-after-persist").join("runtime.state.json");
        let store = StateStore::new(&path);
        let event = event_fixture("evt_1", 10);

        let status = store
            .persist_inbound_event(&event)
            .expect("persisted event should return a status that may be acknowledged");

        assert_eq!(status, InboundEventRecordStatus::Recorded);

        let loaded = store.load().expect("state should load");
        let record = loaded
            .inbound_event(&event.id)
            .expect("status must only be returned after the event is persisted");
        assert_eq!(record.received_at_unix(), 10);
        assert!(record.recorded_at_unix() >= 10);
    }
    #[test]
    fn state_store_returns_duplicate_status_after_existing_record() {
        let path = test_path("state-inbound-event-duplicate-ack").join("runtime.state.json");
        let store = StateStore::new(&path);
        let event = event_fixture("evt_1", 10);

        let first = store
            .persist_inbound_event(&event)
            .expect("first event should persist");
        assert_eq!(first, InboundEventRecordStatus::Recorded);

        let before_duplicate = store.load().expect("state should load");
        let first_record = before_duplicate
            .inbound_event(&event.id)
            .expect("event should be persisted")
            .clone();

        let duplicate = store
            .persist_inbound_event(&event)
            .expect("duplicate event should still return a status that may be acknowledged");

        assert_eq!(duplicate, InboundEventRecordStatus::Duplicate);

        let loaded = store.load().expect("state should load");
        assert_eq!(loaded.inbound_events().len(), 1);
        assert_eq!(loaded.inbound_events()[0], first_record);
    }
    #[test]
    fn state_store_serializes_inbound_event_persistence_across_same_path_handles() {
        let path = test_path("state-inbound-event-concurrent-ack").join("runtime.state.json");
        let store = StateStore::new(&path);
        let worker_count = 16;
        let barrier = Arc::new(Barrier::new(worker_count));
        let mut workers = Vec::new();

        for index in 0..worker_count {
            let worker_store = StateStore::new(&path);
            let worker_barrier = barrier.clone();
            workers.push(thread::spawn(move || {
                let event = event_fixture(&format!("evt_{index}"), 10 + index as u64);
                worker_barrier.wait();

                let status = worker_store
                    .persist_inbound_event(&event)
                    .expect("concurrent event should persist");

                (event.id, status)
            }));
        }

        let mut recorded_event_ids = Vec::new();
        for worker in workers {
            let (event_id, status) = worker.join().expect("worker should not panic");
            assert_eq!(status, InboundEventRecordStatus::Recorded);
            recorded_event_ids.push(event_id);
        }

        let loaded = store.load().expect("state should load");
        assert_eq!(loaded.inbound_events().len(), worker_count);
        for event_id in recorded_event_ids {
            assert!(
                loaded.has_inbound_event(&event_id),
                "acknowledged event {event_id} must remain durable"
            );
        }
    }
    #[test]
    fn state_store_save_preserves_inbound_events_from_stale_snapshot() {
        let path = test_path("state-stale-save-preserves-inbound").join("runtime.state.json");
        let stale_writer = StateStore::new(&path);
        let ack_writer = StateStore::new(&path);
        let stale_snapshot = stale_writer.load().expect("empty state should load");
        let event = event_fixture("evt_1", 10);

        let status = ack_writer
            .persist_inbound_event(&event)
            .expect("event should persist before acknowledgement");
        assert_eq!(status, InboundEventRecordStatus::Recorded);

        stale_writer
            .save(&stale_snapshot)
            .expect("stale save should preserve acknowledged inbound records");

        let loaded = StateStore::new(&path).load().expect("state should load");
        assert!(
            loaded.has_inbound_event(&event.id),
            "stale save must not erase an acknowledged inbound record"
        );
    }
    #[test]
    fn state_store_does_not_return_status_when_inbound_event_persist_fails() {
        let dir = test_path("state-inbound-event-persist-failure");
        let path = dir.join("runtime.state.json");
        let store = StateStore::new(&path);
        let event = event_fixture("evt_1", 10);

        store
            .save(&RuntimeState::new())
            .expect("initial readable state should save");
        fail_next_write_before_replace(store.path());

        let result = store.persist_inbound_event(&event);

        let err = result.expect_err("failed persistence must not return an acknowledgeable status");
        assert!(err.contains("failed to save runtime state"));

        let loaded = store.load().expect("state should still load");
        assert!(
            loaded.inbound_events().is_empty(),
            "failed persistence must not leave an acknowledged inbound event on disk"
        );
    }
    fn test_path(name: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "ferris-agent-bridge-{name}-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).expect("test dir should exist");
        path
    }
    fn event_fixture(id: &str, received_at_unix: u64) -> Event {
        let message = Message::user_text("msg_1", None, "hello", 1).expect("valid message");
        Event::new(
            EventId::new(id).expect("valid event id"),
            EventSource::Platform,
            EventKind::MessageReceived { message },
            received_at_unix,
        )
    }
}
