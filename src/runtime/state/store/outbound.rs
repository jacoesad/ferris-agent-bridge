use crate::runtime::outbox::{OutboundDeliveryEnqueueStatus, OutboundDeliveryRecord};

use super::StateStore;

impl StateStore {
    pub fn enqueue_outbound_delivery(
        &self,
        delivery: OutboundDeliveryRecord,
    ) -> Result<OutboundDeliveryEnqueueStatus, String> {
        let _guard = self.lock_write()?;
        let mut state = self.load()?;
        let enqueue_status = state.enqueue_outbound_delivery(delivery)?;

        if enqueue_status == OutboundDeliveryEnqueueStatus::Queued {
            self.save_unlocked(&state)?;
        }

        Ok(enqueue_status)
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
        message::{Message, MessageAuthor, MessageContent, MessageId},
        outbox::{OutboundDeliveryEnqueueStatus, OutboundDeliveryId, OutboundDeliveryRecord},
        session::{Session, SessionId, SessionScope},
        state::RuntimeState,
    };

    static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn state_store_round_trips_outbound_deliveries() {
        let path = test_path("state-outbound-delivery-round-trip").join("runtime.state.json");
        let store = StateStore::new(&path);
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session = Session::new(scope);
        let session_id = session.id().clone();
        let delivery = outbound_delivery_fixture("out_1", session_id, 10);
        let mut state = RuntimeState::new();

        state.upsert_session(session);
        assert_eq!(
            state
                .enqueue_outbound_delivery(delivery.clone())
                .expect("delivery should enqueue"),
            OutboundDeliveryEnqueueStatus::Queued
        );

        store.save(&state).expect("state should save");
        let loaded = store.load().expect("state should load");

        assert_eq!(
            loaded
                .outbound_delivery(delivery.id())
                .expect("outbound delivery should exist"),
            &delivery
        );
        assert_eq!(loaded, state);
    }
    #[test]
    fn state_store_enqueues_outbound_delivery_before_returning_status() {
        let path = test_path("state-outbound-delivery-before-send").join("runtime.state.json");
        let store = StateStore::new(&path);
        let session = Session::new(SessionScope::new("lark", "chat:oc_123").expect("valid scope"));
        let session_id = session.id().clone();
        let delivery = outbound_delivery_fixture("out_1", session_id, 10);
        let mut initial = RuntimeState::new();
        initial.upsert_session(session);
        store.save(&initial).expect("initial state should save");

        let status = store
            .enqueue_outbound_delivery(delivery.clone())
            .expect("queued delivery should return a status that may be sent");

        assert_eq!(status, OutboundDeliveryEnqueueStatus::Queued);

        let loaded = store.load().expect("state should load");
        assert_eq!(
            loaded.outbound_delivery(delivery.id()),
            Some(&delivery),
            "status must only be returned after the outbound delivery is persisted"
        );
    }
    #[test]
    fn state_store_returns_duplicate_status_after_existing_outbound_delivery() {
        let path = test_path("state-outbound-delivery-duplicate").join("runtime.state.json");
        let store = StateStore::new(&path);
        let session = Session::new(SessionScope::new("lark", "chat:oc_123").expect("valid scope"));
        let session_id = session.id().clone();
        let delivery = outbound_delivery_fixture("out_1", session_id, 10);
        let mut initial = RuntimeState::new();
        initial.upsert_session(session);
        store.save(&initial).expect("initial state should save");

        let first = store
            .enqueue_outbound_delivery(delivery.clone())
            .expect("first delivery should persist");
        assert_eq!(first, OutboundDeliveryEnqueueStatus::Queued);

        let duplicate = store
            .enqueue_outbound_delivery(delivery.clone())
            .expect("duplicate delivery should still return a status");
        assert_eq!(duplicate, OutboundDeliveryEnqueueStatus::Duplicate);

        let loaded = store.load().expect("state should load");
        assert_eq!(loaded.outbound_deliveries(), &[delivery]);
    }
    #[test]
    fn state_store_serializes_outbound_enqueue_across_same_path_handles() {
        let path =
            test_path("state-outbound-delivery-concurrent-enqueue").join("runtime.state.json");
        let store = StateStore::new(&path);
        let session = Session::new(SessionScope::new("lark", "chat:oc_123").expect("valid scope"));
        let session_id = session.id().clone();
        let mut initial = RuntimeState::new();
        initial.upsert_session(session);
        store.save(&initial).expect("initial state should save");
        let worker_count = 16;
        let barrier = Arc::new(Barrier::new(worker_count));
        let mut workers = Vec::new();

        for index in 0..worker_count {
            let worker_store = StateStore::new(&path);
            let worker_barrier = barrier.clone();
            let worker_session_id = session_id.clone();
            workers.push(thread::spawn(move || {
                let delivery = outbound_delivery_fixture(
                    &format!("out_{index}"),
                    worker_session_id,
                    10 + index as u64,
                );
                let delivery_id = delivery.id().clone();
                worker_barrier.wait();

                let status = worker_store
                    .enqueue_outbound_delivery(delivery)
                    .expect("concurrent delivery should persist");

                (delivery_id, status)
            }));
        }

        let mut delivery_ids = Vec::new();
        for worker in workers {
            let (delivery_id, status) = worker.join().expect("worker should not panic");
            assert_eq!(status, OutboundDeliveryEnqueueStatus::Queued);
            delivery_ids.push(delivery_id);
        }

        let loaded = store.load().expect("state should load");
        assert_eq!(loaded.outbound_deliveries().len(), worker_count);
        for delivery_id in delivery_ids {
            assert!(
                loaded.outbound_delivery(&delivery_id).is_some(),
                "queued outbound delivery {delivery_id} must remain durable"
            );
        }
    }
    #[test]
    fn state_store_save_preserves_outbound_deliveries_from_stale_snapshot() {
        let path = test_path("state-stale-save-preserves-outbound").join("runtime.state.json");
        let stale_writer = StateStore::new(&path);
        let enqueue_writer = StateStore::new(&path);
        let session = Session::new(SessionScope::new("lark", "chat:oc_123").expect("valid scope"));
        let session_id = session.id().clone();
        let delivery = outbound_delivery_fixture("out_1", session_id, 10);
        let mut initial = RuntimeState::new();
        initial.upsert_session(session);
        stale_writer
            .save(&initial)
            .expect("initial state should save");
        let stale_snapshot = stale_writer.load().expect("state should load");

        let status = enqueue_writer
            .enqueue_outbound_delivery(delivery.clone())
            .expect("delivery should persist before send");
        assert_eq!(status, OutboundDeliveryEnqueueStatus::Queued);

        stale_writer
            .save(&stale_snapshot)
            .expect("stale save should preserve queued outbound deliveries");

        let loaded = StateStore::new(&path).load().expect("state should load");
        assert!(
            loaded.outbound_delivery(delivery.id()).is_some(),
            "stale save must not erase a queued outbound delivery"
        );
    }
    #[test]
    fn state_store_stale_save_fails_closed_on_conflicting_outbound_delivery() {
        let path =
            test_path("state-stale-save-conflicting-outbound-delivery").join("runtime.state.json");
        let stale_writer = StateStore::new(&path);
        let enqueue_writer = StateStore::new(&path);
        let session = Session::new(SessionScope::new("lark", "chat:oc_123").expect("valid scope"));
        let session_id = session.id().clone();
        let delivery = outbound_delivery_fixture("out_1", session_id.clone(), 10);
        let conflicting_delivery = outbound_delivery_fixture("out_1", session_id, 11);
        let mut initial = RuntimeState::new();
        initial.upsert_session(session);
        stale_writer
            .save(&initial)
            .expect("initial state should save");
        let mut stale_snapshot = stale_writer.load().expect("state should load");
        assert_eq!(
            stale_snapshot
                .enqueue_outbound_delivery(conflicting_delivery.clone())
                .expect("conflicting stale delivery should enqueue in the stale snapshot"),
            OutboundDeliveryEnqueueStatus::Queued
        );

        let status = enqueue_writer
            .enqueue_outbound_delivery(delivery.clone())
            .expect("current delivery should persist before send");
        assert_eq!(status, OutboundDeliveryEnqueueStatus::Queued);

        let err = stale_writer
            .save(&stale_snapshot)
            .expect_err("stale save with a conflicting outbound delivery should fail closed");

        assert!(err.contains("conflicting outbound delivery out_1"));
        let loaded = StateStore::new(&path).load().expect("state should load");
        assert_eq!(loaded.outbound_delivery(delivery.id()), Some(&delivery));
        assert_ne!(
            loaded.outbound_delivery(delivery.id()),
            Some(&conflicting_delivery)
        );
    }
    #[test]
    fn state_store_stale_save_fails_closed_when_outbound_session_is_missing() {
        let path =
            test_path("state-stale-save-missing-outbound-session").join("runtime.state.json");
        let stale_writer = StateStore::new(&path);
        let enqueue_writer = StateStore::new(&path);
        let stale_snapshot = stale_writer.load().expect("missing state should load");
        let session = Session::new(SessionScope::new("lark", "chat:oc_123").expect("valid scope"));
        let session_id = session.id().clone();
        let delivery = outbound_delivery_fixture("out_1", session_id.clone(), 10);
        let mut current = RuntimeState::new();
        current.upsert_session(session);
        enqueue_writer
            .save(&current)
            .expect("current session state should save");
        let status = enqueue_writer
            .enqueue_outbound_delivery(delivery.clone())
            .expect("delivery should persist before send");
        assert_eq!(status, OutboundDeliveryEnqueueStatus::Queued);

        let err = stale_writer
            .save(&stale_snapshot)
            .expect_err("stale save without the referenced session should fail closed");

        assert!(err.contains("references unknown session"));
        let loaded = StateStore::new(&path).load().expect("state should load");
        assert!(loaded.session(&session_id).is_some());
        assert_eq!(loaded.outbound_delivery(delivery.id()), Some(&delivery));
    }
    #[test]
    #[cfg(unix)]
    fn state_store_does_not_return_status_when_outbound_enqueue_fails() {
        use std::os::unix::fs::PermissionsExt;

        let dir = test_path("state-outbound-delivery-persist-failure");
        let path = dir.join("runtime.state.json");
        let store = StateStore::new(&path);
        let session = Session::new(SessionScope::new("lark", "chat:oc_123").expect("valid scope"));
        let session_id = session.id().clone();
        let delivery = outbound_delivery_fixture("out_1", session_id, 10);
        let mut initial = RuntimeState::new();
        initial.upsert_session(session);

        store.save(&initial).expect("initial state should save");
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o500))
            .expect("fixture permissions should be set");

        let result = store.enqueue_outbound_delivery(delivery);

        fs::set_permissions(&dir, fs::Permissions::from_mode(0o700))
            .expect("fixture permissions should be restored");
        let err = result.expect_err("failed persistence must not return a sendable status");
        assert!(err.contains("failed to save runtime state"));

        let loaded = store.load().expect("state should still load");
        assert!(
            loaded.outbound_deliveries().is_empty(),
            "failed persistence must not leave a sendable outbound delivery on disk"
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
    fn outbound_delivery_fixture(
        id: &str,
        session_id: SessionId,
        created_at_unix: u64,
    ) -> OutboundDeliveryRecord {
        let message = Message::new(
            MessageId::new(format!("msg_{id}")).expect("valid message id"),
            Some(session_id.clone()),
            MessageAuthor::Agent,
            MessageContent::text("hello").expect("valid text"),
            created_at_unix,
        );

        OutboundDeliveryRecord::new(
            OutboundDeliveryId::new(id).expect("valid outbound id"),
            session_id,
            message,
            created_at_unix,
        )
        .expect("valid outbound delivery")
    }
}
