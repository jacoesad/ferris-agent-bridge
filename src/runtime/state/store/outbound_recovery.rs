use crate::runtime::{
    outbox::OutboundDeliveryStartupRecoveryReport, persistence::confirm_existing_file_durable,
};

use super::StateStore;

impl StateStore {
    pub fn reconcile_outbound_deliveries_at_startup(
        &self,
        recovered_at_unix: u64,
    ) -> Result<OutboundDeliveryStartupRecoveryReport, String> {
        let _guard = self.lock_write()?;
        let mut state = self.load()?;
        let (report, changed) =
            state.reconcile_outbound_deliveries_at_startup(recovered_at_unix)?;
        if changed {
            self.write_unlocked(&state)?;
        } else {
            confirm_existing_file_durable(self.path()).map_err(|err| {
                format!(
                    "failed to confirm runtime state {} durability: {err}",
                    self.path().display()
                )
            })?;
        }

        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc, Barrier,
            atomic::{AtomicU64, Ordering},
        },
        thread,
    };

    use crate::runtime::{
        message::{Message, MessageAuthor, MessageContent, MessageId},
        outbox::{OutboundDeliveryId, OutboundDeliveryRecord, OutboundDeliveryStatus},
        persistence::{
            fail_next_parent_sync, fail_next_write_after_replace, fail_next_write_before_replace,
        },
        session::{Session, SessionId, SessionScope},
        state::RuntimeState,
    };

    use super::StateStore;

    static NEXT_DIR: AtomicU64 = AtomicU64::new(0);
    const FUTURE_UNIX: u64 = 4_102_444_800;

    #[test]
    fn startup_recovery_classifies_ambiguous_outbound_deliveries_without_retrying() {
        let store = state_store_with_session("outbound-recovery-classification");
        let session_id = only_session_id(&store);
        let delivering_id = OutboundDeliveryId::new("out_delivering").expect("valid id");
        let later_delivering_id =
            OutboundDeliveryId::new("out_delivering_later").expect("valid id");
        let uncertain_id = OutboundDeliveryId::new("out_uncertain").expect("valid id");
        let failed_id = OutboundDeliveryId::new("out_failed").expect("valid id");
        let delivered_id = OutboundDeliveryId::new("out_delivered").expect("valid id");
        let pending_id = OutboundDeliveryId::new("out_pending").expect("valid id");

        enqueue(&store, &delivering_id, &session_id, FUTURE_UNIX);
        store
            .claim_next_outbound_delivery(FUTURE_UNIX + 10)
            .expect("delivery should claim");

        enqueue(&store, &later_delivering_id, &session_id, FUTURE_UNIX + 11);
        store
            .claim_next_outbound_delivery(FUTURE_UNIX + 12)
            .expect("later delivery should claim");

        enqueue(&store, &uncertain_id, &session_id, FUTURE_UNIX + 20);
        store
            .claim_next_outbound_delivery(FUTURE_UNIX + 21)
            .expect("delivery should claim");
        store
            .mark_outbound_delivery_uncertain(
                &uncertain_id,
                FUTURE_UNIX + 22,
                "provider acceptance is unknown",
            )
            .expect("delivery should become uncertain");

        enqueue(&store, &delivered_id, &session_id, FUTURE_UNIX + 30);
        store
            .claim_next_outbound_delivery(FUTURE_UNIX + 31)
            .expect("delivery should claim");
        store
            .mark_outbound_delivery_delivered(&delivered_id, FUTURE_UNIX + 32)
            .expect("delivery should complete");

        enqueue(&store, &failed_id, &session_id, FUTURE_UNIX + 40);
        store
            .claim_next_outbound_delivery(FUTURE_UNIX + 41)
            .expect("delivery should claim");
        store
            .mark_outbound_delivery_failed(
                &failed_id,
                FUTURE_UNIX + 42,
                "provider rejected request",
            )
            .expect("delivery should fail");

        enqueue(&store, &pending_id, &session_id, FUTURE_UNIX + 50);
        let before = store.load().expect("fixture should load");
        let delivering_before = before
            .outbound_delivery(&delivering_id)
            .expect("delivering record should exist")
            .clone();
        let later_delivering_before = before
            .outbound_delivery(&later_delivering_id)
            .expect("later delivering record should exist")
            .clone();
        let uncertain_before = before
            .outbound_delivery(&uncertain_id)
            .expect("uncertain record should exist")
            .clone();
        let failed_before = before
            .outbound_delivery(&failed_id)
            .expect("failed record should exist")
            .clone();
        let delivered_before = before
            .outbound_delivery(&delivered_id)
            .expect("delivered record should exist")
            .clone();
        let pending_before = before
            .outbound_delivery(&pending_id)
            .expect("pending record should exist")
            .clone();

        let report = store
            .reconcile_outbound_deliveries_at_startup(FUTURE_UNIX)
            .expect("startup recovery should tolerate clock rollback");

        assert_eq!(
            report.reconciliation_required_delivery_ids(),
            &[
                delivering_id.clone(),
                later_delivering_id.clone(),
                uncertain_id.clone(),
            ]
        );
        assert!(!report.is_empty());
        let recovered = store.load().expect("recovered state should load");
        let recovered_delivering = recovered
            .outbound_delivery(&delivering_id)
            .expect("recovered delivery should exist");
        assert_eq!(
            recovered_delivering.status(),
            OutboundDeliveryStatus::Uncertain
        );
        assert_eq!(
            recovered_delivering.updated_at_unix(),
            delivering_before.updated_at_unix()
        );
        assert_eq!(
            recovered_delivering.delivery_attempts(),
            delivering_before.delivery_attempts()
        );
        assert!(
            recovered_delivering
                .last_error()
                .is_some_and(|error| !error.trim().is_empty())
        );
        let recovered_later_delivering = recovered
            .outbound_delivery(&later_delivering_id)
            .expect("later recovered delivery should exist");
        assert_eq!(
            recovered_later_delivering.status(),
            OutboundDeliveryStatus::Uncertain
        );
        assert_eq!(
            recovered_later_delivering.updated_at_unix(),
            later_delivering_before.updated_at_unix()
        );
        assert_eq!(
            recovered_later_delivering.delivery_attempts(),
            later_delivering_before.delivery_attempts()
        );
        assert_eq!(
            recovered
                .outbound_delivery(&uncertain_id)
                .expect("existing uncertain delivery should remain"),
            &uncertain_before
        );
        assert_eq!(
            recovered
                .outbound_delivery(&failed_id)
                .expect("failed delivery should remain"),
            &failed_before
        );
        assert_eq!(
            recovered
                .outbound_delivery(&delivered_id)
                .expect("delivered delivery should remain"),
            &delivered_before
        );
        assert_eq!(
            recovered
                .outbound_delivery(&pending_id)
                .expect("pending delivery should remain"),
            &pending_before
        );

        let repeated = store
            .reconcile_outbound_deliveries_at_startup(FUTURE_UNIX + 100)
            .expect("repeated recovery should be stable");
        assert_eq!(repeated, report);
        assert_eq!(store.load().expect("state should load"), recovered);
    }

    #[test]
    fn startup_recovery_does_not_create_missing_state() {
        let store =
            StateStore::new(test_path("outbound-recovery-missing").join("runtime.state.json"));

        let report = store
            .reconcile_outbound_deliveries_at_startup(FUTURE_UNIX)
            .expect("missing state should recover as empty");

        assert!(report.is_empty());
        assert!(!store.path().exists());
    }

    #[test]
    fn startup_recovery_pre_replace_failure_leaves_delivering_state() {
        let (store, delivery_id) = state_store_with_delivering("outbound-recovery-pre-failure");
        fail_next_write_before_replace(store.path());

        let err = store
            .reconcile_outbound_deliveries_at_startup(FUTURE_UNIX + 2)
            .expect_err("pre-replace failure must return no report");

        assert!(err.contains("failed to save runtime state"));
        assert_eq!(
            delivery_status(&store, &delivery_id),
            OutboundDeliveryStatus::Delivering
        );
        let retried = store
            .reconcile_outbound_deliveries_at_startup(FUTURE_UNIX + 2)
            .expect("retry should recover the delivering record");
        assert_eq!(
            retried.reconciliation_required_delivery_ids(),
            std::slice::from_ref(&delivery_id)
        );
    }

    #[test]
    fn startup_recovery_post_replace_failure_reestablishes_durability() {
        let (store, delivery_id) = state_store_with_delivering("outbound-recovery-post-failure");
        fail_next_write_after_replace(store.path());

        let err = store
            .reconcile_outbound_deliveries_at_startup(FUTURE_UNIX + 2)
            .expect_err("post-replace failure must return no report");

        assert!(err.contains("failed to save runtime state"));
        assert_eq!(
            delivery_status(&store, &delivery_id),
            OutboundDeliveryStatus::Uncertain
        );
        fail_next_parent_sync(store.path());
        let retry_err = store
            .reconcile_outbound_deliveries_at_startup(FUTURE_UNIX + 3)
            .expect_err("retry must confirm the visible replacement is durable");
        assert!(retry_err.contains("failed to confirm runtime state"));

        let retried = store
            .reconcile_outbound_deliveries_at_startup(FUTURE_UNIX + 3)
            .expect("retry should rebuild the report after durability confirmation");
        assert_eq!(
            retried.reconciliation_required_delivery_ids(),
            std::slice::from_ref(&delivery_id)
        );
    }

    #[test]
    fn concurrent_startup_recovery_returns_one_durable_classification() {
        let (store, delivery_id) = state_store_with_delivering("outbound-recovery-concurrent");
        let barrier = Arc::new(Barrier::new(3));
        let handles = [store.clone(), StateStore::new(store.path())].map(|worker_store| {
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                worker_store.reconcile_outbound_deliveries_at_startup(FUTURE_UNIX + 2)
            })
        });
        barrier.wait();

        for handle in handles {
            let report = handle
                .join()
                .expect("recovery worker should not panic")
                .expect("recovery worker should succeed");
            assert_eq!(
                report.reconciliation_required_delivery_ids(),
                std::slice::from_ref(&delivery_id)
            );
        }
        assert_eq!(
            delivery_status(&store, &delivery_id),
            OutboundDeliveryStatus::Uncertain
        );
    }

    #[test]
    fn stale_delivering_snapshot_cannot_restore_recovered_state() {
        let (store, delivery_id) = state_store_with_delivering("outbound-recovery-stale-save");
        let stale = store.load().expect("delivering snapshot should load");
        store
            .reconcile_outbound_deliveries_at_startup(FUTURE_UNIX + 2)
            .expect("startup recovery should succeed");

        let err = store
            .save(&stale)
            .expect_err("stale snapshot must fail closed");

        assert!(err.contains("conflicting outbound delivery"));
        assert_eq!(
            delivery_status(&store, &delivery_id),
            OutboundDeliveryStatus::Uncertain
        );
    }

    fn state_store_with_delivering(label: &str) -> (StateStore, OutboundDeliveryId) {
        let store = state_store_with_session(label);
        let session_id = only_session_id(&store);
        let delivery_id = OutboundDeliveryId::new("out_delivering").expect("valid id");
        enqueue(&store, &delivery_id, &session_id, FUTURE_UNIX);
        store
            .claim_next_outbound_delivery(FUTURE_UNIX + 1)
            .expect("delivery should claim");
        (store, delivery_id)
    }

    fn state_store_with_session(label: &str) -> StateStore {
        let store = StateStore::new(test_path(label).join("runtime.state.json"));
        let mut state = RuntimeState::new();
        state.upsert_session(Session::new(
            SessionScope::new("lark", "chat:oc_123").expect("valid scope"),
        ));
        store.save(&state).expect("session should persist");
        store
    }

    fn only_session_id(store: &StateStore) -> SessionId {
        store
            .load()
            .expect("state should load")
            .sessions()
            .first()
            .expect("session should exist")
            .id()
            .clone()
    }

    fn enqueue(
        store: &StateStore,
        id: &OutboundDeliveryId,
        session_id: &SessionId,
        created_at_unix: u64,
    ) {
        let message = Message::new(
            MessageId::new(format!("msg_{}", id.as_str())).expect("valid message id"),
            Some(session_id.clone()),
            MessageAuthor::Agent,
            MessageContent::text("reply").expect("valid text"),
            created_at_unix,
        );
        store
            .enqueue_outbound_delivery(
                OutboundDeliveryRecord::new(
                    id.clone(),
                    session_id.clone(),
                    message,
                    created_at_unix,
                )
                .expect("valid outbound delivery"),
            )
            .expect("delivery should enqueue");
    }

    fn delivery_status(store: &StateStore, id: &OutboundDeliveryId) -> OutboundDeliveryStatus {
        store
            .load()
            .expect("state should load")
            .outbound_delivery(id)
            .expect("delivery should exist")
            .status()
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "ferris-agent-bridge-{name}-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed)
        ))
    }
}
