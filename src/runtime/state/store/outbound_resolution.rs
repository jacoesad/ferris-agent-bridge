use crate::runtime::{
    outbox::{OutboundDeliveryId, OutboundDeliveryRecord, OutboundDeliveryStatus},
    persistence::confirm_existing_file_durable,
};

use super::StateStore;

impl StateStore {
    pub fn resolve_outbound_delivery_as_delivered(
        &self,
        id: &OutboundDeliveryId,
        resolved_at_unix: u64,
    ) -> Result<OutboundDeliveryRecord, String> {
        let _guard = self.lock_write()?;
        let mut state = self.load()?;
        let existing = state
            .outbound_delivery(id)
            .cloned()
            .ok_or_else(|| format!("unknown outbound delivery id {id}"))?;

        match existing.status() {
            OutboundDeliveryStatus::Uncertain => {
                let resolved =
                    state.resolve_outbound_delivery_as_delivered(id, resolved_at_unix)?;
                self.write_unlocked(&state)?;
                Ok(resolved)
            }
            OutboundDeliveryStatus::Delivered
                if existing.delivered_at_unix() == Some(resolved_at_unix) =>
            {
                self.confirm_resolution_replay_durable()?;
                Ok(existing)
            }
            OutboundDeliveryStatus::Delivered => Err(format!(
                "outbound delivery {id} delivered resolution timestamp conflicts with persisted state"
            )),
            status => Err(format!(
                "outbound delivery {id} cannot resolve as delivered from {status:?}"
            )),
        }
    }

    pub fn resolve_outbound_delivery_as_failed(
        &self,
        id: &OutboundDeliveryId,
        resolved_at_unix: u64,
        reason: impl Into<String>,
    ) -> Result<OutboundDeliveryRecord, String> {
        let _guard = self.lock_write()?;
        let mut state = self.load()?;
        let existing = state
            .outbound_delivery(id)
            .cloned()
            .ok_or_else(|| format!("unknown outbound delivery id {id}"))?;
        let reason = reason.into();

        match existing.status() {
            OutboundDeliveryStatus::Uncertain => {
                let resolved =
                    state.resolve_outbound_delivery_as_failed(id, resolved_at_unix, reason)?;
                self.write_unlocked(&state)?;
                Ok(resolved)
            }
            OutboundDeliveryStatus::Failed if reason.trim().is_empty() => Err(format!(
                "outbound delivery {id} resolution failure is empty"
            )),
            OutboundDeliveryStatus::Failed
                if existing.updated_at_unix() == resolved_at_unix
                    && existing.last_error() == Some(reason.as_str()) =>
            {
                self.confirm_resolution_replay_durable()?;
                Ok(existing)
            }
            OutboundDeliveryStatus::Failed => Err(format!(
                "outbound delivery {id} failed resolution conflicts with persisted state"
            )),
            status => Err(format!(
                "outbound delivery {id} cannot resolve as failed from {status:?}"
            )),
        }
    }

    fn confirm_resolution_replay_durable(&self) -> Result<(), String> {
        confirm_existing_file_durable(self.path()).map_err(|err| {
            format!(
                "failed to confirm runtime state {} durability: {err}",
                self.path().display()
            )
        })
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
        outbox::{
            OutboundDeliveryId, OutboundDeliveryRecord, OutboundDeliveryStatus, OutboundRetryPolicy,
        },
        persistence::{
            fail_next_parent_sync, fail_next_write_after_replace, fail_next_write_before_replace,
        },
        session::{Session, SessionId, SessionScope},
        state::RuntimeState,
    };

    use super::StateStore;

    static NEXT_DIR: AtomicU64 = AtomicU64::new(0);
    const FUTURE_UNIX: u64 = 4_102_444_800;
    const CONFIRMED_NOT_ACCEPTED: &str = "provider confirmed non-acceptance";

    #[test]
    fn explicit_resolution_persists_confirmed_outcomes() {
        let store = state_store_with_session("outbound-resolution-outcomes");
        let session_id = only_session_id(&store);
        let delivered_id = OutboundDeliveryId::new("out_delivered").expect("valid id");
        let failed_id = OutboundDeliveryId::new("out_failed").expect("valid id");
        make_uncertain(&store, &delivered_id, &session_id, FUTURE_UNIX);
        make_uncertain(&store, &failed_id, &session_id, FUTURE_UNIX + 10);

        let delivered = store
            .resolve_outbound_delivery_as_delivered(&delivered_id, FUTURE_UNIX + 3)
            .expect("confirmed acceptance should persist");
        let failed = store
            .resolve_outbound_delivery_as_failed(
                &failed_id,
                FUTURE_UNIX + 13,
                CONFIRMED_NOT_ACCEPTED,
            )
            .expect("confirmed non-acceptance should persist");

        assert_eq!(delivered.status(), OutboundDeliveryStatus::Delivered);
        assert_eq!(delivered.delivery_attempts(), 1);
        assert_eq!(delivered.delivered_at_unix(), Some(FUTURE_UNIX + 3));
        assert_eq!(delivered.last_error(), None);
        assert_eq!(failed.status(), OutboundDeliveryStatus::Failed);
        assert_eq!(failed.delivery_attempts(), 1);
        assert_eq!(failed.updated_at_unix(), FUTURE_UNIX + 13);
        assert_eq!(failed.last_error(), Some(CONFIRMED_NOT_ACCEPTED));
        let policy = OutboundRetryPolicy::new(2, 10, 40).expect("valid retry policy");
        assert_eq!(policy.next_attempt_at_unix(&failed), Some(FUTURE_UNIX + 23));
        let reloaded = store.load().expect("resolved state should load");
        assert_eq!(reloaded.outbound_delivery(&delivered_id), Some(&delivered));
        assert_eq!(reloaded.outbound_delivery(&failed_id), Some(&failed));
    }

    #[test]
    fn resolution_rejects_invalid_sources_and_empty_reason_without_writing() {
        let store = state_store_with_session("outbound-resolution-invalid");
        let session_id = only_session_id(&store);
        let pending_id = OutboundDeliveryId::new("out_pending").expect("valid id");
        enqueue(&store, &pending_id, &session_id, FUTURE_UNIX);
        let pending_before = store.load().expect("pending state should load");

        let err = store
            .resolve_outbound_delivery_as_delivered(&pending_id, FUTURE_UNIX + 1)
            .expect_err("pending delivery cannot be resolved");
        assert!(err.contains("from Pending"));
        assert_eq!(store.load().expect("state should load"), pending_before);

        let err = store
            .resolve_outbound_delivery_as_failed(
                &pending_id,
                FUTURE_UNIX + 1,
                CONFIRMED_NOT_ACCEPTED,
            )
            .expect_err("pending delivery cannot be resolved as failed");
        assert!(err.contains("from Pending"));
        assert_eq!(store.load().expect("state should load"), pending_before);

        let (uncertain_store, uncertain_id) =
            state_store_with_uncertain("outbound-resolution-empty-reason");
        let uncertain_before = uncertain_store.load().expect("uncertain state should load");
        let err = uncertain_store
            .resolve_outbound_delivery_as_failed(&uncertain_id, FUTURE_UNIX + 13, "  ")
            .expect_err("failed resolution requires a reason");
        assert!(err.contains("resolution failure is empty"));
        assert_eq!(
            uncertain_store.load().expect("state should load"),
            uncertain_before
        );
    }

    #[test]
    fn delivered_resolution_pre_replace_failure_leaves_uncertain_state() {
        let (store, delivery_id) = state_store_with_uncertain("outbound-resolution-pre-failure");
        fail_next_write_before_replace(store.path());

        let err = store
            .resolve_outbound_delivery_as_delivered(&delivery_id, FUTURE_UNIX + 3)
            .expect_err("pre-replace failure must return no resolution");

        assert!(err.contains("failed to save runtime state"));
        assert_eq!(
            delivery_status(&store, &delivery_id),
            OutboundDeliveryStatus::Uncertain
        );
        let resolved = store
            .resolve_outbound_delivery_as_delivered(&delivery_id, FUTURE_UNIX + 3)
            .expect("retry should resolve the still-uncertain record");
        assert_eq!(resolved.status(), OutboundDeliveryStatus::Delivered);
    }

    #[test]
    fn delivered_resolution_post_replace_replay_confirms_durability() {
        let (store, delivery_id) =
            state_store_with_uncertain("outbound-resolution-delivered-replay");
        fail_next_write_after_replace(store.path());

        let err = store
            .resolve_outbound_delivery_as_delivered(&delivery_id, FUTURE_UNIX + 3)
            .expect_err("post-replace failure must return no resolution");

        assert!(err.contains("failed to save runtime state"));
        assert_eq!(
            delivery_status(&store, &delivery_id),
            OutboundDeliveryStatus::Delivered
        );
        let conflict = store
            .resolve_outbound_delivery_as_delivered(&delivery_id, FUTURE_UNIX + 4)
            .expect_err("replay timestamp must match the persisted resolution");
        assert!(conflict.contains("timestamp conflicts"));

        fail_next_parent_sync(store.path());
        let retry_err = store
            .resolve_outbound_delivery_as_delivered(&delivery_id, FUTURE_UNIX + 3)
            .expect_err("matching replay must confirm durability");
        assert!(retry_err.contains("failed to confirm runtime state"));
        let replayed = store
            .resolve_outbound_delivery_as_delivered(&delivery_id, FUTURE_UNIX + 3)
            .expect("matching replay should succeed after durability confirmation");
        assert_eq!(replayed.status(), OutboundDeliveryStatus::Delivered);
        assert_eq!(replayed.delivered_at_unix(), Some(FUTURE_UNIX + 3));
    }

    #[test]
    fn failed_resolution_post_replace_replay_requires_exact_evidence() {
        let (store, delivery_id) = state_store_with_uncertain("outbound-resolution-failed-replay");
        fail_next_write_after_replace(store.path());

        let err = store
            .resolve_outbound_delivery_as_failed(
                &delivery_id,
                FUTURE_UNIX + 3,
                CONFIRMED_NOT_ACCEPTED,
            )
            .expect_err("post-replace failure must return no resolution");

        assert!(err.contains("failed to save runtime state"));
        assert_eq!(
            delivery_status(&store, &delivery_id),
            OutboundDeliveryStatus::Failed
        );
        let time_conflict = store
            .resolve_outbound_delivery_as_failed(
                &delivery_id,
                FUTURE_UNIX + 4,
                CONFIRMED_NOT_ACCEPTED,
            )
            .expect_err("replay timestamp must match");
        assert!(time_conflict.contains("conflicts with persisted state"));
        let reason_conflict = store
            .resolve_outbound_delivery_as_failed(
                &delivery_id,
                FUTURE_UNIX + 3,
                "different evidence",
            )
            .expect_err("replay reason must match");
        assert!(reason_conflict.contains("conflicts with persisted state"));

        fail_next_parent_sync(store.path());
        let retry_err = store
            .resolve_outbound_delivery_as_failed(
                &delivery_id,
                FUTURE_UNIX + 3,
                CONFIRMED_NOT_ACCEPTED,
            )
            .expect_err("matching replay must confirm durability");
        assert!(retry_err.contains("failed to confirm runtime state"));
        let replayed = store
            .resolve_outbound_delivery_as_failed(
                &delivery_id,
                FUTURE_UNIX + 3,
                CONFIRMED_NOT_ACCEPTED,
            )
            .expect("matching replay should succeed after durability confirmation");
        assert_eq!(replayed.status(), OutboundDeliveryStatus::Failed);
        assert_eq!(replayed.last_error(), Some(CONFIRMED_NOT_ACCEPTED));
    }

    #[test]
    fn resolution_does_not_create_missing_state() {
        let store =
            StateStore::new(test_path("outbound-resolution-missing").join("runtime.state.json"));
        let delivery_id = OutboundDeliveryId::new("out_missing").expect("valid id");

        let delivered_err = store
            .resolve_outbound_delivery_as_delivered(&delivery_id, FUTURE_UNIX)
            .expect_err("missing delivery cannot resolve");
        let failed_err = store
            .resolve_outbound_delivery_as_failed(&delivery_id, FUTURE_UNIX, CONFIRMED_NOT_ACCEPTED)
            .expect_err("missing delivery cannot resolve");

        assert!(delivered_err.contains("unknown outbound delivery id"));
        assert!(failed_err.contains("unknown outbound delivery id"));
        assert!(!store.path().exists());
    }

    #[test]
    fn concurrent_matching_resolution_is_idempotent() {
        let (store, delivery_id) = state_store_with_uncertain("outbound-resolution-concurrent");
        let barrier = Arc::new(Barrier::new(3));
        let handles = [store.clone(), StateStore::new(store.path())].map(|worker_store| {
            let barrier = Arc::clone(&barrier);
            let delivery_id = delivery_id.clone();
            thread::spawn(move || {
                barrier.wait();
                worker_store.resolve_outbound_delivery_as_delivered(&delivery_id, FUTURE_UNIX + 3)
            })
        });
        barrier.wait();

        for handle in handles {
            let resolved = handle
                .join()
                .expect("resolution worker should not panic")
                .expect("matching resolution should succeed");
            assert_eq!(resolved.status(), OutboundDeliveryStatus::Delivered);
        }
        assert_eq!(
            delivery_status(&store, &delivery_id),
            OutboundDeliveryStatus::Delivered
        );
    }

    #[test]
    fn concurrent_conflicting_resolutions_allow_only_one_outcome() {
        let (store, delivery_id) =
            state_store_with_uncertain("outbound-resolution-concurrent-conflict");
        let barrier = Arc::new(Barrier::new(3));
        let delivered_store = store.clone();
        let delivered_id = delivery_id.clone();
        let delivered_barrier = Arc::clone(&barrier);
        let delivered_handle = thread::spawn(move || {
            delivered_barrier.wait();
            delivered_store.resolve_outbound_delivery_as_delivered(&delivered_id, FUTURE_UNIX + 3)
        });
        let failed_store = StateStore::new(store.path());
        let failed_id = delivery_id.clone();
        let failed_barrier = Arc::clone(&barrier);
        let failed_handle = thread::spawn(move || {
            failed_barrier.wait();
            failed_store.resolve_outbound_delivery_as_failed(
                &failed_id,
                FUTURE_UNIX + 3,
                CONFIRMED_NOT_ACCEPTED,
            )
        });
        barrier.wait();

        let results = [
            delivered_handle
                .join()
                .expect("delivered resolution worker should not panic"),
            failed_handle
                .join()
                .expect("failed resolution worker should not panic"),
        ];
        let successful_statuses = results
            .iter()
            .filter_map(|result| result.as_ref().ok().map(OutboundDeliveryRecord::status))
            .collect::<Vec<_>>();

        assert_eq!(successful_statuses.len(), 1);
        assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
        let persisted = store
            .load()
            .expect("resolved state should load")
            .outbound_delivery(&delivery_id)
            .expect("delivery should exist")
            .clone();
        assert_eq!(persisted.status(), successful_statuses[0]);
        if persisted.status() == OutboundDeliveryStatus::Failed {
            assert_eq!(persisted.last_error(), Some(CONFIRMED_NOT_ACCEPTED));
        }
    }

    #[test]
    fn stale_uncertain_snapshot_cannot_restore_resolved_state() {
        let (store, delivery_id) = state_store_with_uncertain("outbound-resolution-stale-save");
        let stale = store.load().expect("uncertain snapshot should load");
        store
            .resolve_outbound_delivery_as_delivered(&delivery_id, FUTURE_UNIX + 3)
            .expect("delivery should resolve");

        let err = store
            .save(&stale)
            .expect_err("stale uncertain snapshot must fail closed");

        assert!(err.contains("conflicting outbound delivery"));
        assert_eq!(
            delivery_status(&store, &delivery_id),
            OutboundDeliveryStatus::Delivered
        );
    }

    fn state_store_with_uncertain(label: &str) -> (StateStore, OutboundDeliveryId) {
        let store = state_store_with_session(label);
        let session_id = only_session_id(&store);
        let delivery_id = OutboundDeliveryId::new("out_uncertain").expect("valid id");
        make_uncertain(&store, &delivery_id, &session_id, FUTURE_UNIX);
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

    fn make_uncertain(
        store: &StateStore,
        id: &OutboundDeliveryId,
        session_id: &SessionId,
        created_at_unix: u64,
    ) {
        enqueue(store, id, session_id, created_at_unix);
        store
            .claim_next_outbound_delivery(created_at_unix + 1)
            .expect("delivery should claim");
        store
            .mark_outbound_delivery_uncertain(
                id,
                created_at_unix + 2,
                "provider acceptance is unknown",
            )
            .expect("delivery should become uncertain");
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
