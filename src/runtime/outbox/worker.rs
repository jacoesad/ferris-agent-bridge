use std::time::{SystemTime, UNIX_EPOCH};

use crate::adapter::{ImAdapter, OutboundDeliveryFailureKind};

use super::{OutboundDeliveryId, OutboundRetryPolicy};
use crate::runtime::{error::RuntimeError, state::StateStore};

#[derive(Debug, Clone)]
pub struct OutboxWorker {
    state_store: StateStore,
    retry_policy: OutboundRetryPolicy,
}

impl OutboxWorker {
    pub fn new(state_store: StateStore, retry_policy: OutboundRetryPolicy) -> Self {
        Self {
            state_store,
            retry_policy,
        }
    }

    pub fn state_store(&self) -> &StateStore {
        &self.state_store
    }

    pub fn retry_policy(&self) -> &OutboundRetryPolicy {
        &self.retry_policy
    }

    pub fn process_next<A>(&self, im_adapter: &mut A) -> Result<OutboxWorkerOutcome, RuntimeError>
    where
        A: ImAdapter,
    {
        self.process_next_with_clock(im_adapter, unix_seconds_now)
    }

    fn process_next_with_clock<A, F>(
        &self,
        im_adapter: &mut A,
        mut now_unix: F,
    ) -> Result<OutboxWorkerOutcome, RuntimeError>
    where
        A: ImAdapter,
        F: FnMut() -> u64,
    {
        let started_at_unix = now_unix();
        let Some(attempt) = self
            .state_store
            .claim_next_outbound_delivery_attempt(started_at_unix, &self.retry_policy)
            .map_err(|err| {
                RuntimeError::recoverable(format!(
                    "failed to durably confirm the next outbound delivery claim before adapter handoff; the adapter was not called, but reload persisted state before recovery because pending, failed, or delivering may be visible: {err}"
                ))
            })?
        else {
            let state = self.state_store.load().map_err(|err| {
                RuntimeError::recoverable(format!(
                    "failed to inspect deferred outbound deliveries: {err}"
                ))
            })?;
            let next_attempt_at_unix = state
                .outbound_deliveries()
                .iter()
                .filter_map(|delivery| self.retry_policy.next_attempt_at_unix(delivery))
                .min();

            return Ok(OutboxWorkerOutcome::Idle {
                next_attempt_at_unix,
            });
        };

        let delivery_id = attempt.delivery_id().clone();
        let attempt_number = attempt.attempt_number();
        let adapter_result = im_adapter.deliver_outbound_message(&attempt);
        let finished_at_unix = now_unix().max(started_at_unix);

        match adapter_result {
            Ok(()) => {
                self.state_store
                    .mark_outbound_delivery_delivered(&delivery_id, finished_at_unix)
                    .map_err(|err| {
                        RuntimeError::recoverable(format!(
                            "outbound adapter delivered {delivery_id}, but the delivered outcome could not be durably confirmed; reload persisted state before recovery because either the previous or updated state may be visible: {err}"
                        ))
                    })?;

                Ok(OutboxWorkerOutcome::Delivered {
                    delivery_id,
                    attempt_number,
                })
            }
            Err(failure) => match failure.kind() {
                OutboundDeliveryFailureKind::Retryable => {
                    let error = failure.message().to_owned();
                    let failed = self
                        .state_store
                        .mark_outbound_delivery_failed(
                            &delivery_id,
                            finished_at_unix,
                            error.clone(),
                        )
                        .map_err(|err| {
                            RuntimeError::recoverable(format!(
                                "outbound adapter reported retryable failure for {delivery_id}, but the failed outcome could not be durably confirmed; reload persisted state before recovery because either the previous or updated state may be visible: {err}"
                            ))
                        })?;
                    let next_attempt_at_unix = self.retry_policy.next_attempt_at_unix(&failed);

                    Ok(OutboxWorkerOutcome::Failed {
                        delivery_id,
                        attempt_number,
                        error,
                        next_attempt_at_unix,
                    })
                }
                OutboundDeliveryFailureKind::Uncertain => {
                    let error = failure.message().to_owned();
                    self.state_store
                        .mark_outbound_delivery_uncertain(
                            &delivery_id,
                            finished_at_unix,
                            error.clone(),
                        )
                        .map_err(|err| {
                            RuntimeError::recoverable(format!(
                                "outbound adapter reported an uncertain outcome for {delivery_id}, but that outcome could not be durably confirmed; reload persisted state before recovery because either the delivering or uncertain state may be visible: {err}"
                            ))
                        })?;

                    Ok(OutboxWorkerOutcome::Uncertain {
                        delivery_id,
                        attempt_number,
                        error,
                    })
                }
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboxWorkerOutcome {
    Idle {
        next_attempt_at_unix: Option<u64>,
    },
    Delivered {
        delivery_id: OutboundDeliveryId,
        attempt_number: u32,
    },
    Failed {
        delivery_id: OutboundDeliveryId,
        attempt_number: u32,
        error: String,
        next_attempt_at_unix: Option<u64>,
    },
    Uncertain {
        delivery_id: OutboundDeliveryId,
        attempt_number: u32,
        error: String,
    },
}

fn unix_seconds_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    use crate::{
        adapter::{ImAdapter, InboundDeliveryAcknowledgement, OutboundDeliveryFailure},
        runtime::{
            error::ErrorClass,
            message::{Message, MessageAuthor, MessageContent, MessageId},
            outbox::{
                OutboundDeliveryAttempt, OutboundDeliveryId, OutboundDeliveryRecord,
                OutboundDeliveryStatus, OutboundRetryPolicy, OutboxWorker, OutboxWorkerOutcome,
            },
            persistence::fail_next_write_after_replace,
            session::{Session, SessionId, SessionScope},
            state::{RuntimeState, StateStore},
        },
    };

    static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn worker_delivers_only_after_persisting_the_claim() {
        let (store, scope) = state_store_with_session("worker-delivers");
        let delivery = outbound_delivery_fixture("out_1", SessionId::for_scope(&scope), 10);
        store
            .enqueue_outbound_delivery(delivery.clone())
            .expect("delivery should enqueue");
        let worker = OutboxWorker::new(store.clone(), OutboundRetryPolicy::default());
        let mut adapter = RecordingImAdapter::with_state_check(store.clone());
        let mut times = [11, 12].into_iter();

        let outcome = worker
            .process_next_with_clock(&mut adapter, || times.next().expect("clock value"))
            .expect("delivery should succeed");

        assert_eq!(
            outcome,
            OutboxWorkerOutcome::Delivered {
                delivery_id: delivery.id().clone(),
                attempt_number: 1,
            }
        );
        assert_eq!(adapter.attempts.len(), 1);
        assert_eq!(adapter.attempts[0].delivery_id(), delivery.id());
        assert_eq!(
            adapter.attempts[0].idempotency_key(),
            delivery.id().as_str()
        );
        assert_eq!(adapter.attempts[0].session_scope(), &scope);
        assert_eq!(adapter.attempts[0].message(), delivery.message());

        let state = store.load().expect("state should load");
        let stored = state
            .outbound_delivery(delivery.id())
            .expect("delivery should remain stored");
        assert_eq!(stored.status(), OutboundDeliveryStatus::Delivered);
        assert_eq!(stored.delivered_at_unix(), Some(12));
    }

    #[test]
    fn worker_defers_failed_delivery_until_backoff_expires() {
        let (store, scope) = state_store_with_session("worker-backoff");
        let delivery = outbound_delivery_fixture("out_1", SessionId::for_scope(&scope), 10);
        store
            .enqueue_outbound_delivery(delivery.clone())
            .expect("delivery should enqueue");
        let policy = OutboundRetryPolicy::new(3, 10, 40).expect("valid retry policy");
        let worker = OutboxWorker::new(store.clone(), policy);
        let mut adapter = RecordingImAdapter {
            failure: Some(OutboundDeliveryFailure::retryable(
                "temporary transport failure",
            )),
            ..RecordingImAdapter::default()
        };
        let mut first_times = [11, 12].into_iter();

        let first = worker
            .process_next_with_clock(&mut adapter, || {
                first_times.next().expect("first clock value")
            })
            .expect("adapter failure should become a persisted outcome");
        assert_eq!(
            first,
            OutboxWorkerOutcome::Failed {
                delivery_id: delivery.id().clone(),
                attempt_number: 1,
                error: "temporary transport failure".to_owned(),
                next_attempt_at_unix: Some(22),
            }
        );

        let deferred = worker
            .process_next_with_clock(&mut adapter, || 21)
            .expect("deferred retry should be idle");
        assert_eq!(
            deferred,
            OutboxWorkerOutcome::Idle {
                next_attempt_at_unix: Some(22),
            }
        );
        assert_eq!(adapter.attempts.len(), 1);

        adapter.failure = None;
        let mut retry_times = [22, 23].into_iter();
        let retry = worker
            .process_next_with_clock(&mut adapter, || {
                retry_times.next().expect("retry clock value")
            })
            .expect("due retry should deliver");
        assert_eq!(
            retry,
            OutboxWorkerOutcome::Delivered {
                delivery_id: delivery.id().clone(),
                attempt_number: 2,
            }
        );
        assert_eq!(adapter.attempts.len(), 2);
    }

    #[test]
    fn worker_leaves_exhausted_failure_inspectable_without_retrying() {
        let (store, scope) = state_store_with_session("worker-exhausted");
        let delivery = outbound_delivery_fixture("out_1", SessionId::for_scope(&scope), 10);
        store
            .enqueue_outbound_delivery(delivery.clone())
            .expect("delivery should enqueue");
        let policy = OutboundRetryPolicy::new(1, 10, 40).expect("valid retry policy");
        let worker = OutboxWorker::new(store.clone(), policy);
        let mut adapter = RecordingImAdapter {
            failure: Some(OutboundDeliveryFailure::retryable(
                "last allowed transport failure",
            )),
            ..RecordingImAdapter::default()
        };
        let mut times = [11, 12].into_iter();

        let failed = worker
            .process_next_with_clock(&mut adapter, || times.next().expect("clock value"))
            .expect("failure should persist");
        assert!(matches!(
            failed,
            OutboxWorkerOutcome::Failed {
                next_attempt_at_unix: None,
                ..
            }
        ));

        let idle = worker
            .process_next_with_clock(&mut adapter, || u64::MAX)
            .expect("exhausted delivery should remain idle");
        assert_eq!(
            idle,
            OutboxWorkerOutcome::Idle {
                next_attempt_at_unix: None,
            }
        );
        assert_eq!(adapter.attempts.len(), 1);

        let state = store.load().expect("state should load");
        let stored = state
            .outbound_delivery(delivery.id())
            .expect("failed delivery should remain stored");
        assert_eq!(stored.status(), OutboundDeliveryStatus::Failed);
        assert_eq!(stored.last_error(), Some("last allowed transport failure"));
    }

    #[test]
    fn deferred_failure_does_not_block_a_later_pending_delivery() {
        let (store, scope) = state_store_with_session("worker-skips-deferred");
        let session_id = SessionId::for_scope(&scope);
        let deferred = outbound_delivery_fixture("out_deferred", session_id.clone(), 10);
        let pending = outbound_delivery_fixture("out_pending", session_id, 12);
        store
            .enqueue_outbound_delivery(deferred.clone())
            .expect("first delivery should enqueue");
        store
            .claim_next_outbound_delivery(11)
            .expect("first delivery should claim");
        store
            .mark_outbound_delivery_failed(deferred.id(), 12, "temporary failure")
            .expect("first delivery should fail");
        store
            .enqueue_outbound_delivery(pending.clone())
            .expect("second delivery should enqueue");
        let policy = OutboundRetryPolicy::new(3, 10, 40).expect("valid retry policy");
        let worker = OutboxWorker::new(store.clone(), policy);
        let mut adapter = RecordingImAdapter::default();
        let mut times = [13, 14].into_iter();

        let outcome = worker
            .process_next_with_clock(&mut adapter, || times.next().expect("clock value"))
            .expect("later pending delivery should be processed");

        assert_eq!(
            outcome,
            OutboxWorkerOutcome::Delivered {
                delivery_id: pending.id().clone(),
                attempt_number: 1,
            }
        );
        assert_eq!(adapter.attempts[0].delivery_id(), pending.id());
        let state = store.load().expect("state should load");
        assert_eq!(
            state
                .outbound_delivery(deferred.id())
                .expect("deferred delivery should remain")
                .status(),
            OutboundDeliveryStatus::Failed
        );
    }

    #[test]
    fn worker_normalizes_empty_adapter_errors_before_persisting() {
        let (store, scope) = state_store_with_session("worker-empty-error");
        let delivery = outbound_delivery_fixture("out_1", SessionId::for_scope(&scope), 10);
        store
            .enqueue_outbound_delivery(delivery.clone())
            .expect("delivery should enqueue");
        let worker = OutboxWorker::new(store.clone(), OutboundRetryPolicy::default());
        let mut adapter = RecordingImAdapter {
            failure: Some(OutboundDeliveryFailure::retryable("  ")),
            ..RecordingImAdapter::default()
        };
        let mut times = [11, 12].into_iter();

        let outcome = worker
            .process_next_with_clock(&mut adapter, || times.next().expect("clock value"))
            .expect("empty adapter error should be normalized");

        let OutboxWorkerOutcome::Failed { error, .. } = outcome else {
            panic!("adapter failure should return a failed outcome");
        };
        assert_eq!(
            error,
            "outbound adapter reported a retryable failure without an error message"
        );
        let state = store.load().expect("state should load");
        assert_eq!(
            state
                .outbound_delivery(delivery.id())
                .expect("delivery should remain stored")
                .last_error(),
            Some("outbound adapter reported a retryable failure without an error message")
        );
    }

    #[test]
    fn worker_persists_uncertain_adapter_outcomes_without_retrying() {
        let (store, scope) = state_store_with_session("worker-uncertain-outcome");
        let delivery = outbound_delivery_fixture("out_1", SessionId::for_scope(&scope), 10);
        store
            .enqueue_outbound_delivery(delivery.clone())
            .expect("delivery should enqueue");
        let worker = OutboxWorker::new(store.clone(), OutboundRetryPolicy::default());
        let mut adapter = RecordingImAdapter {
            failure: Some(OutboundDeliveryFailure::uncertain(
                "provider response timed out after request submission",
            )),
            ..RecordingImAdapter::default()
        };
        let mut times = [11, 12].into_iter();

        let outcome = worker
            .process_next_with_clock(&mut adapter, || times.next().expect("clock value"))
            .expect("uncertain outcome should persist");

        assert_eq!(
            outcome,
            OutboxWorkerOutcome::Uncertain {
                delivery_id: delivery.id().clone(),
                attempt_number: 1,
                error: "provider response timed out after request submission".to_owned(),
            }
        );
        let state = store.load().expect("state should load");
        let stored = state
            .outbound_delivery(delivery.id())
            .expect("delivery should remain stored");
        assert_eq!(stored.status(), OutboundDeliveryStatus::Uncertain);
        assert_eq!(
            stored.last_error(),
            Some("provider response timed out after request submission")
        );

        let idle = worker
            .process_next_with_clock(&mut adapter, || u64::MAX)
            .expect("uncertain delivery should remain non-retryable");
        assert_eq!(
            idle,
            OutboxWorkerOutcome::Idle {
                next_attempt_at_unix: None,
            }
        );
        assert_eq!(adapter.attempts.len(), 1);
    }

    #[test]
    fn worker_does_not_move_outcome_time_backwards_when_the_clock_rolls_back() {
        let (store, scope) = state_store_with_session("worker-clock-rollback");
        let delivery = outbound_delivery_fixture("out_1", SessionId::for_scope(&scope), 10);
        store
            .enqueue_outbound_delivery(delivery.clone())
            .expect("delivery should enqueue");
        let worker = OutboxWorker::new(store.clone(), OutboundRetryPolicy::default());
        let mut adapter = RecordingImAdapter::default();
        let mut times = [11, 9].into_iter();

        worker
            .process_next_with_clock(&mut adapter, || times.next().expect("clock value"))
            .expect("delivery should survive clock rollback");

        let state = store.load().expect("state should load");
        let stored = state
            .outbound_delivery(delivery.id())
            .expect("delivery should remain stored");
        assert_eq!(stored.status(), OutboundDeliveryStatus::Delivered);
        assert_eq!(stored.delivered_at_unix(), Some(11));
    }

    #[test]
    #[cfg(unix)]
    fn claim_persistence_failure_does_not_call_the_adapter() {
        use std::{fs, os::unix::fs::PermissionsExt};

        let (store, scope) = state_store_with_session("worker-claim-persist-failure");
        let delivery = outbound_delivery_fixture("out_1", SessionId::for_scope(&scope), 10);
        store
            .enqueue_outbound_delivery(delivery.clone())
            .expect("delivery should enqueue");
        let worker = OutboxWorker::new(store.clone(), OutboundRetryPolicy::default());
        let mut adapter = RecordingImAdapter::default();
        let parent = store
            .path()
            .parent()
            .expect("state path should have a parent");
        fs::set_permissions(parent, fs::Permissions::from_mode(0o500))
            .expect("fixture permissions should be set");

        let result = worker.process_next_with_clock(&mut adapter, || 11);

        fs::set_permissions(parent, fs::Permissions::from_mode(0o700))
            .expect("fixture permissions should be restored");
        let err = result.expect_err("claim persistence failure should be reported");
        assert_eq!(err.class(), ErrorClass::Recoverable);
        assert!(err.message().contains("before adapter handoff"));
        assert!(err.message().contains("reload persisted state"));
        assert!(adapter.attempts.is_empty());

        let state = store.load().expect("state should load");
        assert_eq!(
            state
                .outbound_delivery(delivery.id())
                .expect("delivery should remain stored")
                .status(),
            OutboundDeliveryStatus::Pending
        );
    }

    #[test]
    #[cfg(unix)]
    fn retry_claim_pre_replace_failure_can_leave_the_previous_failed_state() {
        use std::{fs, os::unix::fs::PermissionsExt};

        let (store, scope) = state_store_with_session("worker-retry-claim-persist-failure");
        let delivery = outbound_delivery_fixture("out_1", SessionId::for_scope(&scope), 10);
        store
            .enqueue_outbound_delivery(delivery.clone())
            .expect("delivery should enqueue");
        store
            .claim_next_outbound_delivery(11)
            .expect("first attempt should claim");
        store
            .mark_outbound_delivery_failed(delivery.id(), 12, "temporary transport failure")
            .expect("first attempt should fail");
        let policy = OutboundRetryPolicy::new(3, 10, 40).expect("valid retry policy");
        let worker = OutboxWorker::new(store.clone(), policy);
        let mut adapter = RecordingImAdapter::default();
        let parent = store
            .path()
            .parent()
            .expect("state path should have a parent");
        fs::set_permissions(parent, fs::Permissions::from_mode(0o500))
            .expect("fixture permissions should be set");

        let result = worker.process_next_with_clock(&mut adapter, || 22);

        fs::set_permissions(parent, fs::Permissions::from_mode(0o700))
            .expect("fixture permissions should be restored");
        let err = result.expect_err("retry claim persistence failure should be reported");
        assert!(err.message().contains("pending, failed, or delivering"));
        assert!(adapter.attempts.is_empty());
        let state = store.load().expect("state should load");
        assert_eq!(
            state
                .outbound_delivery(delivery.id())
                .expect("delivery should remain stored")
                .status(),
            OutboundDeliveryStatus::Failed
        );
    }

    #[test]
    #[cfg(unix)]
    fn pre_replace_delivered_persistence_failure_leaves_the_attempt_claimed() {
        use std::{fs, os::unix::fs::PermissionsExt};

        let (store, scope) = state_store_with_session("worker-outcome-persist-failure");
        let delivery = outbound_delivery_fixture("out_1", SessionId::for_scope(&scope), 10);
        store
            .enqueue_outbound_delivery(delivery.clone())
            .expect("delivery should enqueue");
        let worker = OutboxWorker::new(store.clone(), OutboundRetryPolicy::default());
        let parent = store
            .path()
            .parent()
            .expect("state path should have a parent")
            .to_path_buf();
        let mut adapter = RecordingImAdapter {
            state_check: Some(store.clone()),
            lock_outcome_directory: Some(parent.clone()),
            ..RecordingImAdapter::default()
        };
        let mut times = [11, 12].into_iter();

        let result =
            worker.process_next_with_clock(&mut adapter, || times.next().expect("clock value"));

        fs::set_permissions(&parent, fs::Permissions::from_mode(0o700))
            .expect("fixture permissions should be restored");
        let err = result.expect_err("outcome persistence failure should be reported");
        assert_eq!(err.class(), ErrorClass::Recoverable);
        assert!(err.message().contains("delivered outcome"));
        assert!(err.message().contains("reload persisted state"));
        assert_eq!(adapter.attempts.len(), 1);

        let state = store.load().expect("state should load");
        assert_eq!(
            state
                .outbound_delivery(delivery.id())
                .expect("delivery should remain stored")
                .status(),
            OutboundDeliveryStatus::Delivering
        );

        adapter.lock_outcome_directory = None;
        let idle = worker
            .process_next_with_clock(&mut adapter, || 13)
            .expect("uncertain delivery should not retry immediately");
        assert_eq!(
            idle,
            OutboxWorkerOutcome::Idle {
                next_attempt_at_unix: None,
            }
        );
        assert_eq!(adapter.attempts.len(), 1);
    }

    #[test]
    #[cfg(unix)]
    fn pre_replace_failed_persistence_failure_leaves_the_attempt_claimed() {
        use std::{fs, os::unix::fs::PermissionsExt};

        let (store, scope) = state_store_with_session("worker-failed-persist-failure");
        let delivery = outbound_delivery_fixture("out_1", SessionId::for_scope(&scope), 10);
        store
            .enqueue_outbound_delivery(delivery.clone())
            .expect("delivery should enqueue");
        let worker = OutboxWorker::new(store.clone(), OutboundRetryPolicy::default());
        let parent = store
            .path()
            .parent()
            .expect("state path should have a parent")
            .to_path_buf();
        let mut adapter = RecordingImAdapter {
            failure: Some(OutboundDeliveryFailure::retryable("transport failed")),
            state_check: Some(store.clone()),
            lock_outcome_directory: Some(parent.clone()),
            ..RecordingImAdapter::default()
        };
        let mut times = [11, 12].into_iter();

        let result =
            worker.process_next_with_clock(&mut adapter, || times.next().expect("clock value"));

        fs::set_permissions(&parent, fs::Permissions::from_mode(0o700))
            .expect("fixture permissions should be restored");
        let err = result.expect_err("failed outcome persistence should be reported");
        assert_eq!(err.class(), ErrorClass::Recoverable);
        assert!(err.message().contains("failed outcome"));
        assert!(err.message().contains("reload persisted state"));
        assert_eq!(adapter.attempts.len(), 1);

        let state = store.load().expect("state should load");
        assert_eq!(
            state
                .outbound_delivery(delivery.id())
                .expect("delivery should remain stored")
                .status(),
            OutboundDeliveryStatus::Delivering
        );

        adapter.lock_outcome_directory = None;
        let idle = worker
            .process_next_with_clock(&mut adapter, || 13)
            .expect("uncertain delivery should not retry immediately");
        assert_eq!(
            idle,
            OutboxWorkerOutcome::Idle {
                next_attempt_at_unix: None,
            }
        );
        assert_eq!(adapter.attempts.len(), 1);
    }

    #[test]
    #[cfg(unix)]
    fn pre_replace_uncertain_persistence_failure_leaves_the_attempt_claimed() {
        use std::{fs, os::unix::fs::PermissionsExt};

        let (store, scope) = state_store_with_session("worker-uncertain-persist-failure");
        let delivery = outbound_delivery_fixture("out_1", SessionId::for_scope(&scope), 10);
        store
            .enqueue_outbound_delivery(delivery.clone())
            .expect("delivery should enqueue");
        let worker = OutboxWorker::new(store.clone(), OutboundRetryPolicy::default());
        let parent = store
            .path()
            .parent()
            .expect("state path should have a parent")
            .to_path_buf();
        let mut adapter = RecordingImAdapter {
            failure: Some(OutboundDeliveryFailure::uncertain(
                "provider acceptance is unknown",
            )),
            state_check: Some(store.clone()),
            lock_outcome_directory: Some(parent.clone()),
            ..RecordingImAdapter::default()
        };
        let mut times = [11, 12].into_iter();

        let result =
            worker.process_next_with_clock(&mut adapter, || times.next().expect("clock value"));

        fs::set_permissions(&parent, fs::Permissions::from_mode(0o700))
            .expect("fixture permissions should be restored");
        let err = result.expect_err("uncertain outcome persistence failure should be reported");
        assert_eq!(err.class(), ErrorClass::Recoverable);
        assert!(err.message().contains("uncertain outcome"));
        assert!(err.message().contains("reload persisted state"));
        assert_eq!(adapter.attempts.len(), 1);

        let state = store.load().expect("state should load");
        assert_eq!(
            state
                .outbound_delivery(delivery.id())
                .expect("delivery should remain stored")
                .status(),
            OutboundDeliveryStatus::Delivering
        );

        adapter.lock_outcome_directory = None;
        let idle = worker
            .process_next_with_clock(&mut adapter, || 13)
            .expect("unconfirmed uncertain delivery should not retry");
        assert_eq!(
            idle,
            OutboxWorkerOutcome::Idle {
                next_attempt_at_unix: None,
            }
        );
        assert_eq!(adapter.attempts.len(), 1);
    }

    #[test]
    fn post_replace_claim_failure_keeps_the_adapter_uninvoked() {
        let (store, scope) = state_store_with_session("worker-post-replace-claim-failure");
        let delivery = outbound_delivery_fixture("out_1", SessionId::for_scope(&scope), 10);
        store
            .enqueue_outbound_delivery(delivery.clone())
            .expect("delivery should enqueue");
        let worker = OutboxWorker::new(store.clone(), OutboundRetryPolicy::default());
        let mut adapter = RecordingImAdapter::default();
        fail_next_write_after_replace(store.path());

        let result = worker.process_next_with_clock(&mut adapter, || 11);

        let err = result.expect_err("post-replace claim failure should be reported");
        assert!(err.message().contains("reload persisted state"));
        assert!(adapter.attempts.is_empty());
        let state = store.load().expect("updated state should remain readable");
        assert_eq!(
            state
                .outbound_delivery(delivery.id())
                .expect("delivery should remain stored")
                .status(),
            OutboundDeliveryStatus::Delivering
        );

        let idle = worker
            .process_next_with_clock(&mut adapter, || 12)
            .expect("claimed delivery should not be sent after claim error");
        assert_eq!(
            idle,
            OutboxWorkerOutcome::Idle {
                next_attempt_at_unix: None,
            }
        );
        assert!(adapter.attempts.is_empty());
    }

    #[test]
    fn post_replace_delivered_failure_keeps_the_delivery_non_retryable() {
        let (store, scope) = state_store_with_session("worker-post-replace-delivered-failure");
        let delivery = outbound_delivery_fixture("out_1", SessionId::for_scope(&scope), 10);
        store
            .enqueue_outbound_delivery(delivery.clone())
            .expect("delivery should enqueue");
        let worker = OutboxWorker::new(store.clone(), OutboundRetryPolicy::default());
        let mut adapter = RecordingImAdapter {
            fail_after_replace_on_outcome: Some(store.path().to_path_buf()),
            ..RecordingImAdapter::default()
        };
        let mut times = [11, 12].into_iter();

        let result =
            worker.process_next_with_clock(&mut adapter, || times.next().expect("clock value"));

        let err = result.expect_err("post-replace delivered failure should be reported");
        assert!(err.message().contains("reload persisted state"));
        assert_eq!(adapter.attempts.len(), 1);
        let state = store.load().expect("updated state should remain readable");
        assert_eq!(
            state
                .outbound_delivery(delivery.id())
                .expect("delivery should remain stored")
                .status(),
            OutboundDeliveryStatus::Delivered
        );

        adapter.fail_after_replace_on_outcome = None;
        let idle = worker
            .process_next_with_clock(&mut adapter, || 13)
            .expect("delivered record should not be retried");
        assert_eq!(
            idle,
            OutboxWorkerOutcome::Idle {
                next_attempt_at_unix: None,
            }
        );
        assert_eq!(adapter.attempts.len(), 1);
    }

    #[test]
    fn post_replace_uncertain_failure_keeps_the_delivery_non_retryable() {
        let (store, scope) = state_store_with_session("worker-post-replace-uncertain-failure");
        let delivery = outbound_delivery_fixture("out_1", SessionId::for_scope(&scope), 10);
        store
            .enqueue_outbound_delivery(delivery.clone())
            .expect("delivery should enqueue");
        let worker = OutboxWorker::new(store.clone(), OutboundRetryPolicy::default());
        let mut adapter = RecordingImAdapter {
            failure: Some(OutboundDeliveryFailure::uncertain(
                "provider acceptance is unknown",
            )),
            fail_after_replace_on_outcome: Some(store.path().to_path_buf()),
            ..RecordingImAdapter::default()
        };
        let mut times = [11, 12].into_iter();

        let result =
            worker.process_next_with_clock(&mut adapter, || times.next().expect("clock value"));

        let err = result.expect_err("post-replace uncertain failure should be reported");
        assert!(err.message().contains("reload persisted state"));
        assert_eq!(adapter.attempts.len(), 1);
        let state = store.load().expect("updated state should remain readable");
        assert_eq!(
            state
                .outbound_delivery(delivery.id())
                .expect("delivery should remain stored")
                .status(),
            OutboundDeliveryStatus::Uncertain
        );

        adapter.fail_after_replace_on_outcome = None;
        let idle = worker
            .process_next_with_clock(&mut adapter, || 13)
            .expect("uncertain record should not be retried");
        assert_eq!(
            idle,
            OutboxWorkerOutcome::Idle {
                next_attempt_at_unix: None,
            }
        );
        assert_eq!(adapter.attempts.len(), 1);
    }

    #[derive(Default)]
    struct RecordingImAdapter {
        attempts: Vec<OutboundDeliveryAttempt>,
        failure: Option<OutboundDeliveryFailure>,
        state_check: Option<StateStore>,
        #[cfg(unix)]
        lock_outcome_directory: Option<PathBuf>,
        fail_after_replace_on_outcome: Option<PathBuf>,
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
            _acknowledgement: &InboundDeliveryAcknowledgement,
        ) -> Result<(), String> {
            Ok(())
        }

        fn deliver_outbound_message(
            &mut self,
            attempt: &OutboundDeliveryAttempt,
        ) -> Result<(), OutboundDeliveryFailure> {
            if let Some(state_store) = &self.state_check {
                let state = state_store
                    .load()
                    .map_err(OutboundDeliveryFailure::retryable)?;
                let stored = state
                    .outbound_delivery(attempt.delivery_id())
                    .ok_or_else(|| {
                        OutboundDeliveryFailure::retryable("adapter attempt is missing from state")
                    })?;
                if stored.status() != OutboundDeliveryStatus::Delivering {
                    return Err(OutboundDeliveryFailure::retryable(
                        "adapter received an outbound delivery before durable claim",
                    ));
                }
            }

            self.attempts.push(attempt.clone());

            if let Some(path) = &self.fail_after_replace_on_outcome {
                fail_next_write_after_replace(path);
            }

            #[cfg(unix)]
            if let Some(path) = &self.lock_outcome_directory {
                use std::{fs, os::unix::fs::PermissionsExt};

                fs::set_permissions(path, fs::Permissions::from_mode(0o500)).map_err(|err| {
                    OutboundDeliveryFailure::retryable(format!(
                        "failed to lock outcome directory: {err}"
                    ))
                })?;
            }

            match &self.failure {
                Some(error) => Err(error.clone()),
                None => Ok(()),
            }
        }
    }

    fn state_store_with_session(label: &str) -> (StateStore, SessionScope) {
        let path = test_path(label).join("runtime.state.json");
        let store = StateStore::new(path);
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let mut state = RuntimeState::new();
        state.upsert_session(Session::new(scope.clone()));
        store.save(&state).expect("initial state should save");
        (store, scope)
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
            MessageContent::text(format!("reply for {id}")).expect("valid text"),
            created_at_unix,
        );

        OutboundDeliveryRecord::new(
            OutboundDeliveryId::new(id).expect("valid delivery id"),
            session_id,
            message,
            created_at_unix,
        )
        .expect("valid outbound delivery")
    }

    fn test_path(label: &str) -> PathBuf {
        let sequence = NEXT_DIR.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "ferris-agent-bridge-outbox-worker-{}-{label}-{sequence}",
            std::process::id()
        ))
    }
}
