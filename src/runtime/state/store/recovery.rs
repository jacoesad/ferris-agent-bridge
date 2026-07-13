use crate::runtime::run::RunStartupRecoveryReport;

use super::StateStore;

impl StateStore {
    pub fn reconcile_runs_at_startup(
        &self,
        recovered_at_unix: u64,
    ) -> Result<RunStartupRecoveryReport, String> {
        let _guard = self.lock_write()?;
        let mut state = self.load()?;
        let (report, changed) = state.reconcile_runs_at_startup(recovered_at_unix)?;
        if changed {
            self.write_unlocked(&state)?;
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
        event::{Event, EventId, EventKind, EventSource},
        message::Message,
        persistence::{fail_next_write_after_replace, fail_next_write_before_replace},
        queue::{MessageBatchClaimOutcome, MessageQueuePolicy},
        run::{RunId, RunRecord, RunStatus},
        session::{Session, SessionId, SessionScope},
        state::RuntimeState,
    };

    use super::StateStore;

    static NEXT_DIR: AtomicU64 = AtomicU64::new(0);
    const FUTURE_UNIX: u64 = 4_102_444_800;

    #[test]
    fn startup_recovery_classifies_runs_without_returning_work_handoffs() {
        let (store, session_ids) = state_store_with_sessions(
            "run-recovery-classification",
            &["pending", "running", "failed", "orphan", "history"],
        );
        let pending_id = RunId::new("run_pending").expect("valid run id");
        let running_id = RunId::new("run_running").expect("valid run id");
        let failed_id = RunId::new("run_failed").expect("valid run id");
        let orphan_id = RunId::new("run_orphan").expect("valid run id");
        let completed_id = RunId::new("run_completed").expect("valid run id");
        let cancelled_id = RunId::new("run_cancelled").expect("valid run id");
        let pending_claimed_at = persist_and_claim_message(
            &store,
            &session_ids[0],
            "evt_pending",
            "msg_pending",
            pending_id.clone(),
            FUTURE_UNIX + 10,
        );
        let running_claimed_at = persist_and_claim_message(
            &store,
            &session_ids[1],
            "evt_running",
            "msg_running",
            running_id.clone(),
            FUTURE_UNIX + 20,
        );
        let mut state = store.load().expect("claimed state should load");
        state
            .start_run(&running_id, running_claimed_at + 1)
            .expect("claimed run should start");

        let mut failed =
            RunRecord::new(failed_id.clone(), session_ids[2].clone(), FUTURE_UNIX + 30);
        failed
            .fail(FUTURE_UNIX + 31)
            .expect("run should fail before startup");
        state
            .add_run(failed)
            .expect("failed run should be accepted");
        state
            .add_run(RunRecord::new(
                orphan_id.clone(),
                session_ids[3].clone(),
                FUTURE_UNIX + 40,
            ))
            .expect("input-less pending run should be accepted");

        let mut completed = RunRecord::new(
            completed_id.clone(),
            session_ids[4].clone(),
            FUTURE_UNIX + 50,
        );
        completed
            .start(FUTURE_UNIX + 51)
            .expect("historical run should start");
        completed
            .complete(FUTURE_UNIX + 52)
            .expect("historical run should complete");
        state
            .add_run(completed)
            .expect("completed history should be accepted");
        let mut cancelled = RunRecord::new(cancelled_id, session_ids[4].clone(), FUTURE_UNIX + 60);
        cancelled
            .cancel(FUTURE_UNIX + 61)
            .expect("historical run should cancel");
        state
            .add_run(cancelled)
            .expect("cancelled history should be accepted");
        store.save(&state).expect("recovery fixture should persist");

        let report = store
            .reconcile_runs_at_startup(FUTURE_UNIX)
            .expect("startup recovery should succeed despite clock rollback");

        assert_eq!(
            report.resumable_pending_run_ids(),
            std::slice::from_ref(&pending_id)
        );
        assert_eq!(
            report.interrupted_run_ids(),
            &[running_id.clone(), orphan_id.clone()]
        );
        assert_eq!(report.failed_run_ids(), std::slice::from_ref(&failed_id));
        assert!(!report.is_empty());
        let recovered = store.load().expect("recovered state should load");
        assert_eq!(
            recovered
                .run(&pending_id)
                .expect("pending run should exist")
                .status(),
            RunStatus::Pending
        );
        assert_eq!(
            recovered
                .run(&running_id)
                .expect("running run should exist")
                .status(),
            RunStatus::Interrupted
        );
        assert_eq!(
            recovered
                .run(&running_id)
                .expect("running run should exist")
                .updated_at_unix(),
            running_claimed_at + 1
        );
        assert_eq!(
            recovered
                .run(&orphan_id)
                .expect("orphan run should exist")
                .status(),
            RunStatus::Interrupted
        );
        assert_eq!(recovered.run_inputs().len(), 2);
        assert_eq!(
            recovered
                .run_input(&pending_id)
                .expect("pending input should remain durable")
                .claimed_at_unix(),
            pending_claimed_at
        );

        let second = store
            .reconcile_runs_at_startup(FUTURE_UNIX + 100)
            .expect("repeated recovery should be idempotent");
        assert_eq!(second, report);
        assert_eq!(store.load().expect("state should load"), recovered);

        store
            .persist_inbound_event(&message_event(
                "evt_after_interrupt",
                "msg_after_interrupt",
                &session_ids[1],
                FUTURE_UNIX + 70,
            ))
            .expect("new work should remain queued behind interrupted ownership");
        let blocked = store
            .claim_message_batch(
                RunId::new("run_blocked").expect("valid run id"),
                &MessageQueuePolicy::new(0, 1).expect("valid policy"),
                FUTURE_UNIX + 100,
            )
            .expect("queue claim should inspect blocked work");
        assert!(matches!(
            blocked,
            MessageBatchClaimOutcome::Waiting {
                next_ready_at_unix: None
            }
        ));

        let mut resolved = store.load().expect("blocked state should load");
        resolved
            .fail_run(&running_id, FUTURE_UNIX + 101)
            .expect("interrupted run should resolve as failed");
        resolved
            .cancel_run(&orphan_id, FUTURE_UNIX + 101)
            .expect("input-less interrupted run should resolve as cancelled");
        store.save(&resolved).expect("resolution should persist");
        let released = store
            .claim_message_batch(
                RunId::new("run_released").expect("valid run id"),
                &MessageQueuePolicy::new(0, 1).expect("valid policy"),
                FUTURE_UNIX + 101,
            )
            .expect("resolved scope should be claimable");
        assert!(matches!(released, MessageBatchClaimOutcome::Claimed { .. }));
    }

    #[test]
    fn startup_recovery_does_not_create_missing_state() {
        let store = StateStore::new(test_path("run-recovery-empty").join("runtime.state.json"));

        let report = store
            .reconcile_runs_at_startup(FUTURE_UNIX)
            .expect("empty recovery should succeed");

        assert!(report.is_empty());
        assert!(!store.path().exists());
    }

    #[test]
    fn startup_recovery_pre_replace_failure_leaves_running_state() {
        let (store, run_id, started_at) = state_store_with_running_run("run-recovery-pre-failure");
        fail_next_write_before_replace(store.path());

        let err = store
            .reconcile_runs_at_startup(started_at + 1)
            .expect_err("pre-replace recovery failure must return no report");

        assert!(err.contains("failed to save runtime state"));
        assert_eq!(
            store
                .load()
                .expect("state should load")
                .run(&run_id)
                .expect("run should exist")
                .status(),
            RunStatus::Running
        );
        let retried = store
            .reconcile_runs_at_startup(started_at + 1)
            .expect("retry should reconcile the still-running state");
        assert_eq!(retried.interrupted_run_ids(), &[run_id]);
    }

    #[test]
    fn startup_recovery_post_replace_failure_rebuilds_report() {
        let (store, run_id, started_at) = state_store_with_running_run("run-recovery-post-failure");
        fail_next_write_after_replace(store.path());

        let err = store
            .reconcile_runs_at_startup(started_at + 1)
            .expect_err("post-replace recovery failure must return no report");

        assert!(err.contains("failed to save runtime state"));
        assert_eq!(
            store
                .load()
                .expect("state should load")
                .run(&run_id)
                .expect("run should exist")
                .status(),
            RunStatus::Interrupted
        );
        let retried = store
            .reconcile_runs_at_startup(started_at + 2)
            .expect("retry should rebuild the report from durable interruption");
        assert_eq!(retried.interrupted_run_ids(), &[run_id]);
    }

    #[test]
    fn concurrent_startup_recovery_returns_one_durable_classification() {
        let (store, run_id, started_at) = state_store_with_running_run("run-recovery-concurrent");
        let barrier = Arc::new(Barrier::new(3));
        let handles = [store.clone(), StateStore::new(store.path())].map(|worker_store| {
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                worker_store.reconcile_runs_at_startup(started_at + 1)
            })
        });
        barrier.wait();

        for handle in handles {
            let report = handle
                .join()
                .expect("recovery worker should not panic")
                .expect("recovery worker should succeed");
            assert_eq!(report.interrupted_run_ids(), std::slice::from_ref(&run_id));
        }
        assert_eq!(
            store
                .load()
                .expect("state should load")
                .run(&run_id)
                .expect("run should exist")
                .status(),
            RunStatus::Interrupted
        );
    }

    #[test]
    fn stale_running_snapshot_does_not_erase_startup_interruption() {
        let (store, run_id, started_at) = state_store_with_running_run("run-recovery-stale-save");
        let stale = store.load().expect("running snapshot should load");
        store
            .reconcile_runs_at_startup(started_at + 1)
            .expect("startup recovery should interrupt the run");

        store
            .save(&stale)
            .expect("stale running snapshot should preserve durable interruption");

        assert_eq!(
            store
                .load()
                .expect("state should load")
                .run(&run_id)
                .expect("run should exist")
                .status(),
            RunStatus::Interrupted
        );
    }

    fn state_store_with_running_run(label: &str) -> (StateStore, RunId, u64) {
        let (store, session_ids) = state_store_with_sessions(label, &["running"]);
        let run_id = RunId::new("run_running").expect("valid run id");
        let claimed_at = persist_and_claim_message(
            &store,
            &session_ids[0],
            "evt_running",
            "msg_running",
            run_id.clone(),
            FUTURE_UNIX + 10,
        );
        let started_at = claimed_at + 1;
        let mut state = store.load().expect("claimed state should load");
        state
            .start_run(&run_id, started_at)
            .expect("claimed run should start");
        store.save(&state).expect("running state should persist");
        (store, run_id, started_at)
    }

    fn state_store_with_sessions(label: &str, scopes: &[&str]) -> (StateStore, Vec<SessionId>) {
        let store = StateStore::new(test_path(label).join("runtime.state.json"));
        let mut state = RuntimeState::new();
        let mut session_ids = Vec::new();
        for scope in scopes {
            let session = Session::new(
                SessionScope::new("lark", format!("chat:{scope}")).expect("valid session scope"),
            );
            session_ids.push(session.id().clone());
            state.upsert_session(session);
        }
        store.save(&state).expect("sessions should persist");
        (store, session_ids)
    }

    fn persist_and_claim_message(
        store: &StateStore,
        session_id: &SessionId,
        event_id: &str,
        message_id: &str,
        run_id: RunId,
        received_at_unix: u64,
    ) -> u64 {
        let event = message_event(event_id, message_id, session_id, received_at_unix);
        store
            .persist_inbound_event(&event)
            .expect("message should persist");
        let queued_at = store
            .load()
            .expect("queued state should load")
            .queued_messages()
            .iter()
            .find(|queued| queued.event_id() == &event.id)
            .expect("message should be queued")
            .enqueued_at_unix();
        let outcome = store
            .claim_message_batch(
                run_id,
                &MessageQueuePolicy::new(0, 1).expect("valid policy"),
                queued_at,
            )
            .expect("message should claim");
        assert!(matches!(outcome, MessageBatchClaimOutcome::Claimed { .. }));
        queued_at
    }

    fn message_event(
        event_id: &str,
        message_id: &str,
        session_id: &SessionId,
        received_at_unix: u64,
    ) -> Event {
        Event::new(
            EventId::new(event_id).expect("valid event id"),
            EventSource::Platform,
            EventKind::MessageReceived {
                message: Message::user_text(
                    message_id,
                    Some(session_id.clone()),
                    "hello",
                    received_at_unix,
                )
                .expect("valid message"),
            },
            received_at_unix,
        )
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "ferris-agent-bridge-{name}-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed)
        ))
    }
}
