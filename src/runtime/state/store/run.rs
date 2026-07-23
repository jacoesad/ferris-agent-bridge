#![allow(
    dead_code,
    reason = "reserved for the next M3 concrete orchestrator wiring slice"
)]

use crate::runtime::{
    message::{Message, MessageAuthor},
    outbox::{OutboundDeliveryId, OutboundDeliveryRecord},
    persistence::confirm_existing_file_durable,
    queue::RunInputRecord,
    run::{RunId, RunRecord, RunStatus},
    session::SessionId,
};

use super::StateStore;

impl StateStore {
    pub(crate) fn start_agent_run(
        &self,
        id: &RunId,
        started_at_unix: u64,
    ) -> Result<RunInputRecord, String> {
        let _guard = self.lock_write()?;
        let mut state = self.load()?;
        let run = state
            .run(id)
            .cloned()
            .ok_or_else(|| format!("unknown run id {id}"))?;
        let input = state
            .run_input(id)
            .cloned()
            .ok_or_else(|| format!("agent run {id} has no durable input"))?;

        if input.session_id() != run.session_id() {
            return Err(format!(
                "agent run {id} input does not match session {}",
                run.session_id()
            ));
        }
        if started_at_unix < run.updated_at_unix() {
            return Err(format!(
                "agent run {id} cannot start before updated_at_unix"
            ));
        }

        state.start_run(id, started_at_unix)?;
        self.write_unlocked(&state)?;
        Ok(input)
    }

    pub(crate) fn complete_agent_run(
        &self,
        id: &RunId,
        messages: &[Message],
        finished_at_unix: u64,
    ) -> Result<Vec<OutboundDeliveryRecord>, String> {
        let _guard = self.lock_write()?;
        let mut state = self.load()?;
        let run = state
            .run(id)
            .cloned()
            .ok_or_else(|| format!("unknown run id {id}"))?;
        let expected = output_deliveries(id, run.session_id(), messages, finished_at_unix)?;

        match run.status() {
            RunStatus::Running => {
                let completed =
                    state.complete_run_with_output_deliveries(id, expected, finished_at_unix)?;
                self.write_unlocked(&state)?;
                Ok(completed)
            }
            RunStatus::Completed => {
                verify_completion_replay(&state, &run, &expected, finished_at_unix)?;
                self.confirm_transition_replay_durable()?;
                Ok(run
                    .output_delivery_ids()
                    .iter()
                    .map(|delivery_id| {
                        state
                            .outbound_delivery(delivery_id)
                            .expect("completion replay verified every output delivery")
                            .clone()
                    })
                    .collect())
            }
            status => Err(format!("run {id} cannot complete from {status:?}")),
        }
    }

    pub(crate) fn fail_agent_run(
        &self,
        id: &RunId,
        failed_at_unix: u64,
    ) -> Result<RunRecord, String> {
        let _guard = self.lock_write()?;
        let mut state = self.load()?;
        let run = state
            .run(id)
            .cloned()
            .ok_or_else(|| format!("unknown run id {id}"))?;

        match run.status() {
            RunStatus::Running => {
                require_nondecreasing_outcome_time(&run, failed_at_unix, "fail")?;
                state.fail_run(id, failed_at_unix)?;
                self.write_unlocked(&state)?;
                Ok(state
                    .run(id)
                    .expect("failed run must remain in state")
                    .clone())
            }
            RunStatus::Failed
                if run.started_at_unix().is_some()
                    && run.finished_at_unix() == Some(failed_at_unix) =>
            {
                self.confirm_transition_replay_durable()?;
                Ok(run)
            }
            RunStatus::Failed => Err(format!(
                "agent run {id} failed outcome conflicts with persisted state"
            )),
            status => Err(format!("agent run {id} cannot fail from {status:?}")),
        }
    }

    pub(crate) fn interrupt_agent_run(
        &self,
        id: &RunId,
        interrupted_at_unix: u64,
    ) -> Result<RunRecord, String> {
        let _guard = self.lock_write()?;
        let mut state = self.load()?;
        let run = state
            .run(id)
            .cloned()
            .ok_or_else(|| format!("unknown run id {id}"))?;

        match run.status() {
            RunStatus::Running => {
                require_nondecreasing_outcome_time(&run, interrupted_at_unix, "interrupt")?;
                state.interrupt_run(id, interrupted_at_unix)?;
                self.write_unlocked(&state)?;
                Ok(state
                    .run(id)
                    .expect("interrupted run must remain in state")
                    .clone())
            }
            RunStatus::Interrupted
                if run.started_at_unix().is_some()
                    && run.updated_at_unix() == interrupted_at_unix =>
            {
                self.confirm_transition_replay_durable()?;
                Ok(run)
            }
            RunStatus::Interrupted => Err(format!(
                "agent run {id} interrupted outcome conflicts with persisted state"
            )),
            status => Err(format!("agent run {id} cannot interrupt from {status:?}")),
        }
    }

    fn confirm_transition_replay_durable(&self) -> Result<(), String> {
        confirm_existing_file_durable(self.path()).map_err(|err| {
            format!(
                "failed to confirm runtime state {} durability: {err}",
                self.path().display()
            )
        })
    }
}

fn output_deliveries(
    run_id: &RunId,
    session_id: &SessionId,
    messages: &[Message],
    finished_at_unix: u64,
) -> Result<Vec<OutboundDeliveryRecord>, String> {
    if messages.is_empty() {
        return Err(format!(
            "agent run {run_id} completion must contain output messages"
        ));
    }

    messages
        .iter()
        .enumerate()
        .map(|(index, message)| {
            if message.author != MessageAuthor::Agent {
                return Err(format!(
                    "agent run {run_id} output message {} must be agent-authored",
                    message.id
                ));
            }
            if message.session_id.as_ref() != Some(session_id) {
                return Err(format!(
                    "agent run {run_id} output message {} does not match session {session_id}",
                    message.id
                ));
            }

            OutboundDeliveryRecord::new(
                output_delivery_id(run_id, index)?,
                session_id.clone(),
                message.clone(),
                finished_at_unix,
            )
        })
        .collect()
}

fn output_delivery_id(run_id: &RunId, index: usize) -> Result<OutboundDeliveryId, String> {
    OutboundDeliveryId::new(format!(
        "agent-run-output:{}:{}:{index}",
        run_id.as_str().len(),
        run_id
    ))
}

fn verify_completion_replay(
    state: &crate::runtime::state::RuntimeState,
    run: &RunRecord,
    expected: &[OutboundDeliveryRecord],
    finished_at_unix: u64,
) -> Result<(), String> {
    if run.finished_at_unix() != Some(finished_at_unix) {
        return Err(format!(
            "agent run {} completion timestamp conflicts with persisted state",
            run.id()
        ));
    }

    let expected_ids = expected
        .iter()
        .map(|delivery| delivery.id().clone())
        .collect::<Vec<_>>();
    if run.output_delivery_ids() != expected_ids {
        return Err(format!(
            "agent run {} completion output ids conflict with persisted state",
            run.id()
        ));
    }

    for expected_delivery in expected {
        let persisted = state
            .outbound_delivery(expected_delivery.id())
            .ok_or_else(|| {
                format!(
                    "agent run {} completion is missing outbound delivery {}",
                    run.id(),
                    expected_delivery.id()
                )
            })?;
        if !persisted.has_same_enqueue_identity(expected_delivery) {
            return Err(format!(
                "agent run {} completion output {} conflicts with persisted state",
                run.id(),
                expected_delivery.id()
            ));
        }
    }

    Ok(())
}

fn require_nondecreasing_outcome_time(
    run: &RunRecord,
    outcome_at_unix: u64,
    action: &str,
) -> Result<(), String> {
    if outcome_at_unix < run.updated_at_unix() {
        return Err(format!(
            "agent run {} cannot {action} before updated_at_unix",
            run.id()
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        sync::{
            Arc, Barrier,
            atomic::{AtomicU64, Ordering},
        },
        thread,
    };

    use crate::runtime::{
        event::{Event, EventId, EventKind, EventSource},
        message::{Message, MessageAuthor, MessageContent, MessageId},
        outbox::OutboundDeliveryStatus,
        persistence::{
            fail_next_parent_sync, fail_next_write_after_replace, fail_next_write_before_replace,
        },
        queue::{MessageBatchClaimOutcome, MessageQueuePolicy, RunInputRecord},
        run::{RunId, RunRecord, RunStatus},
        session::{Session, SessionId, SessionScope},
        state::{RuntimeState, StateStore},
    };

    static NEXT_DIR: AtomicU64 = AtomicU64::new(0);
    const FUTURE_UNIX: u64 = 4_102_444_800;

    #[test]
    fn start_agent_run_persists_running_before_returning_input() {
        let (store, run_id, input, _) = pending_agent_run("start-before-handoff");

        let returned = store
            .start_agent_run(&run_id, FUTURE_UNIX + 1)
            .expect("pending run should start");

        assert_eq!(returned, input);
        let state = store.load().expect("state should load");
        let run = state.run(&run_id).expect("run should remain durable");
        assert_eq!(run.status(), RunStatus::Running);
        assert_eq!(run.started_at_unix(), Some(FUTURE_UNIX + 1));
        assert_eq!(state.run_input(&run_id), Some(&input));
    }

    #[test]
    fn concurrent_agent_run_starts_return_only_one_handoff() {
        let (store, run_id, _, _) = pending_agent_run("concurrent-start");
        let barrier = Arc::new(Barrier::new(3));
        let mut workers = Vec::new();

        for _ in 0..2 {
            let worker_store = StateStore::new(store.path().to_path_buf());
            let worker_run_id = run_id.clone();
            let worker_barrier = Arc::clone(&barrier);
            workers.push(thread::spawn(move || {
                worker_barrier.wait();
                worker_store.start_agent_run(&worker_run_id, FUTURE_UNIX + 1)
            }));
        }

        barrier.wait();
        let results = workers
            .into_iter()
            .map(|worker| worker.join().expect("start worker should join"))
            .collect::<Vec<_>>();

        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
        assert_eq!(
            store
                .load()
                .expect("state should load")
                .run(&run_id)
                .expect("run should remain durable")
                .status(),
            RunStatus::Running
        );
    }

    #[test]
    fn snapshot_save_cannot_advance_a_durable_agent_run() {
        let (store, run_id, _, _) = pending_agent_run("snapshot-lifecycle-bypass");
        let stale_pending = store.load().expect("pending state should load");
        let mut starting = store.load().expect("pending state should load");
        starting
            .start_run(&run_id, FUTURE_UNIX + 1)
            .expect("snapshot can represent a running run in memory");

        let err = store
            .save(&starting)
            .expect_err("snapshot save must not create an agent handoff");
        assert!(err.contains("cannot create a handoff or completion"));
        assert_eq!(
            store
                .load()
                .expect("state should remain readable")
                .run(&run_id)
                .expect("run should remain durable")
                .status(),
            RunStatus::Pending
        );

        store
            .start_agent_run(&run_id, FUTURE_UNIX + 1)
            .expect("store transition should start the run");
        store
            .save(&stale_pending)
            .expect("stale ancestor should preserve the durable running run");
        let mut completing = store.load().expect("running state should load");
        completing
            .complete_run(&run_id, FUTURE_UNIX + 2)
            .expect("snapshot can represent outputless completion in memory");

        let err = store
            .save(&completing)
            .expect_err("snapshot save must not bypass atomic output completion");
        assert!(err.contains("cannot create a handoff or completion"));
        assert_eq!(
            store
                .load()
                .expect("state should remain readable")
                .run(&run_id)
                .expect("run should remain durable")
                .status(),
            RunStatus::Running
        );
    }

    #[test]
    fn start_agent_run_requires_input_and_returns_no_handoff_on_write_failure() {
        let missing_input_store = state_store_with_session("start-missing-input");
        let session_id = only_session_id(&missing_input_store);
        let missing_input_id = RunId::new("run_missing_input").expect("valid run id");
        let mut state = missing_input_store.load().expect("state should load");
        state
            .add_run(RunRecord::new(
                missing_input_id.clone(),
                session_id,
                FUTURE_UNIX,
            ))
            .expect("pending run should be accepted");
        missing_input_store
            .save(&state)
            .expect("pending run should save");

        let err = missing_input_store
            .start_agent_run(&missing_input_id, FUTURE_UNIX + 1)
            .expect_err("input-less run must not produce a handoff");
        assert!(err.contains("no durable input"));

        let (pre_store, pre_run_id, _, _) = pending_agent_run("start-pre-replace");
        fail_next_write_before_replace(pre_store.path());
        assert!(
            pre_store
                .start_agent_run(&pre_run_id, FUTURE_UNIX + 1)
                .is_err()
        );
        assert_eq!(
            pre_store
                .load()
                .expect("pre-replace state should load")
                .run(&pre_run_id)
                .expect("run should remain durable")
                .status(),
            RunStatus::Pending
        );

        let (post_store, post_run_id, _, _) = pending_agent_run("start-post-replace");
        fail_next_write_after_replace(post_store.path());
        assert!(
            post_store
                .start_agent_run(&post_run_id, FUTURE_UNIX + 1)
                .is_err()
        );
        assert_eq!(
            post_store
                .load()
                .expect("post-replace state should load")
                .run(&post_run_id)
                .expect("run should remain durable")
                .status(),
            RunStatus::Running
        );
        assert!(
            post_store
                .start_agent_run(&post_run_id, FUTURE_UNIX + 1)
                .is_err(),
            "a visible running state must not return a second handoff"
        );
    }

    #[test]
    fn completion_atomically_links_pending_outbox_records_to_the_run() {
        let (store, run_id, _, session_id) = running_agent_run("complete-output");
        let output = vec![
            agent_message("reply_1", &session_id, "first", FUTURE_UNIX + 2),
            agent_message("reply_2", &session_id, "second", FUTURE_UNIX + 2),
        ];

        let deliveries = store
            .complete_agent_run(&run_id, &output, FUTURE_UNIX + 3)
            .expect("agent output should complete atomically");

        assert_eq!(deliveries.len(), 2);
        assert!(
            deliveries
                .iter()
                .all(|delivery| delivery.status() == OutboundDeliveryStatus::Pending)
        );
        let state = store.load().expect("completed state should load");
        let run = state.run(&run_id).expect("run should remain durable");
        assert_eq!(run.status(), RunStatus::Completed);
        assert_eq!(run.finished_at_unix(), Some(FUTURE_UNIX + 3));
        assert_eq!(
            run.output_delivery_ids(),
            deliveries
                .iter()
                .map(|delivery| delivery.id().clone())
                .collect::<Vec<_>>()
        );
        for (delivery, message) in deliveries.iter().zip(output) {
            assert_eq!(delivery.message(), &message);
            assert_eq!(state.outbound_delivery(delivery.id()), Some(delivery));
        }
    }

    #[test]
    fn completion_rejects_invalid_output_without_changing_running_state() {
        let (store, run_id, _, session_id) = running_agent_run("complete-invalid-output");

        assert!(
            store
                .complete_agent_run(&run_id, &[], FUTURE_UNIX + 3)
                .is_err()
        );
        let user = Message::user_text(
            "reply_user",
            Some(session_id.clone()),
            "not agent output",
            FUTURE_UNIX + 2,
        )
        .expect("valid user message");
        assert!(
            store
                .complete_agent_run(&run_id, &[user], FUTURE_UNIX + 3)
                .is_err()
        );
        let other_session =
            SessionId::for_scope(&SessionScope::new("lark", "chat:other").expect("valid scope"));
        assert!(
            store
                .complete_agent_run(
                    &run_id,
                    &[agent_message(
                        "reply_other",
                        &other_session,
                        "wrong session",
                        FUTURE_UNIX + 2,
                    )],
                    FUTURE_UNIX + 3,
                )
                .is_err()
        );
        assert!(
            store
                .complete_agent_run(
                    &run_id,
                    &[agent_message(
                        "reply_future",
                        &session_id,
                        "future message",
                        FUTURE_UNIX + 4,
                    )],
                    FUTURE_UNIX + 3,
                )
                .is_err()
        );

        let state = store.load().expect("state should remain readable");
        assert_eq!(
            state
                .run(&run_id)
                .expect("run should remain durable")
                .status(),
            RunStatus::Running
        );
        assert!(state.outbound_deliveries().is_empty());
    }

    #[test]
    fn completion_write_failures_preserve_atomic_state_and_exact_replay() {
        let (pre_store, pre_run_id, _, pre_session_id) = running_agent_run("complete-pre-replace");
        let pre_output = [agent_message(
            "reply_pre",
            &pre_session_id,
            "pre",
            FUTURE_UNIX + 2,
        )];
        fail_next_write_before_replace(pre_store.path());
        assert!(
            pre_store
                .complete_agent_run(&pre_run_id, &pre_output, FUTURE_UNIX + 3)
                .is_err()
        );
        let pre_state = pre_store.load().expect("pre-replace state should load");
        assert_eq!(
            pre_state
                .run(&pre_run_id)
                .expect("run should remain durable")
                .status(),
            RunStatus::Running
        );
        assert!(pre_state.outbound_deliveries().is_empty());

        let (post_store, post_run_id, _, post_session_id) =
            running_agent_run("complete-post-replace");
        let post_output = [agent_message(
            "reply_post",
            &post_session_id,
            "post",
            FUTURE_UNIX + 2,
        )];
        fail_next_write_after_replace(post_store.path());
        assert!(
            post_store
                .complete_agent_run(&post_run_id, &post_output, FUTURE_UNIX + 3)
                .is_err()
        );
        let post_state = post_store.load().expect("post-replace state should load");
        assert_eq!(
            post_state
                .run(&post_run_id)
                .expect("run should remain durable")
                .status(),
            RunStatus::Completed
        );
        assert_eq!(post_state.outbound_deliveries().len(), 1);

        let replayed = post_store
            .complete_agent_run(&post_run_id, &post_output, FUTURE_UNIX + 3)
            .expect("exact completion replay should confirm durability");
        assert_eq!(replayed.len(), 1);

        fail_next_parent_sync(post_store.path());
        assert!(
            post_store
                .complete_agent_run(&post_run_id, &post_output, FUTURE_UNIX + 3)
                .is_err(),
            "replay must report a failed durability barrier"
        );
        post_store
            .complete_agent_run(&post_run_id, &post_output, FUTURE_UNIX + 3)
            .expect("later exact replay should re-establish durability");
    }

    #[test]
    fn completion_replay_rejects_changed_output_or_timestamp() {
        let (store, run_id, _, session_id) = running_agent_run("complete-conflict");
        let output = [agent_message(
            "reply_1",
            &session_id,
            "original",
            FUTURE_UNIX + 2,
        )];
        store
            .complete_agent_run(&run_id, &output, FUTURE_UNIX + 3)
            .expect("first completion should persist");

        let changed = [agent_message(
            "reply_1",
            &session_id,
            "changed",
            FUTURE_UNIX + 2,
        )];
        assert!(
            store
                .complete_agent_run(&run_id, &changed, FUTURE_UNIX + 3)
                .is_err()
        );
        assert!(
            store
                .complete_agent_run(&run_id, &output, FUTURE_UNIX + 4)
                .is_err()
        );
    }

    #[test]
    fn completion_replay_accepts_output_after_delivery_status_advances() {
        for target_status in [
            OutboundDeliveryStatus::Delivering,
            OutboundDeliveryStatus::Delivered,
            OutboundDeliveryStatus::Failed,
            OutboundDeliveryStatus::Uncertain,
        ] {
            let label = format!("complete-progressed-{target_status:?}");
            let (store, run_id, _, session_id) = running_agent_run(&label);
            let output = [agent_message(
                &format!("reply_{target_status:?}"),
                &session_id,
                "progressed",
                FUTURE_UNIX + 2,
            )];
            let completed = store
                .complete_agent_run(&run_id, &output, FUTURE_UNIX + 3)
                .expect("first completion should persist");
            let delivery_id = completed[0].id().clone();

            let claimed = store
                .claim_next_outbound_delivery(FUTURE_UNIX + 4)
                .expect("pending output should be claimable")
                .expect("completed run should own one pending output");
            assert_eq!(claimed.id(), &delivery_id);
            match target_status {
                OutboundDeliveryStatus::Delivering => {}
                OutboundDeliveryStatus::Delivered => {
                    store
                        .mark_outbound_delivery_delivered(&delivery_id, FUTURE_UNIX + 5)
                        .expect("claimed output should become delivered");
                }
                OutboundDeliveryStatus::Failed => {
                    store
                        .mark_outbound_delivery_failed(
                            &delivery_id,
                            FUTURE_UNIX + 5,
                            "retryable failure",
                        )
                        .expect("claimed output should become failed");
                }
                OutboundDeliveryStatus::Uncertain => {
                    store
                        .mark_outbound_delivery_uncertain(
                            &delivery_id,
                            FUTURE_UNIX + 5,
                            "uncertain delivery",
                        )
                        .expect("claimed output should become uncertain");
                }
                OutboundDeliveryStatus::Pending => unreachable!("target status is progressed"),
            }

            let replayed = store
                .complete_agent_run(&run_id, &output, FUTURE_UNIX + 3)
                .expect("exact completion replay should ignore mutable delivery status");
            assert_eq!(replayed.len(), 1);
            assert_eq!(replayed[0].status(), target_status);
            assert_eq!(replayed[0].id(), &delivery_id);
        }
    }

    #[test]
    fn definite_and_uncertain_outcomes_release_or_preserve_ownership() {
        let (failed_store, failed_run_id, _, _) = running_agent_run("definite-failure");
        let failed = failed_store
            .fail_agent_run(&failed_run_id, FUTURE_UNIX + 2)
            .expect("definite failure should persist");
        assert_eq!(failed.status(), RunStatus::Failed);
        assert!(failed.is_terminal());
        assert!(failed.output_delivery_ids().is_empty());
        assert_eq!(
            failed_store
                .fail_agent_run(&failed_run_id, FUTURE_UNIX + 2)
                .expect("exact failed replay should confirm durability"),
            failed
        );
        assert!(
            failed_store
                .fail_agent_run(&failed_run_id, FUTURE_UNIX + 3)
                .is_err()
        );

        let (interrupted_store, interrupted_run_id, _, _) = running_agent_run("uncertain-failure");
        let interrupted = interrupted_store
            .interrupt_agent_run(&interrupted_run_id, FUTURE_UNIX + 2)
            .expect("uncertain failure should preserve ownership");
        assert_eq!(interrupted.status(), RunStatus::Interrupted);
        assert!(!interrupted.is_terminal());
        assert!(interrupted.output_delivery_ids().is_empty());
        assert_eq!(
            interrupted_store
                .interrupt_agent_run(&interrupted_run_id, FUTURE_UNIX + 2)
                .expect("exact interrupted replay should confirm durability"),
            interrupted
        );
        assert!(
            interrupted_store
                .interrupt_agent_run(&interrupted_run_id, FUTURE_UNIX + 3)
                .is_err()
        );
    }

    #[test]
    fn failed_outcome_writes_return_no_false_success_and_support_exact_replay() {
        let (pre_store, pre_run_id, _, _) = running_agent_run("failure-pre-replace");
        fail_next_write_before_replace(pre_store.path());
        assert!(
            pre_store
                .fail_agent_run(&pre_run_id, FUTURE_UNIX + 2)
                .is_err()
        );
        assert_eq!(
            pre_store
                .load()
                .expect("pre-replace state should load")
                .run(&pre_run_id)
                .expect("run should remain durable")
                .status(),
            RunStatus::Running
        );

        let (failed_store, failed_run_id, _, _) = running_agent_run("failure-post-replace");
        fail_next_write_after_replace(failed_store.path());
        assert!(
            failed_store
                .fail_agent_run(&failed_run_id, FUTURE_UNIX + 2)
                .is_err()
        );
        assert_eq!(
            failed_store
                .load()
                .expect("post-replace failed state should load")
                .run(&failed_run_id)
                .expect("run should remain durable")
                .status(),
            RunStatus::Failed
        );
        failed_store
            .fail_agent_run(&failed_run_id, FUTURE_UNIX + 2)
            .expect("exact failed replay should confirm durability");

        let (interrupted_store, interrupted_run_id, _, _) =
            running_agent_run("interrupt-post-replace");
        fail_next_write_after_replace(interrupted_store.path());
        assert!(
            interrupted_store
                .interrupt_agent_run(&interrupted_run_id, FUTURE_UNIX + 2)
                .is_err()
        );
        assert_eq!(
            interrupted_store
                .load()
                .expect("post-replace interrupted state should load")
                .run(&interrupted_run_id)
                .expect("run should remain durable")
                .status(),
            RunStatus::Interrupted
        );
        interrupted_store
            .interrupt_agent_run(&interrupted_run_id, FUTURE_UNIX + 2)
            .expect("exact interrupted replay should confirm durability");
    }

    #[test]
    fn stale_snapshot_cannot_undo_completion_or_erase_linked_output() {
        let (store, run_id, _, session_id) = running_agent_run("completion-stale-save");
        let stale = store.load().expect("stale running state should load");
        let output = [agent_message(
            "reply_1",
            &session_id,
            "durable",
            FUTURE_UNIX + 2,
        )];
        let deliveries = store
            .complete_agent_run(&run_id, &output, FUTURE_UNIX + 3)
            .expect("completion should persist");

        store
            .save(&stale)
            .expect("stale save should preserve durable descendants");

        let state = store.load().expect("merged state should load");
        let run = state.run(&run_id).expect("run should remain durable");
        assert_eq!(run.status(), RunStatus::Completed);
        assert_eq!(run.output_delivery_ids(), [deliveries[0].id().clone()]);
        assert_eq!(
            state.outbound_delivery(deliveries[0].id()),
            Some(&deliveries[0])
        );
    }

    fn pending_agent_run(label: &str) -> (StateStore, RunId, RunInputRecord, SessionId) {
        let store = state_store_with_session(label);
        let session_id = only_session_id(&store);
        let event = Event::new(
            EventId::new(format!("evt_{label}")).expect("valid event id"),
            EventSource::Platform,
            EventKind::MessageReceived {
                message: Message::user_text(
                    format!("msg_{label}"),
                    Some(session_id.clone()),
                    "hello",
                    FUTURE_UNIX,
                )
                .expect("valid user message"),
            },
            FUTURE_UNIX,
        );
        store
            .persist_inbound_event(&event)
            .expect("inbound message should persist");
        let run_id = RunId::new(format!("run_{label}")).expect("valid run id");
        let policy = MessageQueuePolicy::new(0, 10).expect("valid queue policy");
        let MessageBatchClaimOutcome::Claimed { input, .. } = store
            .claim_message_batch(run_id.clone(), &policy, FUTURE_UNIX)
            .expect("message batch claim should persist")
        else {
            panic!("message batch should be ready");
        };

        (store, run_id, input, session_id)
    }

    fn running_agent_run(label: &str) -> (StateStore, RunId, RunInputRecord, SessionId) {
        let (store, run_id, input, session_id) = pending_agent_run(label);
        store
            .start_agent_run(&run_id, FUTURE_UNIX + 1)
            .expect("claimed run should start");
        (store, run_id, input, session_id)
    }

    fn state_store_with_session(label: &str) -> StateStore {
        let store = StateStore::new(test_path(label).join("runtime.state.json"));
        let session =
            Session::new(SessionScope::new("lark", format!("chat:{label}")).expect("valid scope"));
        let mut state = RuntimeState::new();
        state.upsert_session(session);
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

    fn agent_message(id: &str, session_id: &SessionId, text: &str, created_at: u64) -> Message {
        Message::new(
            MessageId::new(id).expect("valid message id"),
            Some(session_id.clone()),
            MessageAuthor::Agent,
            MessageContent::text(text).expect("valid content"),
            created_at,
        )
    }

    fn test_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "ferris-agent-bridge-agent-run-store-{}-{label}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed)
        ))
    }
}
