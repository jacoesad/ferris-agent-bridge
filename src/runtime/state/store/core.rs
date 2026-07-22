use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
};

use crate::runtime::{
    outbox::OutboundDeliveryStatus, persistence::write_json_atomic, run::RunStatus,
};

use super::{
    super::{
        format::{parse_state_file, state_file_from_state},
        model::RuntimeState,
    },
    locking::{state_store_lock_key, write_lock_for_path},
};

#[derive(Debug, Clone)]
pub struct StateStore {
    inner: Arc<StateStoreInner>,
}

#[derive(Debug)]
struct StateStoreInner {
    path: PathBuf,
    write_lock: Arc<Mutex<()>>,
}

impl StateStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        let path = state_store_lock_key(&path.into());
        let write_lock = write_lock_for_path(&path);

        Self {
            inner: Arc::new(StateStoreInner { path, write_lock }),
        }
    }

    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    pub fn load(&self) -> Result<RuntimeState, String> {
        let input = match fs::read_to_string(self.path()) {
            Ok(input) => input,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                return Ok(RuntimeState::new());
            }
            Err(err) => {
                return Err(format!(
                    "failed to read runtime state {}: {err}",
                    self.path().display()
                ));
            }
        };

        parse_state_file(&input)
            .map_err(|err| format!("failed to parse {}: {err}", self.path().display()))
    }

    pub fn save(&self, state: &RuntimeState) -> Result<(), String> {
        let _guard = self.lock_write()?;
        self.save_unlocked(state)
    }

    pub(super) fn save_unlocked(&self, state: &RuntimeState) -> Result<(), String> {
        let mut state = state.clone();
        let existing = self.load_existing_for_merge()?;
        validate_snapshot_outbound_additions(&state, existing.as_ref())?;
        validate_snapshot_run_input_additions(&state, existing.as_ref())?;
        validate_snapshot_agent_run_transitions(&state, existing.as_ref())?;
        validate_snapshot_run_output_additions(&state, existing.as_ref())?;

        if let Some(existing) = existing {
            state.validate_shared_inbound_event_identity(&existing)?;
            state.preserve_runs_from(&existing)?;
            state.preserve_run_inputs_from(&existing)?;
            state.preserve_inbound_events_from(&existing)?;
            state.preserve_queued_messages_from(&existing)?;
            state.preserve_outbound_deliveries_from(&existing)?;
        }

        self.write_unlocked(&state)
    }

    pub(super) fn write_unlocked(&self, state: &RuntimeState) -> Result<(), String> {
        state.validate()?;
        let state_file = state_file_from_state(state);
        write_json_atomic(self.path(), &state_file).map_err(|err| {
            format!(
                "failed to save runtime state {}: {err}",
                self.path().display()
            )
        })
    }

    pub(super) fn lock_write(&self) -> Result<MutexGuard<'_, ()>, String> {
        self.inner
            .write_lock
            .lock()
            .map_err(|_| "runtime state write lock poisoned".to_owned())
    }

    #[cfg(test)]
    pub(super) fn shares_write_lock_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner.write_lock, &other.inner.write_lock)
    }

    fn load_existing_for_merge(&self) -> Result<Option<RuntimeState>, String> {
        let input = match fs::read_to_string(self.path()) {
            Ok(input) => input,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(err) => {
                return Err(format!(
                    "failed to read runtime state {}: {err}",
                    self.path().display()
                ));
            }
        };

        parse_state_file(&input)
            .map(Some)
            .map_err(|err| format!("failed to parse {}: {err}", self.path().display()))
    }
}

fn validate_snapshot_outbound_additions(
    candidate: &RuntimeState,
    existing: Option<&RuntimeState>,
) -> Result<(), String> {
    for delivery in candidate.outbound_deliveries() {
        let already_exists = existing
            .and_then(|state| state.outbound_delivery(delivery.id()))
            .is_some();

        if !already_exists && delivery.status() != OutboundDeliveryStatus::Pending {
            return Err(format!(
                "runtime state save cannot introduce outbound delivery {} with status {:?}; new snapshot deliveries must be pending",
                delivery.id(),
                delivery.status()
            ));
        }
    }

    Ok(())
}

fn validate_snapshot_run_input_additions(
    candidate: &RuntimeState,
    existing: Option<&RuntimeState>,
) -> Result<(), String> {
    for input in candidate.run_inputs() {
        let already_exists = existing
            .and_then(|state| state.run_input(input.run_id()))
            .is_some();
        if !already_exists {
            return Err(format!(
                "runtime state save cannot introduce run input {}; message batches must be claimed through StateStore::claim_message_batch",
                input.run_id()
            ));
        }
    }

    Ok(())
}

fn validate_snapshot_run_output_additions(
    candidate: &RuntimeState,
    existing: Option<&RuntimeState>,
) -> Result<(), String> {
    for run in candidate.runs() {
        if run.output_delivery_ids().is_empty() {
            continue;
        }

        let existing_output_ids = existing
            .and_then(|state| state.run(run.id()))
            .map(|existing_run| existing_run.output_delivery_ids());
        if existing_output_ids != Some(run.output_delivery_ids()) {
            return Err(format!(
                "runtime state save cannot introduce or change output delivery ownership for run {}; agent output must be committed through the StateStore agent-run completion transition",
                run.id()
            ));
        }
    }

    Ok(())
}

fn validate_snapshot_agent_run_transitions(
    candidate: &RuntimeState,
    existing: Option<&RuntimeState>,
) -> Result<(), String> {
    let Some(existing) = existing else {
        return Ok(());
    };

    for candidate_run in candidate.runs() {
        if existing.run_input(candidate_run.id()).is_none() {
            continue;
        }

        let existing_run = existing
            .run(candidate_run.id())
            .expect("validated run input must reference an existing run");
        if candidate_run == existing_run || existing_run.is_descendant_of(candidate_run) {
            continue;
        }

        if matches!(
            candidate_run.status(),
            RunStatus::Running | RunStatus::Completed
        ) {
            return Err(format!(
                "runtime state save cannot create a handoff or completion for durable agent run {}; use the StateStore agent-run transition",
                candidate_run.id()
            ));
        }
    }

    Ok(())
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

    use super::StateStore;
    use crate::runtime::{
        event::{Event, EventId, EventKind, EventSource, InboundEventRecord},
        message::{Message, MessageAuthor, MessageContent, MessageId},
        outbox::{OutboundDeliveryId, OutboundDeliveryRecord, OutboundDeliveryStatus},
        run::{RunId, RunRecord, RunStatus},
        session::{Session, SessionScope},
        state::RuntimeState,
    };

    static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn state_store_round_trips_sessions() {
        let path = test_path("state-round-trip").join("runtime.state.json");
        let store = StateStore::new(&path);
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session = Session::new(scope);
        let session_id = session.id().clone();
        let mut state = RuntimeState::new();
        state.upsert_session(session);

        store.save(&state).expect("state should save");
        let loaded = store.load().expect("state should load");

        assert!(loaded.session(&session_id).is_some());
        assert_eq!(loaded, state);
    }
    #[test]
    fn state_store_round_trips_run_records() {
        let path = test_path("state-run-round-trip").join("runtime.state.json");
        let store = StateStore::new(&path);
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session = Session::new(scope);
        let session_id = session.id().clone();
        let run_id = RunId::new("run_1").expect("valid run id");
        let mut run = RunRecord::new(run_id.clone(), session_id, 10);
        run.start(11).expect("run should start");
        run.complete(12).expect("run should complete");
        let mut state = RuntimeState::new();
        state.upsert_session(session);
        state.add_run(run).expect("run should be accepted");

        store.save(&state).expect("state should save");
        let loaded = store.load().expect("state should load");

        assert_eq!(
            loaded
                .run(&run_id)
                .expect("persisted run should exist")
                .status(),
            RunStatus::Completed
        );
        assert_eq!(loaded, state);
    }
    #[test]
    fn state_store_snapshot_cannot_introduce_agent_run_output_ownership() {
        let path = test_path("state-snapshot-run-output").join("runtime.state.json");
        let store = StateStore::new(&path);
        let session = Session::new(SessionScope::new("lark", "chat:oc_123").expect("valid scope"));
        let run_id = RunId::new("run_1").expect("valid run id");
        let delivery_id = OutboundDeliveryId::new("out_1").expect("valid delivery id");
        let message = Message::new(
            MessageId::new("msg_1").expect("valid message id"),
            Some(session.id().clone()),
            MessageAuthor::Agent,
            MessageContent::text("done").expect("valid content"),
            12,
        );
        let delivery =
            OutboundDeliveryRecord::new(delivery_id.clone(), session.id().clone(), message, 12)
                .expect("valid delivery");
        let mut run = RunRecord::new(run_id, session.id().clone(), 10);
        run.start(11).expect("run should start");
        run.complete_with_output_deliveries(12, vec![delivery_id])
            .expect("candidate run should complete");
        let mut candidate = RuntimeState::new();
        candidate.upsert_session(session);
        candidate
            .add_run(run)
            .expect("candidate run should be accepted");
        candidate
            .enqueue_outbound_delivery(delivery)
            .expect("candidate output should be internally valid");

        let err = store
            .save(&candidate)
            .expect_err("snapshot save must not introduce agent output ownership");

        assert!(err.contains("cannot introduce or change output delivery ownership"));
        assert!(
            store
                .load()
                .expect("state should remain readable")
                .runs()
                .is_empty()
        );
    }
    #[test]
    fn state_store_preserves_newer_run_transitions_from_stale_saves() {
        let path = test_path("state-stale-run-transition").join("runtime.state.json");
        let store = StateStore::new(&path);
        let session = Session::new(SessionScope::new("lark", "chat:oc_123").expect("valid scope"));
        let session_id = session.id().clone();
        let run_id = RunId::new("run_1").expect("valid run id");
        let mut initial = RuntimeState::new();
        initial.upsert_session(session);
        initial
            .add_run(RunRecord::new(run_id.clone(), session_id, 10))
            .expect("pending run should be accepted");
        store.save(&initial).expect("initial state should save");
        let stale = store.load().expect("stale state should load");
        let mut current = store.load().expect("current state should load");
        current.start_run(&run_id, 11).expect("run should start");
        store
            .save(&current)
            .expect("forward run transition should save");

        store
            .save(&stale)
            .expect("stale pending snapshot should preserve durable running state");

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
    fn state_store_stale_save_rejects_divergent_run_outcomes() {
        let path = test_path("state-divergent-run-outcomes").join("runtime.state.json");
        let store = StateStore::new(&path);
        let session = Session::new(SessionScope::new("lark", "chat:oc_123").expect("valid scope"));
        let session_id = session.id().clone();
        let run_id = RunId::new("run_1").expect("valid run id");
        let mut initial = RuntimeState::new();
        initial.upsert_session(session);
        initial
            .add_run(RunRecord::new(run_id.clone(), session_id, 10))
            .expect("pending run should be accepted");
        store.save(&initial).expect("initial state should save");
        let mut failed = store.load().expect("failed writer should load");
        let mut cancelled = store.load().expect("cancelled writer should load");
        failed.fail_run(&run_id, 11).expect("run should fail");
        cancelled
            .cancel_run(&run_id, 11)
            .expect("run should cancel");
        store.save(&failed).expect("failed outcome should save");

        let err = store
            .save(&cancelled)
            .expect_err("divergent terminal outcome must fail closed");

        assert!(err.contains("conflicting run record"));
        assert_eq!(
            store
                .load()
                .expect("state should remain readable")
                .run(&run_id)
                .expect("run should remain durable")
                .status(),
            RunStatus::Failed
        );
    }
    #[test]
    fn concurrent_stale_saves_create_only_one_active_run_per_session() {
        let path = test_path("state-concurrent-active-runs").join("runtime.state.json");
        let store = StateStore::new(&path);
        let session = Session::new(SessionScope::new("lark", "chat:oc_123").expect("valid scope"));
        let session_id = session.id().clone();
        let mut initial = RuntimeState::new();
        initial.upsert_session(session);
        store.save(&initial).expect("initial state should save");
        let mut first = store.load().expect("first writer should load");
        let mut second = store.load().expect("second writer should load");
        first
            .add_run(RunRecord::new(
                RunId::new("run_1").expect("valid run id"),
                session_id.clone(),
                10,
            ))
            .expect("first candidate run should be valid");
        second
            .add_run(RunRecord::new(
                RunId::new("run_2").expect("valid run id"),
                session_id,
                10,
            ))
            .expect("second candidate run should be valid");
        let barrier = Arc::new(Barrier::new(3));
        let handles = [first, second].map(|state| {
            let store = store.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                store.save(&state)
            })
        });
        barrier.wait();

        let results = handles.map(|handle| handle.join().expect("writer should not panic"));
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
        let loaded = store.load().expect("state should load");
        assert_eq!(
            loaded
                .runs()
                .iter()
                .filter(|run| !run.is_terminal())
                .count(),
            1
        );
    }
    #[test]
    fn state_store_save_fails_closed_when_existing_state_is_invalid() {
        let path = test_path("state-save-invalid-existing").join("runtime.state.json");
        let store = StateStore::new(&path);
        let event = event_fixture("evt_1", 10);
        let record = state_event_record(&event, 12).expect("inbound event record should build");
        fs::write(
            store.path(),
            format!(
                r#"{{
                "version": {version},
                "sessions": [],
                "runs": [],
                "run_inputs": [],
                "inbound_events": [{record}],
                "queued_messages": [],
                "outbound_deliveries": [],
                "updated_at_unix": 1
            }}"#,
                version = crate::runtime::state::RUNTIME_STATE_FILE_VERSION,
                record = serde_json::to_string(&record).expect("event record should encode")
            ),
        )
        .expect("invalid state fixture should write");

        let err = store
            .save(&RuntimeState::new())
            .expect_err("save should not overwrite invalid existing state");

        assert!(err.contains("before inbound event"));
        let existing = fs::read_to_string(store.path()).expect("state file should remain readable");
        assert!(existing.contains("evt_1"));
    }
    #[test]
    fn state_store_replaces_existing_state_file() {
        let path = test_path("state-replace-existing").join("runtime.state.json");
        let store = StateStore::new(&path);
        let first_scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let second_scope = SessionScope::new("lark", "chat:oc_456").expect("valid scope");
        let mut state = RuntimeState::new();
        state.upsert_session(Session::new(first_scope));
        store.save(&state).expect("initial state should save");

        state.upsert_session(Session::new(second_scope));
        store
            .save(&state)
            .expect("existing state file should be replaced");

        let loaded = store.load().expect("replaced state should load");
        assert_eq!(loaded.sessions().len(), 2);
    }
    #[test]
    fn state_store_save_accepts_new_pending_outbound_delivery() {
        let path = test_path("state-save-new-pending-outbound").join("runtime.state.json");
        let store = StateStore::new(&path);
        let state = state_with_outbound_status(OutboundDeliveryStatus::Pending);

        store.save(&state).expect("pending delivery should save");

        let loaded = store.load().expect("saved state should load");
        assert_eq!(loaded.outbound_deliveries().len(), 1);
        assert_eq!(
            loaded.outbound_deliveries()[0].status(),
            OutboundDeliveryStatus::Pending
        );
    }
    #[test]
    fn state_store_save_rejects_new_non_pending_outbound_deliveries() {
        for status in [
            OutboundDeliveryStatus::Delivering,
            OutboundDeliveryStatus::Delivered,
            OutboundDeliveryStatus::Failed,
            OutboundDeliveryStatus::Uncertain,
        ] {
            let path = test_path(&format!("state-save-new-{status:?}-outbound"))
                .join("runtime.state.json");
            let store = StateStore::new(&path);
            let state = state_with_outbound_status(status);

            let err = store
                .save(&state)
                .expect_err("non-pending delivery must not enter through snapshot save");

            assert!(err.contains("new snapshot deliveries must be pending"));
            assert!(err.contains(&format!("{status:?}")));
            assert!(!path.exists(), "rejected snapshot must not create state");
        }
    }
    #[test]
    fn missing_state_loads_as_empty_state() {
        let path = test_path("missing-state").join("runtime.state.json");
        let store = StateStore::new(path);
        let state = store.load().expect("missing state should be defaulted");

        assert!(state.sessions().is_empty());
        assert!(state.runs().is_empty());
        assert!(state.run_inputs().is_empty());
        assert!(state.inbound_events().is_empty());
        assert!(state.outbound_deliveries().is_empty());
    }
    #[test]
    #[cfg(unix)]
    fn state_load_does_not_default_non_not_found_path_errors() {
        let dir = test_path("state-load-path-error");
        let parent_file = dir.join("not-a-directory");
        fs::write(&parent_file, "not a directory").expect("parent fixture should write");
        let path = parent_file.join("runtime.state.json");
        let store = StateStore::new(&path);

        assert!(
            !path.exists(),
            "Path::exists should collapse this path error to false"
        );

        let err = store
            .load()
            .expect_err("path errors must not be treated as missing state");

        assert!(err.contains("failed to read runtime state"));
    }
    #[test]
    #[cfg(unix)]
    fn state_save_creates_private_parent_directory() {
        use std::os::unix::fs::PermissionsExt;

        let parent = test_path("state-private-parent").join("runtime");

        StateStore::new(parent.join("runtime.state.json"))
            .save(&RuntimeState::new())
            .expect("state should save");

        let mode = fs::metadata(parent)
            .expect("parent metadata should load")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o700);
    }
    #[test]
    #[cfg(unix)]
    fn state_save_does_not_chmod_existing_parent_directory() {
        use std::os::unix::fs::PermissionsExt;

        let dir = test_path("state-existing-parent");
        let parent = dir.join("runtime");
        fs::create_dir_all(&parent).expect("parent should be created");
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o755))
            .expect("parent fixture permissions should be set");

        StateStore::new(parent.join("runtime.state.json"))
            .save(&RuntimeState::new())
            .expect("state should save");

        let mode = fs::metadata(parent)
            .expect("parent metadata should load")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o755);
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
        Event::new(
            EventId::new(id).expect("valid event id"),
            EventSource::Runtime,
            EventKind::RuntimeNotice {
                message: "notice".to_owned(),
            },
            received_at_unix,
        )
    }
    fn state_event_record(
        event: &Event,
        recorded_at_unix: u64,
    ) -> Result<InboundEventRecord, String> {
        InboundEventRecord::from_event(event, recorded_at_unix)
    }
    fn state_with_outbound_status(status: OutboundDeliveryStatus) -> RuntimeState {
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session = Session::new(scope);
        let session_id = session.id().clone();
        let message = Message::new(
            MessageId::new("msg_out_1").expect("valid message id"),
            Some(session_id.clone()),
            MessageAuthor::Agent,
            MessageContent::text("reply").expect("valid text"),
            10,
        );
        let delivery = OutboundDeliveryRecord::new(
            OutboundDeliveryId::new("out_1").expect("valid delivery id"),
            session_id,
            message,
            10,
        )
        .expect("valid outbound delivery");
        let delivery_id = delivery.id().clone();
        let mut state = RuntimeState::new();
        state.upsert_session(session);
        state
            .enqueue_outbound_delivery(delivery)
            .expect("delivery should enqueue");

        if status != OutboundDeliveryStatus::Pending {
            state
                .claim_next_outbound_delivery(11)
                .expect("delivery should claim");
        }

        match status {
            OutboundDeliveryStatus::Pending | OutboundDeliveryStatus::Delivering => {}
            OutboundDeliveryStatus::Delivered => {
                state
                    .mark_outbound_delivery_delivered(&delivery_id, 12)
                    .expect("delivery should complete");
            }
            OutboundDeliveryStatus::Failed => {
                state
                    .mark_outbound_delivery_failed(&delivery_id, 12, "transport failed")
                    .expect("delivery should fail");
            }
            OutboundDeliveryStatus::Uncertain => {
                state
                    .mark_outbound_delivery_uncertain(
                        &delivery_id,
                        12,
                        "provider acceptance is unknown",
                    )
                    .expect("delivery should become uncertain");
            }
        }

        state
    }
}
