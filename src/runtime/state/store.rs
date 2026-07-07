use std::{
    collections::BTreeMap,
    fs, io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard, OnceLock, Weak},
};

use crate::runtime::{
    event::{Event, InboundEventRecordStatus},
    outbox::{OutboundDeliveryEnqueueStatus, OutboundDeliveryRecord},
    persistence::write_json_atomic,
};

use super::{
    format::{parse_state_file, state_file_from_state},
    model::RuntimeState,
};

static STATE_STORE_WRITE_LOCKS: OnceLock<Mutex<BTreeMap<PathBuf, Weak<Mutex<()>>>>> =
    OnceLock::new();

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

    pub fn persist_inbound_event(&self, event: &Event) -> Result<InboundEventRecordStatus, String> {
        let _guard = self.lock_write()?;
        let mut state = self.load()?;
        let record_status = state.record_inbound_event(event)?;

        if record_status == InboundEventRecordStatus::Recorded {
            self.save_unlocked(&state)?;
        }

        Ok(record_status)
    }

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

    fn save_unlocked(&self, state: &RuntimeState) -> Result<(), String> {
        let mut state = state.clone();
        if let Some(existing) = self.load_existing_for_merge()? {
            state.preserve_inbound_events_from(&existing)?;
            state.preserve_outbound_deliveries_from(&existing)?;
        }

        state.validate()?;
        let state_file = state_file_from_state(&state);
        write_json_atomic(self.path(), &state_file).map_err(|err| {
            format!(
                "failed to save runtime state {}: {err}",
                self.path().display()
            )
        })
    }

    fn lock_write(&self) -> Result<MutexGuard<'_, ()>, String> {
        self.inner
            .write_lock
            .lock()
            .map_err(|_| "runtime state write lock poisoned".to_owned())
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

fn write_lock_for_path(path: &Path) -> Arc<Mutex<()>> {
    let key = state_store_lock_key(path);
    let registry = STATE_STORE_WRITE_LOCKS.get_or_init(|| Mutex::new(BTreeMap::new()));
    let mut locks = match registry.lock() {
        Ok(locks) => locks,
        Err(poisoned) => poisoned.into_inner(),
    };

    if let Some(lock) = locks.get(&key).and_then(Weak::upgrade) {
        return lock;
    }

    let lock = Arc::new(Mutex::new(()));
    locks.insert(key, Arc::downgrade(&lock));
    lock
}

fn state_store_lock_key(path: &Path) -> PathBuf {
    if let Ok(canonical_path) = fs::canonicalize(path) {
        return canonical_path;
    }

    if let Some(parent) = non_empty_parent(path) {
        if let (Ok(canonical_parent), Some(file_name)) =
            (fs::canonicalize(parent), path.file_name())
        {
            return canonical_parent.join(file_name);
        }
    }

    std::path::absolute(path).unwrap_or_else(|_| {
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .join(path)
        }
    })
}

fn non_empty_parent(path: &Path) -> Option<&Path> {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
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
        event::{
            Event, EventId, EventKind, EventSource, InboundEventRecord, InboundEventRecordStatus,
        },
        message::{Message, MessageAuthor, MessageContent, MessageId},
        outbox::{OutboundDeliveryEnqueueStatus, OutboundDeliveryId, OutboundDeliveryRecord},
        run::{RunId, RunRecord, RunStatus},
        session::{Session, SessionId, SessionScope},
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
    fn state_store_uses_same_process_write_lock_for_same_path() {
        let path = test_path("state-same-path-lock").join("runtime.state.json");
        let first = StateStore::new(&path);
        let second = StateStore::new(&path);

        assert!(Arc::ptr_eq(
            &first.inner.write_lock,
            &second.inner.write_lock
        ));
    }
    #[test]
    #[cfg(unix)]
    fn state_store_normalizes_symlinked_parent_paths_for_io_and_locking() {
        let root = test_path("state-symlink-path-lock");
        let real_parent = root.join("real");
        let linked_parent = root.join("linked");
        fs::create_dir(&real_parent).expect("real parent should exist");
        std::os::unix::fs::symlink(&real_parent, &linked_parent)
            .expect("parent symlink should be created");

        let real_store = StateStore::new(real_parent.join("runtime.state.json"));
        let linked_store = StateStore::new(linked_parent.join("runtime.state.json"));

        assert_eq!(real_store.path(), linked_store.path());
        assert!(Arc::ptr_eq(
            &real_store.inner.write_lock,
            &linked_store.inner.write_lock
        ));
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
                "inbound_events": [{record}],
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
    #[cfg(unix)]
    fn state_store_does_not_return_status_when_inbound_event_persist_fails() {
        use std::os::unix::fs::PermissionsExt;

        let dir = test_path("state-inbound-event-persist-failure");
        let path = dir.join("runtime.state.json");
        let store = StateStore::new(&path);
        let event = event_fixture("evt_1", 10);

        store
            .save(&RuntimeState::new())
            .expect("initial readable state should save");
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o500))
            .expect("fixture permissions should be set");

        let result = store.persist_inbound_event(&event);

        fs::set_permissions(&dir, fs::Permissions::from_mode(0o700))
            .expect("fixture permissions should be restored");
        let err = result.expect_err("failed persistence must not return an acknowledgeable status");
        assert!(err.contains("failed to save runtime state"));

        let loaded = store.load().expect("state should still load");
        assert!(
            loaded.inbound_events().is_empty(),
            "failed persistence must not leave an acknowledged inbound event on disk"
        );
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
    fn missing_state_loads_as_empty_state() {
        let path = test_path("missing-state").join("runtime.state.json");
        let store = StateStore::new(path);
        let state = store.load().expect("missing state should be defaulted");

        assert!(state.sessions().is_empty());
        assert!(state.runs().is_empty());
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
        let message = Message::user_text("msg_1", None, "hello", 1).expect("valid message");
        Event::new(
            EventId::new(id).expect("valid event id"),
            EventSource::Platform,
            EventKind::MessageReceived { message },
            received_at_unix,
        )
    }

    fn state_event_record(
        event: &Event,
        recorded_at_unix: u64,
    ) -> Result<InboundEventRecord, String> {
        InboundEventRecord::from_event(event, recorded_at_unix)
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
