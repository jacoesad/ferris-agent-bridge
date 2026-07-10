use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
};

use crate::runtime::persistence::write_json_atomic;

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
        if let Some(existing) = self.load_existing_for_merge()? {
            state.preserve_inbound_events_from(&existing)?;
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

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::atomic::{AtomicU64, Ordering},
    };

    use super::StateStore;
    use crate::runtime::{
        event::{Event, EventId, EventKind, EventSource, InboundEventRecord},
        message::Message,
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
}
