use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    process,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Deserializer, Serialize, de, de::DeserializeOwned};

use super::{
    run::{RunId, RunRecord},
    session::{Session, SessionId},
};

pub const RUNTIME_STATE_VERSION: u32 = 2;
const RUNTIME_STATE_V1_VERSION: u32 = 1;

static NEXT_TEMP_FILE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RuntimeState {
    version: u32,
    sessions: Vec<Session>,
    runs: Vec<RunRecord>,
    updated_at_unix: u64,
}

impl RuntimeState {
    pub fn new() -> Self {
        Self {
            version: RUNTIME_STATE_VERSION,
            sessions: Vec::new(),
            runs: Vec::new(),
            updated_at_unix: unix_seconds_now(),
        }
    }

    pub fn upsert_session(&mut self, session: Session) {
        if let Some(existing) = self
            .sessions
            .iter_mut()
            .find(|existing| existing.id() == session.id())
        {
            existing.refresh_from(session);
        } else {
            self.sessions.push(session);
        }

        self.touch_at(unix_seconds_now());
    }

    pub fn session(&self, id: &SessionId) -> Option<&Session> {
        self.sessions.iter().find(|session| session.id() == id)
    }

    pub fn add_run(&mut self, run: RunRecord) -> Result<(), String> {
        self.validate_run_session(&run)?;

        if self.runs.iter().any(|existing| existing.id() == run.id()) {
            return Err(format!("duplicate run id {}", run.id()));
        }

        self.runs.push(run);
        self.touch_at(unix_seconds_now());
        Ok(())
    }

    pub fn run(&self, id: &RunId) -> Option<&RunRecord> {
        self.runs.iter().find(|run| run.id() == id)
    }

    pub fn start_run(&mut self, id: &RunId, started_at_unix: u64) -> Result<(), String> {
        {
            let run = self.run_mut(id)?;
            run.start(started_at_unix)?;
        }

        self.touch_at(started_at_unix.max(unix_seconds_now()));
        Ok(())
    }

    pub fn complete_run(&mut self, id: &RunId, finished_at_unix: u64) -> Result<(), String> {
        {
            let run = self.run_mut(id)?;
            run.complete(finished_at_unix)?;
        }

        self.touch_at(finished_at_unix.max(unix_seconds_now()));
        Ok(())
    }

    pub fn fail_run(&mut self, id: &RunId, finished_at_unix: u64) -> Result<(), String> {
        {
            let run = self.run_mut(id)?;
            run.fail(finished_at_unix)?;
        }

        self.touch_at(finished_at_unix.max(unix_seconds_now()));
        Ok(())
    }

    pub fn cancel_run(&mut self, id: &RunId, finished_at_unix: u64) -> Result<(), String> {
        {
            let run = self.run_mut(id)?;
            run.cancel(finished_at_unix)?;
        }

        self.touch_at(finished_at_unix.max(unix_seconds_now()));
        Ok(())
    }

    pub fn version(&self) -> u32 {
        self.version
    }

    pub fn sessions(&self) -> &[Session] {
        &self.sessions
    }

    pub fn runs(&self) -> &[RunRecord] {
        &self.runs
    }

    pub fn updated_at_unix(&self) -> u64 {
        self.updated_at_unix
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.version != RUNTIME_STATE_VERSION {
            return Err(format!(
                "unsupported runtime state version {}; expected {}",
                self.version, RUNTIME_STATE_VERSION
            ));
        }

        let mut session_ids = BTreeSet::new();
        let mut run_ids = BTreeSet::new();

        for session in &self.sessions {
            session.validate()?;

            if !session_ids.insert(session.id()) {
                return Err(format!("duplicate session id {}", session.id()));
            }
        }

        for run in &self.runs {
            run.validate()?;
            self.validate_run_session(run)?;

            if !run_ids.insert(run.id()) {
                return Err(format!("duplicate run id {}", run.id()));
            }
        }

        Ok(())
    }

    fn validate_run_session(&self, run: &RunRecord) -> Result<(), String> {
        if self.session(run.session_id()).is_none() {
            return Err(format!(
                "run {} references unknown session {}",
                run.id(),
                run.session_id()
            ));
        }

        Ok(())
    }

    fn run_mut(&mut self, id: &RunId) -> Result<&mut RunRecord, String> {
        self.runs
            .iter_mut()
            .find(|run| run.id() == id)
            .ok_or_else(|| format!("unknown run id {id}"))
    }

    fn touch_at(&mut self, updated_at_unix: u64) {
        self.updated_at_unix = self.updated_at_unix.max(updated_at_unix);
    }
}

impl Default for RuntimeState {
    fn default() -> Self {
        Self::new()
    }
}

impl<'de> Deserialize<'de> for RuntimeState {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RuntimeStateWire {
            version: u32,
            sessions: Vec<Session>,
            runs: Option<Vec<RunRecord>>,
            updated_at_unix: u64,
        }

        let wire = RuntimeStateWire::deserialize(deserializer)?;
        let runs = match wire.version {
            RUNTIME_STATE_V1_VERSION => {
                if wire.runs.is_some() {
                    return Err(de::Error::custom(
                        "runtime state version 1 must not contain run records",
                    ));
                }

                Vec::new()
            }
            RUNTIME_STATE_VERSION => wire.runs.ok_or_else(|| {
                de::Error::custom(format!(
                    "runtime state version {RUNTIME_STATE_VERSION} must contain run records"
                ))
            })?,
            version => {
                return Err(de::Error::custom(format!(
                    "unsupported runtime state version {}; expected {}",
                    version, RUNTIME_STATE_VERSION
                )));
            }
        };
        let state = Self {
            version: RUNTIME_STATE_VERSION,
            sessions: wire.sessions,
            runs,
            updated_at_unix: wire.updated_at_unix,
        };
        state.validate().map_err(de::Error::custom)?;
        Ok(state)
    }
}

#[derive(Debug, Clone)]
pub struct StateStore {
    path: PathBuf,
}

impl StateStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn load(&self) -> Result<RuntimeState, String> {
        let input = match fs::read_to_string(&self.path) {
            Ok(input) => input,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                return Ok(RuntimeState::new());
            }
            Err(err) => {
                return Err(format!(
                    "failed to read runtime state {}: {err}",
                    self.path.display()
                ));
            }
        };

        let state: RuntimeState = serde_json::from_str(&input)
            .map_err(|err| format!("failed to parse {}: {err}", self.path.display()))?;
        state.validate()?;
        Ok(state)
    }

    pub fn save(&self, state: &RuntimeState) -> Result<(), String> {
        state.validate()?;
        write_json_atomic(&self.path, state).map_err(|err| {
            format!(
                "failed to save runtime state {}: {err}",
                self.path.display()
            )
        })
    }
}

pub(crate) fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T, String> {
    let input = fs::read_to_string(path)
        .map_err(|err| format!("failed to read {}: {err}", path.display()))?;

    serde_json::from_str(&input).map_err(|err| format!("failed to parse {}: {err}", path.display()))
}

pub(crate) fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> io::Result<()> {
    if let Some(parent) = non_empty_parent(path) {
        ensure_private_parent_dir(parent)?;
    }

    let temp_path = temp_path_for(path);
    let mut encoded = serde_json::to_vec_pretty(value)?;
    encoded.push(b'\n');

    let write_result = (|| {
        let mut file = open_private_new_file(&temp_path)?;
        set_private_file_permissions(&file)?;
        file.write_all(&encoded)?;
        file.sync_all()?;
        drop(file);
        replace_file(&temp_path, path)?;
        sync_parent(path)?;
        Ok(())
    })();

    if write_result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }

    write_result
}

#[cfg(not(windows))]
fn replace_file(src: &Path, dst: &Path) -> io::Result<()> {
    fs::rename(src, dst)
}

#[cfg(windows)]
fn replace_file(src: &Path, dst: &Path) -> io::Result<()> {
    use std::{ffi::OsStr, os::windows::ffi::OsStrExt};

    const MOVEFILE_REPLACE_EXISTING: u32 = 0x1;
    const MOVEFILE_WRITE_THROUGH: u32 = 0x8;

    unsafe extern "system" {
        fn MoveFileExW(src: *const u16, dst: *const u16, flags: u32) -> i32;
    }

    fn wide_null(value: &OsStr) -> Vec<u16> {
        value.encode_wide().chain(std::iter::once(0)).collect()
    }

    let src = wide_null(src.as_os_str());
    let dst = wide_null(dst.as_os_str());
    let flags = MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH;

    // SAFETY: both paths are null-terminated UTF-16 buffers that live for the
    // duration of the call, and MoveFileExW does not retain the pointers.
    match unsafe { MoveFileExW(src.as_ptr(), dst.as_ptr(), flags) } {
        0 => Err(io::Error::last_os_error()),
        _ => Ok(()),
    }
}

fn temp_path_for(path: &Path) -> PathBuf {
    let sequence = NEXT_TEMP_FILE.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("runtime-state");

    path.with_file_name(format!(
        ".{file_name}.tmp.{}.{}.{}",
        process::id(),
        nanos,
        sequence
    ))
}

#[cfg(unix)]
fn sync_parent(path: &Path) -> io::Result<()> {
    if let Some(parent) = non_empty_parent(path) {
        File::open(parent)?.sync_all()?;
    }

    Ok(())
}

#[cfg(not(unix))]
fn sync_parent(_path: &Path) -> io::Result<()> {
    Ok(())
}

fn non_empty_parent(path: &Path) -> Option<&Path> {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
}

fn ensure_private_parent_dir(path: &Path) -> io::Result<()> {
    match fs::metadata(path) {
        Ok(metadata) if metadata.is_dir() => Ok(()),
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("{} exists and is not a directory", path.display()),
        )),
        Err(err) if err.kind() == io::ErrorKind::NotFound => create_private_dir(path),
        Err(err) => Err(err),
    }
}

#[cfg(unix)]
fn create_private_dir(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;

    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    builder.mode(0o700);
    builder.create(path)
}

#[cfg(not(unix))]
fn create_private_dir(path: &Path) -> io::Result<()> {
    fs::create_dir_all(path)
}

fn open_private_new_file(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    configure_private_file_options(&mut options);
    options.open(path)
}

#[cfg(unix)]
fn configure_private_file_options(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;

    options.mode(0o600);
}

#[cfg(not(unix))]
fn configure_private_file_options(_options: &mut OpenOptions) {}

#[cfg(unix)]
fn set_private_file_permissions(file: &File) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = file.metadata()?.permissions();
    permissions.set_mode(0o600);
    file.set_permissions(permissions)
}

#[cfg(not(unix))]
fn set_private_file_permissions(_file: &File) -> io::Result<()> {
    Ok(())
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
        fs,
        sync::atomic::{AtomicU64, Ordering},
    };

    use super::{RuntimeState, StateStore, non_empty_parent, open_private_new_file};
    use crate::runtime::{
        run::{RunId, RunRecord, RunStatus},
        session::{Session, SessionScope},
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
    fn state_transitions_persisted_run_records() {
        let (mut state, run_id) = state_with_pending_run("run_1");

        state.start_run(&run_id, 11).expect("run should start");
        state
            .complete_run(&run_id, 12)
            .expect("run should complete");

        let run = state.run(&run_id).expect("run should exist");
        assert_eq!(run.status(), RunStatus::Completed);
        assert_eq!(run.started_at_unix(), Some(11));
        assert_eq!(run.finished_at_unix(), Some(12));
    }

    #[test]
    fn state_transitions_can_fail_or_cancel_persisted_run_records() {
        let (mut failed, failed_id) = state_with_pending_run("run_failed");
        failed
            .fail_run(&failed_id, 11)
            .expect("pending run can fail");
        assert_eq!(
            failed.run(&failed_id).expect("run should exist").status(),
            RunStatus::Failed
        );

        let (mut cancelled, cancelled_id) = state_with_pending_run("run_cancelled");
        cancelled
            .start_run(&cancelled_id, 11)
            .expect("run should start");
        cancelled
            .cancel_run(&cancelled_id, 12)
            .expect("running run can cancel");
        assert_eq!(
            cancelled
                .run(&cancelled_id)
                .expect("run should exist")
                .status(),
            RunStatus::Cancelled
        );
    }

    #[test]
    fn state_transitions_reject_invalid_or_unknown_runs() {
        let (mut state, run_id) = state_with_pending_run("run_1");
        let unknown_id = RunId::new("run_missing").expect("valid run id");

        let err = state
            .complete_run(&run_id, 11)
            .expect_err("pending run should not complete");
        assert!(err.contains("cannot complete from Pending"));

        let err = state
            .start_run(&unknown_id, 11)
            .expect_err("unknown run should not start");
        assert!(err.contains("unknown run id"));

        state.start_run(&run_id, 11).expect("run should start");
        state
            .complete_run(&run_id, 12)
            .expect("run should complete");

        let err = state
            .start_run(&run_id, 13)
            .expect_err("terminal run should not restart");
        assert!(err.contains("cannot start from Completed"));
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
    fn upsert_session_preserves_created_at_for_existing_session() {
        let path = test_path("state-upsert-preserves-created").join("runtime.state.json");
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session_id = crate::runtime::session::SessionId::for_scope(&scope);
        let first = session_fixture(&scope, 10, 20);
        let second = session_fixture(&scope, 30, 40);
        let mut state = RuntimeState::new();

        state.upsert_session(first);
        state.upsert_session(second);

        let session = state.session(&session_id).expect("session should exist");
        assert_eq!(session.created_at_unix(), 10);
        assert_eq!(session.updated_at_unix(), 40);

        state.upsert_session(session_fixture(&scope, 5, 15));
        let session = state.session(&session_id).expect("session should exist");
        assert_eq!(session.created_at_unix(), 10);
        assert_eq!(session.updated_at_unix(), 40);

        StateStore::new(path)
            .save(&state)
            .expect("state should remain valid");
    }

    #[test]
    fn missing_state_loads_as_empty_state() {
        let path = test_path("missing-state").join("runtime.state.json");
        let store = StateStore::new(path);
        let state = store.load().expect("missing state should be defaulted");

        assert!(state.sessions().is_empty());
        assert!(state.runs().is_empty());
    }

    #[test]
    fn state_load_migrates_version_1_without_runs_field() {
        let path = test_path("state-v1-without-runs-field").join("runtime.state.json");
        fs::write(
            &path,
            r#"{
                "version": 1,
                "sessions": [],
                "updated_at_unix": 1
            }"#,
        )
        .expect("state fixture should write");
        let store = StateStore::new(path);

        let state = store
            .load()
            .expect("version 1 state without runs should migrate");

        assert_eq!(state.version(), super::RUNTIME_STATE_VERSION);
        assert!(state.runs().is_empty());
    }

    #[test]
    fn state_load_rejects_version_1_with_run_records() {
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session = Session::new(scope);
        let session_id = session.id().clone();
        let run = RunRecord::new(RunId::new("run_1").expect("valid run id"), session_id, 10);
        let path = test_path("state-v1-with-runs").join("runtime.state.json");
        let encoded = format!(
            r#"{{
                "version": 1,
                "sessions": [{session}],
                "runs": [{run}],
                "updated_at_unix": 1
            }}"#,
            session = serde_json::to_string(&session).expect("session should encode"),
            run = serde_json::to_string(&run).expect("run should encode")
        );
        fs::write(&path, encoded).expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("version 1 state must not carry run records");

        assert!(err.contains("version 1 must not contain run records"));
    }

    #[test]
    fn state_load_rejects_current_version_without_run_records() {
        let path = test_path("state-v2-without-runs").join("runtime.state.json");
        fs::write(
            &path,
            format!(
                r#"{{
                    "version": {},
                    "sessions": [],
                    "updated_at_unix": 1
                }}"#,
                super::RUNTIME_STATE_VERSION
            ),
        )
        .expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("current state version must carry run records");

        assert!(err.contains("must contain run records"));
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

    #[test]
    #[cfg(unix)]
    fn private_new_file_uses_private_mode_at_create_time() {
        use std::os::unix::fs::PermissionsExt;

        let path = test_path("private-new-file").join("secret.tmp");
        let file = open_private_new_file(&path).expect("private file should be created");

        let mode = file
            .metadata()
            .expect("file metadata should load")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[test]
    fn state_validation_rejects_duplicate_session_ids() {
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session = Session::new(scope);
        let path = test_path("state-duplicate-session-ids").join("runtime.state.json");
        let encoded = format!(
            r#"{{
                "version": 1,
                "sessions": [{session}, {session}],
                "updated_at_unix": 1
            }}"#,
            session = serde_json::to_string(&session).expect("session should encode")
        );
        fs::write(&path, encoded).expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("duplicate session ids should be rejected");

        assert!(err.contains("duplicate session id"));
    }

    #[test]
    fn state_validation_rejects_duplicate_run_ids() {
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session = Session::new(scope);
        let session_id = session.id().clone();
        let run = RunRecord::new(RunId::new("run_1").expect("valid run id"), session_id, 10);
        let path = test_path("state-duplicate-run-ids").join("runtime.state.json");
        let encoded = format!(
            r#"{{
                "version": {version},
                "sessions": [{session}],
                "runs": [{run}, {run}],
                "updated_at_unix": 1
            }}"#,
            version = super::RUNTIME_STATE_VERSION,
            session = serde_json::to_string(&session).expect("session should encode"),
            run = serde_json::to_string(&run).expect("run should encode")
        );
        fs::write(&path, encoded).expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("duplicate run ids should be rejected");

        assert!(err.contains("duplicate run id"));
    }

    #[test]
    fn state_validation_rejects_run_without_session() {
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session_id = crate::runtime::session::SessionId::for_scope(&scope);
        let run = RunRecord::new(RunId::new("run_1").expect("valid run id"), session_id, 10);
        let path = test_path("state-run-without-session").join("runtime.state.json");
        let encoded = format!(
            r#"{{
                "version": {version},
                "sessions": [],
                "runs": [{run}],
                "updated_at_unix": 1
            }}"#,
            version = super::RUNTIME_STATE_VERSION,
            run = serde_json::to_string(&run).expect("run should encode")
        );
        fs::write(&path, encoded).expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("run without known session should be rejected");

        assert!(err.contains("references unknown session"));
    }

    #[test]
    fn state_load_rejects_session_id_scope_mismatch() {
        let path = test_path("state-session-id-mismatch").join("runtime.state.json");
        fs::write(
            &path,
            r#"{
                "version": 1,
                "sessions": [{
                    "id": "session_wrong",
                    "scope": {"platform": "lark", "scope": "chat:oc_123"},
                    "created_at_unix": 1,
                    "updated_at_unix": 1
                }],
                "updated_at_unix": 1
            }"#,
        )
        .expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("session id should match derived scope id");

        assert!(err.contains("does not match derived id"));
    }

    #[test]
    fn state_load_rejects_session_time_order_mismatch() {
        let path = test_path("state-session-time-order").join("runtime.state.json");
        fs::write(
            &path,
            r#"{
                "version": 1,
                "sessions": [{
                    "id": "session_v1_4_6c61726b_b_636861743a6f635f313233",
                    "scope": {"platform": "lark", "scope": "chat:oc_123"},
                    "created_at_unix": 100,
                    "updated_at_unix": 1
                }],
                "updated_at_unix": 1
            }"#,
        )
        .expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("session updated_at should not be before created_at");

        assert!(err.contains("updated_at_unix before created_at_unix"));
    }

    #[test]
    fn relative_file_path_has_no_parent_directory_to_create() {
        assert!(non_empty_parent(std::path::Path::new("runtime.state.json")).is_none());
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

    fn session_fixture(
        scope: &SessionScope,
        created_at_unix: u64,
        updated_at_unix: u64,
    ) -> Session {
        serde_json::from_str(&format!(
            r#"{{
                "id": "{}",
                "scope": {{"platform": "{}", "scope": "{}"}},
                "created_at_unix": {created_at_unix},
                "updated_at_unix": {updated_at_unix}
            }}"#,
            crate::runtime::session::SessionId::for_scope(scope),
            scope.platform(),
            scope.scope()
        ))
        .expect("session fixture should decode")
    }

    fn state_with_pending_run(run_id: &str) -> (RuntimeState, RunId) {
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session = Session::new(scope);
        let session_id = session.id().clone();
        let run_id = RunId::new(run_id).expect("valid run id");
        let run = RunRecord::new(run_id.clone(), session_id, 10);
        let mut state = RuntimeState::new();
        state.upsert_session(session);
        state.add_run(run).expect("run should be accepted");

        (state, run_id)
    }
}
