use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    process,
    sync::{
        Arc, Mutex, MutexGuard, OnceLock, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Deserializer, Serialize, de, de::DeserializeOwned};

use super::{
    event::{Event, EventId, InboundEventRecord, InboundEventRecordStatus},
    run::{RunId, RunRecord},
    session::{Session, SessionId},
};

pub const RUNTIME_STATE_FILE_VERSION: u32 = 3;
const RUNTIME_STATE_FILE_V1_VERSION: u32 = 1;
const RUNTIME_STATE_FILE_V2_VERSION: u32 = 2;

static NEXT_TEMP_FILE: AtomicU64 = AtomicU64::new(0);
static STATE_STORE_WRITE_LOCKS: OnceLock<Mutex<BTreeMap<PathBuf, Weak<Mutex<()>>>>> =
    OnceLock::new();

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RuntimeState {
    sessions: Vec<Session>,
    runs: Vec<RunRecord>,
    inbound_events: Vec<InboundEventRecord>,
    updated_at_unix: u64,
}

impl RuntimeState {
    pub fn new() -> Self {
        Self {
            sessions: Vec::new(),
            runs: Vec::new(),
            inbound_events: Vec::new(),
            updated_at_unix: unix_seconds_now(),
        }
    }

    pub fn upsert_session(&mut self, session: Session) {
        let updated_at_unix = if let Some(existing) = self
            .sessions
            .iter_mut()
            .find(|existing| existing.id() == session.id())
        {
            existing.refresh_from(session);
            existing.updated_at_unix()
        } else {
            let updated_at_unix = session.updated_at_unix();
            self.sessions.push(session);
            updated_at_unix
        };

        self.touch_at(updated_at_unix.max(unix_seconds_now()));
    }

    pub fn session(&self, id: &SessionId) -> Option<&Session> {
        self.sessions.iter().find(|session| session.id() == id)
    }

    pub fn add_run(&mut self, run: RunRecord) -> Result<(), String> {
        self.validate_run_session(&run)?;

        if self.runs.iter().any(|existing| existing.id() == run.id()) {
            return Err(format!("duplicate run id {}", run.id()));
        }

        let updated_at_unix = run.updated_at_unix();
        self.runs.push(run);
        self.touch_at(updated_at_unix.max(unix_seconds_now()));
        Ok(())
    }

    pub fn run(&self, id: &RunId) -> Option<&RunRecord> {
        self.runs.iter().find(|run| run.id() == id)
    }

    pub fn record_inbound_event(
        &mut self,
        event: &Event,
    ) -> Result<InboundEventRecordStatus, String> {
        self.record_inbound_event_at(event, unix_seconds_now())
    }

    pub fn inbound_event(&self, id: &EventId) -> Option<&InboundEventRecord> {
        self.inbound_events.iter().find(|event| event.id() == id)
    }

    pub fn has_inbound_event(&self, id: &EventId) -> bool {
        self.inbound_event(id).is_some()
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

    pub fn sessions(&self) -> &[Session] {
        &self.sessions
    }

    pub fn runs(&self) -> &[RunRecord] {
        &self.runs
    }

    pub fn inbound_events(&self) -> &[InboundEventRecord] {
        &self.inbound_events
    }

    pub fn updated_at_unix(&self) -> u64 {
        self.updated_at_unix
    }

    pub fn validate(&self) -> Result<(), String> {
        let mut session_ids = BTreeSet::new();
        let mut run_ids = BTreeSet::new();
        let mut inbound_event_ids = BTreeSet::new();

        for session in &self.sessions {
            session.validate()?;

            if !session_ids.insert(session.id()) {
                return Err(format!("duplicate session id {}", session.id()));
            }

            if self.updated_at_unix < session.updated_at_unix() {
                return Err(format!(
                    "runtime state updated_at_unix before session {} updated_at_unix",
                    session.id()
                ));
            }
        }

        for run in &self.runs {
            run.validate()?;
            self.validate_run_session(run)?;

            if !run_ids.insert(run.id()) {
                return Err(format!("duplicate run id {}", run.id()));
            }

            if self.updated_at_unix < run.updated_at_unix() {
                return Err(format!(
                    "runtime state updated_at_unix before run {} updated_at_unix",
                    run.id()
                ));
            }
        }

        for event in &self.inbound_events {
            event.validate()?;

            if !inbound_event_ids.insert(event.id()) {
                return Err(format!("duplicate inbound event id {}", event.id()));
            }

            if self.updated_at_unix < event.recorded_at_unix() {
                return Err(format!(
                    "runtime state updated_at_unix before inbound event {} recorded_at_unix",
                    event.id()
                ));
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

    fn record_inbound_event_at(
        &mut self,
        event: &Event,
        recorded_at_unix: u64,
    ) -> Result<InboundEventRecordStatus, String> {
        if self.has_inbound_event(&event.id) {
            return Ok(InboundEventRecordStatus::Duplicate);
        }

        let record = InboundEventRecord::from_event(event, recorded_at_unix)?;
        let recorded_at_unix = record.recorded_at_unix();
        self.inbound_events.push(record);
        self.touch_at(recorded_at_unix);
        Ok(InboundEventRecordStatus::Recorded)
    }

    fn touch_at(&mut self, updated_at_unix: u64) {
        self.updated_at_unix = self.updated_at_unix.max(updated_at_unix);
    }

    fn normalize_migrated_aggregate_updated_at(&mut self) {
        let updated_at_unix = self
            .sessions
            .iter()
            .map(Session::updated_at_unix)
            .chain(self.runs.iter().map(RunRecord::updated_at_unix))
            .chain(
                self.inbound_events
                    .iter()
                    .map(InboundEventRecord::recorded_at_unix),
            )
            .fold(self.updated_at_unix, u64::max);
        self.updated_at_unix = updated_at_unix;
    }

    fn preserve_inbound_events_from(&mut self, existing: &RuntimeState) -> Result<(), String> {
        for existing_event in &existing.inbound_events {
            match self
                .inbound_events
                .iter()
                .find(|event| event.id() == existing_event.id())
            {
                Some(candidate_event) if candidate_event == existing_event => {}
                Some(_) => {
                    return Err(format!(
                        "conflicting inbound event record {}",
                        existing_event.id()
                    ));
                }
                None => {
                    self.touch_at(existing_event.recorded_at_unix());
                    self.inbound_events.push(existing_event.clone());
                }
            }
        }

        Ok(())
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
        #[serde(deny_unknown_fields)]
        struct RuntimeStateWire {
            sessions: Vec<Session>,
            runs: Vec<RunRecord>,
            inbound_events: Vec<InboundEventRecord>,
            updated_at_unix: u64,
        }

        let wire = RuntimeStateWire::deserialize(deserializer)?;
        let state = Self {
            sessions: wire.sessions,
            runs: wire.runs,
            inbound_events: wire.inbound_events,
            updated_at_unix: wire.updated_at_unix,
        };
        state.validate().map_err(de::Error::custom)?;
        Ok(state)
    }
}

#[derive(Serialize)]
struct RuntimeStateFile<'a> {
    version: u32,
    sessions: &'a [Session],
    runs: &'a [RunRecord],
    inbound_events: &'a [InboundEventRecord],
    updated_at_unix: u64,
}

impl<'a> RuntimeStateFile<'a> {
    fn from_state(state: &'a RuntimeState) -> Self {
        Self {
            version: RUNTIME_STATE_FILE_VERSION,
            sessions: &state.sessions,
            runs: &state.runs,
            inbound_events: &state.inbound_events,
            updated_at_unix: state.updated_at_unix,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeStateFileWire {
    version: u32,
    sessions: Vec<Session>,
    #[serde(default, deserialize_with = "deserialize_wire_field")]
    runs: WireField<Vec<RunRecord>>,
    #[serde(default, deserialize_with = "deserialize_wire_field")]
    inbound_events: WireField<Vec<InboundEventRecord>>,
    updated_at_unix: u64,
}

impl RuntimeStateFileWire {
    fn into_state(self) -> Result<RuntimeState, String> {
        let (runs, inbound_events, normalize_aggregate_updated_at) = match self.version {
            RUNTIME_STATE_FILE_V1_VERSION => {
                if self.runs.is_present() {
                    return Err("runtime state version 1 must not contain run records".to_string());
                }

                if self.inbound_events.is_present() {
                    return Err(
                        "runtime state version 1 must not contain inbound event records"
                            .to_string(),
                    );
                }

                (Vec::new(), Vec::new(), true)
            }
            RUNTIME_STATE_FILE_V2_VERSION => {
                let runs = self
                    .runs
                    .into_required("runtime state version 2 must contain run records")?;

                if self.inbound_events.is_present() {
                    return Err(
                        "runtime state version 2 must not contain inbound event records"
                            .to_string(),
                    );
                }

                (runs, Vec::new(), true)
            }
            RUNTIME_STATE_FILE_VERSION => {
                let runs = self.runs.into_required(format!(
                    "runtime state version {RUNTIME_STATE_FILE_VERSION} must contain run records"
                ))?;
                let inbound_events = self.inbound_events.into_required(format!(
                    "runtime state version {RUNTIME_STATE_FILE_VERSION} must contain inbound event records"
                ))?;

                (runs, inbound_events, false)
            }
            version => {
                return Err(format!(
                    "unsupported runtime state version {}; expected {}",
                    version, RUNTIME_STATE_FILE_VERSION
                ));
            }
        };
        let mut state = RuntimeState {
            sessions: self.sessions,
            runs,
            inbound_events,
            updated_at_unix: self.updated_at_unix,
        };
        if normalize_aggregate_updated_at {
            state.normalize_migrated_aggregate_updated_at();
        }
        state.validate()?;
        Ok(state)
    }
}

#[derive(Default)]
enum WireField<T> {
    #[default]
    Missing,
    Null,
    Value(T),
}

impl<T> WireField<T> {
    fn is_present(&self) -> bool {
        !matches!(self, Self::Missing)
    }

    fn into_required<M>(self, message: M) -> Result<T, String>
    where
        M: fmt::Display,
    {
        match self {
            Self::Value(value) => Ok(value),
            Self::Missing | Self::Null => Err(message.to_string()),
        }
    }
}

fn deserialize_wire_field<'de, D, T>(deserializer: D) -> Result<WireField<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Ok(match Option::<T>::deserialize(deserializer)? {
        Some(value) => WireField::Value(value),
        None => WireField::Null,
    })
}

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

        let state_file: RuntimeStateFileWire = serde_json::from_str(&input)
            .map_err(|err| format!("failed to parse {}: {err}", self.path().display()))?;
        state_file
            .into_state()
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

    fn save_unlocked(&self, state: &RuntimeState) -> Result<(), String> {
        let mut state = state.clone();
        if let Some(existing) = self.load_existing_for_merge()? {
            state.preserve_inbound_events_from(&existing)?;
        }

        state.validate()?;
        let state_file = RuntimeStateFile::from_state(&state);
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

        let state_file: RuntimeStateFileWire = serde_json::from_str(&input)
            .map_err(|err| format!("failed to parse {}: {err}", self.path().display()))?;
        state_file
            .into_state()
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
        sync::{
            Arc, Barrier,
            atomic::{AtomicU64, Ordering},
        },
        thread,
    };

    use super::{RuntimeState, StateStore, non_empty_parent, open_private_new_file};
    use crate::runtime::{
        event::{
            Event, EventId, EventKind, EventSource, InboundEventRecord, InboundEventRecordStatus,
        },
        message::Message,
        run::{RunId, RunRecord, RunStatus},
        session::{Session, SessionScope},
    };

    static NEXT_DIR: AtomicU64 = AtomicU64::new(0);
    const FUTURE_UNIX: u64 = 4_102_444_800;

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
    fn runtime_state_json_does_not_embed_file_version() {
        let encoded = serde_json::to_value(RuntimeState::new()).expect("state should encode");

        assert!(encoded.get("version").is_none());
        assert!(encoded.get("sessions").is_some());
        assert!(encoded.get("runs").is_some());
        assert!(encoded.get("inbound_events").is_some());
        assert!(encoded.get("updated_at_unix").is_some());
    }

    #[test]
    fn runtime_state_json_rejects_file_version_field() {
        let err = serde_json::from_str::<RuntimeState>(
            r#"{
                "version": 1,
                "sessions": [],
                "runs": [],
                "inbound_events": [],
                "updated_at_unix": 1
            }"#,
        )
        .expect_err("RuntimeState should not accept file envelope fields");

        assert!(err.to_string().contains("unknown field `version`"));
    }

    #[test]
    fn state_store_writes_file_version_envelope() {
        let path = test_path("state-file-version-envelope").join("runtime.state.json");
        let store = StateStore::new(&path);

        store.save(&RuntimeState::new()).expect("state should save");

        let encoded: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(path).expect("state file should read"))
                .expect("state file should decode");

        assert_eq!(
            encoded.get("version").and_then(serde_json::Value::as_u64),
            Some(u64::from(super::RUNTIME_STATE_FILE_VERSION))
        );
        assert!(encoded.get("sessions").is_some());
        assert!(encoded.get("runs").is_some());
        assert!(encoded.get("updated_at_unix").is_some());
        assert!(encoded.get("inbound_events").is_some());
    }

    #[test]
    fn state_store_round_trips_inbound_event_ledger() {
        let path = test_path("state-inbound-event-round-trip").join("runtime.state.json");
        let store = StateStore::new(&path);
        let event = event_fixture("evt_1", 10);
        let mut state = RuntimeState::new();

        assert_eq!(
            state
                .record_inbound_event_at(&event, 12)
                .expect("event should record"),
            InboundEventRecordStatus::Recorded
        );

        store.save(&state).expect("state should save");
        let loaded = store.load().expect("state should load");

        let record = loaded
            .inbound_event(&event.id)
            .expect("inbound event record should exist");
        assert_eq!(record.received_at_unix(), 10);
        assert_eq!(record.recorded_at_unix(), 12);
        assert_eq!(loaded, state);
    }

    #[test]
    fn state_records_inbound_events_idempotently() {
        let event = event_fixture("evt_1", 10);
        let mut state = RuntimeState::new();
        state.updated_at_unix = 20;

        assert_eq!(
            state
                .record_inbound_event_at(&event, 12)
                .expect("event should record"),
            InboundEventRecordStatus::Recorded
        );
        assert!(state.has_inbound_event(&event.id));
        assert_eq!(state.inbound_events().len(), 1);
        assert_eq!(state.updated_at_unix(), 20);

        assert_eq!(
            state
                .record_inbound_event_at(&event, 30)
                .expect("duplicate event should not fail"),
            InboundEventRecordStatus::Duplicate
        );
        assert_eq!(state.inbound_events().len(), 1);
        assert_eq!(state.updated_at_unix(), 20);
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
                    "updated_at_unix": 1
                }}"#,
                version = super::RUNTIME_STATE_FILE_VERSION,
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
    fn upsert_session_advances_state_updated_at_to_inserted_session() {
        let path = test_path("state-upsert-inserted-session-updated-at").join("runtime.state.json");
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session = session_fixture(&scope, 10, FUTURE_UNIX);
        let mut state = RuntimeState::new();

        state.upsert_session(session);

        assert!(state.updated_at_unix() >= FUTURE_UNIX);
        StateStore::new(path)
            .save(&state)
            .expect("state should remain valid after inserting a future-dated session");
    }

    #[test]
    fn upsert_session_advances_state_updated_at_to_refreshed_session() {
        let path =
            test_path("state-upsert-refreshed-session-updated-at").join("runtime.state.json");
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let first = session_fixture(&scope, 10, 20);
        let replacement = session_fixture(&scope, 10, FUTURE_UNIX);
        let mut state = RuntimeState::new();

        state.upsert_session(first);
        state.upsert_session(replacement);

        assert!(state.updated_at_unix() >= FUTURE_UNIX);
        StateStore::new(path)
            .save(&state)
            .expect("state should remain valid after refreshing a future-dated session");
    }

    #[test]
    fn add_run_advances_state_updated_at_to_run_record() {
        let path = test_path("state-add-run-updated-at").join("runtime.state.json");
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session = session_fixture(&scope, 10, 10);
        let session_id = session.id().clone();
        let run = RunRecord::new(
            RunId::new("run_future").expect("valid run id"),
            session_id,
            FUTURE_UNIX,
        );
        let mut state = RuntimeState::new();

        state.upsert_session(session);
        state.add_run(run).expect("run should be accepted");

        assert!(state.updated_at_unix() >= FUTURE_UNIX);
        StateStore::new(path)
            .save(&state)
            .expect("state should remain valid after adding a future-dated run");
    }

    #[test]
    fn missing_state_loads_as_empty_state() {
        let path = test_path("missing-state").join("runtime.state.json");
        let store = StateStore::new(path);
        let state = store.load().expect("missing state should be defaulted");

        assert!(state.sessions().is_empty());
        assert!(state.runs().is_empty());
        assert!(state.inbound_events().is_empty());
    }

    #[test]
    fn state_load_migrates_released_version_1_without_runs_or_inbound_event_records() {
        let path = test_path("state-v1-released-shape").join("runtime.state.json");
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
            .expect("released version 1 state should migrate");

        assert!(state.runs().is_empty());
        assert!(state.inbound_events().is_empty());
    }

    #[test]
    fn state_load_migrates_version_1_stale_aggregate_updated_at() {
        let path = test_path("state-v1-stale-aggregate-updated-at").join("runtime.state.json");
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session = session_fixture(&scope, 10, 20);
        let encoded = format!(
            r#"{{
                "version": 1,
                "sessions": [{session}],
                "updated_at_unix": 1
            }}"#,
            session = serde_json::to_string(&session).expect("session should encode")
        );
        fs::write(&path, encoded).expect("state fixture should write");
        let store = StateStore::new(path);

        let state = store
            .load()
            .expect("version 1 state should normalize aggregate timestamps while migrating");

        assert_eq!(state.updated_at_unix(), 20);
        assert!(state.runs().is_empty());
        assert!(state.inbound_events().is_empty());
    }

    #[test]
    fn state_load_rejects_version_1_with_run_records() {
        let path = test_path("state-v1-with-runs").join("runtime.state.json");
        fs::write(
            &path,
            r#"{
                "version": 1,
                "sessions": [],
                "runs": [],
                "updated_at_unix": 1
            }"#,
        )
        .expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("version 1 state must not carry run records");

        assert!(err.contains("version 1 must not contain run records"));
    }

    #[test]
    fn state_load_rejects_version_1_with_null_run_records() {
        let path = test_path("state-v1-with-null-runs").join("runtime.state.json");
        fs::write(
            &path,
            r#"{
                "version": 1,
                "sessions": [],
                "runs": null,
                "updated_at_unix": 1
            }"#,
        )
        .expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("version 1 state must reject present null runs");

        assert!(err.contains("version 1 must not contain run records"));
    }

    #[test]
    fn state_load_rejects_version_1_with_inbound_event_records() {
        let path = test_path("state-v1-with-inbound-events").join("runtime.state.json");
        fs::write(
            &path,
            r#"{
                "version": 1,
                "sessions": [],
                "inbound_events": [],
                "updated_at_unix": 1
            }"#,
        )
        .expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("version 1 state must not carry inbound event records");

        assert!(err.contains("version 1 must not contain inbound event records"));
    }

    #[test]
    fn state_load_migrates_version_2_without_inbound_event_records() {
        let path = test_path("state-v2-without-inbound-events").join("runtime.state.json");
        fs::write(
            &path,
            r#"{
                "version": 2,
                "sessions": [],
                "runs": [],
                "updated_at_unix": 1
            }"#,
        )
        .expect("state fixture should write");
        let store = StateStore::new(path);

        let state = store
            .load()
            .expect("version 2 state without inbound event records should migrate");

        assert!(state.runs().is_empty());
        assert!(state.inbound_events().is_empty());
    }

    #[test]
    fn state_load_migrates_version_2_stale_aggregate_updated_at() {
        let path = test_path("state-v2-stale-aggregate-updated-at").join("runtime.state.json");
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session = session_fixture(&scope, 1, 2);
        let run = RunRecord::new(
            RunId::new("run_1").expect("valid run id"),
            session.id().clone(),
            20,
        );
        let encoded = format!(
            r#"{{
                "version": 2,
                "sessions": [{session}],
                "runs": [{run}],
                "updated_at_unix": 1
            }}"#,
            session = serde_json::to_string(&session).expect("session should encode"),
            run = serde_json::to_string(&run).expect("run should encode")
        );
        fs::write(&path, encoded).expect("state fixture should write");
        let store = StateStore::new(path);

        let state = store
            .load()
            .expect("version 2 state should normalize aggregate timestamps while migrating");

        assert_eq!(state.updated_at_unix(), 20);
        assert_eq!(state.runs().len(), 1);
        assert!(state.inbound_events().is_empty());
    }

    #[test]
    fn state_load_rejects_version_2_with_inbound_event_records() {
        let path = test_path("state-v2-with-inbound-events").join("runtime.state.json");
        fs::write(
            &path,
            r#"{
                "version": 2,
                "sessions": [],
                "runs": [],
                "inbound_events": [],
                "updated_at_unix": 1
            }"#,
        )
        .expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("version 2 state must not carry inbound event records");

        assert!(err.contains("version 2 must not contain inbound event records"));
    }

    #[test]
    fn state_load_rejects_version_2_with_null_inbound_event_records() {
        let path = test_path("state-v2-with-null-inbound-events").join("runtime.state.json");
        fs::write(
            &path,
            r#"{
                "version": 2,
                "sessions": [],
                "runs": [],
                "inbound_events": null,
                "updated_at_unix": 1
            }"#,
        )
        .expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("version 2 state must reject present null inbound event records");

        assert!(err.contains("version 2 must not contain inbound event records"));
    }

    #[test]
    fn state_load_rejects_future_file_version() {
        let path = test_path("state-future-version").join("runtime.state.json");
        fs::write(
            &path,
            r#"{
                "version": 4,
                "sessions": [],
                "runs": [],
                "inbound_events": [],
                "updated_at_unix": 1
            }"#,
        )
        .expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("future state versions must not be loaded");

        assert!(err.contains("unsupported runtime state version 4; expected 3"));
    }

    #[test]
    fn state_load_rejects_current_version_without_run_records() {
        let path = test_path("state-v3-without-runs").join("runtime.state.json");
        fs::write(
            &path,
            format!(
                r#"{{
                    "version": {},
                    "sessions": [],
                    "updated_at_unix": 1
                }}"#,
                super::RUNTIME_STATE_FILE_VERSION
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
    fn state_load_rejects_current_version_without_inbound_event_records() {
        let path = test_path("state-v3-without-inbound-events").join("runtime.state.json");
        fs::write(
            &path,
            format!(
                r#"{{
                    "version": {},
                    "sessions": [],
                    "runs": [],
                    "updated_at_unix": 1
                }}"#,
                super::RUNTIME_STATE_FILE_VERSION
            ),
        )
        .expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("current state version must carry inbound events");

        assert!(err.contains("must contain inbound event records"));
    }

    #[test]
    fn state_load_rejects_current_version_with_null_inbound_event_records() {
        let path = test_path("state-v3-with-null-inbound-events").join("runtime.state.json");
        fs::write(
            &path,
            format!(
                r#"{{
                    "version": {},
                    "sessions": [],
                    "runs": [],
                    "inbound_events": null,
                    "updated_at_unix": 1
                }}"#,
                super::RUNTIME_STATE_FILE_VERSION
            ),
        )
        .expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("current state version must reject null inbound events");

        assert!(err.contains("must contain inbound event records"));
    }

    #[test]
    fn state_load_rejects_stale_state_updated_at_for_sessions() {
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session = session_fixture(&scope, 10, 20);
        let path = test_path("state-stale-updated-at-session").join("runtime.state.json");
        let encoded = format!(
            r#"{{
                "version": {version},
                "sessions": [{session}],
                "runs": [],
                "inbound_events": [],
                "updated_at_unix": 1
            }}"#,
            version = super::RUNTIME_STATE_FILE_VERSION,
            session = serde_json::to_string(&session).expect("session should encode")
        );
        fs::write(&path, encoded).expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("state updated_at should not lag session records");

        assert!(err.contains("before session"));
    }

    #[test]
    fn state_load_rejects_stale_state_updated_at_for_runs() {
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session = session_fixture(&scope, 1, 1);
        let run = RunRecord::new(
            RunId::new("run_1").expect("valid run id"),
            session.id().clone(),
            10,
        );
        let path = test_path("state-stale-updated-at-run").join("runtime.state.json");
        let encoded = format!(
            r#"{{
                "version": {version},
                "sessions": [{session}],
                "runs": [{run}],
                "inbound_events": [],
                "updated_at_unix": 1
            }}"#,
            version = super::RUNTIME_STATE_FILE_VERSION,
            session = serde_json::to_string(&session).expect("session should encode"),
            run = serde_json::to_string(&run).expect("run should encode")
        );
        fs::write(&path, encoded).expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("state updated_at should not lag run records");

        assert!(err.contains("before run"));
    }

    #[test]
    fn state_load_rejects_stale_state_updated_at_for_inbound_events() {
        let event = event_fixture("evt_1", 10);
        let record = state_event_record(&event, 12).expect("inbound event record should build");
        let path = test_path("state-stale-updated-at-inbound-event").join("runtime.state.json");
        let encoded = format!(
            r#"{{
                "version": {version},
                "sessions": [],
                "runs": [],
                "inbound_events": [{record}],
                "updated_at_unix": 1
            }}"#,
            version = super::RUNTIME_STATE_FILE_VERSION,
            record = serde_json::to_string(&record).expect("event record should encode")
        );
        fs::write(&path, encoded).expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("state updated_at should not lag inbound event records");

        assert!(err.contains("before inbound event"));
    }

    #[test]
    fn state_load_rejects_unknown_file_fields() {
        let path = test_path("state-unknown-file-fields").join("runtime.state.json");
        fs::write(
            &path,
            format!(
                r#"{{
                    "version": {},
                    "sessions": [],
                    "runs": [],
                    "inbound_events": [],
                    "future_field": [],
                    "updated_at_unix": 1
                }}"#,
                super::RUNTIME_STATE_FILE_VERSION
            ),
        )
        .expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("unknown state file fields must not be dropped");

        assert!(err.contains("unknown field `future_field`"));
    }

    #[test]
    fn state_load_rejects_unknown_session_fields() {
        let path = test_path("state-unknown-session-fields").join("runtime.state.json");
        fs::write(
            &path,
            format!(
                r#"{{
                "version": {},
                "sessions": [{{
                    "id": "session_v1_4_6c61726b_b_636861743a6f635f313233",
                    "scope": {{"platform": "lark", "scope": "chat:oc_123"}},
                    "created_at_unix": 1,
                    "updated_at_unix": 1,
                    "future_field": true
                }}],
                "runs": [],
                "inbound_events": [],
                "updated_at_unix": 1
            }}"#,
                super::RUNTIME_STATE_FILE_VERSION
            ),
        )
        .expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("unknown session fields must not be dropped");

        assert!(err.contains("unknown field `future_field`"));
    }

    #[test]
    fn state_load_rejects_unknown_session_scope_fields() {
        let path = test_path("state-unknown-session-scope-fields").join("runtime.state.json");
        fs::write(
            &path,
            format!(
                r#"{{
                "version": {},
                "sessions": [{{
                    "id": "session_v1_4_6c61726b_b_636861743a6f635f313233",
                    "scope": {{
                        "platform": "lark",
                        "scope": "chat:oc_123",
                        "future_field": true
                    }},
                    "created_at_unix": 1,
                    "updated_at_unix": 1
                }}],
                "runs": [],
                "inbound_events": [],
                "updated_at_unix": 1
            }}"#,
                super::RUNTIME_STATE_FILE_VERSION
            ),
        )
        .expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("unknown session scope fields must not be dropped");

        assert!(err.contains("unknown field `future_field`"));
    }

    #[test]
    fn state_load_rejects_unknown_run_fields() {
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session = Session::new(scope);
        let session_id = session.id().clone();
        let run = RunRecord::new(RunId::new("run_1").expect("valid run id"), session_id, 10);
        let path = test_path("state-unknown-run-fields").join("runtime.state.json");
        let encoded = format!(
            r#"{{
                "version": {version},
                "sessions": [{session}],
                "runs": [{{
                    "id": "run_1",
                    "session_id": "{session_id}",
                    "status": "pending",
                    "created_at_unix": 10,
                    "updated_at_unix": 10,
                    "started_at_unix": null,
                    "finished_at_unix": null,
                    "future_field": true
                }}],
                "inbound_events": [],
                "updated_at_unix": 1
            }}"#,
            version = super::RUNTIME_STATE_FILE_VERSION,
            session = serde_json::to_string(&session).expect("session should encode"),
            session_id = run.session_id()
        );
        fs::write(&path, encoded).expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("unknown run fields must not be dropped");

        assert!(err.contains("unknown field `future_field`"));
    }

    #[test]
    fn state_load_rejects_unknown_inbound_event_fields() {
        let path = test_path("state-unknown-inbound-event-fields").join("runtime.state.json");
        let encoded = format!(
            r#"{{
                "version": {},
                "sessions": [],
                "runs": [],
                "inbound_events": [{{
                    "id": "evt_1",
                    "received_at_unix": 10,
                    "recorded_at_unix": 12,
                    "future_field": true
                }}],
                "updated_at_unix": 1
            }}"#,
            super::RUNTIME_STATE_FILE_VERSION
        );
        fs::write(&path, encoded).expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("unknown inbound event fields must not be dropped");

        assert!(err.contains("unknown field `future_field`"));
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
        let updated_at_unix = session.updated_at_unix();
        let path = test_path("state-duplicate-session-ids").join("runtime.state.json");
        let encoded = format!(
            r#"{{
                "version": {version},
                "sessions": [{session}, {session}],
                "runs": [],
                "inbound_events": [],
                "updated_at_unix": {updated_at_unix}
            }}"#,
            version = super::RUNTIME_STATE_FILE_VERSION,
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
        let updated_at_unix = session.updated_at_unix().max(run.updated_at_unix());
        let path = test_path("state-duplicate-run-ids").join("runtime.state.json");
        let encoded = format!(
            r#"{{
                "version": {version},
                "sessions": [{session}],
                "runs": [{run}, {run}],
                "inbound_events": [],
                "updated_at_unix": {updated_at_unix}
            }}"#,
            version = super::RUNTIME_STATE_FILE_VERSION,
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
        let updated_at_unix = run.updated_at_unix();
        let path = test_path("state-run-without-session").join("runtime.state.json");
        let encoded = format!(
            r#"{{
                "version": {version},
                "sessions": [],
                "runs": [{run}],
                "inbound_events": [],
                "updated_at_unix": {updated_at_unix}
            }}"#,
            version = super::RUNTIME_STATE_FILE_VERSION,
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
    fn state_validation_rejects_duplicate_inbound_event_ids() {
        let event = event_fixture("evt_1", 10);
        let record = state_event_record(&event, 12).expect("inbound event record should build");
        let updated_at_unix = record.recorded_at_unix();
        let path = test_path("state-duplicate-inbound-event-ids").join("runtime.state.json");
        let encoded = format!(
            r#"{{
                "version": {version},
                "sessions": [],
                "runs": [],
                "inbound_events": [{record}, {record}],
                "updated_at_unix": {updated_at_unix}
            }}"#,
            version = super::RUNTIME_STATE_FILE_VERSION,
            record = serde_json::to_string(&record).expect("event record should encode")
        );
        fs::write(&path, encoded).expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("duplicate inbound event ids should be rejected");

        assert!(err.contains("duplicate inbound event id"));
    }

    #[test]
    fn state_load_rejects_session_id_scope_mismatch() {
        let path = test_path("state-session-id-mismatch").join("runtime.state.json");
        fs::write(
            &path,
            format!(
                r#"{{
                "version": {},
                "sessions": [{{
                    "id": "session_wrong",
                    "scope": {{"platform": "lark", "scope": "chat:oc_123"}},
                    "created_at_unix": 1,
                    "updated_at_unix": 1
                }}],
                "runs": [],
                "inbound_events": [],
                "updated_at_unix": 1
            }}"#,
                super::RUNTIME_STATE_FILE_VERSION
            ),
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
            format!(
                r#"{{
                "version": {},
                "sessions": [{{
                    "id": "session_v1_4_6c61726b_b_636861743a6f635f313233",
                    "scope": {{"platform": "lark", "scope": "chat:oc_123"}},
                    "created_at_unix": 100,
                    "updated_at_unix": 1
                }}],
                "runs": [],
                "inbound_events": [],
                "updated_at_unix": 1
            }}"#,
                super::RUNTIME_STATE_FILE_VERSION
            ),
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
