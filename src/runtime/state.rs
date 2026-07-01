use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    process,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use super::session::{Session, SessionId};

pub const RUNTIME_STATE_VERSION: u32 = 1;

static NEXT_TEMP_FILE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeState {
    pub version: u32,
    pub sessions: Vec<Session>,
    pub updated_at_unix: u64,
}

impl RuntimeState {
    pub fn new() -> Self {
        Self {
            version: RUNTIME_STATE_VERSION,
            sessions: Vec::new(),
            updated_at_unix: unix_seconds_now(),
        }
    }

    pub fn upsert_session(&mut self, session: Session) {
        if let Some(existing) = self
            .sessions
            .iter_mut()
            .find(|existing| existing.id == session.id)
        {
            *existing = session;
        } else {
            self.sessions.push(session);
        }

        self.updated_at_unix = unix_seconds_now();
    }

    pub fn session(&self, id: &SessionId) -> Option<&Session> {
        self.sessions.iter().find(|session| &session.id == id)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.version != RUNTIME_STATE_VERSION {
            return Err(format!(
                "unsupported runtime state version {}; expected {}",
                self.version, RUNTIME_STATE_VERSION
            ));
        }

        for session in &self.sessions {
            if session.scope.platform.trim().is_empty() {
                return Err(format!("session {} has empty platform", session.id));
            }

            if session.scope.scope.trim().is_empty() {
                return Err(format!("session {} has empty scope", session.id));
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
        if !self.path.exists() {
            return Ok(RuntimeState::new());
        }

        let state: RuntimeState = read_json(&self.path)?;
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
        fs::create_dir_all(parent)?;
    }

    let temp_path = temp_path_for(path);
    let mut encoded = serde_json::to_vec_pretty(value)?;
    encoded.push(b'\n');

    let write_result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)?;
        set_private_file_permissions(&file)?;
        file.write_all(&encoded)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temp_path, path)?;
        sync_parent(path)?;
        Ok(())
    })();

    if write_result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }

    write_result
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

fn sync_parent(path: &Path) -> io::Result<()> {
    if let Some(parent) = non_empty_parent(path) {
        File::open(parent)?.sync_all()?;
    }

    Ok(())
}

fn non_empty_parent(path: &Path) -> Option<&Path> {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
}

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

    use super::{RuntimeState, StateStore, non_empty_parent};
    use crate::runtime::session::{Session, SessionScope};

    static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn state_store_round_trips_sessions() {
        let path = test_path("state-round-trip").join("runtime.state.json");
        let store = StateStore::new(&path);
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session = Session::new(scope);
        let session_id = session.id.clone();
        let mut state = RuntimeState::new();
        state.upsert_session(session);

        store.save(&state).expect("state should save");
        let loaded = store.load().expect("state should load");

        assert!(loaded.session(&session_id).is_some());
        assert_eq!(loaded, state);
    }

    #[test]
    fn missing_state_loads_as_empty_state() {
        let path = test_path("missing-state").join("runtime.state.json");
        let store = StateStore::new(path);
        let state = store.load().expect("missing state should be defaulted");

        assert!(state.sessions.is_empty());
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
}
