use std::{env, ffi::OsString, path::PathBuf};

use super::{HOME_ENV, LOCK_FILE, LOG_FILE, STATE_FILE, STOP_FILE};

#[derive(Debug, Clone)]
pub struct DaemonPaths {
    pub runtime_dir: PathBuf,
    pub(super) lock_file: PathBuf,
    pub(super) state_file: PathBuf,
    pub(super) stop_file: PathBuf,
    pub(super) log_file: PathBuf,
}

impl DaemonPaths {
    pub fn from_env() -> Result<Self, String> {
        if let Some(value) = non_empty_env(HOME_ENV) {
            return Ok(Self::new(value));
        }

        if let Some(home) = non_empty_env("HOME") {
            return Ok(Self::new(PathBuf::from(home).join(".ferris-agent-bridge")));
        }

        #[cfg(windows)]
        if let Some(home) = non_empty_env("USERPROFILE") {
            return Ok(Self::new(PathBuf::from(home).join(".ferris-agent-bridge")));
        }

        Err(format!(
            "could not determine runtime directory; set {HOME_ENV}"
        ))
    }

    pub fn new(runtime_dir: impl Into<PathBuf>) -> Self {
        let runtime_dir = runtime_dir.into();

        Self {
            lock_file: runtime_dir.join(LOCK_FILE),
            state_file: runtime_dir.join(STATE_FILE),
            stop_file: runtime_dir.join(STOP_FILE),
            log_file: runtime_dir.join(LOG_FILE),
            runtime_dir,
        }
    }
}

fn non_empty_env(name: &str) -> Option<OsString> {
    let value = env::var_os(name)?;

    if value.is_empty() {
        return None;
    }

    Some(value)
}
