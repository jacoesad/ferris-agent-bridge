use std::{
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
    process, thread,
    time::{Instant, SystemTime},
};

use super::{
    DaemonPaths, LIFECYCLE_LOCK_FILE, LIFECYCLE_LOCK_POLL_INTERVAL, LIFECYCLE_LOCK_STALE_TTL,
    LIFECYCLE_LOCK_TIMEOUT,
    files::{configure_private_file_options, remove_file_if_exists, set_private_file_permissions},
    process_ops::{generate_token, is_process_running, now_unix_seconds},
};

pub(super) struct LifecycleLockGuard {
    lock_file: PathBuf,
    pub(super) token: String,
}

impl Drop for LifecycleLockGuard {
    fn drop(&mut self) {
        let _ = remove_lifecycle_lock_if_matches(&self.lock_file, &self.token);
    }
}

pub(super) fn acquire_lifecycle_lock(paths: &DaemonPaths) -> io::Result<LifecycleLockGuard> {
    let lock_file = paths.runtime_dir.join(LIFECYCLE_LOCK_FILE);
    let started = Instant::now();

    loop {
        match create_lifecycle_lock(&lock_file) {
            Ok(guard) => return Ok(guard),
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                match inspect_stale_lifecycle_lock(&lock_file) {
                    StaleLifecycleLock::Owned(token) => {
                        let _ = remove_lifecycle_lock_if_matches(&lock_file, &token);
                        continue;
                    }
                    StaleLifecycleLock::Incomplete(snapshot) => {
                        let _ = cleanup_incomplete_lifecycle_lock_if_matches(&lock_file, &snapshot);
                        continue;
                    }
                    StaleLifecycleLock::NotStale => {}
                }

                if started.elapsed() >= LIFECYCLE_LOCK_TIMEOUT {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!(
                            "timed out waiting for daemon lifecycle lock {}",
                            lock_file.display()
                        ),
                    ));
                }

                thread::sleep(LIFECYCLE_LOCK_POLL_INTERVAL);
            }
            Err(err) => return Err(err),
        }
    }
}

pub(super) fn create_lifecycle_lock(path: &Path) -> io::Result<LifecycleLockGuard> {
    let token = generate_token();
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    configure_private_file_options(&mut options);
    let mut file = options.open(path)?;

    let result = file
        .write_all(lifecycle_lock_contents(&token).as_bytes())
        .and_then(|()| file.sync_all())
        .and_then(|()| set_private_file_permissions(path));

    if let Err(err) = result {
        let _ = remove_file_if_exists(path);
        return Err(err);
    }

    Ok(LifecycleLockGuard {
        lock_file: path.to_owned(),
        token,
    })
}

pub(super) fn lifecycle_lock_contents(token: &str) -> String {
    format!(
        "pid={}\ntoken={}\nstarted_at_unix={}\n",
        process::id(),
        token,
        now_unix_seconds()
    )
}

pub(super) enum StaleLifecycleLock {
    NotStale,
    Owned(String),
    Incomplete(FileSnapshot),
}

pub(super) fn inspect_stale_lifecycle_lock(path: &Path) -> StaleLifecycleLock {
    let snapshot = match FileSnapshot::from_path(path) {
        Ok(Some(snapshot)) => snapshot,
        Ok(None) | Err(_) => return StaleLifecycleLock::NotStale,
    };

    if !snapshot.is_stale() {
        return StaleLifecycleLock::NotStale;
    }

    match read_lifecycle_lock(path) {
        Ok(Some(LifecycleLockContent::Complete(record))) => {
            if is_process_running(record.pid) {
                StaleLifecycleLock::NotStale
            } else {
                StaleLifecycleLock::Owned(record.token)
            }
        }
        Ok(Some(LifecycleLockContent::Incomplete { pid })) => {
            if pid.map(is_process_running).unwrap_or(false) {
                StaleLifecycleLock::NotStale
            } else {
                StaleLifecycleLock::Incomplete(snapshot)
            }
        }
        Ok(None) | Err(_) => StaleLifecycleLock::Incomplete(snapshot),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LifecycleLockRecord {
    pid: u32,
    pub(super) token: String,
    _started_at_unix: u64,
}

enum LifecycleLockContent {
    Complete(LifecycleLockRecord),
    Incomplete { pid: Option<u32> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct FileSnapshot {
    len: u64,
    modified: SystemTime,
    #[cfg(unix)]
    dev: u64,
    #[cfg(unix)]
    ino: u64,
}

impl FileSnapshot {
    pub(super) fn from_path(path: &Path) -> io::Result<Option<Self>> {
        match fs::metadata(path) {
            Ok(metadata) => Self::from_metadata(metadata).map(Some),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err),
        }
    }

    fn from_metadata(metadata: fs::Metadata) -> io::Result<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;

            Ok(Self {
                len: metadata.len(),
                modified: metadata.modified()?,
                dev: metadata.dev(),
                ino: metadata.ino(),
            })
        }

        #[cfg(not(unix))]
        {
            Ok(Self {
                len: metadata.len(),
                modified: metadata.modified()?,
            })
        }
    }

    fn is_stale(&self) -> bool {
        self.modified
            .elapsed()
            .map(|elapsed| elapsed > LIFECYCLE_LOCK_STALE_TTL)
            .unwrap_or(false)
    }

    fn matches_path(&self, path: &Path) -> io::Result<bool> {
        Ok(Self::from_path(path)?.as_ref() == Some(self))
    }
}

pub(super) fn cleanup_incomplete_lifecycle_lock_if_matches(
    path: &Path,
    expected: &FileSnapshot,
) -> io::Result<bool> {
    if !expected.matches_path(path)? || !expected.is_stale() {
        return Ok(false);
    }

    match read_lifecycle_lock(path)? {
        Some(LifecycleLockContent::Complete(_)) => return Ok(false),
        Some(LifecycleLockContent::Incomplete { pid })
            if pid.map(is_process_running).unwrap_or(false) =>
        {
            return Ok(false);
        }
        Some(LifecycleLockContent::Incomplete { .. }) | None => {}
    }

    if !expected.matches_path(path)? {
        return Ok(false);
    }

    remove_file_if_exists(path)?;
    Ok(true)
}

fn read_lifecycle_lock(path: &Path) -> io::Result<Option<LifecycleLockContent>> {
    let content = match fs::read_to_string(path) {
        Ok(content) => content,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };

    Ok(Some(parse_lifecycle_lock_content(&content)))
}

fn parse_lifecycle_lock_content(input: &str) -> LifecycleLockContent {
    let mut pid = None;
    let mut token = None;
    let mut started_at_unix = None;

    for line in input.lines().filter(|line| !line.trim().is_empty()) {
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };

        match key {
            "pid" => {
                if let Ok(value) = value.parse::<u32>() {
                    pid = Some(value);
                }
            }
            "token" => token = Some(value.to_owned()),
            "started_at_unix" => {
                if let Ok(value) = value.parse::<u64>() {
                    started_at_unix = Some(value);
                }
            }
            _ => {}
        }
    }

    match (pid, token, started_at_unix) {
        (Some(pid), Some(token), Some(started_at_unix)) if !token.is_empty() => {
            LifecycleLockContent::Complete(LifecycleLockRecord {
                pid,
                token,
                _started_at_unix: started_at_unix,
            })
        }
        (pid, _, _) => LifecycleLockContent::Incomplete { pid },
    }
}

#[cfg(test)]
pub(super) fn read_lifecycle_lock_token(path: &Path) -> io::Result<Option<String>> {
    Ok(match read_lifecycle_lock(path)? {
        Some(LifecycleLockContent::Complete(record)) => Some(record.token),
        Some(LifecycleLockContent::Incomplete { .. }) | None => None,
    })
}

fn remove_lifecycle_lock_if_matches(path: &Path, token: &str) -> io::Result<bool> {
    let should_remove = matches!(
        read_lifecycle_lock(path)?,
        Some(LifecycleLockContent::Complete(record)) if record.token == token
    );

    if should_remove {
        remove_file_if_exists(path)?;
    }

    Ok(should_remove)
}
