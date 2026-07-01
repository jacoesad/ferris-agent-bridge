use std::{
    env,
    ffi::OsString,
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
    process::{self, Child, Command, Stdio},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const NAME: &str = env!("CARGO_PKG_NAME");
const HOME_ENV: &str = "FERRIS_AGENT_BRIDGE_HOME";
const LOCK_FILE: &str = "daemon.lock";
const STATE_FILE: &str = "daemon.state";
const STOP_FILE: &str = "daemon.stop";
const LOG_FILE: &str = "daemon.log";
const LIFECYCLE_LOCK_FILE: &str = "daemon.lifecycle.lock";
const START_TIMEOUT: Duration = Duration::from_secs(2);
const STARTING_LOCK_TTL: Duration = Duration::from_secs(15);
const STOP_TIMEOUT: Duration = Duration::from_secs(5);
const START_FAILURE_STOP_TIMEOUT: Duration = Duration::from_millis(500);
const LIFECYCLE_LOCK_TIMEOUT: Duration = Duration::from_secs(15);
const LIFECYCLE_LOCK_STALE_TTL: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(100);
const LIFECYCLE_LOCK_POLL_INTERVAL: Duration = Duration::from_millis(20);

#[derive(Debug, Clone)]
pub struct DaemonPaths {
    pub runtime_dir: PathBuf,
    lock_file: PathBuf,
    state_file: PathBuf,
    stop_file: PathBuf,
    log_file: PathBuf,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DaemonStatus {
    Running(DaemonRecord),
    RunningUnverified {
        record: DaemonRecord,
        reason: String,
    },
    Stopped,
    Stale {
        pid: Option<u32>,
        record: Option<DaemonRecord>,
        reason: String,
    },
    Unowned {
        pid: u32,
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonRecord {
    pid: u32,
    token: String,
    exe: String,
    started_at_unix: u64,
    mode: String,
}

impl DaemonRecord {
    fn new(pid: u32, token: String, exe: String, mode: &str) -> Self {
        Self {
            pid,
            token,
            exe,
            started_at_unix: now_unix_seconds(),
            mode: mode.to_owned(),
        }
    }

    fn encode(&self) -> String {
        format!(
            "pid={}\ntoken={}\nexe={}\nstarted_at_unix={}\nmode={}\n",
            self.pid, self.token, self.exe, self.started_at_unix, self.mode
        )
    }

    fn decode(input: &str) -> Result<Self, String> {
        let mut pid = None;
        let mut token = None;
        let mut exe = None;
        let mut started_at_unix = None;
        let mut mode = None;

        for line in input.lines().filter(|line| !line.trim().is_empty()) {
            let (key, value) = line
                .split_once('=')
                .ok_or_else(|| format!("invalid state line: {line}"))?;

            match key {
                "pid" => {
                    pid = Some(
                        value
                            .parse::<u32>()
                            .map_err(|err| format!("invalid pid `{value}`: {err}"))?,
                    );
                }
                "token" => token = Some(value.to_owned()),
                "exe" => exe = Some(value.to_owned()),
                "started_at_unix" => {
                    started_at_unix = Some(
                        value
                            .parse::<u64>()
                            .map_err(|err| format!("invalid started_at_unix `{value}`: {err}"))?,
                    );
                }
                "mode" => mode = Some(value.to_owned()),
                _ => {}
            }
        }

        Ok(Self {
            pid: pid.ok_or_else(|| "missing pid".to_owned())?,
            token: token.ok_or_else(|| "missing token".to_owned())?,
            exe: exe.ok_or_else(|| "missing exe".to_owned())?,
            started_at_unix: started_at_unix.ok_or_else(|| "missing started_at_unix".to_owned())?,
            mode: mode.ok_or_else(|| "missing mode".to_owned())?,
        })
    }
}

pub fn start_background(paths: &DaemonPaths) -> Result<String, String> {
    let (token, lifecycle_lock) = match prepare_start(paths)? {
        StartPreparation::AlreadyRunning(record) => {
            return Ok(already_running_text(paths, &record));
        }
        StartPreparation::Ready {
            token,
            lifecycle_lock,
        } => (token, lifecycle_lock),
    };
    let mut startup_cleanup = StartCleanupGuard::new(paths, &token);

    let exe =
        env::current_exe().map_err(|err| format!("failed to locate current executable: {err}"))?;
    let log = open_log_file(paths)?;
    let log_for_stderr = log
        .try_clone()
        .map_err(|err| format!("failed to prepare daemon log: {err}"))?;

    let mut command = Command::new(&exe);
    command
        .arg("__daemon")
        .arg("--token")
        .arg(&token)
        .arg("--starter-pid")
        .arg(process::id().to_string())
        .env(HOME_ENV, &paths.runtime_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log_for_stderr));
    detach_background_command(&mut command);

    let mut child = command
        .spawn()
        .map_err(|err| format!("failed to start daemon: {err}"))?;
    startup_cleanup.disarm();
    drop(startup_cleanup);
    drop(lifecycle_lock);

    wait_for_start_ready(paths, &mut child, &token)?;

    Ok(format!(
        "{NAME} daemon started.\npid: {}\nruntime dir: {}\nlog: {}",
        child.id(),
        paths.runtime_dir.display(),
        paths.log_file.display()
    ))
}

pub fn run_foreground(paths: &DaemonPaths) -> Result<String, String> {
    let (token, lifecycle_lock) = match prepare_start(paths)? {
        StartPreparation::AlreadyRunning(record) => {
            return Ok(already_running_text(paths, &record));
        }
        StartPreparation::Ready {
            token,
            lifecycle_lock,
        } => (token, lifecycle_lock),
    };
    let mut startup_cleanup = StartCleanupGuard::new(paths, &token);

    let exe =
        env::current_exe().map_err(|err| format!("failed to locate current executable: {err}"))?;
    let mut command = Command::new(&exe);
    command
        .arg("__daemon")
        .arg("--token")
        .arg(&token)
        .arg("--starter-pid")
        .arg(process::id().to_string())
        .arg("--foreground")
        .env(HOME_ENV, &paths.runtime_dir)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());

    println!(
        "{NAME} daemon running in foreground.\nruntime dir: {}\nstop it with `{NAME} stop` from another shell.",
        paths.runtime_dir.display()
    );

    let mut child = command
        .spawn()
        .map_err(|err| format!("failed to run foreground daemon: {err}"))?;
    startup_cleanup.disarm();
    drop(startup_cleanup);
    drop(lifecycle_lock);

    wait_for_start_ready(paths, &mut child, &token)?;

    let status = child
        .wait()
        .map_err(|err| format!("failed to wait for foreground daemon: {err}"))?;

    if !status.success() {
        cleanup_runtime_files_for_token(paths, &token);
        return Err(format!("foreground daemon exited with status: {status}"));
    }

    Ok(format!("{NAME} daemon stopped."))
}

enum StartPreparation {
    AlreadyRunning(DaemonRecord),
    Ready {
        token: String,
        lifecycle_lock: LifecycleLockGuard,
    },
}

struct StartCleanupGuard<'a> {
    paths: &'a DaemonPaths,
    token: &'a str,
    armed: bool,
}

impl<'a> StartCleanupGuard<'a> {
    fn new(paths: &'a DaemonPaths, token: &'a str) -> Self {
        Self {
            paths,
            token,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for StartCleanupGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            cleanup_runtime_files_for_token_unlocked(self.paths, self.token);
        }
    }
}

fn prepare_start(paths: &DaemonPaths) -> Result<StartPreparation, String> {
    prepare_runtime_dir(paths)?;
    let lifecycle_lock = acquire_lifecycle_lock(paths)
        .map_err(|err| format!("failed to acquire daemon lifecycle lock: {err}"))?;

    match inspect_status(paths) {
        DaemonStatus::Running(record) | DaemonStatus::RunningUnverified { record, .. } => {
            return Ok(StartPreparation::AlreadyRunning(record));
        }
        DaemonStatus::Unowned { pid, reason } => {
            return Err(format!(
                "refusing to start: recorded pid {pid} is not owned by this daemon ({reason})"
            ));
        }
        DaemonStatus::Stale { record, .. } => cleanup_stale_files_unlocked(paths, record.as_ref()),
        DaemonStatus::Stopped => {}
    }

    let token = generate_token();
    acquire_start_lock(paths, token.clone())?;

    Ok(StartPreparation::Ready {
        token,
        lifecycle_lock,
    })
}

fn already_running_text(paths: &DaemonPaths, record: &DaemonRecord) -> String {
    format!(
        "{NAME} daemon is already running.\npid: {}\nruntime dir: {}",
        record.pid,
        paths.runtime_dir.display()
    )
}

pub fn status_text(paths: &DaemonPaths) -> String {
    match inspect_status(paths) {
        DaemonStatus::Running(record) => format!(
            "{NAME} daemon is running.\npid: {}\nstarted_at_unix: {}\nruntime dir: {}\nlog: {}",
            record.pid,
            record.started_at_unix,
            paths.runtime_dir.display(),
            paths.log_file.display()
        ),
        DaemonStatus::RunningUnverified { record, reason } => format!(
            "{NAME} daemon is running.\nidentity: unverified ({reason})\npid: {}\nstarted_at_unix: {}\nruntime dir: {}\nlog: {}",
            record.pid,
            record.started_at_unix,
            paths.runtime_dir.display(),
            paths.log_file.display()
        ),
        DaemonStatus::Stopped => format!(
            "{NAME} daemon is stopped.\nruntime dir: {}",
            paths.runtime_dir.display()
        ),
        DaemonStatus::Stale { pid, reason, .. } => {
            let pid_text = pid
                .map(|pid| pid.to_string())
                .unwrap_or_else(|| "unknown".to_owned());
            format!(
                "{NAME} daemon is stopped.\nreason: stale state detected for pid {pid_text}: {reason}\nruntime dir: {}",
                paths.runtime_dir.display()
            )
        }
        DaemonStatus::Unowned { pid, reason } => format!(
            "{NAME} daemon status is unsafe to trust.\nreason: recorded pid {pid} is not owned by this daemon ({reason})\nruntime dir: {}",
            paths.runtime_dir.display()
        ),
    }
}

pub fn stop(paths: &DaemonPaths) -> Result<String, String> {
    if !paths.runtime_dir.exists() {
        return Ok(format!("{NAME} daemon is not running."));
    }

    let _lifecycle_lock = acquire_lifecycle_lock(paths)
        .map_err(|err| format!("failed to acquire daemon lifecycle lock: {err}"))?;

    match inspect_status(paths) {
        DaemonStatus::Running(record) => {
            let stopped = request_graceful_stop(paths, &record)?;

            if !stopped {
                force_stop_verified(paths, &record)?;
            }

            cleanup_runtime_files_for_record_unlocked(paths, &record);

            Ok(format!("{NAME} daemon stopped.\npid: {}", record.pid))
        }
        DaemonStatus::RunningUnverified { record, reason } => {
            let stopped = request_graceful_stop(paths, &record)?;

            if !stopped {
                return Err(format!(
                    "refusing to force stop unverified daemon pid {}: {reason}",
                    record.pid
                ));
            }

            cleanup_runtime_files_for_record_unlocked(paths, &record);

            Ok(format!("{NAME} daemon stopped.\npid: {}", record.pid))
        }
        DaemonStatus::Stopped => Ok(format!("{NAME} daemon is not running.")),
        DaemonStatus::Stale { record, .. } => {
            cleanup_stale_files_unlocked(paths, record.as_ref());
            Ok(format!(
                "{NAME} daemon is not running; stale state removed."
            ))
        }
        DaemonStatus::Unowned { pid, reason } => Err(format!(
            "refusing to stop pid {pid}: recorded process is not owned by this daemon ({reason})"
        )),
    }
}

pub fn run_daemon_from_env(
    token: String,
    starter_pid: u32,
    foreground: bool,
) -> Result<String, String> {
    let paths = DaemonPaths::from_env()?;
    let mode = if foreground {
        "foreground"
    } else {
        "background"
    };
    run_daemon(&paths, token, starter_pid, mode)?;
    Ok(format!("{NAME} daemon stopped."))
}

fn run_daemon(
    paths: &DaemonPaths,
    token: String,
    starter_pid: u32,
    mode: &str,
) -> Result<(), String> {
    prepare_runtime_dir(paths)?;
    let lifecycle_lock = acquire_lifecycle_lock(paths)
        .map_err(|err| format!("failed to acquire daemon lifecycle lock: {err}"))?;
    validate_startup_lock(paths, &token, starter_pid)?;
    let exe = current_exe_string();
    let record = DaemonRecord::new(process::id(), token.clone(), exe, mode);

    write_record(&paths.lock_file, &record)?;
    write_record(&paths.state_file, &record)?;
    drop(lifecycle_lock);
    append_log(paths, "daemon started")?;

    loop {
        if should_stop(paths, &token) {
            append_log(paths, "daemon stop requested")?;
            return Ok(());
        }

        thread::sleep(POLL_INTERVAL);
    }
}

fn inspect_status(paths: &DaemonPaths) -> DaemonStatus {
    let record = match read_record(&paths.state_file) {
        Ok(Some(record)) => record,
        Ok(None) => return DaemonStatus::Stopped,
        Err(reason) => {
            return DaemonStatus::Stale {
                pid: None,
                record: None,
                reason,
            };
        }
    };

    if !is_process_running(record.pid) {
        return DaemonStatus::Stale {
            pid: Some(record.pid),
            record: Some(record),
            reason: "recorded process is not running".to_owned(),
        };
    }

    match inspect_process_identity(&record) {
        ProcessIdentity::Verified => DaemonStatus::Running(record),
        ProcessIdentity::Unverified(reason) => DaemonStatus::RunningUnverified { record, reason },
        ProcessIdentity::Mismatch => DaemonStatus::Unowned {
            pid: record.pid,
            reason: "process command line does not match daemon token or mode".to_owned(),
        },
    }
}

fn wait_for_start_ready(paths: &DaemonPaths, child: &mut Child, token: &str) -> Result<(), String> {
    if let Err(err) = wait_for_running(paths, child.id(), token) {
        cleanup_failed_start(paths, child, token);
        return Err(err);
    }

    Ok(())
}

fn wait_for_running(paths: &DaemonPaths, pid: u32, token: &str) -> Result<(), String> {
    let started = Instant::now();

    while started.elapsed() < START_TIMEOUT {
        if !is_process_running(pid) {
            return Err(format!("daemon process {pid} exited before becoming ready"));
        }

        if ready_record_matches(&paths.lock_file, pid, token)?
            && ready_record_matches(&paths.state_file, pid, token)?
        {
            return Ok(());
        }

        thread::sleep(POLL_INTERVAL);
    }

    Err("daemon did not become ready before the start timeout".to_owned())
}

fn ready_record_matches(path: &Path, pid: u32, token: &str) -> Result<bool, String> {
    match read_record(path) {
        Ok(Some(record)) => {
            Ok(record.pid == pid && record.token == token && record.mode != "starting")
        }
        Ok(None) => Ok(false),
        Err(reason) => Err(format!(
            "daemon wrote invalid ready record {}: {reason}",
            path.display()
        )),
    }
}

fn validate_startup_lock(paths: &DaemonPaths, token: &str, starter_pid: u32) -> Result<(), String> {
    let record = read_record(&paths.lock_file)?
        .ok_or_else(|| "refusing to run daemon: startup lock is missing".to_owned())?;

    if record.mode != "starting" {
        return Err(format!(
            "refusing to run daemon: startup lock mode is `{}` instead of `starting`",
            record.mode
        ));
    }

    if record.token != token {
        return Err("refusing to run daemon: startup lock token does not match".to_owned());
    }

    if record.pid != starter_pid {
        return Err(format!(
            "refusing to run daemon: startup lock pid {} does not match starter pid {starter_pid}",
            record.pid
        ));
    }

    if !is_process_running(starter_pid) {
        return Err(format!(
            "refusing to run daemon: starter process {starter_pid} is not running"
        ));
    }

    Ok(())
}

fn request_graceful_stop(paths: &DaemonPaths, record: &DaemonRecord) -> Result<bool, String> {
    write_private_file(&paths.stop_file, &record.token)
        .map_err(|err| format!("failed to write stop request: {err}"))?;

    Ok(wait_for_stop(paths, record.pid))
}

fn force_stop_verified(paths: &DaemonPaths, record: &DaemonRecord) -> Result<(), String> {
    match read_record(&paths.state_file) {
        Ok(Some(current)) if record_identity_matches(&current, record) => {}
        Ok(Some(_)) => {
            return Err(
                "refusing to force stop daemon: daemon state changed while stopping".to_owned(),
            );
        }
        Ok(None) => return Ok(()),
        Err(reason) => {
            return Err(format!(
                "refusing to force stop daemon: failed to re-read daemon state: {reason}"
            ));
        }
    }

    if !is_process_running(record.pid) {
        return Ok(());
    }

    match inspect_process_identity(record) {
        ProcessIdentity::Verified => {
            terminate_process(record.pid)?;
            wait_for_process_exit(record.pid, STOP_TIMEOUT)
        }
        ProcessIdentity::Unverified(reason) => Err(format!(
            "refusing to force stop unverified daemon pid {}: {reason}",
            record.pid
        )),
        ProcessIdentity::Mismatch => Err(format!(
            "refusing to force stop pid {}: process identity changed while stopping",
            record.pid
        )),
    }
}

fn wait_for_stop(paths: &DaemonPaths, pid: u32) -> bool {
    let started = Instant::now();

    while started.elapsed() < STOP_TIMEOUT {
        if !is_process_running(pid) || !paths.state_file.exists() {
            return true;
        }

        thread::sleep(POLL_INTERVAL);
    }

    false
}

fn wait_for_process_exit(pid: u32, timeout: Duration) -> Result<(), String> {
    let started = Instant::now();

    while started.elapsed() < timeout {
        if !is_process_running(pid) {
            return Ok(());
        }

        thread::sleep(POLL_INTERVAL);
    }

    Err(format!("process {pid} did not exit before timeout"))
}

fn cleanup_failed_start(paths: &DaemonPaths, child: &mut Child, token: &str) {
    let lifecycle_lock = acquire_lifecycle_lock(paths).ok();
    let _ = write_private_file(&paths.stop_file, token);

    if !wait_for_child_exit(child, START_FAILURE_STOP_TIMEOUT) {
        let _ = child.kill();
        let _ = child.wait();
    }

    if lifecycle_lock.is_some() {
        cleanup_runtime_files_for_token_unlocked(paths, token);
    } else {
        cleanup_runtime_files_for_token(paths, token);
    }
}

fn wait_for_child_exit(child: &mut Child, timeout: Duration) -> bool {
    let started = Instant::now();

    while started.elapsed() < timeout {
        match child.try_wait() {
            Ok(Some(_)) => return true,
            Ok(None) => thread::sleep(POLL_INTERVAL),
            Err(_) => return true,
        }
    }

    false
}

fn acquire_start_lock(paths: &DaemonPaths, token: String) -> Result<(), String> {
    let record = DaemonRecord::new(process::id(), token, current_exe_string(), "starting");

    loop {
        match write_new_record(&paths.lock_file, &record) {
            Ok(()) => return Ok(()),
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                match inspect_start_lock(paths) {
                    StartLockStatus::Live => {
                        return Err(format!(
                            "daemon start is already in progress or daemon lock is active: {}",
                            paths.lock_file.display()
                        ));
                    }
                    StartLockStatus::Unsafe(reason) => {
                        return Err(format!("daemon lock state is unsafe to replace: {reason}"));
                    }
                    StartLockStatus::Stale(record) => {
                        if let Some(record) = record {
                            remove_record_file_if_matches(&paths.lock_file, &record).map_err(
                                |err| {
                                    format!(
                                        "failed to remove stale daemon lock {}: {err}",
                                        paths.lock_file.display()
                                    )
                                },
                            )?;
                        }
                    }
                }
            }
            Err(err) => return Err(format!("failed to acquire daemon lock: {err}")),
        }
    }
}

enum StartLockStatus {
    Live,
    Stale(Option<DaemonRecord>),
    Unsafe(String),
}

fn inspect_start_lock(paths: &DaemonPaths) -> StartLockStatus {
    let record = match read_record(&paths.lock_file) {
        Ok(Some(record)) => record,
        Ok(None) => return StartLockStatus::Stale(None),
        Err(reason) => return StartLockStatus::Unsafe(reason),
    };

    if record.mode == "starting" {
        let is_fresh =
            Duration::from_secs(now_unix_seconds().saturating_sub(record.started_at_unix))
                <= STARTING_LOCK_TTL;

        if is_fresh || is_process_running(record.pid) {
            return StartLockStatus::Live;
        }

        return StartLockStatus::Stale(Some(record));
    }

    if !is_process_running(record.pid) {
        return StartLockStatus::Stale(Some(record));
    }

    match inspect_process_identity(&record) {
        ProcessIdentity::Verified | ProcessIdentity::Unverified(_) => StartLockStatus::Live,
        ProcessIdentity::Mismatch => StartLockStatus::Unsafe(
            "lock holder process is running but ownership does not match".to_owned(),
        ),
    }
}

fn should_stop(paths: &DaemonPaths, token: &str) -> bool {
    match fs::read_to_string(&paths.stop_file) {
        Ok(content) => content.trim() == token,
        Err(_) => false,
    }
}

fn prepare_runtime_dir(paths: &DaemonPaths) -> Result<(), String> {
    create_private_dir(&paths.runtime_dir).map_err(|err| {
        format!(
            "failed to create runtime directory {}: {err}",
            paths.runtime_dir.display()
        )
    })?;
    set_private_dir_permissions(&paths.runtime_dir).map_err(|err| {
        format!(
            "failed to set runtime directory permissions {}: {err}",
            paths.runtime_dir.display()
        )
    })
}

fn cleanup_stale_files_unlocked(paths: &DaemonPaths, record: Option<&DaemonRecord>) {
    if let Some(record) = record {
        cleanup_runtime_files_for_record_unlocked(paths, record);
    } else {
        cleanup_invalid_state_files_unlocked(paths);
    }
}

#[cfg(test)]
fn cleanup_runtime_files_for_record(paths: &DaemonPaths, record: &DaemonRecord) {
    let Ok(_guard) = acquire_lifecycle_lock(paths) else {
        return;
    };

    cleanup_runtime_files_for_record_unlocked(paths, record);
}

fn cleanup_runtime_files_for_record_unlocked(paths: &DaemonPaths, record: &DaemonRecord) {
    let _ = remove_record_file_if_matches_unlocked(&paths.lock_file, record);
    cleanup_state_files_for_record_unlocked(paths, record);
}

fn cleanup_runtime_files_for_token(paths: &DaemonPaths, token: &str) {
    let Ok(_guard) = acquire_lifecycle_lock(paths) else {
        return;
    };

    cleanup_runtime_files_for_token_unlocked(paths, token);
}

fn cleanup_runtime_files_for_token_unlocked(paths: &DaemonPaths, token: &str) {
    remove_record_file_if_token_unlocked(&paths.lock_file, token);
    remove_record_file_if_token_unlocked(&paths.state_file, token);
    remove_stop_file_if_matches_unlocked(paths, token);
}

fn cleanup_invalid_state_files_unlocked(paths: &DaemonPaths) {
    if read_record(&paths.state_file).is_err() {
        let _ = remove_file_if_exists(&paths.state_file);
        let _ = remove_file_if_exists(&paths.stop_file);
    }
}

fn remove_record_file_if_matches(path: &Path, expected: &DaemonRecord) -> io::Result<bool> {
    remove_record_file_if_matches_unlocked(path, expected)
}

fn cleanup_state_files_for_record_unlocked(paths: &DaemonPaths, record: &DaemonRecord) {
    let _ = remove_record_file_if_matches_unlocked(&paths.state_file, record);
    remove_stop_file_if_matches_unlocked(paths, &record.token);
}

fn remove_record_file_if_matches_unlocked(
    path: &Path,
    expected: &DaemonRecord,
) -> io::Result<bool> {
    let should_remove = matches!(
        read_record(path),
        Ok(Some(record)) if record_identity_matches(&record, expected)
    );

    if should_remove {
        remove_file_if_exists(path)?;
    }

    Ok(should_remove)
}

fn remove_record_file_if_token_unlocked(path: &Path, token: &str) {
    let should_remove = matches!(read_record(path), Ok(Some(record)) if record.token == token);

    if should_remove {
        let _ = remove_file_if_exists(path);
    }
}

fn record_identity_matches(left: &DaemonRecord, right: &DaemonRecord) -> bool {
    left.pid == right.pid
        && left.token == right.token
        && left.started_at_unix == right.started_at_unix
        && left.mode == right.mode
}

fn remove_stop_file_if_matches_unlocked(paths: &DaemonPaths, token: &str) {
    let should_remove = fs::read_to_string(&paths.stop_file)
        .map(|content| content.trim() == token)
        .unwrap_or(false);

    if should_remove {
        let _ = remove_file_if_exists(&paths.stop_file);
    }
}

fn remove_file_if_exists(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

struct LifecycleLockGuard {
    lock_file: PathBuf,
    token: String,
}

impl Drop for LifecycleLockGuard {
    fn drop(&mut self) {
        let _ = remove_lifecycle_lock_if_matches(&self.lock_file, &self.token);
    }
}

fn acquire_lifecycle_lock(paths: &DaemonPaths) -> io::Result<LifecycleLockGuard> {
    let lock_file = paths.runtime_dir.join(LIFECYCLE_LOCK_FILE);
    let started = Instant::now();

    loop {
        match create_lifecycle_lock(&lock_file) {
            Ok(guard) => return Ok(guard),
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                if let Some(token) = stale_lifecycle_lock_token(&lock_file) {
                    let _ = remove_lifecycle_lock_if_matches(&lock_file, &token);
                    continue;
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

fn create_lifecycle_lock(path: &Path) -> io::Result<LifecycleLockGuard> {
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
        cleanup_incomplete_lifecycle_lock(path);
        return Err(err);
    }

    Ok(LifecycleLockGuard {
        lock_file: path.to_owned(),
        token,
    })
}

fn cleanup_incomplete_lifecycle_lock(path: &Path) {
    let _ = remove_file_if_exists(path);
}

fn lifecycle_lock_contents(token: &str) -> String {
    format!(
        "pid={}\ntoken={}\nstarted_at_unix={}\n",
        process::id(),
        token,
        now_unix_seconds()
    )
}

fn stale_lifecycle_lock_token(path: &Path) -> Option<String> {
    let is_stale = fs::metadata(path)
        .and_then(|metadata| metadata.modified())
        .ok()
        .and_then(|modified| modified.elapsed().ok())
        .map(|elapsed| elapsed > LIFECYCLE_LOCK_STALE_TTL)
        .unwrap_or(false);

    if !is_stale {
        return None;
    }

    read_lifecycle_lock_token(path).ok().flatten()
}

fn read_lifecycle_lock_token(path: &Path) -> io::Result<Option<String>> {
    let content = match fs::read_to_string(path) {
        Ok(content) => content,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };

    Ok(content.lines().find_map(|line| {
        line.strip_prefix("token=")
            .filter(|token| !token.is_empty())
            .map(str::to_owned)
    }))
}

fn remove_lifecycle_lock_if_matches(path: &Path, token: &str) -> io::Result<bool> {
    let should_remove = read_lifecycle_lock_token(path)?
        .map(|current| current == token)
        .unwrap_or(false);

    if should_remove {
        remove_file_if_exists(path)?;
    }

    Ok(should_remove)
}

fn open_log_file(paths: &DaemonPaths) -> Result<fs::File, String> {
    open_private_append_file(&paths.log_file).map_err(|err| {
        format!(
            "failed to open daemon log {}: {err}",
            paths.log_file.display()
        )
    })
}

fn append_log(paths: &DaemonPaths, message: &str) -> Result<(), String> {
    let line = format!("{} {message}\n", now_unix_seconds());
    open_private_append_file(&paths.log_file)
        .and_then(|mut file| file.write_all(line.as_bytes()))
        .map_err(|err| format!("failed to write daemon log: {err}"))
}

fn read_record(path: &Path) -> Result<Option<DaemonRecord>, String> {
    match fs::read_to_string(path) {
        Ok(content) => DaemonRecord::decode(&content).map(Some),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(format!("failed to read {}: {err}", path.display())),
    }
}

fn write_record(path: &Path, record: &DaemonRecord) -> Result<(), String> {
    let tmp = tmp_record_path(path);
    write_private_file(&tmp, &record.encode())
        .map_err(|err| format!("failed to write {}: {err}", tmp.display()))?;
    replace_file(&tmp, path).map_err(|err| {
        let _ = fs::remove_file(&tmp);
        format!("failed to replace {}: {err}", path.display())
    })
}

fn write_new_record(path: &Path, record: &DaemonRecord) -> io::Result<()> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    configure_private_file_options(&mut options);
    let mut file = options.open(path)?;

    file.write_all(record.encode().as_bytes())?;
    set_private_file_permissions(path)
}

fn tmp_record_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("record");

    path.with_file_name(format!(
        ".{file_name}.tmp-{}-{}",
        process::id(),
        now_unix_nanos()
    ))
}

fn replace_file(src: &Path, dst: &Path) -> io::Result<()> {
    match fs::rename(src, dst) {
        Ok(()) => set_private_file_permissions(dst),
        Err(err) if cfg!(windows) && err.kind() == io::ErrorKind::AlreadyExists => {
            fs::remove_file(dst)?;
            fs::rename(src, dst)?;
            set_private_file_permissions(dst)
        }
        Err(err) => Err(err),
    }
}

fn write_private_file(path: &Path, contents: &str) -> io::Result<()> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    configure_private_file_options(&mut options);
    let mut file = options.open(path)?;
    file.write_all(contents.as_bytes())?;
    file.sync_all()?;
    set_private_file_permissions(path)
}

fn open_private_append_file(path: &Path) -> io::Result<fs::File> {
    let mut options = fs::OpenOptions::new();
    options.create(true).append(true);
    configure_private_file_options(&mut options);
    let file = options.open(path)?;
    set_private_file_permissions(path)?;
    Ok(file)
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

#[cfg(unix)]
fn set_private_dir_permissions(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}

#[cfg(not(unix))]
fn set_private_dir_permissions(_path: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn configure_private_file_options(options: &mut fs::OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;

    options.mode(0o600);
}

#[cfg(not(unix))]
fn configure_private_file_options(_options: &mut fs::OpenOptions) {}

#[cfg(unix)]
fn set_private_file_permissions(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn set_private_file_permissions(_path: &Path) -> io::Result<()> {
    Ok(())
}

enum ProcessIdentity {
    Verified,
    Unverified(String),
    Mismatch,
}

fn inspect_process_identity(record: &DaemonRecord) -> ProcessIdentity {
    let Some(command_line) = process_command_line(record.pid) else {
        return ProcessIdentity::Unverified("process command line is unavailable".to_owned());
    };

    if command_line.is_empty() {
        return ProcessIdentity::Unverified("process command line is empty".to_owned());
    }

    if record.mode == "foreground" {
        if command_line.contains("__daemon")
            && command_line.contains(&record.token)
            && command_line.contains("--foreground")
        {
            return ProcessIdentity::Verified;
        }

        return ProcessIdentity::Mismatch;
    }

    if command_line.contains("__daemon") && command_line.contains(&record.token) {
        ProcessIdentity::Verified
    } else {
        ProcessIdentity::Mismatch
    }
}

#[cfg(test)]
fn record_matches_process(record: &DaemonRecord) -> bool {
    matches!(inspect_process_identity(record), ProcessIdentity::Verified)
}

fn current_exe_string() -> String {
    env::current_exe()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|_| NAME.to_owned())
}

fn generate_token() -> String {
    format!("{}-{}", process::id(), now_unix_nanos())
}

fn now_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn now_unix_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0)
}

#[cfg(unix)]
unsafe extern "C" {
    fn setsid() -> i32;
}

#[cfg(unix)]
fn detach_background_command(command: &mut Command) {
    use std::os::unix::process::CommandExt;

    // SAFETY: pre_exec runs in the child after fork and before exec. The closure
    // only calls async-signal-safe setsid and reads errno via last_os_error on
    // failure, then returns to exec or aborts spawn with that error.
    unsafe {
        command.pre_exec(|| {
            if setsid() == -1 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        });
    }
}

#[cfg(windows)]
fn detach_background_command(command: &mut Command) {
    use std::os::windows::process::CommandExt;

    const DETACHED_PROCESS: u32 = 0x0000_0008;
    const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
    command.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP);
}

#[cfg(not(any(unix, windows)))]
fn detach_background_command(_command: &mut Command) {}

#[cfg(unix)]
fn is_process_running(pid: u32) -> bool {
    Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

#[cfg(windows)]
fn is_process_running(pid: u32) -> bool {
    let filter = format!("PID eq {pid}");
    let output = Command::new("tasklist")
        .args(["/FI", filter.as_str(), "/FO", "CSV", "/NH"])
        .output();

    output
        .map(|output| String::from_utf8_lossy(&output.stdout).contains(&pid.to_string()))
        .unwrap_or(false)
}

#[cfg(unix)]
fn terminate_process(pid: u32) -> Result<(), String> {
    let status = Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .map_err(|err| format!("failed to send stop signal to pid {pid}: {err}"))?;

    if status.success() {
        Ok(())
    } else {
        Err(format!("failed to send stop signal to pid {pid}"))
    }
}

#[cfg(windows)]
fn terminate_process(pid: u32) -> Result<(), String> {
    let status = Command::new("taskkill")
        .args(["/PID", &pid.to_string(), "/T"])
        .status()
        .map_err(|err| format!("failed to terminate pid {pid}: {err}"))?;

    if status.success() {
        Ok(())
    } else {
        Err(format!("failed to terminate pid {pid}"))
    }
}

#[cfg(unix)]
fn process_command_line(pid: u32) -> Option<String> {
    let pid = pid.to_string();
    let output = Command::new("ps")
        .args(["-p", pid.as_str(), "-o", "command="])
        .output()
        .ok()?;

    if output.status.success() {
        Some(String::from_utf8_lossy(&output.stdout).trim().to_owned())
    } else {
        None
    }
}

#[cfg(windows)]
fn process_command_line(pid: u32) -> Option<String> {
    let script =
        format!("(Get-CimInstance Win32_Process -Filter \"ProcessId = {pid}\").CommandLine");
    let output = Command::new("powershell")
        .args(["-NoProfile", "-Command", script.as_str()])
        .output()
        .ok()?;

    if output.status.success() {
        Some(String::from_utf8_lossy(&output.stdout).trim().to_owned())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_roundtrips() {
        let record = DaemonRecord::new(42, "token".to_owned(), "/tmp/fab".to_owned(), "background");

        let decoded = DaemonRecord::decode(&record.encode()).expect("record should decode");

        assert_eq!(decoded, record);
    }

    #[test]
    fn status_is_stopped_without_state() {
        let dir = temp_test_dir("status-stopped");
        let paths = DaemonPaths::new(&dir);

        assert_eq!(inspect_status(&paths), DaemonStatus::Stopped);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn status_reports_stale_dead_pid() {
        let dir = temp_test_dir("status-stale");
        let paths = DaemonPaths::new(&dir);
        fs::create_dir_all(&dir).expect("test dir should be created");

        let record = DaemonRecord::new(
            u32::MAX,
            "token".to_owned(),
            "missing".to_owned(),
            "background",
        );
        write_record(&paths.state_file, &record).expect("state should be written");

        let status = inspect_status(&paths);
        assert!(matches!(
            status,
            DaemonStatus::Stale {
                pid: Some(u32::MAX),
                ..
            }
        ));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn stop_does_not_create_missing_runtime_dir() {
        let dir = temp_test_dir("stop-missing-runtime");
        let paths = DaemonPaths::new(&dir);

        assert!(!dir.exists());
        let output = stop(&paths).expect("stop should succeed without runtime dir");

        assert!(output.contains("daemon is not running"));
        assert!(!dir.exists());
    }

    #[test]
    fn invalid_lock_is_not_replaced() {
        let dir = temp_test_dir("invalid-lock");
        let paths = DaemonPaths::new(&dir);
        fs::create_dir_all(&dir).expect("test dir should be created");
        fs::write(&paths.lock_file, "pid=").expect("invalid lock should be written");

        let err = acquire_start_lock(&paths, "next-token".to_owned())
            .expect_err("invalid lock should block replacement");

        assert!(err.contains("unsafe"));
        assert_eq!(
            fs::read_to_string(&paths.lock_file).expect("lock should still exist"),
            "pid="
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn stale_lock_is_replaced() {
        let dir = temp_test_dir("stale-lock");
        let paths = DaemonPaths::new(&dir);
        fs::create_dir_all(&dir).expect("test dir should be created");
        let stale = DaemonRecord::new(
            u32::MAX,
            "stale-token".to_owned(),
            "missing".to_owned(),
            "background",
        );
        write_new_record(&paths.lock_file, &stale).expect("stale lock should be written");

        acquire_start_lock(&paths, "next-token".to_owned())
            .expect("stale lock should be replaceable");
        let next = read_record(&paths.lock_file)
            .expect("lock should read")
            .expect("lock should exist");

        assert_eq!(next.token, "next-token");
        assert_eq!(next.mode, "starting");

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn internal_daemon_requires_matching_startup_lock() {
        let dir = temp_test_dir("internal-start-lock");
        let paths = DaemonPaths::new(&dir);
        fs::create_dir_all(&dir).expect("test dir should be created");
        let active = DaemonRecord::new(
            u32::MAX,
            "active-token".to_owned(),
            "active-exe".to_owned(),
            "background",
        );
        write_record(&paths.lock_file, &active).expect("active lock should be written");
        write_record(&paths.state_file, &active).expect("active state should be written");

        let err = run_daemon(
            &paths,
            "rogue-token".to_owned(),
            process::id(),
            "background",
        )
        .expect_err("internal daemon should require a startup lock");

        assert!(err.contains("startup lock mode"));
        assert_eq!(
            read_record(&paths.lock_file)
                .expect("lock should read")
                .expect("lock should exist"),
            active
        );
        assert_eq!(
            read_record(&paths.state_file)
                .expect("state should read")
                .expect("state should exist"),
            active
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn startup_lock_requires_matching_starter_pid() {
        let dir = temp_test_dir("startup-lock-starter");
        let paths = DaemonPaths::new(&dir);
        fs::create_dir_all(&dir).expect("test dir should be created");
        let token = "token".to_owned();
        let starting = DaemonRecord::new(
            process::id(),
            token.clone(),
            "starter".to_owned(),
            "starting",
        );
        write_new_record(&paths.lock_file, &starting).expect("starting lock should be written");

        validate_startup_lock(&paths, &token, process::id())
            .expect("matching startup lock should be accepted");
        let err = validate_startup_lock(&paths, &token, process::id().saturating_add(1))
            .expect_err("wrong starter pid should be rejected");

        assert!(err.contains("starter pid"));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn start_cleanup_guard_removes_startup_files_before_spawn() {
        let dir = temp_test_dir("start-cleanup-guard");
        let paths = DaemonPaths::new(&dir);
        fs::create_dir_all(&dir).expect("test dir should be created");
        let token = "token".to_owned();
        let record = DaemonRecord::new(
            process::id(),
            token.clone(),
            current_exe_string(),
            "starting",
        );

        write_new_record(&paths.lock_file, &record).expect("lock should be written");
        write_record(&paths.state_file, &record).expect("state should be written");
        write_private_file(&paths.stop_file, &token).expect("stop should be written");

        {
            let _guard = StartCleanupGuard::new(&paths, &token);
        }

        assert!(!paths.lock_file.exists());
        assert!(!paths.state_file.exists());
        assert!(!paths.stop_file.exists());

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn lifecycle_lock_drop_does_not_remove_replaced_lock() {
        let dir = temp_test_dir("lifecycle-lock-drop");
        fs::create_dir_all(&dir).expect("test dir should be created");
        let lock_file = dir.join(LIFECYCLE_LOCK_FILE);

        let old_guard = create_lifecycle_lock(&lock_file).expect("old lock should be created");
        remove_file_if_exists(&lock_file).expect("stale lock should be removed");
        let new_guard = create_lifecycle_lock(&lock_file).expect("new lock should be created");
        let new_token = new_guard.token.clone();

        drop(old_guard);

        assert_eq!(
            read_lifecycle_lock_token(&lock_file).expect("lock token should read"),
            Some(new_token)
        );

        drop(new_guard);
        assert!(!lock_file.exists());

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn incomplete_lifecycle_lock_cleanup_removes_tokenless_file() {
        let dir = temp_test_dir("incomplete-lifecycle-lock");
        fs::create_dir_all(&dir).expect("test dir should be created");
        let lock_file = dir.join(LIFECYCLE_LOCK_FILE);
        fs::write(&lock_file, "pid=123\n").expect("incomplete lock should be written");

        cleanup_incomplete_lifecycle_lock(&lock_file);

        assert!(!lock_file.exists());

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn force_stop_refuses_when_state_changed() {
        let dir = temp_test_dir("force-stop-state-changed");
        let paths = DaemonPaths::new(&dir);
        fs::create_dir_all(&dir).expect("test dir should be created");
        let old = DaemonRecord::new(
            process::id(),
            "old-token".to_owned(),
            current_exe_string(),
            "background",
        );
        let new = DaemonRecord::new(
            process::id(),
            "new-token".to_owned(),
            current_exe_string(),
            "background",
        );
        write_record(&paths.state_file, &new).expect("new state should be written");

        let err =
            force_stop_verified(&paths, &old).expect_err("force stop should reject changed state");

        assert!(err.contains("state changed"));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn cleanup_for_old_record_does_not_remove_new_owner_files() {
        let dir = temp_test_dir("cleanup-owner");
        let paths = DaemonPaths::new(&dir);
        fs::create_dir_all(&dir).expect("test dir should be created");
        let old = DaemonRecord::new(
            u32::MAX - 1,
            "old-token".to_owned(),
            "old-exe".to_owned(),
            "background",
        );
        let new = DaemonRecord::new(
            u32::MAX,
            "new-token".to_owned(),
            "new-exe".to_owned(),
            "background",
        );

        write_record(&paths.lock_file, &new).expect("new lock should be written");
        write_record(&paths.state_file, &new).expect("new state should be written");
        write_private_file(&paths.stop_file, &new.token).expect("new stop file should be written");

        cleanup_runtime_files_for_record(&paths, &old);

        assert_eq!(
            read_record(&paths.lock_file)
                .expect("lock should read")
                .expect("lock should exist"),
            new
        );
        assert_eq!(
            read_record(&paths.state_file)
                .expect("state should read")
                .expect("state should exist"),
            new
        );
        assert_eq!(
            fs::read_to_string(&paths.stop_file).expect("stop should exist"),
            "new-token"
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn matching_exe_without_daemon_token_is_not_owned() {
        let record = DaemonRecord::new(
            process::id(),
            "missing-token".to_owned(),
            current_exe_string(),
            "background",
        );

        assert!(!record_matches_process(&record));
    }

    #[test]
    #[cfg(unix)]
    fn failed_start_cleanup_stops_child_and_removes_runtime_files() {
        let dir = temp_test_dir("failed-start-cleanup");
        let paths = DaemonPaths::new(&dir);
        fs::create_dir_all(&dir).expect("test dir should be created");
        let mut child = Command::new("sleep")
            .arg("10")
            .spawn()
            .expect("sleep should start");

        write_new_record(
            &paths.lock_file,
            &DaemonRecord::new(
                child.id(),
                "token".to_owned(),
                "sleep".to_owned(),
                "background",
            ),
        )
        .expect("lock should be written");
        write_record(
            &paths.state_file,
            &DaemonRecord::new(
                child.id(),
                "token".to_owned(),
                "sleep".to_owned(),
                "background",
            ),
        )
        .expect("state should be written");

        cleanup_failed_start(&paths, &mut child, "token");

        assert!(!is_process_running(child.id()));
        assert!(!paths.lock_file.exists());
        assert!(!paths.state_file.exists());
        assert!(!paths.stop_file.exists());

        let _ = fs::remove_dir_all(dir);
    }

    fn temp_test_dir(name: &str) -> PathBuf {
        env::temp_dir().join(format!(
            "{NAME}-{name}-{}-{}",
            process::id(),
            generate_token()
        ))
    }
}
