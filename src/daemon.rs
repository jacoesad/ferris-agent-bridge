use std::{
    env,
    path::Path,
    process,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

#[cfg(test)]
use std::{fs, path::PathBuf};

mod cleanup;
mod files;
mod lifecycle;
mod paths;
mod process_ops;
mod record;
mod startup;
mod status;

#[cfg(test)]
mod tests;

const NAME: &str = env!("CARGO_PKG_NAME");
const HOME_ENV: &str = "FERRIS_AGENT_BRIDGE_HOME";
const LOCK_FILE: &str = "daemon.lock";
const STATE_FILE: &str = "daemon.state";
const STOP_FILE: &str = "daemon.stop";
const LOG_FILE: &str = "daemon.log";
const LIFECYCLE_LOCK_FILE: &str = "daemon.lifecycle.lock";
const MODE_STARTING: &str = "starting";
const MODE_BACKGROUND: &str = "background";
const MODE_FOREGROUND: &str = "foreground";
const START_TIMEOUT: Duration = Duration::from_secs(2);
const STARTING_LOCK_TTL: Duration = Duration::from_secs(15);
const STOP_TIMEOUT: Duration = Duration::from_secs(5);
const START_FAILURE_STOP_TIMEOUT: Duration = Duration::from_millis(500);
// A normal stop can wait once for graceful exit and once after force-stop.
// The stale TTL stays above that budget, and the wait timeout stays above the
// stale TTL so the first waiter after a crash can recover the lock.
const LIFECYCLE_LOCK_STALE_TTL: Duration = Duration::from_secs(15);
const LIFECYCLE_LOCK_TIMEOUT: Duration = Duration::from_secs(20);
const POLL_INTERVAL: Duration = Duration::from_millis(100);
const LIFECYCLE_LOCK_POLL_INTERVAL: Duration = Duration::from_millis(20);

pub use paths::DaemonPaths;
pub use record::{DaemonRecord, DaemonStatus};

#[cfg(test)]
use cleanup::cleanup_runtime_files_for_record;
use cleanup::{
    cleanup_invalid_state_files_unlocked, cleanup_runtime_files_for_record_unlocked,
    cleanup_runtime_files_for_token, cleanup_runtime_files_for_token_unlocked,
    cleanup_stale_files_unlocked, prepare_runtime_dir, record_identity_matches, should_stop,
};
use files::{append_log, open_log_file, read_record, write_private_file, write_record};
#[cfg(test)]
use files::{remove_file_if_exists, write_new_record};
#[cfg(test)]
use lifecycle::{
    FileSnapshot, StaleLifecycleLock, cleanup_incomplete_lifecycle_lock_if_matches,
    create_lifecycle_lock, inspect_stale_lifecycle_lock, lifecycle_lock_contents,
    read_lifecycle_lock_token,
};
use lifecycle::{LifecycleLockGuard, acquire_lifecycle_lock};
use process_ops::{
    ProcessIdentity, current_exe_string, detach_background_command, generate_token,
    inspect_process_identity, is_process_running, terminate_process,
};
#[cfg(test)]
use process_ops::{kill_zero_stderr_indicates_live_process, record_matches_process};
use startup::{acquire_start_lock, validate_startup_lock};
use status::inspect_status;
#[cfg(test)]
use status::{StateFileIssue, inspect_lock_without_state};

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
        DaemonStatus::Starting(record) => {
            return Err(format!(
                "refusing to start: daemon start is already in progress by pid {}",
                record.pid
            ));
        }
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
        DaemonStatus::Starting(record) => format!(
            "{NAME} daemon is starting.\nstarter_pid: {}\nstarted_at_unix: {}\nruntime dir: {}",
            record.pid,
            record.started_at_unix,
            paths.runtime_dir.display()
        ),
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
        DaemonStatus::Starting(record) => Err(format!(
            "refusing to stop daemon while startup is in progress by pid {}; try again shortly",
            record.pid
        )),
        DaemonStatus::Running(record) => {
            let stopped = request_graceful_stop(paths, &record)?;

            if !stopped {
                force_stop_verified(paths, &record)?;
            }

            cleanup_runtime_files_for_record_unlocked(paths, &record);

            Ok(format!("{NAME} daemon stopped.\npid: {}", record.pid))
        }
        DaemonStatus::RunningUnverified { record, reason } => {
            let stopped = request_graceful_stop_for_unverified_record(paths, &record)?;

            if !stopped {
                return Err(format!(
                    "refusing to force stop unverified daemon pid {}: {reason}",
                    record.pid
                ));
            }

            cleanup_runtime_files_for_record_unlocked(paths, &record);
            cleanup_invalid_state_files_unlocked(paths);

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
        MODE_FOREGROUND
    } else {
        MODE_BACKGROUND
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
            Ok(record.pid == pid && record.token == token && record.mode != MODE_STARTING)
        }
        Ok(None) => Ok(false),
        Err(reason) => Err(format!(
            "daemon wrote invalid ready record {}: {reason}",
            path.display()
        )),
    }
}

fn request_graceful_stop(paths: &DaemonPaths, record: &DaemonRecord) -> Result<bool, String> {
    write_private_file(&paths.stop_file, &record.token)
        .map_err(|err| format!("failed to write stop request: {err}"))?;

    Ok(wait_for_stop(paths, record.pid))
}

fn request_graceful_stop_for_unverified_record(
    paths: &DaemonPaths,
    record: &DaemonRecord,
) -> Result<bool, String> {
    write_private_file(&paths.stop_file, &record.token)
        .map_err(|err| format!("failed to write stop request: {err}"))?;

    Ok(wait_for_process_to_exit(record.pid, STOP_TIMEOUT))
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

fn wait_for_process_to_exit(pid: u32, timeout: Duration) -> bool {
    let started = Instant::now();

    while started.elapsed() < timeout {
        if !is_process_running(pid) {
            return true;
        }

        thread::sleep(POLL_INTERVAL);
    }

    false
}

fn wait_for_process_exit(pid: u32, timeout: Duration) -> Result<(), String> {
    if wait_for_process_to_exit(pid, timeout) {
        Ok(())
    } else {
        Err(format!("process {pid} did not exit before timeout"))
    }
}

fn cleanup_failed_start(paths: &DaemonPaths, child: &mut Child, token: &str) {
    let _ = write_private_file(&paths.stop_file, token);

    if !wait_for_child_exit(child, START_FAILURE_STOP_TIMEOUT) {
        let _ = child.kill();
        let _ = child.wait();
    }

    if let Ok(_guard) = acquire_lifecycle_lock(paths) {
        cleanup_runtime_files_for_token_unlocked(paths, token);
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
