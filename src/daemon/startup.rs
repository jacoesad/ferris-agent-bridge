use std::{io, process, time::Duration};

use super::{
    DaemonPaths, DaemonRecord, MODE_STARTING, STARTING_LOCK_TTL,
    cleanup::remove_record_file_if_matches_unlocked,
    files::{read_record, write_new_record},
    process_ops::{
        ProcessIdentity, current_exe_string, inspect_process_identity, is_process_running,
        now_unix_seconds,
    },
};

pub(super) fn validate_startup_lock(
    paths: &DaemonPaths,
    token: &str,
    starter_pid: u32,
) -> Result<(), String> {
    let record = read_record(&paths.lock_file)?
        .ok_or_else(|| "refusing to run daemon: startup lock is missing".to_owned())?;

    if record.mode != MODE_STARTING {
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

pub(super) fn acquire_start_lock(paths: &DaemonPaths, token: String) -> Result<(), String> {
    let record = DaemonRecord::new(process::id(), token, current_exe_string(), MODE_STARTING);

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
                            remove_record_file_if_matches_unlocked(&paths.lock_file, &record)
                                .map_err(|err| {
                                    format!(
                                        "failed to remove stale daemon lock {}: {err}",
                                        paths.lock_file.display()
                                    )
                                })?;
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

    if record.mode == MODE_STARTING {
        if starting_lock_is_live(&record) {
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

pub(super) fn starting_lock_is_live(record: &DaemonRecord) -> bool {
    let is_fresh = Duration::from_secs(now_unix_seconds().saturating_sub(record.started_at_unix))
        <= STARTING_LOCK_TTL;

    is_fresh && is_process_running(record.pid)
}
