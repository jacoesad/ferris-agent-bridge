use std::{fs, io, path::Path};

use super::{
    DaemonPaths, DaemonRecord,
    files::{create_private_dir, read_record, remove_file_if_exists, set_private_dir_permissions},
    lifecycle::acquire_lifecycle_lock,
};

pub(super) fn should_stop(paths: &DaemonPaths, token: &str) -> bool {
    match fs::read_to_string(&paths.stop_file) {
        Ok(content) => content.trim() == token,
        Err(_) => false,
    }
}

pub(super) fn prepare_runtime_dir(paths: &DaemonPaths) -> Result<(), String> {
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

pub(super) fn cleanup_stale_files_unlocked(paths: &DaemonPaths, record: Option<&DaemonRecord>) {
    if let Some(record) = record {
        cleanup_runtime_files_for_record_unlocked(paths, record);
    } else {
        cleanup_invalid_state_files_unlocked(paths);
    }
}

#[cfg(test)]
pub(super) fn cleanup_runtime_files_for_record(paths: &DaemonPaths, record: &DaemonRecord) {
    let Ok(_guard) = acquire_lifecycle_lock(paths) else {
        return;
    };

    cleanup_runtime_files_for_record_unlocked(paths, record);
}

pub(super) fn cleanup_runtime_files_for_record_unlocked(
    paths: &DaemonPaths,
    record: &DaemonRecord,
) {
    let _ = remove_record_file_if_matches_unlocked(&paths.lock_file, record);
    cleanup_state_files_for_record_unlocked(paths, record);
}

pub(super) fn cleanup_runtime_files_for_token(paths: &DaemonPaths, token: &str) {
    let Ok(_guard) = acquire_lifecycle_lock(paths) else {
        return;
    };

    cleanup_runtime_files_for_token_unlocked(paths, token);
}

pub(super) fn cleanup_runtime_files_for_token_unlocked(paths: &DaemonPaths, token: &str) {
    remove_record_file_if_token_unlocked(&paths.lock_file, token);
    remove_record_file_if_token_unlocked(&paths.state_file, token);
    remove_stop_file_if_matches_unlocked(paths, token);
}

pub(super) fn cleanup_invalid_state_files_unlocked(paths: &DaemonPaths) {
    if read_record(&paths.state_file).is_err() {
        let _ = remove_file_if_exists(&paths.state_file);
        let _ = remove_file_if_exists(&paths.stop_file);
    }
}

fn cleanup_state_files_for_record_unlocked(paths: &DaemonPaths, record: &DaemonRecord) {
    let _ = remove_record_file_if_matches_unlocked(&paths.state_file, record);
    remove_stop_file_if_matches_unlocked(paths, &record.token);
}

pub(super) fn remove_record_file_if_matches_unlocked(
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

pub(super) fn record_identity_matches(left: &DaemonRecord, right: &DaemonRecord) -> bool {
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
