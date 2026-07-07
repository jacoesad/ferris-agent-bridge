use super::{
    DaemonPaths, DaemonRecord, DaemonStatus, MODE_STARTING,
    files::read_record,
    process_ops::{ProcessIdentity, inspect_process_identity, is_process_running},
    startup::starting_lock_is_live,
};

pub(super) fn inspect_status(paths: &DaemonPaths) -> DaemonStatus {
    match read_record(&paths.state_file) {
        Ok(Some(record)) => inspect_record_status(record),
        Ok(None) => inspect_lock_without_state(paths, StateFileIssue::Missing),
        Err(reason) => inspect_lock_without_state(paths, StateFileIssue::Invalid(reason)),
    }
}

pub(super) enum StateFileIssue {
    Missing,
    Invalid(String),
}

impl StateFileIssue {
    fn reason(&self) -> String {
        match self {
            Self::Missing => "daemon state is missing".to_owned(),
            Self::Invalid(reason) => format!("daemon state is invalid: {reason}"),
        }
    }
}

pub(super) fn inspect_lock_without_state(
    paths: &DaemonPaths,
    state_issue: StateFileIssue,
) -> DaemonStatus {
    let state_reason = state_issue.reason();
    let record = match read_record(&paths.lock_file) {
        Ok(Some(record)) => record,
        Ok(None) if matches!(state_issue, StateFileIssue::Missing) => {
            return DaemonStatus::Stopped;
        }
        Ok(None) => {
            return DaemonStatus::Stale {
                pid: None,
                record: None,
                reason: state_reason,
            };
        }
        Err(reason) => {
            return DaemonStatus::Stale {
                pid: None,
                record: None,
                reason: format!("{state_reason}; daemon lock is invalid: {reason}"),
            };
        }
    };

    if record.mode == MODE_STARTING {
        if starting_lock_is_live(&record) {
            return DaemonStatus::Starting(record);
        }

        return DaemonStatus::Stale {
            pid: Some(record.pid),
            record: Some(record),
            reason: format!("{state_reason}; startup lock holder is not running"),
        };
    }

    match inspect_record_status(record.clone()) {
        DaemonStatus::Running(record) => DaemonStatus::RunningUnverified {
            record,
            reason: state_reason,
        },
        DaemonStatus::RunningUnverified { record, reason } => DaemonStatus::RunningUnverified {
            record,
            reason: format!("{state_reason}; {reason}"),
        },
        DaemonStatus::Unowned { reason, .. } => DaemonStatus::RunningUnverified {
            record,
            reason: format!("{state_reason}; {reason}"),
        },
        other => other,
    }
}

fn inspect_record_status(record: DaemonRecord) -> DaemonStatus {
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
