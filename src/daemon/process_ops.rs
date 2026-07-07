use std::{
    env, io,
    process::{self, Command},
    time::{SystemTime, UNIX_EPOCH},
};

use super::{DaemonRecord, MODE_FOREGROUND, NAME};

pub(super) enum ProcessIdentity {
    Verified,
    Unverified(String),
    Mismatch,
}

pub(super) fn inspect_process_identity(record: &DaemonRecord) -> ProcessIdentity {
    let Some(command_line) = process_command_line(record.pid) else {
        return ProcessIdentity::Unverified("process command line is unavailable".to_owned());
    };

    if command_line.is_empty() {
        return ProcessIdentity::Unverified("process command line is empty".to_owned());
    }

    let has_internal_daemon = command_line.contains("__daemon");
    let has_token = command_line.contains(&record.token);

    if record.mode == MODE_FOREGROUND {
        if has_internal_daemon
            && has_token
            && (command_line.contains("--foreground") || command_line.contains(&record.exe))
        {
            return ProcessIdentity::Verified;
        }

        return ProcessIdentity::Mismatch;
    }

    if has_internal_daemon && has_token {
        ProcessIdentity::Verified
    } else {
        ProcessIdentity::Mismatch
    }
}

#[cfg(test)]
pub(super) fn record_matches_process(record: &DaemonRecord) -> bool {
    matches!(inspect_process_identity(record), ProcessIdentity::Verified)
}

pub(super) fn current_exe_string() -> String {
    env::current_exe()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|_| NAME.to_owned())
}

pub(super) fn generate_token() -> String {
    format!("{}-{}", process::id(), now_unix_nanos())
}

pub(super) fn now_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

pub(super) fn now_unix_nanos() -> u128 {
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
pub(super) fn detach_background_command(command: &mut Command) {
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
pub(super) fn detach_background_command(command: &mut Command) {
    use std::os::windows::process::CommandExt;

    const DETACHED_PROCESS: u32 = 0x0000_0008;
    const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
    command.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP);
}

#[cfg(not(any(unix, windows)))]
pub(super) fn detach_background_command(_command: &mut Command) {}

#[cfg(unix)]
pub(super) fn is_process_running(pid: u32) -> bool {
    let output = Command::new("kill").arg("-0").arg(pid.to_string()).output();

    match output {
        Ok(output) if output.status.success() => true,
        Ok(output) => kill_zero_stderr_indicates_live_process(&output.stderr),
        Err(_) => false,
    }
}

#[cfg(unix)]
pub(super) fn kill_zero_stderr_indicates_live_process(stderr: &[u8]) -> bool {
    String::from_utf8_lossy(stderr)
        .to_ascii_lowercase()
        .contains("operation not permitted")
}

#[cfg(windows)]
pub(super) fn is_process_running(pid: u32) -> bool {
    let filter = format!("PID eq {pid}");
    let output = Command::new("tasklist")
        .args(["/FI", filter.as_str(), "/FO", "CSV", "/NH"])
        .output();

    output
        .map(|output| String::from_utf8_lossy(&output.stdout).contains(&pid.to_string()))
        .unwrap_or(false)
}

#[cfg(unix)]
pub(super) fn terminate_process(pid: u32) -> Result<(), String> {
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
pub(super) fn terminate_process(pid: u32) -> Result<(), String> {
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
pub(super) fn process_command_line(pid: u32) -> Option<String> {
    let pid = pid.to_string();
    let output = Command::new("ps")
        .args(["-ww", "-p", pid.as_str(), "-o", "command="])
        .output()
        .ok()?;

    if output.status.success() {
        Some(String::from_utf8_lossy(&output.stdout).trim().to_owned())
    } else {
        None
    }
}

#[cfg(windows)]
pub(super) fn process_command_line(pid: u32) -> Option<String> {
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
