use super::{MODE_BACKGROUND, MODE_FOREGROUND, MODE_STARTING, process_ops::now_unix_seconds};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DaemonStatus {
    Starting(DaemonRecord),
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
    pub(super) pid: u32,
    pub(super) token: String,
    pub(super) exe: String,
    pub(super) started_at_unix: u64,
    pub(super) mode: String,
}

impl DaemonRecord {
    pub(super) fn new(pid: u32, token: String, exe: String, mode: &str) -> Self {
        Self {
            pid,
            token,
            exe,
            started_at_unix: now_unix_seconds(),
            mode: mode.to_owned(),
        }
    }

    pub(super) fn encode(&self) -> String {
        format!(
            "pid={}\ntoken={}\nexe={}\nstarted_at_unix={}\nmode={}\n",
            self.pid, self.token, self.exe, self.started_at_unix, self.mode
        )
    }

    pub(super) fn decode(input: &str) -> Result<Self, String> {
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
                "token" => token = Some(non_empty_record_value("token", value)?),
                "exe" => exe = Some(non_empty_record_value("exe", value)?),
                "started_at_unix" => {
                    started_at_unix = Some(
                        value
                            .parse::<u64>()
                            .map_err(|err| format!("invalid started_at_unix `{value}`: {err}"))?,
                    );
                }
                "mode" => {
                    let value = non_empty_record_value("mode", value)?;
                    if !is_known_mode(&value) {
                        return Err(format!("invalid mode `{value}`"));
                    }
                    mode = Some(value);
                }
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

fn non_empty_record_value(key: &str, value: &str) -> Result<String, String> {
    if value.is_empty() {
        return Err(format!("empty {key}"));
    }

    Ok(value.to_owned())
}

fn is_known_mode(value: &str) -> bool {
    matches!(value, MODE_STARTING | MODE_BACKGROUND | MODE_FOREGROUND)
}
