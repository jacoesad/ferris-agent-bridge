use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, de};

use super::session::SessionId;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct RunId(String);

impl RunId {
    pub fn new(value: impl Into<String>) -> Result<Self, String> {
        let value = value.into();

        if !is_valid_id(&value) {
            return Err(format!("invalid run id `{value}`"));
        }

        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RunId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for RunId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl RunStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RunRecord {
    id: RunId,
    session_id: SessionId,
    status: RunStatus,
    created_at_unix: u64,
    updated_at_unix: u64,
    started_at_unix: Option<u64>,
    finished_at_unix: Option<u64>,
}

impl RunRecord {
    pub fn new(id: RunId, session_id: SessionId, created_at_unix: u64) -> Self {
        Self {
            id,
            session_id,
            status: RunStatus::Pending,
            created_at_unix,
            updated_at_unix: created_at_unix,
            started_at_unix: None,
            finished_at_unix: None,
        }
    }

    pub fn id(&self) -> &RunId {
        &self.id
    }

    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    pub fn status(&self) -> RunStatus {
        self.status
    }

    pub fn created_at_unix(&self) -> u64 {
        self.created_at_unix
    }

    pub fn updated_at_unix(&self) -> u64 {
        self.updated_at_unix
    }

    pub fn started_at_unix(&self) -> Option<u64> {
        self.started_at_unix
    }

    pub fn finished_at_unix(&self) -> Option<u64> {
        self.finished_at_unix
    }

    pub fn is_terminal(&self) -> bool {
        self.status.is_terminal()
    }

    pub fn start(&mut self, started_at_unix: u64) -> Result<(), String> {
        if self.status != RunStatus::Pending {
            return Err(format!(
                "run {} cannot start from {:?}",
                self.id, self.status
            ));
        }

        if started_at_unix < self.created_at_unix {
            return Err(format!(
                "run {} cannot start before created_at_unix",
                self.id
            ));
        }

        self.status = RunStatus::Running;
        self.started_at_unix = Some(started_at_unix);
        self.touch_at(started_at_unix);
        Ok(())
    }

    pub fn complete(&mut self, finished_at_unix: u64) -> Result<(), String> {
        if self.status != RunStatus::Running {
            return Err(format!(
                "run {} cannot complete from {:?}",
                self.id, self.status
            ));
        }

        self.finish(RunStatus::Completed, finished_at_unix)
    }

    pub fn fail(&mut self, finished_at_unix: u64) -> Result<(), String> {
        if self.status.is_terminal() {
            return Err(format!(
                "run {} cannot fail from terminal status {:?}",
                self.id, self.status
            ));
        }

        self.finish(RunStatus::Failed, finished_at_unix)
    }

    pub fn cancel(&mut self, finished_at_unix: u64) -> Result<(), String> {
        if self.status.is_terminal() {
            return Err(format!(
                "run {} cannot cancel from terminal status {:?}",
                self.id, self.status
            ));
        }

        self.finish(RunStatus::Cancelled, finished_at_unix)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.updated_at_unix < self.created_at_unix {
            return Err(format!(
                "run {} has updated_at_unix before created_at_unix",
                self.id
            ));
        }

        if let Some(started_at_unix) = self.started_at_unix {
            if started_at_unix < self.created_at_unix {
                return Err(format!(
                    "run {} has started_at_unix before created_at_unix",
                    self.id
                ));
            }

            if self.updated_at_unix < started_at_unix {
                return Err(format!(
                    "run {} has updated_at_unix before started_at_unix",
                    self.id
                ));
            }
        }

        if let Some(finished_at_unix) = self.finished_at_unix {
            if finished_at_unix < self.created_at_unix {
                return Err(format!(
                    "run {} has finished_at_unix before created_at_unix",
                    self.id
                ));
            }

            if let Some(started_at_unix) = self.started_at_unix {
                if finished_at_unix < started_at_unix {
                    return Err(format!(
                        "run {} has finished_at_unix before started_at_unix",
                        self.id
                    ));
                }
            }

            if self.updated_at_unix < finished_at_unix {
                return Err(format!(
                    "run {} has updated_at_unix before finished_at_unix",
                    self.id
                ));
            }
        }

        match self.status {
            RunStatus::Pending => {
                if self.started_at_unix.is_some() || self.finished_at_unix.is_some() {
                    return Err(format!(
                        "pending run {} must not have start or finish timestamps",
                        self.id
                    ));
                }
            }
            RunStatus::Running => {
                if self.started_at_unix.is_none() {
                    return Err(format!("running run {} must have started_at_unix", self.id));
                }

                if self.finished_at_unix.is_some() {
                    return Err(format!(
                        "running run {} must not have finished_at_unix",
                        self.id
                    ));
                }
            }
            RunStatus::Completed => {
                if self.started_at_unix.is_none() || self.finished_at_unix.is_none() {
                    return Err(format!(
                        "completed run {} must have start and finish timestamps",
                        self.id
                    ));
                }
            }
            RunStatus::Failed | RunStatus::Cancelled => {
                if self.finished_at_unix.is_none() {
                    return Err(format!(
                        "{:?} run {} must have finished_at_unix",
                        self.status, self.id
                    ));
                }
            }
        }

        Ok(())
    }

    fn finish(&mut self, status: RunStatus, finished_at_unix: u64) -> Result<(), String> {
        if finished_at_unix < self.created_at_unix {
            return Err(format!(
                "run {} cannot finish before created_at_unix",
                self.id
            ));
        }

        if let Some(started_at_unix) = self.started_at_unix {
            if finished_at_unix < started_at_unix {
                return Err(format!(
                    "run {} cannot finish before started_at_unix",
                    self.id
                ));
            }
        }

        self.status = status;
        self.finished_at_unix = Some(finished_at_unix);
        self.touch_at(finished_at_unix);
        Ok(())
    }

    fn touch_at(&mut self, updated_at_unix: u64) {
        self.updated_at_unix = self.updated_at_unix.max(updated_at_unix);
    }
}

impl<'de> Deserialize<'de> for RunRecord {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct RunRecordWire {
            id: RunId,
            session_id: SessionId,
            status: RunStatus,
            created_at_unix: u64,
            updated_at_unix: u64,
            started_at_unix: Option<u64>,
            finished_at_unix: Option<u64>,
        }

        let wire = RunRecordWire::deserialize(deserializer)?;
        let record = Self {
            id: wire.id,
            session_id: wire.session_id,
            status: wire.status,
            created_at_unix: wire.created_at_unix,
            updated_at_unix: wire.updated_at_unix,
            started_at_unix: wire.started_at_unix,
            finished_at_unix: wire.finished_at_unix,
        };

        record.validate().map_err(de::Error::custom)?;
        Ok(record)
    }
}

fn is_valid_id(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':'))
}

#[cfg(test)]
mod tests {
    use super::{RunId, RunRecord, RunStatus};
    use crate::runtime::session::{SessionId, SessionScope};

    #[test]
    fn run_record_transitions_from_pending_to_completed() {
        let mut run = run_fixture("run_1", 10);

        assert_eq!(run.status(), RunStatus::Pending);
        assert!(!run.is_terminal());
        assert_eq!(run.updated_at_unix(), 10);

        run.start(12).expect("run should start");
        assert_eq!(run.status(), RunStatus::Running);
        assert_eq!(run.started_at_unix(), Some(12));
        assert_eq!(run.updated_at_unix(), 12);

        run.complete(15).expect("run should complete");
        assert_eq!(run.status(), RunStatus::Completed);
        assert_eq!(run.finished_at_unix(), Some(15));
        assert_eq!(run.updated_at_unix(), 15);
        assert!(run.is_terminal());
    }

    #[test]
    fn run_record_can_fail_or_cancel_before_start() {
        let mut failed = run_fixture("run_failed", 10);
        failed.fail(11).expect("pending run can fail");
        assert_eq!(failed.status(), RunStatus::Failed);
        assert_eq!(failed.started_at_unix(), None);
        assert_eq!(failed.finished_at_unix(), Some(11));

        let mut cancelled = run_fixture("run_cancelled", 10);
        cancelled.cancel(12).expect("pending run can cancel");
        assert_eq!(cancelled.status(), RunStatus::Cancelled);
        assert_eq!(cancelled.started_at_unix(), None);
        assert_eq!(cancelled.finished_at_unix(), Some(12));
    }

    #[test]
    fn run_record_rejects_invalid_transitions() {
        let mut run = run_fixture("run_1", 10);

        assert!(run.complete(11).is_err());
        assert!(run.start(9).is_err());

        run.start(11).expect("run should start");
        assert!(run.start(12).is_err());
        assert!(run.complete(10).is_err());

        run.complete(12).expect("run should complete");
        assert!(run.fail(13).is_err());
        assert!(run.cancel(13).is_err());
    }

    #[test]
    fn run_record_touch_does_not_move_updated_at_backwards() {
        let mut run = run_fixture("run_1", 10);
        run.updated_at_unix = 20;

        run.start(15).expect("run should start");
        assert_eq!(run.updated_at_unix(), 20);
    }

    #[test]
    fn run_record_round_trips_as_json() {
        let mut run = run_fixture("run_1", 10);
        run.start(11).expect("run should start");
        run.complete(12).expect("run should complete");

        let encoded = serde_json::to_string(&run).expect("run should serialize");
        let decoded: RunRecord = serde_json::from_str(&encoded).expect("run should decode");

        assert_eq!(decoded, run);
    }

    #[test]
    fn rejects_invalid_run_ids() {
        assert!(RunId::new("").is_err());
        assert!(RunId::new("bad id").is_err());
        assert!(RunId::new("run_ok-1").is_ok());
    }

    #[test]
    fn rejects_invalid_run_ids_from_json() {
        let err =
            serde_json::from_str::<RunId>("\"bad id\"").expect_err("invalid run id should fail");

        assert!(err.to_string().contains("invalid run id"));
    }

    #[test]
    fn rejects_invalid_run_time_order_from_json() {
        let err = serde_json::from_str::<RunRecord>(&format!(
            r#"{{
                "id": "run_1",
                "session_id": "{}",
                "status": "completed",
                "created_at_unix": 10,
                "updated_at_unix": 12,
                "started_at_unix": 12,
                "finished_at_unix": 11
            }}"#,
            session_id()
        ))
        .expect_err("finished before started should fail");

        assert!(
            err.to_string()
                .contains("finished_at_unix before started_at_unix")
        );
    }

    #[test]
    fn rejects_invalid_run_status_timestamp_shape_from_json() {
        let err = serde_json::from_str::<RunRecord>(&format!(
            r#"{{
                "id": "run_1",
                "session_id": "{}",
                "status": "running",
                "created_at_unix": 10,
                "updated_at_unix": 10,
                "started_at_unix": null,
                "finished_at_unix": null
            }}"#,
            session_id()
        ))
        .expect_err("running run without start timestamp should fail");

        assert!(err.to_string().contains("must have started_at_unix"));
    }

    fn run_fixture(id: &str, created_at_unix: u64) -> RunRecord {
        RunRecord::new(
            RunId::new(id).expect("valid run id"),
            session_id(),
            created_at_unix,
        )
    }

    fn session_id() -> SessionId {
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        SessionId::for_scope(&scope)
    }
}
