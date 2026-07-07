use std::{
    collections::BTreeSet,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Deserializer, Serialize, de};

use crate::runtime::{
    event::{Event, EventId, InboundEventRecord, InboundEventRecordStatus},
    outbox::{
        OutboundDeliveryEnqueueStatus, OutboundDeliveryId, OutboundDeliveryRecord,
        OutboundDeliveryStatus,
    },
    run::{RunId, RunRecord},
    session::{Session, SessionId},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RuntimeState {
    sessions: Vec<Session>,
    runs: Vec<RunRecord>,
    inbound_events: Vec<InboundEventRecord>,
    outbound_deliveries: Vec<OutboundDeliveryRecord>,
    updated_at_unix: u64,
}

impl RuntimeState {
    pub fn new() -> Self {
        Self {
            sessions: Vec::new(),
            runs: Vec::new(),
            inbound_events: Vec::new(),
            outbound_deliveries: Vec::new(),
            updated_at_unix: unix_seconds_now(),
        }
    }

    pub fn upsert_session(&mut self, session: Session) {
        let updated_at_unix = if let Some(existing) = self
            .sessions
            .iter_mut()
            .find(|existing| existing.id() == session.id())
        {
            existing.refresh_from(session);
            existing.updated_at_unix()
        } else {
            let updated_at_unix = session.updated_at_unix();
            self.sessions.push(session);
            updated_at_unix
        };

        self.touch_at(updated_at_unix.max(unix_seconds_now()));
    }

    pub fn session(&self, id: &SessionId) -> Option<&Session> {
        self.sessions.iter().find(|session| session.id() == id)
    }

    pub fn add_run(&mut self, run: RunRecord) -> Result<(), String> {
        self.validate_run_session(&run)?;

        if self.runs.iter().any(|existing| existing.id() == run.id()) {
            return Err(format!("duplicate run id {}", run.id()));
        }

        let updated_at_unix = run.updated_at_unix();
        self.runs.push(run);
        self.touch_at(updated_at_unix.max(unix_seconds_now()));
        Ok(())
    }

    pub fn run(&self, id: &RunId) -> Option<&RunRecord> {
        self.runs.iter().find(|run| run.id() == id)
    }

    pub fn record_inbound_event(
        &mut self,
        event: &Event,
    ) -> Result<InboundEventRecordStatus, String> {
        self.record_inbound_event_at(event, unix_seconds_now())
    }

    pub fn inbound_event(&self, id: &EventId) -> Option<&InboundEventRecord> {
        self.inbound_events.iter().find(|event| event.id() == id)
    }

    pub fn has_inbound_event(&self, id: &EventId) -> bool {
        self.inbound_event(id).is_some()
    }

    pub fn enqueue_outbound_delivery(
        &mut self,
        delivery: OutboundDeliveryRecord,
    ) -> Result<OutboundDeliveryEnqueueStatus, String> {
        delivery.validate()?;
        self.validate_outbound_delivery_session(&delivery)?;

        if delivery.status() != OutboundDeliveryStatus::Pending {
            return Err(format!(
                "outbound delivery {} cannot enqueue from {:?}",
                delivery.id(),
                delivery.status()
            ));
        }

        if let Some(existing) = self.outbound_delivery(delivery.id()) {
            if existing == &delivery {
                return Ok(OutboundDeliveryEnqueueStatus::Duplicate);
            }

            return Err(format!("conflicting outbound delivery {}", delivery.id()));
        }

        let updated_at_unix = delivery.updated_at_unix();
        self.outbound_deliveries.push(delivery);
        self.touch_at(updated_at_unix.max(unix_seconds_now()));
        Ok(OutboundDeliveryEnqueueStatus::Queued)
    }

    pub fn outbound_delivery(&self, id: &OutboundDeliveryId) -> Option<&OutboundDeliveryRecord> {
        self.outbound_deliveries
            .iter()
            .find(|delivery| delivery.id() == id)
    }

    pub fn start_run(&mut self, id: &RunId, started_at_unix: u64) -> Result<(), String> {
        {
            let run = self.run_mut(id)?;
            run.start(started_at_unix)?;
        }

        self.touch_at(started_at_unix.max(unix_seconds_now()));
        Ok(())
    }

    pub fn complete_run(&mut self, id: &RunId, finished_at_unix: u64) -> Result<(), String> {
        {
            let run = self.run_mut(id)?;
            run.complete(finished_at_unix)?;
        }

        self.touch_at(finished_at_unix.max(unix_seconds_now()));
        Ok(())
    }

    pub fn fail_run(&mut self, id: &RunId, finished_at_unix: u64) -> Result<(), String> {
        {
            let run = self.run_mut(id)?;
            run.fail(finished_at_unix)?;
        }

        self.touch_at(finished_at_unix.max(unix_seconds_now()));
        Ok(())
    }

    pub fn cancel_run(&mut self, id: &RunId, finished_at_unix: u64) -> Result<(), String> {
        {
            let run = self.run_mut(id)?;
            run.cancel(finished_at_unix)?;
        }

        self.touch_at(finished_at_unix.max(unix_seconds_now()));
        Ok(())
    }

    pub fn sessions(&self) -> &[Session] {
        &self.sessions
    }

    pub fn runs(&self) -> &[RunRecord] {
        &self.runs
    }

    pub fn inbound_events(&self) -> &[InboundEventRecord] {
        &self.inbound_events
    }

    pub fn outbound_deliveries(&self) -> &[OutboundDeliveryRecord] {
        &self.outbound_deliveries
    }

    pub fn updated_at_unix(&self) -> u64 {
        self.updated_at_unix
    }

    pub fn validate(&self) -> Result<(), String> {
        let mut session_ids = BTreeSet::new();
        let mut run_ids = BTreeSet::new();
        let mut inbound_event_ids = BTreeSet::new();
        let mut outbound_delivery_ids = BTreeSet::new();

        for session in &self.sessions {
            session.validate()?;

            if !session_ids.insert(session.id()) {
                return Err(format!("duplicate session id {}", session.id()));
            }

            if self.updated_at_unix < session.updated_at_unix() {
                return Err(format!(
                    "runtime state updated_at_unix before session {} updated_at_unix",
                    session.id()
                ));
            }
        }

        for run in &self.runs {
            run.validate()?;
            self.validate_run_session(run)?;

            if !run_ids.insert(run.id()) {
                return Err(format!("duplicate run id {}", run.id()));
            }

            if self.updated_at_unix < run.updated_at_unix() {
                return Err(format!(
                    "runtime state updated_at_unix before run {} updated_at_unix",
                    run.id()
                ));
            }
        }

        for event in &self.inbound_events {
            event.validate()?;

            if !inbound_event_ids.insert(event.id()) {
                return Err(format!("duplicate inbound event id {}", event.id()));
            }

            if self.updated_at_unix < event.recorded_at_unix() {
                return Err(format!(
                    "runtime state updated_at_unix before inbound event {} recorded_at_unix",
                    event.id()
                ));
            }
        }

        for delivery in &self.outbound_deliveries {
            delivery.validate()?;

            if !outbound_delivery_ids.insert(delivery.id()) {
                return Err(format!("duplicate outbound delivery id {}", delivery.id()));
            }

            self.validate_outbound_delivery_session(delivery)?;

            if self.updated_at_unix < delivery.updated_at_unix() {
                return Err(format!(
                    "runtime state updated_at_unix before outbound delivery {} updated_at_unix",
                    delivery.id()
                ));
            }
        }

        Ok(())
    }

    pub(super) fn from_persisted_parts(
        sessions: Vec<Session>,
        runs: Vec<RunRecord>,
        inbound_events: Vec<InboundEventRecord>,
        outbound_deliveries: Vec<OutboundDeliveryRecord>,
        updated_at_unix: u64,
        normalize_aggregate_updated_at: bool,
    ) -> Result<Self, String> {
        let mut state = Self {
            sessions,
            runs,
            inbound_events,
            outbound_deliveries,
            updated_at_unix,
        };
        if normalize_aggregate_updated_at {
            state.normalize_migrated_aggregate_updated_at();
        }
        state.validate()?;
        Ok(state)
    }

    fn validate_run_session(&self, run: &RunRecord) -> Result<(), String> {
        if self.session(run.session_id()).is_none() {
            return Err(format!(
                "run {} references unknown session {}",
                run.id(),
                run.session_id()
            ));
        }

        Ok(())
    }

    fn validate_outbound_delivery_session(
        &self,
        delivery: &OutboundDeliveryRecord,
    ) -> Result<(), String> {
        if self.session(delivery.session_id()).is_none() {
            return Err(format!(
                "outbound delivery {} references unknown session {}",
                delivery.id(),
                delivery.session_id()
            ));
        }

        Ok(())
    }

    fn run_mut(&mut self, id: &RunId) -> Result<&mut RunRecord, String> {
        self.runs
            .iter_mut()
            .find(|run| run.id() == id)
            .ok_or_else(|| format!("unknown run id {id}"))
    }

    fn record_inbound_event_at(
        &mut self,
        event: &Event,
        recorded_at_unix: u64,
    ) -> Result<InboundEventRecordStatus, String> {
        if self.has_inbound_event(&event.id) {
            return Ok(InboundEventRecordStatus::Duplicate);
        }

        let record = InboundEventRecord::from_event(event, recorded_at_unix)?;
        let recorded_at_unix = record.recorded_at_unix();
        self.inbound_events.push(record);
        self.touch_at(recorded_at_unix);
        Ok(InboundEventRecordStatus::Recorded)
    }

    fn touch_at(&mut self, updated_at_unix: u64) {
        self.updated_at_unix = self.updated_at_unix.max(updated_at_unix);
    }

    fn normalize_migrated_aggregate_updated_at(&mut self) {
        let updated_at_unix = self
            .sessions
            .iter()
            .map(Session::updated_at_unix)
            .chain(self.runs.iter().map(RunRecord::updated_at_unix))
            .chain(
                self.inbound_events
                    .iter()
                    .map(InboundEventRecord::recorded_at_unix),
            )
            .chain(
                self.outbound_deliveries
                    .iter()
                    .map(OutboundDeliveryRecord::updated_at_unix),
            )
            .fold(self.updated_at_unix, u64::max);
        self.updated_at_unix = updated_at_unix;
    }

    pub(super) fn preserve_inbound_events_from(
        &mut self,
        existing: &RuntimeState,
    ) -> Result<(), String> {
        for existing_event in &existing.inbound_events {
            match self
                .inbound_events
                .iter()
                .find(|event| event.id() == existing_event.id())
            {
                Some(candidate_event) if candidate_event == existing_event => {}
                Some(_) => {
                    return Err(format!(
                        "conflicting inbound event record {}",
                        existing_event.id()
                    ));
                }
                None => {
                    self.touch_at(existing_event.recorded_at_unix());
                    self.inbound_events.push(existing_event.clone());
                }
            }
        }

        Ok(())
    }

    pub(super) fn preserve_outbound_deliveries_from(
        &mut self,
        existing: &RuntimeState,
    ) -> Result<(), String> {
        for existing_delivery in &existing.outbound_deliveries {
            match self
                .outbound_deliveries
                .iter()
                .find(|delivery| delivery.id() == existing_delivery.id())
            {
                Some(candidate_delivery) if candidate_delivery == existing_delivery => {}
                Some(_) => {
                    return Err(format!(
                        "conflicting outbound delivery {}",
                        existing_delivery.id()
                    ));
                }
                None => {
                    self.touch_at(existing_delivery.updated_at_unix());
                    self.outbound_deliveries.push(existing_delivery.clone());
                }
            }
        }

        Ok(())
    }
}

impl Default for RuntimeState {
    fn default() -> Self {
        Self::new()
    }
}

impl<'de> Deserialize<'de> for RuntimeState {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct RuntimeStateWire {
            sessions: Vec<Session>,
            runs: Vec<RunRecord>,
            inbound_events: Vec<InboundEventRecord>,
            outbound_deliveries: Vec<OutboundDeliveryRecord>,
            updated_at_unix: u64,
        }

        let wire = RuntimeStateWire::deserialize(deserializer)?;
        let state = Self {
            sessions: wire.sessions,
            runs: wire.runs,
            inbound_events: wire.inbound_events,
            outbound_deliveries: wire.outbound_deliveries,
            updated_at_unix: wire.updated_at_unix,
        };
        state.validate().map_err(de::Error::custom)?;
        Ok(state)
    }
}

fn unix_seconds_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::RuntimeState;
    use crate::runtime::{
        event::{Event, EventId, EventKind, EventSource, InboundEventRecordStatus},
        message::{Message, MessageAuthor, MessageContent, MessageId},
        outbox::{OutboundDeliveryEnqueueStatus, OutboundDeliveryId, OutboundDeliveryRecord},
        run::{RunId, RunRecord, RunStatus},
        session::{Session, SessionId, SessionScope},
    };

    const FUTURE_UNIX: u64 = 4_102_444_800;

    #[test]
    fn runtime_state_json_does_not_embed_file_version() {
        let encoded = serde_json::to_value(RuntimeState::new()).expect("state should encode");

        assert!(encoded.get("version").is_none());
        assert!(encoded.get("sessions").is_some());
        assert!(encoded.get("runs").is_some());
        assert!(encoded.get("inbound_events").is_some());
        assert!(encoded.get("outbound_deliveries").is_some());
        assert!(encoded.get("updated_at_unix").is_some());
    }
    #[test]
    fn runtime_state_json_rejects_file_version_field() {
        let err = serde_json::from_str::<RuntimeState>(
            r#"{
            "version": 1,
            "sessions": [],
            "runs": [],
            "inbound_events": [],
            "outbound_deliveries": [],
            "updated_at_unix": 1
        }"#,
        )
        .expect_err("RuntimeState should not accept file envelope fields");

        assert!(err.to_string().contains("unknown field `version`"));
    }
    #[test]
    fn state_records_inbound_events_idempotently() {
        let event = event_fixture("evt_1", 10);
        let mut state = RuntimeState::new();
        state.updated_at_unix = 20;

        assert_eq!(
            state
                .record_inbound_event_at(&event, 12)
                .expect("event should record"),
            InboundEventRecordStatus::Recorded
        );
        assert!(state.has_inbound_event(&event.id));
        assert_eq!(state.inbound_events().len(), 1);
        assert_eq!(state.updated_at_unix(), 20);

        assert_eq!(
            state
                .record_inbound_event_at(&event, 30)
                .expect("duplicate event should not fail"),
            InboundEventRecordStatus::Duplicate
        );
        assert_eq!(state.inbound_events().len(), 1);
        assert_eq!(state.updated_at_unix(), 20);
    }
    #[test]
    fn state_enqueues_outbound_deliveries_idempotently() {
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session = session_fixture(&scope, 10, 10);
        let session_id = session.id().clone();
        let delivery = outbound_delivery_fixture("out_1", session_id.clone(), 12);
        let mut state = RuntimeState::new();
        state.upsert_session(session);
        state.updated_at_unix = FUTURE_UNIX;
        let updated_before_enqueue = state.updated_at_unix();

        assert_eq!(
            state
                .enqueue_outbound_delivery(delivery.clone())
                .expect("delivery should enqueue"),
            OutboundDeliveryEnqueueStatus::Queued
        );
        assert!(state.outbound_delivery(delivery.id()).is_some());
        assert_eq!(state.outbound_deliveries().len(), 1);
        assert_eq!(state.updated_at_unix(), updated_before_enqueue);

        assert_eq!(
            state
                .enqueue_outbound_delivery(delivery.clone())
                .expect("duplicate delivery should not fail"),
            OutboundDeliveryEnqueueStatus::Duplicate
        );
        assert_eq!(state.outbound_deliveries().len(), 1);
        assert_eq!(state.updated_at_unix(), updated_before_enqueue);

        let conflicting = outbound_delivery_fixture("out_1", session_id, 13);
        let err = state
            .enqueue_outbound_delivery(conflicting)
            .expect_err("same id with different payload should fail closed");
        assert!(err.contains("conflicting outbound delivery"));
    }
    #[test]
    fn state_rejects_outbound_delivery_without_known_session() {
        let scope = SessionScope::new("lark", "chat:oc_missing").expect("valid scope");
        let delivery = outbound_delivery_fixture("out_missing", SessionId::for_scope(&scope), 12);
        let mut state = RuntimeState::new();

        let err = state
            .enqueue_outbound_delivery(delivery)
            .expect_err("delivery without a known session should fail");

        assert!(err.contains("references unknown session"));
        assert!(state.outbound_deliveries().is_empty());
    }
    #[test]
    fn state_rejects_non_pending_outbound_delivery_enqueue() {
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session = session_fixture(&scope, 10, 10);
        let session_id = session.id().clone();
        let mut delivery = outbound_delivery_fixture("out_1", session_id, 12);
        delivery
            .begin_delivery(13)
            .expect("delivery can enter delivering state");
        let mut state = RuntimeState::new();
        state.upsert_session(session);

        let err = state
            .enqueue_outbound_delivery(delivery)
            .expect_err("enqueue should only accept pending deliveries");

        assert!(err.contains("cannot enqueue from Delivering"));
        assert!(state.outbound_deliveries().is_empty());
    }
    #[test]
    fn state_transitions_persisted_run_records() {
        let (mut state, run_id) = state_with_pending_run("run_1");

        state.start_run(&run_id, 11).expect("run should start");
        state
            .complete_run(&run_id, 12)
            .expect("run should complete");

        let run = state.run(&run_id).expect("run should exist");
        assert_eq!(run.status(), RunStatus::Completed);
        assert_eq!(run.started_at_unix(), Some(11));
        assert_eq!(run.finished_at_unix(), Some(12));
    }
    #[test]
    fn state_transitions_can_fail_or_cancel_persisted_run_records() {
        let (mut failed, failed_id) = state_with_pending_run("run_failed");
        failed
            .fail_run(&failed_id, 11)
            .expect("pending run can fail");
        assert_eq!(
            failed.run(&failed_id).expect("run should exist").status(),
            RunStatus::Failed
        );

        let (mut cancelled, cancelled_id) = state_with_pending_run("run_cancelled");
        cancelled
            .start_run(&cancelled_id, 11)
            .expect("run should start");
        cancelled
            .cancel_run(&cancelled_id, 12)
            .expect("running run can cancel");
        assert_eq!(
            cancelled
                .run(&cancelled_id)
                .expect("run should exist")
                .status(),
            RunStatus::Cancelled
        );
    }
    #[test]
    fn state_transitions_reject_invalid_or_unknown_runs() {
        let (mut state, run_id) = state_with_pending_run("run_1");
        let unknown_id = RunId::new("run_missing").expect("valid run id");

        let err = state
            .complete_run(&run_id, 11)
            .expect_err("pending run should not complete");
        assert!(err.contains("cannot complete from Pending"));

        let err = state
            .start_run(&unknown_id, 11)
            .expect_err("unknown run should not start");
        assert!(err.contains("unknown run id"));

        state.start_run(&run_id, 11).expect("run should start");
        state
            .complete_run(&run_id, 12)
            .expect("run should complete");

        let err = state
            .start_run(&run_id, 13)
            .expect_err("terminal run should not restart");
        assert!(err.contains("cannot start from Completed"));
    }
    #[test]
    fn upsert_session_preserves_created_at_for_existing_session() {
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session_id = crate::runtime::session::SessionId::for_scope(&scope);
        let first = session_fixture(&scope, 10, 20);
        let second = session_fixture(&scope, 30, 40);
        let mut state = RuntimeState::new();

        state.upsert_session(first);
        state.upsert_session(second);

        let session = state.session(&session_id).expect("session should exist");
        assert_eq!(session.created_at_unix(), 10);
        assert_eq!(session.updated_at_unix(), 40);

        state.upsert_session(session_fixture(&scope, 5, 15));
        let session = state.session(&session_id).expect("session should exist");
        assert_eq!(session.created_at_unix(), 10);
        assert_eq!(session.updated_at_unix(), 40);

        state.validate().expect("state should remain valid");
    }
    #[test]
    fn add_outbound_delivery_advances_state_updated_at_to_delivery_record() {
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session = session_fixture(&scope, 10, 10);
        let session_id = session.id().clone();
        let delivery = outbound_delivery_fixture("out_future", session_id, FUTURE_UNIX);
        let mut state = RuntimeState::new();

        state.upsert_session(session);
        state
            .enqueue_outbound_delivery(delivery)
            .expect("delivery should be accepted");

        assert!(state.updated_at_unix() >= FUTURE_UNIX);
        state.validate().expect("state should remain valid");
    }
    #[test]
    fn upsert_session_advances_state_updated_at_to_inserted_session() {
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session = session_fixture(&scope, 10, FUTURE_UNIX);
        let mut state = RuntimeState::new();

        state.upsert_session(session);

        assert!(state.updated_at_unix() >= FUTURE_UNIX);
        state.validate().expect("state should remain valid");
    }
    #[test]
    fn upsert_session_advances_state_updated_at_to_refreshed_session() {
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let first = session_fixture(&scope, 10, 20);
        let replacement = session_fixture(&scope, 10, FUTURE_UNIX);
        let mut state = RuntimeState::new();

        state.upsert_session(first);
        state.upsert_session(replacement);

        assert!(state.updated_at_unix() >= FUTURE_UNIX);
        state.validate().expect("state should remain valid");
    }
    #[test]
    fn add_run_advances_state_updated_at_to_run_record() {
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session = session_fixture(&scope, 10, 10);
        let session_id = session.id().clone();
        let run = RunRecord::new(
            RunId::new("run_future").expect("valid run id"),
            session_id,
            FUTURE_UNIX,
        );
        let mut state = RuntimeState::new();

        state.upsert_session(session);
        state.add_run(run).expect("run should be accepted");

        assert!(state.updated_at_unix() >= FUTURE_UNIX);
        state.validate().expect("state should remain valid");
    }

    fn session_fixture(
        scope: &SessionScope,
        created_at_unix: u64,
        updated_at_unix: u64,
    ) -> Session {
        serde_json::from_str(&format!(
            r#"{{
            "id": "{}",
            "scope": {{"platform": "{}", "scope": "{}"}},
            "created_at_unix": {created_at_unix},
            "updated_at_unix": {updated_at_unix}
        }}"#,
            crate::runtime::session::SessionId::for_scope(scope),
            scope.platform(),
            scope.scope()
        ))
        .expect("session fixture should decode")
    }

    fn event_fixture(id: &str, received_at_unix: u64) -> Event {
        let message = Message::user_text("msg_1", None, "hello", 1).expect("valid message");
        Event::new(
            EventId::new(id).expect("valid event id"),
            EventSource::Platform,
            EventKind::MessageReceived { message },
            received_at_unix,
        )
    }

    fn outbound_delivery_fixture(
        id: &str,
        session_id: SessionId,
        created_at_unix: u64,
    ) -> OutboundDeliveryRecord {
        let message = Message::new(
            MessageId::new(format!("msg_{id}")).expect("valid message id"),
            Some(session_id.clone()),
            MessageAuthor::Agent,
            MessageContent::text("hello").expect("valid text"),
            created_at_unix,
        );

        OutboundDeliveryRecord::new(
            OutboundDeliveryId::new(id).expect("valid outbound id"),
            session_id,
            message,
            created_at_unix,
        )
        .expect("valid outbound delivery")
    }

    fn state_with_pending_run(run_id: &str) -> (RuntimeState, RunId) {
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session = Session::new(scope);
        let session_id = session.id().clone();
        let run_id = RunId::new(run_id).expect("valid run id");
        let run = RunRecord::new(run_id.clone(), session_id, 10);
        let mut state = RuntimeState::new();
        state.upsert_session(session);
        state.add_run(run).expect("run should be accepted");

        (state, run_id)
    }
}
