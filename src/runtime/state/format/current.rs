use serde::{Deserialize, Serialize};

use crate::runtime::{
    event::InboundEventRecord, outbox::OutboundDeliveryRecord, run::RunRecord, session::Session,
};

use super::{
    super::model::RuntimeState,
    versioned,
    wire::{WireField, deserialize_wire_field},
};

pub const RUNTIME_STATE_FILE_VERSION: u32 = 4;
pub(super) const RUNTIME_STATE_FILE_V1_VERSION: u32 = 1;
pub(super) const RUNTIME_STATE_FILE_V2_VERSION: u32 = 2;
pub(super) const RUNTIME_STATE_FILE_V3_VERSION: u32 = 3;

pub(in crate::runtime::state) fn state_file_from_state(
    state: &RuntimeState,
) -> impl Serialize + '_ {
    RuntimeStateFile::from_state(state)
}

pub(in crate::runtime::state) fn parse_state_file(
    input: &str,
) -> Result<RuntimeState, serde_json::Error> {
    let state_file: RuntimeStateFileWire = serde_json::from_str(input)?;
    state_file
        .into_state()
        .map_err(<serde_json::Error as serde::de::Error>::custom)
}

#[derive(Serialize)]
struct RuntimeStateFile<'a> {
    version: u32,
    sessions: &'a [Session],
    runs: &'a [RunRecord],
    inbound_events: &'a [InboundEventRecord],
    outbound_deliveries: &'a [OutboundDeliveryRecord],
    updated_at_unix: u64,
}

impl<'a> RuntimeStateFile<'a> {
    fn from_state(state: &'a RuntimeState) -> Self {
        Self {
            version: RUNTIME_STATE_FILE_VERSION,
            sessions: state.sessions(),
            runs: state.runs(),
            inbound_events: state.inbound_events(),
            outbound_deliveries: state.outbound_deliveries(),
            updated_at_unix: state.updated_at_unix(),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeStateFileWire {
    version: u32,
    sessions: Vec<Session>,
    #[serde(default, deserialize_with = "deserialize_wire_field")]
    runs: WireField<Vec<RunRecord>>,
    #[serde(default, deserialize_with = "deserialize_wire_field")]
    inbound_events: WireField<Vec<InboundEventRecord>>,
    #[serde(default, deserialize_with = "deserialize_wire_field")]
    outbound_deliveries: WireField<Vec<OutboundDeliveryRecord>>,
    updated_at_unix: u64,
}

impl RuntimeStateFileWire {
    fn into_state(self) -> Result<RuntimeState, String> {
        let collections = versioned::decode_persisted_collections(
            self.version,
            self.runs,
            self.inbound_events,
            self.outbound_deliveries,
        )?;
        RuntimeState::from_persisted_parts(
            self.sessions,
            collections.runs,
            collections.inbound_events,
            collections.outbound_deliveries,
            self.updated_at_unix,
            collections.normalize_aggregate_updated_at,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::atomic::{AtomicU64, Ordering},
    };

    use crate::runtime::{
        event::{Event, EventId, EventKind, EventSource, InboundEventRecord},
        message::{Message, MessageAuthor, MessageContent, MessageId},
        outbox::{OutboundDeliveryId, OutboundDeliveryRecord},
        run::{RunId, RunRecord},
        session::{Session, SessionId, SessionScope},
        state::{RuntimeState, StateStore},
    };

    static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn state_store_writes_file_version_envelope() {
        let path = test_path("state-file-version-envelope").join("runtime.state.json");
        let store = StateStore::new(&path);

        store.save(&RuntimeState::new()).expect("state should save");

        let encoded: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(path).expect("state file should read"))
                .expect("state file should decode");

        assert_eq!(
            encoded.get("version").and_then(serde_json::Value::as_u64),
            Some(u64::from(super::RUNTIME_STATE_FILE_VERSION))
        );
        assert!(encoded.get("sessions").is_some());
        assert!(encoded.get("runs").is_some());
        assert!(encoded.get("updated_at_unix").is_some());
        assert!(encoded.get("inbound_events").is_some());
        assert!(encoded.get("outbound_deliveries").is_some());
    }
    #[test]
    fn state_load_rejects_stale_state_updated_at_for_sessions() {
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session = session_fixture(&scope, 10, 20);
        let path = test_path("state-stale-updated-at-session").join("runtime.state.json");
        let encoded = format!(
            r#"{{
            "version": {version},
            "sessions": [{session}],
            "runs": [],
            "inbound_events": [],
            "outbound_deliveries": [],
            "updated_at_unix": 1
        }}"#,
            version = super::RUNTIME_STATE_FILE_VERSION,
            session = serde_json::to_string(&session).expect("session should encode")
        );
        fs::write(&path, encoded).expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("state updated_at should not lag session records");

        assert!(err.contains("before session"));
    }
    #[test]
    fn state_load_rejects_stale_state_updated_at_for_runs() {
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session = session_fixture(&scope, 1, 1);
        let run = RunRecord::new(
            RunId::new("run_1").expect("valid run id"),
            session.id().clone(),
            10,
        );
        let path = test_path("state-stale-updated-at-run").join("runtime.state.json");
        let encoded = format!(
            r#"{{
            "version": {version},
            "sessions": [{session}],
            "runs": [{run}],
            "inbound_events": [],
            "outbound_deliveries": [],
            "updated_at_unix": 1
        }}"#,
            version = super::RUNTIME_STATE_FILE_VERSION,
            session = serde_json::to_string(&session).expect("session should encode"),
            run = serde_json::to_string(&run).expect("run should encode")
        );
        fs::write(&path, encoded).expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("state updated_at should not lag run records");

        assert!(err.contains("before run"));
    }
    #[test]
    fn state_load_rejects_stale_state_updated_at_for_inbound_events() {
        let event = event_fixture("evt_1", 10);
        let record = state_event_record(&event, 12).expect("inbound event record should build");
        let path = test_path("state-stale-updated-at-inbound-event").join("runtime.state.json");
        let encoded = format!(
            r#"{{
            "version": {version},
            "sessions": [],
            "runs": [],
            "inbound_events": [{record}],
            "outbound_deliveries": [],
            "updated_at_unix": 1
        }}"#,
            version = super::RUNTIME_STATE_FILE_VERSION,
            record = serde_json::to_string(&record).expect("event record should encode")
        );
        fs::write(&path, encoded).expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("state updated_at should not lag inbound event records");

        assert!(err.contains("before inbound event"));
    }
    #[test]
    fn state_load_rejects_stale_state_updated_at_for_outbound_deliveries() {
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session = session_fixture(&scope, 1, 1);
        let delivery = outbound_delivery_fixture("out_1", session.id().clone(), 12);
        let path = test_path("state-stale-updated-at-outbound-delivery").join("runtime.state.json");
        let encoded = format!(
            r#"{{
            "version": {version},
            "sessions": [{session}],
            "runs": [],
            "inbound_events": [],
            "outbound_deliveries": [{delivery}],
            "updated_at_unix": 1
        }}"#,
            version = super::RUNTIME_STATE_FILE_VERSION,
            session = serde_json::to_string(&session).expect("session should encode"),
            delivery = serde_json::to_string(&delivery).expect("delivery should encode")
        );
        fs::write(&path, encoded).expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("state updated_at should not lag outbound delivery records");

        assert!(err.contains("before outbound delivery"));
    }
    #[test]
    fn state_load_rejects_unknown_file_fields() {
        let path = test_path("state-unknown-file-fields").join("runtime.state.json");
        fs::write(
            &path,
            format!(
                r#"{{
                "version": {},
                "sessions": [],
                "runs": [],
                "inbound_events": [],
                "outbound_deliveries": [],
                "future_field": [],
                "updated_at_unix": 1
            }}"#,
                super::RUNTIME_STATE_FILE_VERSION
            ),
        )
        .expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("unknown state file fields must not be dropped");

        assert!(err.contains("unknown field `future_field`"));
    }
    #[test]
    fn state_load_rejects_unknown_session_fields() {
        let path = test_path("state-unknown-session-fields").join("runtime.state.json");
        fs::write(
            &path,
            format!(
                r#"{{
            "version": {},
            "sessions": [{{
                "id": "session_v1_4_6c61726b_b_636861743a6f635f313233",
                "scope": {{"platform": "lark", "scope": "chat:oc_123"}},
                "created_at_unix": 1,
                "updated_at_unix": 1,
                "future_field": true
            }}],
            "runs": [],
            "inbound_events": [],
            "outbound_deliveries": [],
            "updated_at_unix": 1
        }}"#,
                super::RUNTIME_STATE_FILE_VERSION
            ),
        )
        .expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("unknown session fields must not be dropped");

        assert!(err.contains("unknown field `future_field`"));
    }
    #[test]
    fn state_load_rejects_unknown_session_scope_fields() {
        let path = test_path("state-unknown-session-scope-fields").join("runtime.state.json");
        fs::write(
            &path,
            format!(
                r#"{{
            "version": {},
            "sessions": [{{
                "id": "session_v1_4_6c61726b_b_636861743a6f635f313233",
                "scope": {{
                    "platform": "lark",
                    "scope": "chat:oc_123",
                    "future_field": true
                }},
                "created_at_unix": 1,
                "updated_at_unix": 1
            }}],
            "runs": [],
            "inbound_events": [],
            "outbound_deliveries": [],
            "updated_at_unix": 1
        }}"#,
                super::RUNTIME_STATE_FILE_VERSION
            ),
        )
        .expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("unknown session scope fields must not be dropped");

        assert!(err.contains("unknown field `future_field`"));
    }
    #[test]
    fn state_load_rejects_unknown_run_fields() {
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session = Session::new(scope);
        let session_id = session.id().clone();
        let run = RunRecord::new(RunId::new("run_1").expect("valid run id"), session_id, 10);
        let path = test_path("state-unknown-run-fields").join("runtime.state.json");
        let encoded = format!(
            r#"{{
            "version": {version},
            "sessions": [{session}],
            "runs": [{{
                "id": "run_1",
                "session_id": "{session_id}",
                "status": "pending",
                "created_at_unix": 10,
                "updated_at_unix": 10,
                "started_at_unix": null,
                "finished_at_unix": null,
                "future_field": true
            }}],
            "inbound_events": [],
            "outbound_deliveries": [],
            "updated_at_unix": 1
        }}"#,
            version = super::RUNTIME_STATE_FILE_VERSION,
            session = serde_json::to_string(&session).expect("session should encode"),
            session_id = run.session_id()
        );
        fs::write(&path, encoded).expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("unknown run fields must not be dropped");

        assert!(err.contains("unknown field `future_field`"));
    }
    #[test]
    fn state_load_rejects_unknown_inbound_event_fields() {
        let path = test_path("state-unknown-inbound-event-fields").join("runtime.state.json");
        let encoded = format!(
            r#"{{
            "version": {},
            "sessions": [],
            "runs": [],
            "inbound_events": [{{
                "id": "evt_1",
                "received_at_unix": 10,
                "recorded_at_unix": 12,
                "future_field": true
            }}],
            "outbound_deliveries": [],
            "updated_at_unix": 1
        }}"#,
            super::RUNTIME_STATE_FILE_VERSION
        );
        fs::write(&path, encoded).expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("unknown inbound event fields must not be dropped");

        assert!(err.contains("unknown field `future_field`"));
    }
    #[test]
    fn state_load_rejects_unknown_outbound_delivery_fields() {
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session = session_fixture(&scope, 1, 1);
        let delivery = outbound_delivery_fixture("out_1", session.id().clone(), 12);
        let mut delivery_json =
            serde_json::to_value(&delivery).expect("delivery should encode as json");
        delivery_json["future_field"] = serde_json::Value::Bool(true);
        let path = test_path("state-unknown-outbound-delivery-fields").join("runtime.state.json");
        let encoded = format!(
            r#"{{
            "version": {version},
            "sessions": [{session}],
            "runs": [],
            "inbound_events": [],
            "outbound_deliveries": [{delivery}],
            "updated_at_unix": 12
        }}"#,
            version = super::RUNTIME_STATE_FILE_VERSION,
            session = serde_json::to_string(&session).expect("session should encode"),
            delivery = serde_json::to_string(&delivery_json).expect("delivery should encode")
        );
        fs::write(&path, encoded).expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("unknown outbound delivery fields must not be dropped");

        assert!(err.contains("unknown field `future_field`"));
    }
    #[test]
    fn state_load_rejects_unknown_outbound_delivery_message_fields() {
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session = session_fixture(&scope, 1, 1);
        let delivery = outbound_delivery_fixture("out_1", session.id().clone(), 12);
        let mut delivery_json =
            serde_json::to_value(&delivery).expect("delivery should encode as json");
        delivery_json["message"]["future_field"] = serde_json::Value::Bool(true);
        let path =
            test_path("state-unknown-outbound-delivery-message-fields").join("runtime.state.json");
        let encoded = format!(
            r#"{{
            "version": {version},
            "sessions": [{session}],
            "runs": [],
            "inbound_events": [],
            "outbound_deliveries": [{delivery}],
            "updated_at_unix": 12
        }}"#,
            version = super::RUNTIME_STATE_FILE_VERSION,
            session = serde_json::to_string(&session).expect("session should encode"),
            delivery = serde_json::to_string(&delivery_json).expect("delivery should encode")
        );
        fs::write(&path, encoded).expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("unknown nested message fields must not be dropped");

        assert!(err.contains("unknown field `future_field`"));
    }
    #[test]
    fn state_load_rejects_unknown_outbound_delivery_message_content_fields() {
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session = session_fixture(&scope, 1, 1);
        let delivery = outbound_delivery_fixture("out_1", session.id().clone(), 12);
        let mut delivery_json =
            serde_json::to_value(&delivery).expect("delivery should encode as json");
        delivery_json["message"]["content"]["future_field"] = serde_json::Value::Bool(true);
        let path = test_path("state-unknown-outbound-delivery-message-content-fields")
            .join("runtime.state.json");
        let encoded = format!(
            r#"{{
            "version": {version},
            "sessions": [{session}],
            "runs": [],
            "inbound_events": [],
            "outbound_deliveries": [{delivery}],
            "updated_at_unix": 12
        }}"#,
            version = super::RUNTIME_STATE_FILE_VERSION,
            session = serde_json::to_string(&session).expect("session should encode"),
            delivery = serde_json::to_string(&delivery_json).expect("delivery should encode")
        );
        fs::write(&path, encoded).expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("unknown nested message content fields must not be dropped");

        assert!(err.contains("unknown field `future_field`"));
    }
    #[test]
    fn state_validation_rejects_duplicate_session_ids() {
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session = Session::new(scope);
        let updated_at_unix = session.updated_at_unix();
        let path = test_path("state-duplicate-session-ids").join("runtime.state.json");
        let encoded = format!(
            r#"{{
            "version": {version},
            "sessions": [{session}, {session}],
            "runs": [],
            "inbound_events": [],
            "outbound_deliveries": [],
            "updated_at_unix": {updated_at_unix}
        }}"#,
            version = super::RUNTIME_STATE_FILE_VERSION,
            session = serde_json::to_string(&session).expect("session should encode")
        );
        fs::write(&path, encoded).expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("duplicate session ids should be rejected");

        assert!(err.contains("duplicate session id"));
    }
    #[test]
    fn state_validation_rejects_duplicate_run_ids() {
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session = Session::new(scope);
        let session_id = session.id().clone();
        let run = RunRecord::new(RunId::new("run_1").expect("valid run id"), session_id, 10);
        let updated_at_unix = session.updated_at_unix().max(run.updated_at_unix());
        let path = test_path("state-duplicate-run-ids").join("runtime.state.json");
        let encoded = format!(
            r#"{{
            "version": {version},
            "sessions": [{session}],
            "runs": [{run}, {run}],
            "inbound_events": [],
            "outbound_deliveries": [],
            "updated_at_unix": {updated_at_unix}
        }}"#,
            version = super::RUNTIME_STATE_FILE_VERSION,
            session = serde_json::to_string(&session).expect("session should encode"),
            run = serde_json::to_string(&run).expect("run should encode")
        );
        fs::write(&path, encoded).expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("duplicate run ids should be rejected");

        assert!(err.contains("duplicate run id"));
    }
    #[test]
    fn state_validation_rejects_run_without_session() {
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session_id = crate::runtime::session::SessionId::for_scope(&scope);
        let run = RunRecord::new(RunId::new("run_1").expect("valid run id"), session_id, 10);
        let updated_at_unix = run.updated_at_unix();
        let path = test_path("state-run-without-session").join("runtime.state.json");
        let encoded = format!(
            r#"{{
            "version": {version},
            "sessions": [],
            "runs": [{run}],
            "inbound_events": [],
            "outbound_deliveries": [],
            "updated_at_unix": {updated_at_unix}
        }}"#,
            version = super::RUNTIME_STATE_FILE_VERSION,
            run = serde_json::to_string(&run).expect("run should encode")
        );
        fs::write(&path, encoded).expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("run without known session should be rejected");

        assert!(err.contains("references unknown session"));
    }
    #[test]
    fn state_validation_rejects_duplicate_inbound_event_ids() {
        let event = event_fixture("evt_1", 10);
        let record = state_event_record(&event, 12).expect("inbound event record should build");
        let updated_at_unix = record.recorded_at_unix();
        let path = test_path("state-duplicate-inbound-event-ids").join("runtime.state.json");
        let encoded = format!(
            r#"{{
            "version": {version},
            "sessions": [],
            "runs": [],
            "inbound_events": [{record}, {record}],
            "outbound_deliveries": [],
            "updated_at_unix": {updated_at_unix}
        }}"#,
            version = super::RUNTIME_STATE_FILE_VERSION,
            record = serde_json::to_string(&record).expect("event record should encode")
        );
        fs::write(&path, encoded).expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("duplicate inbound event ids should be rejected");

        assert!(err.contains("duplicate inbound event id"));
    }
    #[test]
    fn state_validation_rejects_duplicate_outbound_delivery_ids() {
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session = session_fixture(&scope, 1, 1);
        let delivery = outbound_delivery_fixture("out_1", session.id().clone(), 12);
        let path = test_path("state-duplicate-outbound-delivery-ids").join("runtime.state.json");
        let encoded = format!(
            r#"{{
            "version": {version},
            "sessions": [{session}],
            "runs": [],
            "inbound_events": [],
            "outbound_deliveries": [{delivery}, {delivery}],
            "updated_at_unix": 12
        }}"#,
            version = super::RUNTIME_STATE_FILE_VERSION,
            session = serde_json::to_string(&session).expect("session should encode"),
            delivery = serde_json::to_string(&delivery).expect("delivery should encode")
        );
        fs::write(&path, encoded).expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("duplicate outbound delivery ids should be rejected");

        assert!(err.contains("duplicate outbound delivery id"));
    }
    #[test]
    fn state_validation_rejects_outbound_delivery_without_session() {
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session_id = SessionId::for_scope(&scope);
        let delivery = outbound_delivery_fixture("out_1", session_id, 12);
        let path = test_path("state-outbound-delivery-without-session").join("runtime.state.json");
        let encoded = format!(
            r#"{{
            "version": {version},
            "sessions": [],
            "runs": [],
            "inbound_events": [],
            "outbound_deliveries": [{delivery}],
            "updated_at_unix": 12
        }}"#,
            version = super::RUNTIME_STATE_FILE_VERSION,
            delivery = serde_json::to_string(&delivery).expect("delivery should encode")
        );
        fs::write(&path, encoded).expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("outbound delivery without known session should be rejected");

        assert!(err.contains("references unknown session"));
    }
    #[test]
    fn state_load_rejects_session_id_scope_mismatch() {
        let path = test_path("state-session-id-mismatch").join("runtime.state.json");
        fs::write(
            &path,
            format!(
                r#"{{
            "version": {},
            "sessions": [{{
                "id": "session_wrong",
                "scope": {{"platform": "lark", "scope": "chat:oc_123"}},
                "created_at_unix": 1,
                "updated_at_unix": 1
            }}],
            "runs": [],
            "inbound_events": [],
            "outbound_deliveries": [],
            "updated_at_unix": 1
        }}"#,
                super::RUNTIME_STATE_FILE_VERSION
            ),
        )
        .expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("session id should match derived scope id");

        assert!(err.contains("does not match derived id"));
    }
    #[test]
    fn state_load_rejects_session_time_order_mismatch() {
        let path = test_path("state-session-time-order").join("runtime.state.json");
        fs::write(
            &path,
            format!(
                r#"{{
            "version": {},
            "sessions": [{{
                "id": "session_v1_4_6c61726b_b_636861743a6f635f313233",
                "scope": {{"platform": "lark", "scope": "chat:oc_123"}},
                "created_at_unix": 100,
                "updated_at_unix": 1
            }}],
            "runs": [],
            "inbound_events": [],
            "outbound_deliveries": [],
            "updated_at_unix": 1
        }}"#,
                super::RUNTIME_STATE_FILE_VERSION
            ),
        )
        .expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("session updated_at should not be before created_at");

        assert!(err.contains("updated_at_unix before created_at_unix"));
    }
    fn test_path(name: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "ferris-agent-bridge-{name}-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).expect("test dir should exist");
        path
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
    fn state_event_record(
        event: &Event,
        recorded_at_unix: u64,
    ) -> Result<InboundEventRecord, String> {
        InboundEventRecord::from_event(event, recorded_at_unix)
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
}
