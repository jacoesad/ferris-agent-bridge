use crate::runtime::{
    event::InboundEventRecord,
    outbox::OutboundDeliveryRecord,
    queue::{QueuedMessage, RunInputRecord},
    run::{RunRecord, RunStatus},
};

use super::{
    current::{
        RUNTIME_STATE_FILE_V1_VERSION, RUNTIME_STATE_FILE_V2_VERSION,
        RUNTIME_STATE_FILE_V3_VERSION, RUNTIME_STATE_FILE_V4_VERSION,
        RUNTIME_STATE_FILE_V5_VERSION, RUNTIME_STATE_FILE_V6_VERSION,
        RUNTIME_STATE_FILE_V7_VERSION, RUNTIME_STATE_FILE_VERSION,
    },
    wire::WireField,
};

pub(super) struct PersistedCollections {
    pub(super) runs: Vec<RunRecord>,
    pub(super) run_inputs: Vec<RunInputRecord>,
    pub(super) inbound_events: Vec<InboundEventRecord>,
    pub(super) queued_messages: Vec<QueuedMessage>,
    pub(super) outbound_deliveries: Vec<OutboundDeliveryRecord>,
    pub(super) normalize_aggregate_updated_at: bool,
}

pub(super) fn decode_persisted_collections(
    version: u32,
    runs: WireField<Vec<RunRecord>>,
    run_inputs: WireField<Vec<RunInputRecord>>,
    inbound_events: WireField<Vec<InboundEventRecord>>,
    queued_messages: WireField<Vec<QueuedMessage>>,
    outbound_deliveries: WireField<Vec<OutboundDeliveryRecord>>,
) -> Result<PersistedCollections, String> {
    let (
        runs,
        run_inputs,
        inbound_events,
        queued_messages,
        outbound_deliveries,
        normalize_aggregate_updated_at,
    ) = match version {
        RUNTIME_STATE_FILE_V1_VERSION => {
            if runs.is_present() {
                return Err("runtime state version 1 must not contain run records".to_string());
            }

            if inbound_events.is_present() {
                return Err(
                    "runtime state version 1 must not contain inbound event records".to_string(),
                );
            }

            if outbound_deliveries.is_present() {
                return Err(
                    "runtime state version 1 must not contain outbound deliveries".to_string(),
                );
            }

            if queued_messages.is_present() {
                return Err("runtime state version 1 must not contain queued messages".to_string());
            }

            if run_inputs.is_present() {
                return Err("runtime state version 1 must not contain run inputs".to_string());
            }

            (
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                true,
            )
        }
        RUNTIME_STATE_FILE_V2_VERSION => {
            let runs = runs.into_required("runtime state version 2 must contain run records")?;
            reject_interrupted_runs(RUNTIME_STATE_FILE_V2_VERSION, &runs)?;
            reject_output_linked_runs(RUNTIME_STATE_FILE_V2_VERSION, &runs)?;

            if inbound_events.is_present() {
                return Err(
                    "runtime state version 2 must not contain inbound event records".to_string(),
                );
            }

            if outbound_deliveries.is_present() {
                return Err(
                    "runtime state version 2 must not contain outbound deliveries".to_string(),
                );
            }

            if queued_messages.is_present() {
                return Err("runtime state version 2 must not contain queued messages".to_string());
            }

            if run_inputs.is_present() {
                return Err("runtime state version 2 must not contain run inputs".to_string());
            }

            (runs, Vec::new(), Vec::new(), Vec::new(), Vec::new(), true)
        }
        RUNTIME_STATE_FILE_V3_VERSION => {
            let runs = runs.into_required("runtime state version 3 must contain run records")?;
            reject_interrupted_runs(RUNTIME_STATE_FILE_V3_VERSION, &runs)?;
            reject_output_linked_runs(RUNTIME_STATE_FILE_V3_VERSION, &runs)?;
            let inbound_events = inbound_events
                .into_required("runtime state version 3 must contain inbound event records")?;

            if outbound_deliveries.is_present() {
                return Err(
                    "runtime state version 3 must not contain outbound deliveries".to_string(),
                );
            }

            if queued_messages.is_present() {
                return Err("runtime state version 3 must not contain queued messages".to_string());
            }

            if run_inputs.is_present() {
                return Err("runtime state version 3 must not contain run inputs".to_string());
            }

            (
                runs,
                Vec::new(),
                inbound_events,
                Vec::new(),
                Vec::new(),
                false,
            )
        }
        RUNTIME_STATE_FILE_V4_VERSION => {
            let runs = runs.into_required("runtime state version 4 must contain run records")?;
            reject_interrupted_runs(RUNTIME_STATE_FILE_V4_VERSION, &runs)?;
            reject_output_linked_runs(RUNTIME_STATE_FILE_V4_VERSION, &runs)?;
            let inbound_events = inbound_events
                .into_required("runtime state version 4 must contain inbound event records")?;
            let outbound_deliveries = outbound_deliveries
                .into_required("runtime state version 4 must contain outbound deliveries")?;

            if queued_messages.is_present() {
                return Err("runtime state version 4 must not contain queued messages".to_string());
            }

            if run_inputs.is_present() {
                return Err("runtime state version 4 must not contain run inputs".to_string());
            }

            (
                runs,
                Vec::new(),
                inbound_events,
                Vec::new(),
                outbound_deliveries,
                false,
            )
        }
        RUNTIME_STATE_FILE_V5_VERSION => {
            let runs = runs.into_required("runtime state version 5 must contain run records")?;
            reject_interrupted_runs(RUNTIME_STATE_FILE_V5_VERSION, &runs)?;
            reject_output_linked_runs(RUNTIME_STATE_FILE_V5_VERSION, &runs)?;
            let inbound_events = inbound_events
                .into_required("runtime state version 5 must contain inbound event records")?;
            let queued_messages = queued_messages
                .into_required("runtime state version 5 must contain queued messages")?;
            let outbound_deliveries = outbound_deliveries
                .into_required("runtime state version 5 must contain outbound deliveries")?;

            if run_inputs.is_present() {
                return Err("runtime state version 5 must not contain run inputs".to_string());
            }

            (
                runs,
                Vec::new(),
                inbound_events,
                queued_messages,
                outbound_deliveries,
                false,
            )
        }
        RUNTIME_STATE_FILE_V6_VERSION => {
            let runs = runs.into_required("runtime state version 6 must contain run records")?;
            reject_interrupted_runs(RUNTIME_STATE_FILE_V6_VERSION, &runs)?;
            reject_output_linked_runs(RUNTIME_STATE_FILE_V6_VERSION, &runs)?;
            let inbound_events = inbound_events
                .into_required("runtime state version 6 must contain inbound event records")?;
            let outbound_deliveries = outbound_deliveries
                .into_required("runtime state version 6 must contain outbound deliveries")?;
            let queued_messages = queued_messages
                .into_required("runtime state version 6 must contain queued messages")?;
            let run_inputs =
                run_inputs.into_required("runtime state version 6 must contain run inputs")?;

            (
                runs,
                run_inputs,
                inbound_events,
                queued_messages,
                outbound_deliveries,
                false,
            )
        }
        RUNTIME_STATE_FILE_V7_VERSION => {
            let runs = runs.into_required("runtime state version 7 must contain run records")?;
            reject_output_linked_runs(RUNTIME_STATE_FILE_V7_VERSION, &runs)?;
            let inbound_events = inbound_events
                .into_required("runtime state version 7 must contain inbound event records")?;
            let outbound_deliveries = outbound_deliveries
                .into_required("runtime state version 7 must contain outbound deliveries")?;
            let queued_messages = queued_messages
                .into_required("runtime state version 7 must contain queued messages")?;
            let run_inputs =
                run_inputs.into_required("runtime state version 7 must contain run inputs")?;

            (
                runs,
                run_inputs,
                inbound_events,
                queued_messages,
                outbound_deliveries,
                false,
            )
        }
        RUNTIME_STATE_FILE_VERSION => {
            let runs = runs.into_required(format!(
                "runtime state version {RUNTIME_STATE_FILE_VERSION} must contain run records"
            ))?;
            let inbound_events = inbound_events.into_required(format!(
                "runtime state version {RUNTIME_STATE_FILE_VERSION} must contain inbound event records"
            ))?;
            let outbound_deliveries = outbound_deliveries.into_required(format!(
                "runtime state version {RUNTIME_STATE_FILE_VERSION} must contain outbound deliveries"
            ))?;
            let queued_messages = queued_messages.into_required(format!(
                "runtime state version {RUNTIME_STATE_FILE_VERSION} must contain queued messages"
            ))?;
            let run_inputs = run_inputs.into_required(format!(
                "runtime state version {RUNTIME_STATE_FILE_VERSION} must contain run inputs"
            ))?;

            (
                runs,
                run_inputs,
                inbound_events,
                queued_messages,
                outbound_deliveries,
                false,
            )
        }
        version => {
            return Err(format!(
                "unsupported runtime state version {}; expected {}",
                version, RUNTIME_STATE_FILE_VERSION
            ));
        }
    };

    Ok(PersistedCollections {
        runs,
        run_inputs,
        inbound_events,
        queued_messages,
        outbound_deliveries,
        normalize_aggregate_updated_at,
    })
}

fn reject_interrupted_runs(version: u32, runs: &[RunRecord]) -> Result<(), String> {
    if runs
        .iter()
        .any(|run| run.status() == RunStatus::Interrupted)
    {
        return Err(format!(
            "runtime state version {version} must not contain interrupted runs"
        ));
    }

    Ok(())
}

fn reject_output_linked_runs(version: u32, runs: &[RunRecord]) -> Result<(), String> {
    if runs.iter().any(|run| !run.output_delivery_ids().is_empty()) {
        return Err(format!(
            "runtime state version {version} must not contain run output delivery ids"
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::atomic::{AtomicU64, Ordering},
    };

    use crate::runtime::{
        message::{Message, MessageAuthor, MessageContent, MessageId},
        outbox::{OutboundDeliveryId, OutboundDeliveryRecord},
        run::{RunId, RunRecord},
        session::{Session, SessionScope},
        state::StateStore,
    };

    static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn state_load_migrates_released_version_1_without_runs_or_inbound_event_records() {
        let path = test_path("state-v1-released-shape").join("runtime.state.json");
        fs::write(
            &path,
            r#"{
            "version": 1,
            "sessions": [],
            "updated_at_unix": 1
        }"#,
        )
        .expect("state fixture should write");
        let store = StateStore::new(path);

        let state = store
            .load()
            .expect("released version 1 state should migrate");

        assert!(state.runs().is_empty());
        assert!(state.inbound_events().is_empty());
    }
    #[test]
    fn state_load_migrates_version_1_stale_aggregate_updated_at() {
        let path = test_path("state-v1-stale-aggregate-updated-at").join("runtime.state.json");
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session = session_fixture(&scope, 10, 20);
        let encoded = format!(
            r#"{{
            "version": 1,
            "sessions": [{session}],
            "updated_at_unix": 1
        }}"#,
            session = serde_json::to_string(&session).expect("session should encode")
        );
        fs::write(&path, encoded).expect("state fixture should write");
        let store = StateStore::new(path);

        let state = store
            .load()
            .expect("version 1 state should normalize aggregate timestamps while migrating");

        assert_eq!(state.updated_at_unix(), 20);
        assert!(state.runs().is_empty());
        assert!(state.inbound_events().is_empty());
    }
    #[test]
    fn state_load_rejects_version_1_with_run_records() {
        let path = test_path("state-v1-with-runs").join("runtime.state.json");
        fs::write(
            &path,
            r#"{
            "version": 1,
            "sessions": [],
            "runs": [],
            "updated_at_unix": 1
        }"#,
        )
        .expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("version 1 state must not carry run records");

        assert!(err.contains("version 1 must not contain run records"));
    }
    #[test]
    fn state_load_rejects_version_1_with_null_run_records() {
        let path = test_path("state-v1-with-null-runs").join("runtime.state.json");
        fs::write(
            &path,
            r#"{
            "version": 1,
            "sessions": [],
            "runs": null,
            "updated_at_unix": 1
        }"#,
        )
        .expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("version 1 state must reject present null runs");

        assert!(err.contains("version 1 must not contain run records"));
    }
    #[test]
    fn state_load_rejects_version_1_with_inbound_event_records() {
        let path = test_path("state-v1-with-inbound-events").join("runtime.state.json");
        fs::write(
            &path,
            r#"{
            "version": 1,
            "sessions": [],
            "inbound_events": [],
            "updated_at_unix": 1
        }"#,
        )
        .expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("version 1 state must not carry inbound event records");

        assert!(err.contains("version 1 must not contain inbound event records"));
    }
    #[test]
    fn state_load_migrates_version_2_without_inbound_event_records() {
        let path = test_path("state-v2-without-inbound-events").join("runtime.state.json");
        fs::write(
            &path,
            r#"{
            "version": 2,
            "sessions": [],
            "runs": [],
            "updated_at_unix": 1
        }"#,
        )
        .expect("state fixture should write");
        let store = StateStore::new(path);

        let state = store
            .load()
            .expect("version 2 state without inbound event records should migrate");

        assert!(state.runs().is_empty());
        assert!(state.inbound_events().is_empty());
    }
    #[test]
    fn state_load_migrates_version_2_stale_aggregate_updated_at() {
        let path = test_path("state-v2-stale-aggregate-updated-at").join("runtime.state.json");
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session = session_fixture(&scope, 1, 2);
        let run = RunRecord::new(
            RunId::new("run_1").expect("valid run id"),
            session.id().clone(),
            20,
        );
        let encoded = format!(
            r#"{{
            "version": 2,
            "sessions": [{session}],
            "runs": [{run}],
            "updated_at_unix": 1
        }}"#,
            session = serde_json::to_string(&session).expect("session should encode"),
            run = serde_json::to_string(&run).expect("run should encode")
        );
        fs::write(&path, encoded).expect("state fixture should write");
        let store = StateStore::new(path);

        let state = store
            .load()
            .expect("version 2 state should normalize aggregate timestamps while migrating");

        assert_eq!(state.updated_at_unix(), 20);
        assert_eq!(state.runs().len(), 1);
        assert!(state.inbound_events().is_empty());
    }
    #[test]
    fn state_load_rejects_version_2_with_inbound_event_records() {
        let path = test_path("state-v2-with-inbound-events").join("runtime.state.json");
        fs::write(
            &path,
            r#"{
            "version": 2,
            "sessions": [],
            "runs": [],
            "inbound_events": [],
            "updated_at_unix": 1
        }"#,
        )
        .expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("version 2 state must not carry inbound event records");

        assert!(err.contains("version 2 must not contain inbound event records"));
    }
    #[test]
    fn state_load_rejects_version_2_with_null_inbound_event_records() {
        let path = test_path("state-v2-with-null-inbound-events").join("runtime.state.json");
        fs::write(
            &path,
            r#"{
            "version": 2,
            "sessions": [],
            "runs": [],
            "inbound_events": null,
            "updated_at_unix": 1
        }"#,
        )
        .expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("version 2 state must reject present null inbound event records");

        assert!(err.contains("version 2 must not contain inbound event records"));
    }
    #[test]
    fn state_load_migrates_version_3_without_outbound_deliveries() {
        let path = test_path("state-v3-without-outbound-deliveries").join("runtime.state.json");
        fs::write(
            &path,
            r#"{
            "version": 3,
            "sessions": [],
            "runs": [],
            "inbound_events": [],
            "updated_at_unix": 1
        }"#,
        )
        .expect("state fixture should write");
        let store = StateStore::new(path);

        let state = store
            .load()
            .expect("version 3 state without outbound deliveries should migrate");

        assert!(state.runs().is_empty());
        assert!(state.inbound_events().is_empty());
        assert!(state.outbound_deliveries().is_empty());
    }
    #[test]
    fn state_load_rejects_version_3_with_outbound_deliveries() {
        let path = test_path("state-v3-with-outbound-deliveries").join("runtime.state.json");
        fs::write(
            &path,
            r#"{
            "version": 3,
            "sessions": [],
            "runs": [],
            "inbound_events": [],
            "outbound_deliveries": [],
            "updated_at_unix": 1
        }"#,
        )
        .expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("version 3 state must not carry outbound deliveries");

        assert!(err.contains("version 3 must not contain outbound deliveries"));
    }
    #[test]
    fn state_load_migrates_version_4_without_queued_messages() {
        let path = test_path("state-v4-without-queued-messages").join("runtime.state.json");
        fs::write(
            &path,
            r#"{
            "version": 4,
            "sessions": [],
            "runs": [],
            "inbound_events": [],
            "outbound_deliveries": [],
            "updated_at_unix": 1
        }"#,
        )
        .expect("state fixture should write");
        let store = StateStore::new(path);

        let state = store
            .load()
            .expect("version 4 state without queued messages should migrate");

        assert!(state.queued_messages().is_empty());
        assert!(state.outbound_deliveries().is_empty());
    }
    #[test]
    fn state_load_rejects_version_4_with_queued_messages() {
        let path = test_path("state-v4-with-queued-messages").join("runtime.state.json");
        fs::write(
            &path,
            r#"{
            "version": 4,
            "sessions": [],
            "runs": [],
            "inbound_events": [],
            "queued_messages": [],
            "outbound_deliveries": [],
            "updated_at_unix": 1
        }"#,
        )
        .expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("version 4 state must not carry queued messages");

        assert!(err.contains("version 4 must not contain queued messages"));
    }
    #[test]
    fn state_load_migrates_version_5_without_run_inputs() {
        let path = test_path("state-v5-without-run-inputs").join("runtime.state.json");
        fs::write(
            &path,
            r#"{
            "version": 5,
            "sessions": [],
            "runs": [],
            "inbound_events": [],
            "queued_messages": [],
            "outbound_deliveries": [],
            "updated_at_unix": 1
        }"#,
        )
        .expect("state fixture should write");
        let state = StateStore::new(path)
            .load()
            .expect("version 5 state should migrate with empty run inputs");

        assert!(state.run_inputs().is_empty());
    }
    #[test]
    fn state_load_rejects_version_5_with_run_inputs() {
        let path = test_path("state-v5-with-run-inputs").join("runtime.state.json");
        fs::write(
            &path,
            r#"{
            "version": 5,
            "sessions": [],
            "runs": [],
            "run_inputs": [],
            "inbound_events": [],
            "queued_messages": [],
            "outbound_deliveries": [],
            "updated_at_unix": 1
        }"#,
        )
        .expect("state fixture should write");
        let err = StateStore::new(path)
            .load()
            .expect_err("version 5 must reject present run inputs");

        assert!(err.contains("version 5 must not contain run inputs"));
    }
    #[test]
    fn state_load_migrates_version_6_without_interrupted_runs() {
        let path = test_path("state-v6-without-interrupted-runs").join("runtime.state.json");
        fs::write(
            &path,
            r#"{
            "version": 6,
            "sessions": [],
            "runs": [],
            "run_inputs": [],
            "inbound_events": [],
            "queued_messages": [],
            "outbound_deliveries": [],
            "updated_at_unix": 1
        }"#,
        )
        .expect("state fixture should write");
        let store = StateStore::new(&path);

        let state = store
            .load()
            .expect("version 6 state should migrate without changing collections");
        store.save(&state).expect("migrated state should save");
        let encoded: serde_json::Value = serde_json::from_slice(
            &fs::read(&path).expect("migrated state should remain readable"),
        )
        .expect("migrated state should decode");

        assert_eq!(
            encoded["version"].as_u64(),
            Some(u64::from(super::RUNTIME_STATE_FILE_VERSION))
        );
        assert!(state.runs().is_empty());
        assert!(state.run_inputs().is_empty());
    }
    #[test]
    fn state_load_rejects_interrupted_run_in_version_6() {
        let path = test_path("state-v6-with-interrupted-run").join("runtime.state.json");
        let store = StateStore::new(&path);
        let session = Session::new(
            SessionScope::new("lark", "chat:interrupted").expect("valid session scope"),
        );
        let mut run = RunRecord::new(
            RunId::new("run_1").expect("valid run id"),
            session.id().clone(),
            10,
        );
        run.interrupt(11).expect("run should interrupt");
        let mut state = crate::runtime::state::RuntimeState::new();
        state.upsert_session(session);
        state.add_run(run).expect("interrupted run should be valid");
        store.save(&state).expect("current state should save");
        let mut encoded: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).expect("current state should remain readable"))
                .expect("current state should decode");
        encoded["version"] = serde_json::json!(6);
        fs::write(
            &path,
            serde_json::to_vec(&encoded).expect("version 6 fixture should encode"),
        )
        .expect("version 6 fixture should write");

        let err = store
            .load()
            .expect_err("version 6 must not accept version 7 run statuses");

        assert!(err.contains("version 6 must not contain interrupted runs"));
    }
    #[test]
    fn state_load_migrates_version_7_without_output_delivery_links() {
        let path = test_path("state-v7-without-output-links").join("runtime.state.json");
        let session =
            Session::new(SessionScope::new("lark", "chat:v7").expect("valid session scope"));
        let run_id = RunId::new("run_v7").expect("valid run id");
        let mut state = crate::runtime::state::RuntimeState::new();
        state.upsert_session(session.clone());
        state
            .add_run(RunRecord::new(run_id.clone(), session.id().clone(), 10))
            .expect("version 7 pending run should be valid");
        let mut encoded = serde_json::to_value(&state).expect("version 7 state should encode");
        encoded["version"] = serde_json::json!(7);
        encoded["runs"][0]
            .as_object_mut()
            .expect("run should be an object")
            .remove("output_delivery_ids");
        fs::write(
            &path,
            serde_json::to_vec(&encoded).expect("version 7 fixture should encode"),
        )
        .expect("state fixture should write");
        let store = StateStore::new(&path);

        let state = store
            .load()
            .expect("version 7 state should migrate without output links");
        assert!(
            state
                .run(&run_id)
                .expect("migrated run should remain present")
                .output_delivery_ids()
                .is_empty()
        );
        store.save(&state).expect("migrated state should save");
        let encoded: serde_json::Value = serde_json::from_slice(
            &fs::read(&path).expect("migrated state should remain readable"),
        )
        .expect("migrated state should decode");

        assert_eq!(
            encoded["version"].as_u64(),
            Some(u64::from(super::RUNTIME_STATE_FILE_VERSION))
        );
        assert!(encoded["runs"][0].get("output_delivery_ids").is_some());
    }
    #[test]
    fn state_load_rejects_output_delivery_links_in_version_7() {
        let path = test_path("state-v7-with-output-links").join("runtime.state.json");
        let store = StateStore::new(&path);
        let session =
            Session::new(SessionScope::new("lark", "chat:output").expect("valid session scope"));
        let run_id = RunId::new("run_1").expect("valid run id");
        let delivery_id = OutboundDeliveryId::new("out_1").expect("valid delivery id");
        let message = Message::new(
            MessageId::new("msg_1").expect("valid message id"),
            Some(session.id().clone()),
            MessageAuthor::Agent,
            MessageContent::text("done").expect("valid content"),
            12,
        );
        let delivery =
            OutboundDeliveryRecord::new(delivery_id.clone(), session.id().clone(), message, 12)
                .expect("valid delivery");
        let mut run = RunRecord::new(run_id, session.id().clone(), 10);
        run.start(11).expect("run should start");
        run.complete_with_output_deliveries(12, vec![delivery_id])
            .expect("run should complete with output ownership");
        let mut state = crate::runtime::state::RuntimeState::new();
        state.upsert_session(session);
        state.add_run(run).expect("completed run should be valid");
        state
            .enqueue_outbound_delivery(delivery)
            .expect("linked output should enqueue");
        let mut encoded = serde_json::to_value(&state).expect("current state should encode");
        encoded["version"] = serde_json::json!(7);
        fs::write(
            &path,
            serde_json::to_vec(&encoded).expect("version 7 fixture should encode"),
        )
        .expect("version 7 fixture should write");

        let err = store
            .load()
            .expect_err("version 7 must not accept output delivery links");

        assert!(err.contains("version 7 must not contain run output delivery ids"));
    }
    #[test]
    fn state_load_rejects_future_file_version() {
        let path = test_path("state-future-version").join("runtime.state.json");
        fs::write(
            &path,
            r#"{
            "version": 9,
            "sessions": [],
            "runs": [],
            "inbound_events": [],
            "outbound_deliveries": [],
            "updated_at_unix": 1
        }"#,
        )
        .expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("future state versions must not be loaded");

        assert!(err.contains("unsupported runtime state version 9; expected 8"));
    }
    #[test]
    fn state_load_rejects_current_version_without_run_records() {
        let path = test_path("state-v3-without-runs").join("runtime.state.json");
        fs::write(
            &path,
            format!(
                r#"{{
                "version": {},
                "sessions": [],
                "updated_at_unix": 1
            }}"#,
                super::RUNTIME_STATE_FILE_VERSION
            ),
        )
        .expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("current state version must carry run records");

        assert!(err.contains("must contain run records"));
    }
    #[test]
    fn state_load_rejects_current_version_without_inbound_event_records() {
        let path = test_path("state-v3-without-inbound-events").join("runtime.state.json");
        fs::write(
            &path,
            format!(
                r#"{{
                "version": {},
                "sessions": [],
                "runs": [],
                "run_inputs": [],
                "updated_at_unix": 1
            }}"#,
                super::RUNTIME_STATE_FILE_VERSION
            ),
        )
        .expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("current state version must carry inbound events");

        assert!(err.contains("must contain inbound event records"));
    }
    #[test]
    fn state_load_rejects_current_version_with_null_inbound_event_records() {
        let path = test_path("state-v3-with-null-inbound-events").join("runtime.state.json");
        fs::write(
            &path,
            format!(
                r#"{{
                "version": {},
                "sessions": [],
                "runs": [],
                "run_inputs": [],
                "inbound_events": null,
                "updated_at_unix": 1
            }}"#,
                super::RUNTIME_STATE_FILE_VERSION
            ),
        )
        .expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("current state version must reject null inbound events");

        assert!(err.contains("must contain inbound event records"));
    }
    #[test]
    fn state_load_rejects_current_version_without_queued_messages() {
        let path = test_path("state-current-without-queued-messages").join("runtime.state.json");
        fs::write(
            &path,
            format!(
                r#"{{
                "version": {},
                "sessions": [],
                "runs": [],
                "run_inputs": [],
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
            .expect_err("current state version must carry queued messages");

        assert!(err.contains("must contain queued messages"));
    }
    #[test]
    fn state_load_rejects_current_version_with_null_queued_messages() {
        let path = test_path("state-current-with-null-queued-messages").join("runtime.state.json");
        fs::write(
            &path,
            format!(
                r#"{{
                "version": {},
                "sessions": [],
                "runs": [],
                "run_inputs": [],
                "inbound_events": [],
                "queued_messages": null,
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
            .expect_err("current state version must reject null queued messages");

        assert!(err.contains("must contain queued messages"));
    }
    #[test]
    fn state_load_rejects_current_version_without_outbound_deliveries() {
        let path = test_path("state-v4-without-outbound-deliveries").join("runtime.state.json");
        fs::write(
            &path,
            format!(
                r#"{{
                "version": {},
                "sessions": [],
                "runs": [],
                "run_inputs": [],
                "inbound_events": [],
                "queued_messages": [],
                "updated_at_unix": 1
            }}"#,
                super::RUNTIME_STATE_FILE_VERSION
            ),
        )
        .expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("current state version must carry outbound deliveries");

        assert!(err.contains("must contain outbound deliveries"));
    }
    #[test]
    fn state_load_rejects_current_version_with_null_outbound_deliveries() {
        let path = test_path("state-v4-with-null-outbound-deliveries").join("runtime.state.json");
        fs::write(
            &path,
            format!(
                r#"{{
                "version": {},
                "sessions": [],
                "runs": [],
                "run_inputs": [],
                "inbound_events": [],
                "queued_messages": [],
                "outbound_deliveries": null,
                "updated_at_unix": 1
            }}"#,
                super::RUNTIME_STATE_FILE_VERSION
            ),
        )
        .expect("state fixture should write");
        let store = StateStore::new(path);

        let err = store
            .load()
            .expect_err("current state version must reject null outbound deliveries");

        assert!(err.contains("must contain outbound deliveries"));
    }
    #[test]
    fn state_load_rejects_current_version_without_run_inputs() {
        let path = test_path("state-current-without-run-inputs").join("runtime.state.json");
        fs::write(
            &path,
            format!(
                r#"{{
                "version": {},
                "sessions": [],
                "runs": [],
                "inbound_events": [],
                "queued_messages": [],
                "outbound_deliveries": [],
                "updated_at_unix": 1
            }}"#,
                super::RUNTIME_STATE_FILE_VERSION
            ),
        )
        .expect("state fixture should write");
        let err = StateStore::new(path)
            .load()
            .expect_err("current state version must carry run inputs");

        assert!(err.contains("must contain run inputs"));
    }
    #[test]
    fn state_load_rejects_current_version_with_null_run_inputs() {
        let path = test_path("state-current-with-null-run-inputs").join("runtime.state.json");
        fs::write(
            &path,
            format!(
                r#"{{
                "version": {},
                "sessions": [],
                "runs": [],
                "run_inputs": null,
                "inbound_events": [],
                "queued_messages": [],
                "outbound_deliveries": [],
                "updated_at_unix": 1
            }}"#,
                super::RUNTIME_STATE_FILE_VERSION
            ),
        )
        .expect("state fixture should write");
        let err = StateStore::new(path)
            .load()
            .expect_err("current state version must reject null run inputs");

        assert!(err.contains("must contain run inputs"));
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
}
