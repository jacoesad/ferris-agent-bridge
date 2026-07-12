#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc, Barrier,
            atomic::{AtomicU64, Ordering},
        },
        thread,
    };

    use super::super::StateStore;
    use crate::runtime::{
        event::{Event, EventId, EventKind, EventSource, InboundEventRecordStatus},
        message::{Message, MessageAuthor, MessageContent, MessageId},
        persistence::fail_next_write_before_replace,
        session::{Session, SessionId, SessionScope},
        state::RuntimeState,
    };

    static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn inbound_message_persistence_records_ledger_and_queue_together() {
        let (store, session_id) = state_store_with_session("queue-persist", "chat:a");
        let event = message_event("evt_1", "msg_1", &session_id, 10);

        let status = store
            .persist_inbound_event(&event)
            .expect("message should persist");

        assert_eq!(status, InboundEventRecordStatus::Recorded);
        let state = store.load().expect("state should load");
        assert!(state.has_inbound_event(&event.id));
        assert_eq!(state.queued_messages().len(), 1);
        assert_eq!(state.queued_messages()[0].event_id(), &event.id);
        assert_eq!(state.queued_messages()[0].message().id.as_str(), "msg_1");
    }

    #[test]
    fn duplicate_inbound_message_does_not_duplicate_the_queue() {
        let (store, session_id) = state_store_with_session("queue-duplicate", "chat:a");
        let event = message_event("evt_1", "msg_1", &session_id, 10);

        assert_eq!(
            store
                .persist_inbound_event(&event)
                .expect("first message should persist"),
            InboundEventRecordStatus::Recorded
        );
        assert_eq!(
            store
                .persist_inbound_event(&event)
                .expect("duplicate message should be recognized"),
            InboundEventRecordStatus::Duplicate
        );

        let state = store.load().expect("state should load");
        assert_eq!(state.inbound_events().len(), 1);
        assert_eq!(state.queued_messages().len(), 1);
    }

    #[test]
    fn duplicate_inbound_messages_still_validate_shape_ownership_and_identity() {
        let (store, session_id) = state_store_with_session("queue-invalid-duplicate", "chat:a");
        let original = message_event("evt_1", "msg_1", &session_id, 10);
        store
            .persist_inbound_event(&original)
            .expect("original message should persist");

        let changed_kind = Event::new(
            original.id.clone(),
            EventSource::Platform,
            EventKind::RuntimeNotice {
                message: "changed kind".to_owned(),
            },
            10,
        );
        let err = store
            .persist_inbound_event(&changed_kind)
            .expect_err("queued message identity must not become a non-message event");
        assert!(err.contains("conflicts with a queued message record"));

        let missing_session = Event::new(
            original.id.clone(),
            EventSource::Platform,
            EventKind::MessageReceived {
                message: Message::user_text("msg_1", None, "hello", 10)
                    .expect("valid unbound message"),
            },
            10,
        );
        let err = store
            .persist_inbound_event(&missing_session)
            .expect_err("duplicate message without a session must fail closed");
        assert!(err.contains("must reference a session"));

        let missing_scope = SessionScope::new("lark", "chat:missing").expect("valid scope");
        let unknown_session =
            message_event("evt_1", "msg_1", &SessionId::for_scope(&missing_scope), 10);
        let err = store
            .persist_inbound_event(&unknown_session)
            .expect_err("duplicate message for an unknown session must fail closed");
        assert!(err.contains("references unknown session"));

        let wrong_source = Event::new(
            original.id.clone(),
            EventSource::Runtime,
            EventKind::MessageReceived {
                message: Message::user_text("msg_1", Some(session_id.clone()), "hello", 10)
                    .expect("valid message"),
            },
            10,
        );
        let err = store
            .persist_inbound_event(&wrong_source)
            .expect_err("duplicate message from a non-platform source must fail closed");
        assert!(err.contains("must come from a platform"));

        let non_user = Event::new(
            original.id.clone(),
            EventSource::Platform,
            EventKind::MessageReceived {
                message: Message::new(
                    MessageId::new("msg_1").expect("valid message id"),
                    Some(session_id.clone()),
                    MessageAuthor::Agent,
                    MessageContent::text("hello").expect("valid text"),
                    10,
                ),
            },
            10,
        );
        let err = store
            .persist_inbound_event(&non_user)
            .expect_err("duplicate non-user message must fail closed");
        assert!(err.contains("must contain a user-authored message"));

        let conflicting_payload = Event::new(
            original.id.clone(),
            EventSource::Platform,
            EventKind::MessageReceived {
                message: Message::user_text(
                    "msg_1",
                    Some(session_id.clone()),
                    "different payload",
                    10,
                )
                .expect("valid conflicting message"),
            },
            10,
        );
        let err = store
            .persist_inbound_event(&conflicting_payload)
            .expect_err("duplicate id with a different payload must fail closed");
        assert!(err.contains("conflicting queued message event"));

        let conflicting_receive_time = Event::new(
            original.id.clone(),
            EventSource::Platform,
            EventKind::MessageReceived {
                message: Message::user_text("msg_1", Some(session_id), "hello", 10)
                    .expect("valid message"),
            },
            11,
        );
        let err = store
            .persist_inbound_event(&conflicting_receive_time)
            .expect_err("duplicate id with a different receive time must fail closed");
        assert!(err.contains("conflicting inbound event"));

        let state = store.load().expect("state should remain readable");
        assert_eq!(state.inbound_events().len(), 1);
        assert_eq!(state.queued_messages().len(), 1);
        assert_eq!(state.queued_messages()[0].event_id(), &original.id);
    }

    #[test]
    fn message_duplicate_without_an_existing_queue_record_fails_closed() {
        let (store, session_id) =
            state_store_with_session("queue-duplicate-without-record", "chat:a");
        let notice = Event::new(
            EventId::new("evt_1").expect("valid event id"),
            EventSource::Runtime,
            EventKind::RuntimeNotice {
                message: "notice".to_owned(),
            },
            10,
        );
        store
            .persist_inbound_event(&notice)
            .expect("non-message event should persist without queue work");
        let message = message_event("evt_1", "msg_1", &session_id, 10);

        let err = store
            .persist_inbound_event(&message)
            .expect_err("message identity without an existing queue record must fail closed");

        assert!(err.contains("has no queued message record"));
        let state = store.load().expect("state should remain readable");
        assert_eq!(state.inbound_events().len(), 1);
        assert!(state.queued_messages().is_empty());
    }

    #[test]
    fn inbound_message_requires_a_known_session_before_persistence() {
        let store = StateStore::new(test_path("queue-unknown-session").join("runtime.state.json"));
        let scope = SessionScope::new("lark", "chat:missing").expect("valid scope");
        let event = message_event("evt_1", "msg_1", &SessionId::for_scope(&scope), 10);

        let err = store
            .persist_inbound_event(&event)
            .expect_err("unknown session must reject queue persistence");

        assert!(err.contains("references unknown session"));
        let state = store.load().expect("state should remain readable");
        assert!(state.inbound_events().is_empty());
        assert!(state.queued_messages().is_empty());
    }

    #[test]
    fn persistence_failure_returns_no_acknowledgeable_status_or_queue_entry() {
        let (store, session_id) = state_store_with_session("queue-persist-failure", "chat:a");
        let event = message_event("evt_1", "msg_1", &session_id, 10);
        fail_next_write_before_replace(store.path());

        let err = store
            .persist_inbound_event(&event)
            .expect_err("failed persistence must not return a status");

        assert!(err.contains("failed to save runtime state"));
        let state = store.load().expect("previous state should remain readable");
        assert!(state.inbound_events().is_empty());
        assert!(state.queued_messages().is_empty());
    }

    #[test]
    fn stale_snapshot_save_preserves_durably_queued_messages() {
        let (store, session_id) = state_store_with_session("queue-stale-save", "chat:a");
        let stale_snapshot = store.load().expect("state should load");
        let event = message_event("evt_1", "msg_1", &session_id, 10);
        store
            .persist_inbound_event(&event)
            .expect("message should persist");

        store
            .save(&stale_snapshot)
            .expect("stale save should preserve queue additions");

        let state = store.load().expect("state should load");
        assert!(state.has_inbound_event(&event.id));
        assert_eq!(state.queued_messages().len(), 1);
        assert_eq!(state.queued_messages()[0].event_id(), &event.id);
    }

    #[test]
    fn stale_snapshot_save_keeps_durable_messages_before_candidate_additions() {
        const FUTURE_UNIX: u64 = 4_102_444_800;

        let (store, session_id) = state_store_with_session("queue-stale-order", "chat:a");
        let mut stale_snapshot = store.load().expect("state should load");
        let durable_event = message_event("evt_durable", "msg_durable", &session_id, FUTURE_UNIX);
        let candidate_event =
            message_event("evt_candidate", "msg_candidate", &session_id, FUTURE_UNIX);
        store
            .persist_inbound_event(&durable_event)
            .expect("durable message should persist first");
        stale_snapshot
            .record_inbound_event(&candidate_event)
            .expect("candidate message should record in the stale snapshot");

        store
            .save(&stale_snapshot)
            .expect("stale save should append candidate work after durable work");

        let state = store.load().expect("state should load");
        let inbound_ids = state
            .inbound_events()
            .iter()
            .map(|event| event.id().as_str())
            .collect::<Vec<_>>();
        let queued_ids = state
            .queued_messages()
            .iter()
            .map(|queued| queued.event_id().as_str())
            .collect::<Vec<_>>();
        assert_eq!(inbound_ids, ["evt_durable", "evt_candidate"]);
        assert_eq!(queued_ids, ["evt_durable", "evt_candidate"]);
    }

    #[test]
    fn stale_snapshot_rebases_candidate_enqueue_time_after_durable_tail() {
        const FUTURE_UNIX: u64 = 4_102_444_800;

        let (store, session_id) = state_store_with_session("queue-stale-rebase", "chat:a");
        let mut stale_snapshot = store.load().expect("state should load");
        let candidate_event =
            message_event("evt_candidate", "msg_candidate", &session_id, FUTURE_UNIX);
        let durable_event =
            message_event("evt_durable", "msg_durable", &session_id, FUTURE_UNIX + 10);
        stale_snapshot
            .record_inbound_event(&candidate_event)
            .expect("candidate message should record at the earlier time");
        store
            .persist_inbound_event(&durable_event)
            .expect("newer durable message should persist first");

        store
            .save(&stale_snapshot)
            .expect("stale candidate should rebase after the durable queue tail");

        let state = store.load().expect("state should load");
        let queued = state.queued_messages();
        assert_eq!(queued[0].event_id(), &durable_event.id);
        assert_eq!(queued[1].event_id(), &candidate_event.id);
        assert_eq!(queued[0].enqueued_at_unix(), FUTURE_UNIX + 10);
        assert_eq!(queued[1].enqueued_at_unix(), FUTURE_UNIX + 10);
    }

    #[test]
    fn stale_snapshot_without_queue_session_fails_closed() {
        let path = test_path("queue-stale-save-missing-session").join("runtime.state.json");
        let stale_writer = StateStore::new(&path);
        let current_writer = StateStore::new(&path);
        let stale_snapshot = stale_writer.load().expect("missing state should load");
        let session = Session::new(SessionScope::new("lark", "chat:a").expect("valid scope"));
        let session_id = session.id().clone();
        let event = message_event("evt_1", "msg_1", &session_id, 10);
        let mut current = RuntimeState::new();
        current.upsert_session(session);
        current_writer
            .save(&current)
            .expect("current session should persist");
        current_writer
            .persist_inbound_event(&event)
            .expect("current message should persist");

        let err = stale_writer
            .save(&stale_snapshot)
            .expect_err("stale state without the queue session must fail closed");

        assert!(err.contains("references unknown session"));
        let state = current_writer
            .load()
            .expect("current state should remain readable");
        assert!(state.session(&session_id).is_some());
        assert!(state.has_inbound_event(&event.id));
        assert_eq!(state.queued_messages().len(), 1);
        assert_eq!(state.queued_messages()[0].event_id(), &event.id);
    }

    #[test]
    fn stale_snapshot_save_rejects_shared_event_ids_with_different_queue_presence() {
        const FUTURE_UNIX: u64 = 4_102_444_800;

        for (label, save_message_first) in [
            ("queue-stale-kind-message-first", true),
            ("queue-stale-kind-notice-first", false),
        ] {
            let (store, session_id) = state_store_with_session(label, "chat:a");
            let initial = store.load().expect("state should load");
            let mut message_snapshot = initial.clone();
            let mut notice_snapshot = initial;
            let message = message_event("evt_shared", "msg_shared", &session_id, FUTURE_UNIX);
            let notice = Event::new(
                EventId::new("evt_shared").expect("valid event id"),
                EventSource::Runtime,
                EventKind::RuntimeNotice {
                    message: "notice".to_owned(),
                },
                FUTURE_UNIX,
            );
            message_snapshot
                .record_inbound_event(&message)
                .expect("message snapshot should record");
            notice_snapshot
                .record_inbound_event(&notice)
                .expect("notice snapshot should record");

            let (winner, conflicting) = if save_message_first {
                (&message_snapshot, &notice_snapshot)
            } else {
                (&notice_snapshot, &message_snapshot)
            };
            store.save(winner).expect("first snapshot should persist");
            let err = store
                .save(conflicting)
                .expect_err("shared event id with different queue presence must fail closed");

            assert!(err.contains("conflicting inbound event queue identity"));
            let state = store.load().expect("winning state should remain readable");
            assert_eq!(state.inbound_events().len(), 1);
            assert_eq!(
                state.queued_messages().len(),
                usize::from(save_message_first)
            );
        }
    }

    #[test]
    fn stale_snapshot_save_rejects_conflicting_shared_message_payloads() {
        const FUTURE_UNIX: u64 = 4_102_444_800;

        let (store, session_id) = state_store_with_session("queue-stale-payload", "chat:a");
        let initial = store.load().expect("state should load");
        let mut first_snapshot = initial.clone();
        let mut second_snapshot = initial;
        let first = message_event("evt_shared", "msg_shared", &session_id, FUTURE_UNIX);
        let second = Event::new(
            EventId::new("evt_shared").expect("valid event id"),
            EventSource::Platform,
            EventKind::MessageReceived {
                message: Message::user_text(
                    "msg_shared",
                    Some(session_id),
                    "different payload",
                    FUTURE_UNIX,
                )
                .expect("valid conflicting message"),
            },
            FUTURE_UNIX,
        );
        first_snapshot
            .record_inbound_event(&first)
            .expect("first snapshot should record");
        second_snapshot
            .record_inbound_event(&second)
            .expect("second snapshot should record");
        store
            .save(&first_snapshot)
            .expect("first snapshot should persist");

        let err = store
            .save(&second_snapshot)
            .expect_err("conflicting shared queue payload must fail closed");

        assert!(err.contains("conflicting inbound event queue identity"));
        let state = store.load().expect("first state should remain readable");
        assert_eq!(
            state.queued_messages()[0].message().content.as_text(),
            Some("hello")
        );
    }

    #[test]
    fn stale_snapshot_keeps_durable_time_for_compatible_shared_queue_record() {
        const FUTURE_UNIX: u64 = 4_102_444_800;

        let (store, session_id) =
            state_store_with_session("queue-stale-shared-enqueue-time", "chat:a");
        let initial = store.load().expect("state should load");
        let mut durable_snapshot = initial.clone();
        let mut stale_snapshot = initial;
        let shared_event = message_event("evt_shared", "msg_shared", &session_id, FUTURE_UNIX);
        let candidate_only_event = message_event(
            "evt_candidate",
            "msg_candidate",
            &session_id,
            FUTURE_UNIX + 10,
        );
        durable_snapshot
            .record_inbound_event(&shared_event)
            .expect("durable snapshot should record the shared event");
        stale_snapshot
            .record_inbound_event(&candidate_only_event)
            .expect("stale snapshot should record candidate-only work");
        stale_snapshot
            .record_inbound_event(&shared_event)
            .expect("stale snapshot should record the shared event at its later queue tail");
        assert_eq!(
            stale_snapshot.queued_messages()[1].enqueued_at_unix(),
            FUTURE_UNIX + 10
        );
        store
            .save(&durable_snapshot)
            .expect("durable snapshot should persist first");

        store
            .save(&stale_snapshot)
            .expect("compatible shared identity should keep the durable queue record");

        let state = store.load().expect("merged state should load");
        let queued = state.queued_messages();
        assert_eq!(queued.len(), 2);
        assert_eq!(queued[0].event_id(), &shared_event.id);
        assert_eq!(queued[0].enqueued_at_unix(), FUTURE_UNIX);
        assert_eq!(queued[1].event_id(), &candidate_only_event.id);
        assert_eq!(queued[1].enqueued_at_unix(), FUTURE_UNIX + 10);
    }

    #[test]
    fn concurrent_writers_preserve_every_queued_message() {
        let (store, session_id) = state_store_with_session("queue-concurrent", "chat:a");
        let worker_count = 12;
        let barrier = Arc::new(Barrier::new(worker_count));
        let mut workers = Vec::new();
        for index in 0..worker_count {
            let worker_store = StateStore::new(store.path());
            let worker_session_id = session_id.clone();
            let worker_barrier = barrier.clone();
            workers.push(thread::spawn(move || {
                let event = message_event(
                    &format!("evt_{index}"),
                    &format!("msg_{index}"),
                    &worker_session_id,
                    10 + index as u64,
                );
                worker_barrier.wait();
                worker_store.persist_inbound_event(&event)
            }));
        }

        for worker in workers {
            assert_eq!(
                worker
                    .join()
                    .expect("worker should not panic")
                    .expect("message should persist"),
                InboundEventRecordStatus::Recorded
            );
        }
        let state = store.load().expect("state should load");
        assert_eq!(state.inbound_events().len(), worker_count);
        assert_eq!(state.queued_messages().len(), worker_count);
    }

    fn state_store_with_session(label: &str, scope: &str) -> (StateStore, SessionId) {
        let store = StateStore::new(test_path(label).join("runtime.state.json"));
        let session = Session::new(SessionScope::new("lark", scope).expect("valid scope"));
        let session_id = session.id().clone();
        let mut state = RuntimeState::new();
        state.upsert_session(session);
        store.save(&state).expect("session should persist");
        (store, session_id)
    }

    fn message_event(
        event_id: &str,
        message_id: &str,
        session_id: &SessionId,
        received_at_unix: u64,
    ) -> Event {
        Event::new(
            EventId::new(event_id).expect("valid event id"),
            EventSource::Platform,
            EventKind::MessageReceived {
                message: Message::user_text(
                    message_id,
                    Some(session_id.clone()),
                    "hello",
                    received_at_unix,
                )
                .expect("valid message"),
            },
            received_at_unix,
        )
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "ferris-agent-bridge-{name}-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).expect("test dir should exist");
        path
    }
}
