use std::{error::Error, fmt};

use crate::runtime::{
    message::{Message, MessageAuthor},
    policy::WorkspaceRoot,
    run::RunId,
    session::SessionId,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRunRequest {
    run_id: RunId,
    session_id: SessionId,
    messages: Vec<Message>,
    workspace_root: WorkspaceRoot,
}

impl AgentRunRequest {
    pub fn new(
        run_id: RunId,
        session_id: SessionId,
        messages: Vec<Message>,
        workspace_root: WorkspaceRoot,
    ) -> Result<Self, String> {
        if messages.is_empty() {
            return Err(format!(
                "agent run {run_id} request must contain at least one message"
            ));
        }

        for message in &messages {
            if message.author != MessageAuthor::User {
                return Err(format!(
                    "agent run {run_id} request message {} must be user-authored",
                    message.id
                ));
            }

            if message.session_id.as_ref() != Some(&session_id) {
                return Err(format!(
                    "agent run {run_id} request message {} does not match session {session_id}",
                    message.id
                ));
            }
        }

        Ok(Self {
            run_id,
            session_id,
            messages,
            workspace_root,
        })
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    pub fn messages(&self) -> &[Message] {
        &self.messages
    }

    pub fn workspace_root(&self) -> &WorkspaceRoot {
        &self.workspace_root
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRunOutput {
    session_id: SessionId,
    messages: Vec<Message>,
}

impl AgentRunOutput {
    pub fn new(request: &AgentRunRequest, messages: Vec<Message>) -> Result<Self, String> {
        if messages.is_empty() {
            return Err(format!(
                "agent run {} output must contain at least one message",
                request.run_id()
            ));
        }

        for message in &messages {
            if message.author != MessageAuthor::Agent {
                return Err(format!(
                    "agent run {} output message {} must be agent-authored",
                    request.run_id(),
                    message.id
                ));
            }

            if message.session_id.as_ref() != Some(request.session_id()) {
                return Err(format!(
                    "agent run {} output message {} does not match session {}",
                    request.run_id(),
                    message.id,
                    request.session_id()
                ));
            }
        }

        Ok(Self {
            session_id: request.session_id().clone(),
            messages,
        })
    }

    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    pub fn messages(&self) -> &[Message] {
        &self.messages
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentRunFailureKind {
    /// All adapter-owned execution, including its process tree, has terminated.
    Definite,
    /// The adapter cannot prove that all adapter-owned execution has terminated.
    Uncertain,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRunFailure {
    kind: AgentRunFailureKind,
    message: String,
}

impl AgentRunFailure {
    pub fn definite(message: impl Into<String>) -> Self {
        Self::new(AgentRunFailureKind::Definite, message)
    }

    pub fn uncertain(message: impl Into<String>) -> Self {
        Self::new(AgentRunFailureKind::Uncertain, message)
    }

    pub fn kind(&self) -> AgentRunFailureKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    fn new(kind: AgentRunFailureKind, message: impl Into<String>) -> Self {
        let message = message.into();
        let message = if message.trim().is_empty() {
            match kind {
                AgentRunFailureKind::Definite => {
                    "agent adapter reported a definite failure without an error message".to_owned()
                }
                AgentRunFailureKind::Uncertain => {
                    "agent adapter reported an uncertain failure without an error message"
                        .to_owned()
                }
            }
        } else {
            message
        };

        Self { kind, message }
    }
}

impl fmt::Display for AgentRunFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

impl Error for AgentRunFailure {}

pub trait AgentAdapter {
    /// A successful result asserts complete termination, just like a definite failure.
    fn execute_run(&mut self, request: &AgentRunRequest)
    -> Result<AgentRunOutput, AgentRunFailure>;
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    use super::{
        AgentAdapter, AgentRunFailure, AgentRunFailureKind, AgentRunOutput, AgentRunRequest,
    };
    use crate::runtime::{
        event::{Event, EventId, EventKind, EventSource},
        message::{Message, MessageAuthor, MessageContent, MessageId},
        policy::WorkspaceRoot,
        queue::{MessageBatchClaimOutcome, MessageQueuePolicy},
        run::RunId,
        session::{Session, SessionId, SessionScope},
        state::{RuntimeState, StateStore},
    };

    const FUTURE_UNIX: u64 = 4_102_444_800;
    static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn request_preserves_durable_identity_message_order_and_workspace() {
        let request = request_fixture("request-shape");

        assert_eq!(request.run_id().as_str(), "run_1");
        assert_eq!(
            request
                .messages()
                .iter()
                .map(|message| message.id.as_str())
                .collect::<Vec<_>>(),
            ["msg_1", "msg_2"]
        );
        assert_eq!(
            request.workspace_root().as_path(),
            absolute_path("workspace")
        );
        assert!(
            request
                .messages()
                .iter()
                .all(|message| message.session_id.as_ref() == Some(request.session_id()))
        );
    }

    #[test]
    fn request_rejects_empty_non_user_and_mismatched_session_messages() {
        let run_id = RunId::new("run_invalid").expect("valid run id");
        let session_id = SessionId::for_scope(
            &SessionScope::new("lark", "chat:oc_123").expect("valid session scope"),
        );
        let workspace_root =
            WorkspaceRoot::new(absolute_path("workspace")).expect("valid workspace root");

        assert!(
            AgentRunRequest::new(
                run_id.clone(),
                session_id.clone(),
                Vec::new(),
                workspace_root.clone(),
            )
            .is_err()
        );

        let err = AgentRunRequest::new(
            run_id.clone(),
            session_id.clone(),
            vec![agent_message("reply_1", session_id.clone(), "not input")],
            workspace_root.clone(),
        )
        .expect_err("agent-authored input should be rejected");
        assert!(err.contains("user-authored"));

        let other_session = SessionId::for_scope(
            &SessionScope::new("lark", "chat:other").expect("valid session scope"),
        );
        let err = AgentRunRequest::new(
            run_id,
            session_id,
            vec![
                Message::user_text(
                    "msg_other",
                    Some(other_session),
                    "wrong session",
                    FUTURE_UNIX,
                )
                .expect("valid user message"),
            ],
            workspace_root,
        )
        .expect_err("cross-session input should be rejected");
        assert!(err.contains("does not match session"));
    }

    #[test]
    fn output_accepts_non_empty_agent_messages_for_the_request_session() {
        let request = request_fixture("valid-output");
        let messages = vec![agent_message(
            "reply_1",
            request.session_id().clone(),
            "done",
        )];

        let output = AgentRunOutput::new(&request, messages.clone()).expect("valid output");

        assert_eq!(output.session_id(), request.session_id());
        assert_eq!(output.messages(), messages);
    }

    #[test]
    fn output_rejects_empty_non_agent_and_mismatched_session_messages() {
        let request = request_fixture("invalid-output");

        assert!(AgentRunOutput::new(&request, Vec::new()).is_err());

        let user_message = Message::user_text(
            "user_reply",
            Some(request.session_id().clone()),
            "not an agent reply",
            FUTURE_UNIX + 2,
        )
        .expect("valid user message");
        let err = AgentRunOutput::new(&request, vec![user_message])
            .expect_err("user output should be rejected");
        assert!(err.contains("agent-authored"));

        let system_message = Message::new(
            MessageId::new("system_reply").expect("valid message id"),
            Some(request.session_id().clone()),
            MessageAuthor::System,
            MessageContent::text("not an agent reply").expect("valid message content"),
            FUTURE_UNIX + 2,
        );
        let err = AgentRunOutput::new(&request, vec![system_message])
            .expect_err("system output should be rejected");
        assert!(err.contains("agent-authored"));

        let other_session =
            SessionId::for_scope(&SessionScope::new("lark", "chat:other").expect("valid scope"));
        let err = AgentRunOutput::new(
            &request,
            vec![agent_message("other_reply", other_session, "wrong session")],
        )
        .expect_err("cross-session output should be rejected");
        assert!(err.contains("does not match session"));
    }

    #[test]
    fn failures_preserve_termination_evidence_classification() {
        let definite = AgentRunFailure::definite("process tree terminated with an error");
        assert_eq!(definite.kind(), AgentRunFailureKind::Definite);
        assert_eq!(definite.message(), "process tree terminated with an error");

        let uncertain = AgentRunFailure::uncertain("process tree state is unknown");
        assert_eq!(uncertain.kind(), AgentRunFailureKind::Uncertain);
        assert_eq!(uncertain.message(), "process tree state is unknown");
    }

    #[test]
    fn failures_normalize_empty_messages() {
        assert!(
            AgentRunFailure::definite(" ")
                .message()
                .contains("definite")
        );
        assert!(
            AgentRunFailure::uncertain("")
                .message()
                .contains("uncertain")
        );
    }

    #[test]
    fn trait_exposes_the_normalized_request_and_output_contract() {
        struct EchoAdapter;

        impl AgentAdapter for EchoAdapter {
            fn execute_run(
                &mut self,
                request: &AgentRunRequest,
            ) -> Result<AgentRunOutput, AgentRunFailure> {
                AgentRunOutput::new(
                    request,
                    vec![agent_message(
                        "reply_1",
                        request.session_id().clone(),
                        "echo",
                    )],
                )
                .map_err(AgentRunFailure::definite)
            }
        }

        let request = request_fixture("trait-contract");
        let output = EchoAdapter
            .execute_run(&request)
            .expect("echo adapter should return valid output");

        assert_eq!(output.session_id(), request.session_id());
        assert_eq!(output.messages()[0].content.as_text(), Some("echo"));
    }

    fn request_fixture(name: &str) -> AgentRunRequest {
        let store = StateStore::new(test_path(name).join("runtime.state.json"));
        let session =
            Session::new(SessionScope::new("lark", "chat:oc_123").expect("valid session scope"));
        let session_id = session.id().clone();
        let mut state = RuntimeState::new();
        state.upsert_session(session);
        store.save(&state).expect("initial state should save");

        for (event_id, message_id, received_at) in [
            ("evt_1", "msg_1", FUTURE_UNIX),
            ("evt_2", "msg_2", FUTURE_UNIX + 1),
        ] {
            store
                .persist_inbound_event(&message_event(
                    event_id,
                    message_id,
                    &session_id,
                    received_at,
                ))
                .expect("inbound message should persist");
        }

        let run_id = RunId::new("run_1").expect("valid run id");
        let policy = MessageQueuePolicy::new(0, 2).expect("valid queue policy");
        let MessageBatchClaimOutcome::Claimed { input, .. } = store
            .claim_message_batch(run_id, &policy, FUTURE_UNIX + 1)
            .expect("message batch claim should succeed")
        else {
            panic!("full message batch should be ready");
        };
        let workspace_root =
            WorkspaceRoot::new(absolute_path("workspace")).expect("valid workspace root");

        AgentRunRequest::new(
            input.run_id().clone(),
            input.session_id().clone(),
            input
                .messages()
                .iter()
                .map(|queued| queued.message().clone())
                .collect(),
            workspace_root,
        )
        .expect("durable run input should create a valid request")
    }

    fn message_event(
        event_id: &str,
        message_id: &str,
        session_id: &SessionId,
        received_at_unix: u64,
    ) -> Event {
        let message = Message::user_text(
            message_id,
            Some(session_id.clone()),
            format!("message {message_id}"),
            received_at_unix,
        )
        .expect("valid user message");

        Event::new(
            EventId::new(event_id).expect("valid event id"),
            EventSource::Platform,
            EventKind::MessageReceived { message },
            received_at_unix,
        )
    }

    fn agent_message(id: &str, session_id: SessionId, text: &str) -> Message {
        Message::new(
            MessageId::new(id).expect("valid message id"),
            Some(session_id),
            MessageAuthor::Agent,
            MessageContent::text(text).expect("valid message content"),
            FUTURE_UNIX + 2,
        )
    }

    fn absolute_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("ferris-agent-bridge-{name}"))
    }

    fn test_path(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "ferris-agent-bridge-agent-adapter-{name}-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).expect("test dir should exist");
        path
    }
}
