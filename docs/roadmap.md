# Roadmap

This roadmap describes the intended direction for `ferris-agent-bridge` during early `0.x` development. It is a planning guide, not a compatibility promise.

The project should stay Rust-first and local-first:

- Local agent processes run on the user's machine.
- Credentials, files, session state, and workspace policy stay local.
- Chat platforms and agent CLIs are connected through explicit adapter boundaries.

## Version Strategy

Patch releases in the `0.0.x` line should be limited to release cleanup, metadata, documentation, and small CLI polish.

Feature work starts with `0.1.0`. Each minor version should have one clear user-facing milestone and enough tests to keep the next milestone from rebuilding the previous one.

## Development Workflow

Roadmap milestones should be developed on focused feature branches. Branch naming, merge strategy, version updates, changelog updates, and release tagging are documented in [CONTRIBUTING.md](../CONTRIBUTING.md).

## Implementation Order

Implementation should start from the local service boundary and move outward:

- Build the daemon lifecycle first.
- Add the core runtime foundations (config, state, session, domain models).
- Add the runtime orchestrator (run lifecycle, events, policies, adapter contracts).
- Prove the agent side with one controlled adapter and a mock end-to-end test.
- Add the first chat platform adapter after the runtime and agent side are stable.

This keeps the early milestones independent of any single chat platform SDK. For Lark / Feishu, the OpenAPI HTTP client can be implemented in Rust when needed, while event transport remains a later IM adapter concern.

Adapter contracts should be capability-oriented rather than SDK-oriented. Future official SDKs or channel APIs should be replaceable behind those contracts without changing the core runtime.

Common interfaces should describe bridge capabilities rather than provider internals. Provider API clients, event transports, and event dispatchers are examples of how one platform implementation may be built; they are not required shapes for every IM adapter. Future adapters may wrap an official channel SDK directly, compose separate REST and event clients, or implement the missing side themselves. WebSocket and webhook should be transport implementations, not fixed architecture choices.

## Cross-Cutting Concerns

These concerns span multiple milestones and should be introduced early:

- **Structured logging and observability** — introduced in `0.2.0`. Structured log events with key fields (session, run, event ids) so runtime behavior is debuggable without `println!` debugging. Redact sensitive values (secrets, tokens) in log output.
- **Config compatibility** — config structs carry a `version` field from `0.2.0`. Future versions can read old configs and migrate them without breaking user setups.
- **Error handling policy** — established in `0.2.0`. Define which errors are fatal (panic/exit), which are recoverable (log + continue), and which are user-visible (returned as structured errors). Adapters must follow the same policy so failure behavior is consistent across platforms.

## 0.1.0 - Local Daemon Foundation

Goal:

Provide a usable local service foundation with basic lifecycle commands. This version should prove that the project can manage a local background process safely before it connects to chat platforms or real agent CLIs.

Design note: [0.1 Daemon Lifecycle Design](design/0.1-daemon-lifecycle.md).

Included:

- `run`, `start`, `stop`, and `status` CLI commands.
- `run` foreground mode for easier debugging.
- Runtime directory layout for local state.
- Atomic lock handling to prevent duplicate service instances.
- Stale lock detection and PID ownership validation.
- Basic service state reporting.
- Minimal logging suitable for local troubleshooting.
- Tests for command parsing and daemon lifecycle behavior.

Not included:

- Chat platform integration.
- Lark / Feishu OpenAPI or channel integration.
- Real agent CLI execution.
- Session continuity across chat conversations.
- Streaming replies.

Acceptance criteria:

- `ferris-agent-bridge run` runs the local daemon in the foreground.
- `ferris-agent-bridge start` starts a local background service.
- `ferris-agent-bridge status` reports whether the service is running.
- `ferris-agent-bridge stop` stops the service cleanly.
- Repeated starts do not create duplicate active services.
- Concurrent starts are serialized by the lock.
- Stale locks and PID reuse are detected without stopping unrelated processes.
- Shutdown has a graceful timeout and a clear failure path.
- The implementation is covered by focused tests and works on macOS as the first target.

## 0.2.0 - Runtime Foundations

Goal:

Introduce the config, state, session, and domain model layers. This is the infrastructure step — no orchestration or adapter logic yet. The focus is on stable data structures and safe persistence.

Design note: [0.2 Runtime Foundations Design](design/0.2-runtime-foundations.md).

Included:

- Profile and config loading with a `version` field for future migration.
- Config structs with `secret: Option<SecretInput>` placeholder (no keystore implementation yet).
- Runtime state storage with atomic write semantics.
- Session identity and continuity model.
- Core `message` and `event` domain models (platform-neutral).
- Structured logging with session and event identifiers.
- Secret-value redaction in log output.
- Error handling policy (fatal vs. recoverable vs. user-visible).

Not included:

- Run lifecycle state machine.
- Inbound event ledger or outbound outbox.
- Message queueing or batching.
- `ImAdapter` and `AgentAdapter` trait definitions.
- Policy evaluation.
- Any adapter implementation.

Acceptance criteria:

- Config can be loaded, validated, and saved safely.
- Sessions have stable identifiers and can be persisted and reloaded.
- Core domain types (`Message`, `Event`, `Session`) are defined and testable.
- Structured log events carry session and event context; run context is carried once available in later milestones.
- Secret-like values are redacted in log output.
- The daemon foundation from `0.1.0` remains intact.

## 0.3.0 - Runtime Orchestrator

Goal:

Layer the runtime orchestrator on top of `0.2.0` foundations. This includes the run lifecycle, event handling, policy evaluation, and adapter contracts.

Included:

- Run state machine (pending → running → completed / failed / cancelled).
- Inbound event ledger for duplicate delivery handling.
- Ack-after-persist contract.
- Outbound outbox for reliable reply delivery.
- Per-scope message queue with debounce (quiet window) and batching.
- Scope-level mutual exclusion: at most one active run per scope. Queued messages are blocked while a run is active, then flushed after completion.
- Concrete runtime orchestrator.
- `ImAdapter` and `AgentAdapter` capability traits.
- Workspace policy skeleton.
- Access policy skeleton.
- Startup recovery strategy (recover pending/running/failed runs into explicit states).

Not included:

- Full IM adapter support.
- Full agent adapter support.
- Platform-specific permissions or card rendering.
- Platform-specific auth, transport, OpenAPI, message, or event payload types.

Acceptance criteria:

- A run can be created, transitioned through states, and completed.
- Duplicate inbound events do not create duplicate runs.
- Messages within the same scope are debounced and batched before triggering a run.
- A scope with an active run does not start a new run until the current one completes.
- Runtime restart recovers pending, running, or failed runs into explicit states.
- Event acknowledgement happens only after the minimum durable state is recorded.
- Workspace and access policy decisions are explicit and testable.
- The daemon foundation from `0.1.0` remains intact.

## 0.4.0 - First Agent Adapter

Goal:

Prove the agent side of the bridge with one controlled adapter, and validate the full runtime → agent pipeline with a mock end-to-end smoke test.

Included:

- A mock or echo adapter as the first stable adapter target.
- A controlled fixture CLI that exercises the real process boundary.
- Process spawning abstraction for future real CLI adapters.
- Basic stdout, stderr, exit-code, timeout, and cancellation handling.
- Internal `AgentEvent` stream shape.
- Cancellation and timeout behavior.
- Mock end-to-end smoke test: mock IM adapter → runtime → mock agent adapter, validating the full scheduling pipeline without a real chat platform.
- Tests using fake processes or controlled adapters.

Not included:

- Broad support for multiple coding agents.
- Full Claude, Codex, or Trae compatibility.
- Chat platform integration.

Acceptance criteria:

- The runtime can start an adapter run and receive structured events.
- A fixture CLI proves spawning, stdout/stderr capture, exit-code mapping, timeout, and cancellation.
- Cancellation and timeout behavior are deterministic.
- Adapter output can be mapped into the common internal event model.
- The mock e2e smoke test passes: a simulated inbound message flows through the runtime to an agent adapter and produces a simulated reply.

## 0.5.0 - First Chat Platform Integration

Goal:

Complete the first minimal end-to-end bridge path.

Included:

- One initial chat platform adapter (Lark / Feishu).
- First concrete implementation of the `ImAdapter` boundary (defined in 0.3.0).
- Platform-specific auth: `app_secret` managed via local AES-256-GCM keystore. Env vars and files are supported as secret input sources, but persisted secrets must be stored in the encrypted keystore — never as plaintext in config.
- Event transport (WebSocket or webhook) for the selected platform.
- Platform API client for sending messages and reading metadata.
- Incoming message normalization.
- Outgoing reply path (text or markdown messages; no interactive cards).
- Connection between chat events, runtime sessions, and the first agent adapter.
- Minimal local configuration for the selected platform.
- Minimum safety envelope for remote-triggered local runs:
  - Three-tier access control: DM (owner + allowed users), group (owner + allowed chats), admin commands (owner only).
  - Owner resolved at runtime via the platform API, refreshed periodically.
  - Deny-by-default: unknown chats and users are rejected before any local execution.

Not included:

- Broad multi-platform support.
- Rich interactive cards.
- Advanced attachment handling.
- Production-grade permission policy beyond the minimum safety envelope.

Acceptance criteria:

- A chat message can trigger a local runtime run.
- The runtime can call the first agent adapter.
- A reply can be sent back to the chat platform.
- `app_secret` is stored encrypted on disk (AES-256-GCM) and never appears in plaintext in config files or logs.
- Unknown chats and users are denied by default.
- Workspace allowlists and agent command or profile allowlists are enforced before local execution.
- Policy decisions are logged for local troubleshooting.
- Failures produce clear local logs and user-facing errors.

## Later Milestones

Future work should be planned after the first end-to-end path is stable.

Likely areas:

- Additional agent adapters (Claude, Codex, Trae).
- Additional chat platform adapters (Slack, Discord, Telegram).
- Alternative platform adapter implementations that wrap official SDKs when available.
- Attachment handling.
- Streaming updates.
- Workspace allowlists and stronger access policy.
- Cross-platform service management (launchd, systemd, Task Scheduler).
- Installer and release automation.
