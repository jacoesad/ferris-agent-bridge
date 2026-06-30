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
- Add the core runtime and session model.
- Prove the agent side with one controlled adapter.
- Add the first chat platform adapter after the runtime and agent side are stable.

This keeps the early milestones independent of any single chat platform SDK. For Lark / Feishu, the OpenAPI HTTP client can be implemented in Rust when needed, while event transport remains a later IM adapter concern.

Adapter contracts should be capability-oriented rather than SDK-oriented. Future official SDKs or channel APIs should be replaceable behind those contracts without changing the core runtime.

Common interfaces should describe bridge capabilities rather than provider internals. Provider API clients, event transports, and event dispatchers are examples of how one platform implementation may be built; they are not required shapes for every IM adapter. Future adapters may wrap an official channel SDK directly, compose separate REST and event clients, or implement the missing side themselves. WebSocket and webhook should be transport implementations, not fixed architecture choices.

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

## 0.2.0 - Runtime and Session Foundation

Goal:

Introduce the internal runtime model needed by later adapters.

Included:

- Profile or config loading.
- Runtime state storage.
- Session identity and continuity model.
- Basic message queue and run lifecycle types.
- Concrete runtime orchestrator for normalized IM events and agent runs.
- Inbound event ledger for duplicate delivery handling.
- Run state machine and startup recovery strategy.
- Ack-after-persist and outbound outbox contracts.
- Core `message` and `event` domain models.
- Minimal `ImAdapter` and `AgentAdapter` capability boundaries.
- Workspace policy skeleton.
- Access policy skeleton.
- Structured internal events.

Not included:

- Full IM adapter support.
- Full agent adapter support.
- A broad replaceable `Runtime` trait.
- Platform-specific permissions or card rendering.
- Platform-specific auth, transport, OpenAPI, message, or event payload types.

Acceptance criteria:

- Runtime state can be created, loaded, and updated safely.
- Sessions and queued messages have stable identifiers.
- Duplicate inbound events do not create duplicate runs.
- Runtime restart can recover pending, running, or failed runs into explicit states.
- Event acknowledgement happens only after the minimum durable state is recorded.
- Workspace and access policy decisions are explicit and testable.
- The daemon foundation from `0.1.0` remains intact.

## 0.3.0 - First Agent Adapter

Goal:

Prove the agent side of the bridge with one controlled adapter.

Included:

- A mock or echo adapter as the first stable adapter target.
- A controlled fixture CLI that exercises the real process boundary.
- Process spawning abstraction for future real CLI adapters.
- Basic stdout, stderr, exit-code, timeout, and cancellation handling.
- Internal `AgentEvent` stream shape.
- Cancellation and timeout behavior.
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

## 0.4.0 - First Chat Platform Integration

Goal:

Complete the first minimal end-to-end bridge path.

Included:

- One initial chat platform adapter.
- Internal `ImAdapter` capability boundary for inbound events and outbound replies.
- Platform-specific auth, event transport, API client, message types, and event types for the selected platform.
- Incoming message normalization.
- Outgoing reply path.
- Connection between chat events, runtime sessions, and the first agent adapter.
- Minimal local configuration for the selected platform.
- Minimum safety envelope for remote-triggered local runs.

Not included:

- Broad multi-platform support.
- Rich interactive cards.
- Advanced attachment handling.
- Production-grade permission policy beyond the minimum safety envelope.

Acceptance criteria:

- A chat message can trigger a local runtime run.
- The runtime can call the first agent adapter.
- A reply can be sent back to the chat platform.
- Unknown chats and users are denied by default.
- Workspace allowlists and agent command or profile allowlists are enforced before local execution.
- Policy decisions are logged for local troubleshooting.
- Failures produce clear local logs and user-facing errors.

## Later Milestones

Future work should be planned after the first end-to-end path is stable.

Likely areas:

- Additional agent adapters.
- Additional chat platform adapters.
- Alternative platform adapter implementations that wrap official SDKs when available.
- Attachment handling.
- Streaming updates.
- Workspace allowlists and stronger access policy.
- Cross-platform service management.
- Installer and release automation.
