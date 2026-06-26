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

## 0.1.0 - Local Daemon Foundation

Goal:

Provide a usable local service foundation with basic lifecycle commands. This version should prove that the project can manage a local background process safely before it connects to chat platforms or real agent CLIs.

Included:

- `start`, `stop`, and `status` CLI commands.
- Foreground or development mode for easier debugging.
- Runtime directory layout for local state.
- PID or lock handling to prevent duplicate service instances.
- Basic service state reporting.
- Minimal logging suitable for local troubleshooting.
- Tests for command parsing and daemon lifecycle behavior.

Not included:

- Chat platform integration.
- Real agent CLI execution.
- Session continuity across chat conversations.
- Streaming replies.

Acceptance criteria:

- `ferris-agent-bridge start` starts a local service or foreground runtime.
- `ferris-agent-bridge status` reports whether the service is running.
- `ferris-agent-bridge stop` stops the service cleanly.
- Repeated starts do not create duplicate active services.
- The implementation is covered by focused tests and works on macOS as the first target.

## 0.2.0 - Runtime and Session Foundation

Goal:

Introduce the internal runtime model needed by later adapters.

Included:

- Profile or config loading.
- Runtime state storage.
- Session identity and continuity model.
- Basic message queue and run lifecycle types.
- Workspace policy skeleton.
- Access policy skeleton.
- Structured internal events.

Not included:

- Full IM adapter support.
- Full agent adapter support.
- Platform-specific permissions or card rendering.

Acceptance criteria:

- Runtime state can be created, loaded, and updated safely.
- Sessions and queued messages have stable identifiers.
- Workspace and access policy decisions are explicit and testable.
- The daemon foundation from `0.1.0` remains intact.

## 0.3.0 - First Agent Adapter

Goal:

Prove the agent side of the bridge with one controlled adapter.

Included:

- A mock or echo adapter as the first stable adapter target.
- Process spawning abstraction for future real CLI adapters.
- Basic stdout and stderr handling.
- Internal `AgentEvent` stream shape.
- Cancellation and timeout behavior.
- Tests using fake processes or controlled adapters.

Not included:

- Broad support for multiple coding agents.
- Full Claude, Codex, or Trae compatibility.
- Chat platform integration.

Acceptance criteria:

- The runtime can start an adapter run and receive structured events.
- Cancellation and timeout behavior are deterministic.
- Adapter output can be mapped into the common internal event model.

## 0.4.0 - First Chat Platform Integration

Goal:

Complete the first minimal end-to-end bridge path.

Included:

- One initial chat platform adapter.
- Incoming message normalization.
- Outgoing reply path.
- Connection between chat events, runtime sessions, and the first agent adapter.
- Minimal local configuration for the selected platform.

Not included:

- Broad multi-platform support.
- Rich interactive cards.
- Advanced attachment handling.
- Production-grade permission policy.

Acceptance criteria:

- A chat message can trigger a local runtime run.
- The runtime can call the first agent adapter.
- A reply can be sent back to the chat platform.
- Failures produce clear local logs and user-facing errors.

## Later Milestones

Future work should be planned after the first end-to-end path is stable.

Likely areas:

- Additional agent adapters.
- Additional chat platform adapters.
- Attachment handling.
- Streaming updates.
- Workspace allowlists and stronger access policy.
- Cross-platform service management.
- Installer and release automation.
