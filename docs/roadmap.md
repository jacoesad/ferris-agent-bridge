# Roadmap

This document tracks the planned direction for `ferris-agent-bridge` during early `0.x` development. It is a planning guide, not a compatibility promise.

The near-term goal is a local-first bridge that can receive normalized chat events, run local agent CLIs through explicit adapter boundaries, and send replies back without moving credentials, workspace state, or policy decisions out of the user's machine.

## Current Status

The project has completed the local daemon foundation and runtime foundation work:

- `v0.0.1` established the initial Rust CLI, package metadata, README, architecture notes, licenses, and changelog.
- Milestone 1 shipped the local daemon lifecycle: foreground `run`, background `start`, `status`, `stop`, local runtime files, ownership-aware locks, stale recovery, and daemon lifecycle tests (#2). It was released as `v0.1.0` (#3).
- Milestone 2 added versioned config, runtime state storage, stable sessions, platform-neutral message/event models, structured logging, redaction, and runtime error classes (#4). It is included in version `0.2.0`.

The next feature milestone is Milestone 3, the runtime orchestrator. It should be delivered through focused PRs rather than one large branch.

## Architecture Boundary

The bridge has three major boundaries:

```text
IM adapters <-> core runtime <-> agent adapters
```

The core daemon and runtime must not depend on Lark / Feishu, Slack, Discord, or a specific agent CLI. Provider SDKs, event transports, OpenAPI clients, message payloads, and agent process details belong behind adapter boundaries.

The detailed architecture guidance lives in [architecture.md](architecture.md). This roadmap should stay focused on status, version policy, and PR-sized milestone bullets.

Design notes under [design/](design/) are still useful. They capture milestone-specific invariants, acceptance notes, and edge-case reasoning that would make the roadmap too noisy. The roadmap should link to design notes instead of duplicating them.

## Version Policy

During the early `0.x` series, releases generally correspond to completed roadmap milestones rather than every merged feature PR:

- `v0.0.1`: project and minimal CLI scaffold
- `v0.1.0`: Milestone 1, local daemon foundation
- `v0.2.0`: Milestone 2, runtime foundations
- `v0.3.0`: Milestone 3, runtime orchestrator
- `v0.4.0`: Milestone 4, first agent adapter
- `v0.5.0`: Milestone 5, first chat platform integration

Patch versions such as `v0.2.1` are reserved for bug fixes, release cleanup, documentation, and small follow-up improvements within a completed milestone.

Release PRs should stay separate and contain only release metadata, changelog/readme version updates, package metadata fixes, and publish dry-run fixes. Branch naming, merge strategy, version updates, changelog updates, and release tagging are documented in [CONTRIBUTING.md](../CONTRIBUTING.md).

Milestone bullets are intended as default PR boundaries. A PR should usually complete one bullet. Closely related small bullets may be grouped into one PR, and unusually large bullets may be split into several focused PRs. Completed bullets may include the PRs that delivered them.

## Milestone 0: Project Foundation

- Rust crate scaffold and package metadata
- Minimal CLI metadata commands, `--help`, and `--version`
- README, architecture notes, changelog, and licenses
- Initial roadmap and CI workflow (#1)

Milestone 0 is complete when the repository has enough structure for reviewed feature work to proceed through focused follow-up PRs.

## Milestone 1: Local Daemon Foundation

Design note: [0.1 Daemon Lifecycle Design](design/0.1-daemon-lifecycle.md).

Status: complete, released as `v0.1.0`.

- Foreground `run` command for local debugging (#2)
- Background `start`, `status`, and `stop` lifecycle commands (#2)
- Private local runtime directory with daemon lock, state, stop request, and log files (#2)
- Ownership-aware daemon locks, stale lock detection, and PID reuse validation (#2)
- Graceful shutdown with clear timeout and failure behavior (#2)
- macOS daemon lifecycle integration tests for start/status/stop, duplicate starts, concurrent starts, foreground stop, file permissions, and invalid-state fallback stop (#2)
- `v0.1.0` release preparation (#3)

## Milestone 2: Runtime Foundations

Design note: [0.2 Runtime Foundations Design](design/0.2-runtime-foundations.md).

Status: version `0.2.0` shipped the foundation; post-`0.2.0` M2 hardening continues on `main` before the next release.

- Versioned local config and profile loading (#4)
- `SecretInput` placeholder for future keystore-backed secret sources (#4)
- Runtime state storage through shared atomic write semantics (#4)
- Unix/macOS private permissions for newly created runtime JSON files and missing parent directories (#4)
- Stable session identity and continuity model (#4)
- Platform-neutral `Message` and `Event` domain models (#4)
- Structured log events with session and event context (#4)
- Secret-value redaction for structured fields and inline values (#4)
- Runtime error classification into fatal, recoverable, and user-visible classes (#4)
- Run records and local state transitions: pending, running, completed, failed, and cancelled (#8)
- Focused tests for config/state/session/message/event/logging/error behavior (#4)

## Milestone 3: Runtime Orchestrator

Design note: [0.3 Runtime Orchestrator Design](design/0.3-runtime-orchestrator.md).

Goal: layer durable orchestration on top of the runtime foundations without adding real IM or agent implementations yet.

- Durable inbound event ledger for duplicate delivery handling (#9)
- Store-level ack-after-persist persistence primitive for inbound events (#11)
- Explicit transport acknowledgement wiring through the initial `ImAdapter` and runtime orchestrator intake boundary
- Durable outbound outbox records and enqueue-before-send persistence primitive
- Outbox consumption state primitives: claim queued deliveries, persist delivery attempts, and mark delivered or failed before returning control to callers
- Outbox worker, retry scheduling/backoff, and concrete outbound adapter handoff
- Per-scope message queue with debounce and batching
- Scope-level mutual exclusion: at most one active run per scope
- Startup recovery for pending, running, and failed runs into explicit states
- Workspace policy skeleton with testable decisions
- Access policy skeleton with testable decisions
- Remaining `ImAdapter` capabilities and `AgentAdapter` capability traits
- Concrete runtime orchestrator that wires storage, queues, policies, and adapter boundaries
- Runtime-level tests that prove duplicate handling, queueing, recovery, and policy decisions

## Milestone 4: First Agent Adapter

Goal: prove the agent side of the bridge with a controlled adapter and a mock end-to-end runtime path.

- Mock or echo adapter as the first stable adapter target
- Controlled fixture CLI that exercises the real process boundary
- Process spawning abstraction for future real CLI adapters
- Stdout, stderr, exit-code, timeout, and cancellation handling
- Internal `AgentEvent` stream shape
- Deterministic cancellation and timeout behavior
- Mock end-to-end smoke test: mock IM adapter -> runtime -> mock agent adapter -> simulated reply

## Milestone 5: First Chat Platform Integration

Goal: complete the first minimal end-to-end bridge path with one chat platform and one agent adapter.

- Initial Lark / Feishu IM adapter
- First concrete implementation of the `ImAdapter` boundary
- Platform-specific auth with `app_secret` persisted through a local encrypted keystore, never plaintext config
- Event transport as an adapter implementation detail, such as WebSocket, webhook, or future official channel SDK
- Platform API client for sending messages and reading metadata
- Incoming message normalization and outgoing text/markdown reply path
- Connection between chat events, runtime sessions, and the first agent adapter
- Minimal local configuration for the selected platform
- Minimum remote-trigger safety envelope: owner-only admin commands, allowlisted users/chats, deny-by-default unknown scopes, workspace allowlists, and clear policy logs

## Later Scope

- Additional agent adapters such as Claude Code, Codex CLI, and Trae CLI
- Additional chat platform adapters such as Slack, Discord, and Telegram
- Alternative platform adapter implementations that wrap official SDKs when available
- Attachment handling
- Streaming updates
- Rich interactive cards
- Stronger access, workspace, and attachment policies
- Cross-platform service management through launchd, systemd, and Task Scheduler
- Installer and release automation

## Non-Goals

- Reimplementing a full Lark / Feishu OpenAPI SDK inside this repository
- Moving platform-independent runtime behavior into a platform adapter
- Treating WebSocket, webhook, or any single provider SDK as the fixed architecture
- Guaranteeing multi-platform parity before the first end-to-end path is stable
