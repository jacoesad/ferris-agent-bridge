# Architecture Notes

`ferris-agent-bridge` is planned as a local service with three major boundaries:

```text
IM adapters <-> core runtime <-> agent adapters
```

The implementation should grow from the center outward. The daemon and core runtime should not depend on Lark / Feishu, Slack, Discord, or any specific agent CLI. Platform-specific behavior belongs behind adapter boundaries.

## Workspace Layout

The repository may evolve into a Cargo workspace as boundaries become stable. Independent code should move under `crates/` when it has a clear ownership boundary, a focused public API, and tests that can run without the full bridge application.

Do not split code into crates only to mirror directories. A crate should exist because it can be reasoned about, tested, and versioned as a unit.

Likely future crates:

```text
crates/ferris-agent-bridge-core
crates/ferris-agent-bridge-runtime
crates/ferris-agent-bridge-daemon
crates/ferris-agent-bridge-agent
crates/ferris-agent-bridge-im
crates/ferris-agent-bridge-im-lark
```

External SDK-style code should stay outside this repository unless it is owned by the bridge project itself. For example, a reusable Lark / Feishu channel SDK can remain an external crate and be consumed by `ferris-agent-bridge-im-lark` instead of being copied into this workspace.

## IM Adapters

IM adapters receive platform-specific events and convert them into a common internal event model.

Examples:

- Lark / Feishu
- Slack
- Discord
- Telegram

Adapter responsibilities:

- Receive messages, mentions, card actions, reactions, and attachments.
- Send replies, stream updates, and final results back to the platform.
- Hide platform-specific API details from the core runtime.

The shared project abstraction should be capability-oriented, not shaped after any single provider SDK. Concepts such as provider API clients, event transports, and event dispatchers are useful implementation details for specific platforms, but they should not become mandatory architecture layers for every IM platform. Other platforms may expose a ready-made channel SDK, only a REST API, only webhooks, a gateway connection, long polling, or a different split entirely.

The common boundary should stay at the bridge capability level:

```text
Core Runtime
  -> ImAdapter
      -> normalized inbound events
      -> outbound replies and updates
      -> attachment access
      -> platform-specific acknowledgements or cancellation when needed
```

Each IM adapter can choose its own internal structure. For example, a Slack adapter might combine a Web API client with Socket Mode or Events API, while a Discord adapter might combine a REST client with a Gateway connection. These implementation choices should stay inside the platform adapter.

Event delivery should be modeled as a domain-level transport capability, not as a fixed protocol choice. A transport trait should describe what the adapter needs from the interaction channel: connection lifecycle, incoming raw events, delivery acknowledgements when required, connection status, and shutdown. WebSocket, webhook, long polling, gateway connections, and official channel SDKs are concrete implementations of that transport boundary.

When a transport supports explicit delivery acknowledgement, the adapter should ask the runtime to persist or de-duplicate the normalized inbound event before acknowledging the platform delivery. The foundation layer provides the persistence primitive, and the initial runtime orchestrator boundary wires it through `InboundDelivery` and `ImAdapter::acknowledge_inbound_delivery`: the runtime records a new event or recognizes a duplicate before it calls the adapter acknowledgement. A session-bound inbound message is placed in its durable per-scope queue in the same state replacement as the ledger record, so acknowledgement cannot race ahead of pending work. Duplicate detection uses the normalized `EventId`, so IM adapters must namespace provider delivery identifiers by platform and scope before handing events to the runtime. A failed persistence attempt must leave the delivery unacknowledged so the platform can retry according to its own transport semantics. Real provider transports still belong inside concrete IM adapters.

Queue consumption is owned by a separate durable boundary. `StateStore::claim_message_batch` selects work only for a scope without a pending, running, or interrupted run, then creates a pending run, persists its recoverable input messages, and removes exactly that bounded queue prefix in one atomic state replacement. The runtime returns the claim only after that replacement succeeds, so concurrent workers cannot receive the same scope or message batch inside the owned process.

Run startup reconciliation is another store-owned boundary. `StateStore::reconcile_runs_at_startup` keeps pending runs with durable input resumable without returning an execution handoff, converts running or input-less pending runs into non-terminal `interrupted` ownership, and surfaces failed runs without retrying them. Interrupted runs continue excluding their scope until explicitly failed or cancelled, preventing new work from overlapping unresolved agent-side effects.

Outbound delivery follows the inverse durable boundary. The runtime claims an outbox record before constructing an `OutboundDeliveryAttempt` with a stable delivery id, normalized scope, message, and attempt number. `ImAdapter::deliver_outbound_message` receives that platform-neutral attempt and must classify failures as retryable only when provider non-acceptance is known; ambiguous transport outcomes remain uncertain and are not automatically retried. Provider request types, idempotency mechanisms, and transport details remain inside the concrete adapter. The runtime records the adapter outcome before scheduling another attempt.

Outbound startup reconciliation is store-owned under the same single-daemon boundary. `StateStore::reconcile_outbound_deliveries_at_startup` moves leftover `delivering` records to `uncertain` and reports the complete unresolved id set without another handoff. Only explicit accepted or confirmed-not-accepted evidence can resolve those records; exact same-target replay is used solely to confirm durability after an ambiguous write. Same-process resolution is serialized, while cross-process writer coordination remains outside this architecture stage.

### Core and Platform Modules

Core runtime modules should define platform-neutral domain models and behavior:

- `message`: internal message content, attachments, and outbound reply intent.
- `event`: internal inbound events, run events, and normalized user actions.
- `adapter`: capability traits such as `ImAdapter` and `AgentAdapter`.
- `runtime`: session, queue, run lifecycle, cancellation, and adapter orchestration.
- `policy`: cross-platform access, workspace, attachment, and run policies.

Platform-specific modules should own provider details:

- `auth`: credentials, token refresh, signing, and provider-specific identity.
- `transport`: event delivery lifecycle such as WebSocket, webhook, long polling, gateway connections, or official channel SDK wrappers.
- `openapi` or provider API clients: outbound API calls and metadata lookups.
- `message`: provider request/response payloads, cards, mentions, and media resources.
- `event`: raw provider event payloads and provider-specific event names.
- `normalizer`: provider events into core events.
- `outbound`: core outbound intents into provider API calls.

This means `message` and `event` exist at both layers with different meanings. Core `message` and `event` types are stable bridge domain models. Platform `message` and `event` types are adapter implementation details and should not leak into the core runtime.

### Lark / Feishu Notes

Lark / Feishu support has two separate pieces:

- OpenAPI calls for sending messages, reading metadata, uploading files, and updating interactive cards.
- Event intake for receiving messages, mentions, card actions, and other platform events.

The OpenAPI side can be implemented in Rust with HTTP and JSON types. A full provider SDK is useful but not required for the first milestones.

The event intake side is the larger design point. The Rust project should treat a channel connector, webhook receiver, or other event source as an IM adapter implementation detail. The core runtime should only see normalized internal events.

Official SDKs or channel APIs should be replaceable behind the adapter boundary. The core runtime should depend on internal capability traits and normalized event types, not on provider SDK types. If an official Rust OpenAPI SDK or channel client becomes available later, it should be possible to add a new adapter implementation without changing session handling, run lifecycle, or agent adapters.

Recommended internal shape:

```text
Core Runtime
  -> LarkImAdapter
      -> LarkEventTransport
      -> LarkEventSource
      -> LarkOpenApi
      -> LarkEventNormalizer
      -> LarkOutboundSender
```

`LarkImAdapter` is the Lark platform adapter. It should provide a channel-like boundary for Lark-specific behavior, but it should not clone a full channel SDK API or move platform-independent runtime behavior into the adapter.

`LarkEventTransport` should represent the event delivery mechanism as a domain capability. Concrete implementations can use a long-connection / WebSocket client, a webhook receiver, an official channel SDK, or another event stream implementation. The rest of the Lark adapter should not need to know which transport implementation is active.

`LarkEventSource` is the preferred trait name for receiving normalized Lark-side events from the transport and dispatch layer. It should not be named `LarkChannelApi` at the trait boundary because events may come from a channel client, webhook receiver, official channel API, or another event stream implementation.

`LarkOpenApi` should represent outbound platform capabilities such as sending messages, updating cards, uploading files, and reading metadata.

`LarkEventNormalizer` should translate raw Lark events into internal events owned by the core runtime. `LarkOutboundSender` should contain Lark-specific outbound formatting and API routing, such as message reply behavior, card updates, attachment downloads, and receive-id handling.

Platform-independent behavior should remain in the core runtime, not in `LarkImAdapter`. This includes session queueing, run lifecycle, cross-platform access policy, workspace policy, cancellation policy, and concurrency control. Lark-specific behavior such as event type decoding, card action parsing, Lark message resources, and Lark receive-id rules belongs inside the Lark adapter.

Replacement should be independent by capability:

- If an official channel API becomes available, replace the `LarkEventTransport` or `LarkEventSource` implementation as appropriate.
- If an official OpenAPI SDK becomes available, replace the `LarkOpenApi` implementation.
- If one official SDK covers both event intake and OpenAPI calls, one concrete implementation may implement both traits.

## Core Runtime

The core runtime owns behavior that should be independent of any single IM platform or agent CLI.

Runtime means the bridge's business orchestrator, not the async executor used by Rust tasks. It should start as a concrete component rather than a broad `Runtime` trait. Smaller dependency traits such as session storage, queues, policy evaluation, and adapter interfaces can be introduced where they make testing or replacement easier.

Responsibilities:

- Session identity and continuity.
- Pending message queueing and batching.
- Access policy.
- Workspace policy.
- Attachment policy.
- Run lifecycle and cancellation.
- State storage and service locks.
- Routing and orchestration between normalized IM events and agent events.

## Runtime State Schema Evolution

The runtime state schema version is an internal persisted-data compatibility marker. It is independent of the crate version and release version. Runtime state contains durable ownership, deduplication, queue, run, and delivery information, so it must not be treated as a disposable cache.

- Increment the schema version when the serialized representation or durable meaning changes incompatibly. Refactors, tests, documentation, and compatible field handling do not require a new schema version.
- Keep schema numbers monotonic and never reuse a number, including numbers used only by development snapshots.
- During milestone development, readers for intermediate schemas may remain on `main` so persisted state written by those snapshots can migrate forward.
- Before a milestone release, use a separate compatibility-consolidation PR to remove migration paths only for intermediate schemas that were never written by a tagged release. Retain migration paths for supported tagged-release schemas and the final schema being released. Complete this before cutting the release branch so the release PR remains limited to release preparation.
- Reject unsupported or future schema versions with a clear error. Never silently delete, downgrade, or reinterpret persisted state.

## Agent Adapters

Agent adapters run local CLI tools and convert their output into a common `AgentEvent` stream.

Initial candidates:

- Claude Code
- Codex CLI
- Trae CLI

Adapter responsibilities:

- Build command-line arguments.
- Spawn the local process.
- Pass prompts and attachments.
- Parse stdout/stderr.
- Emit text, tool, usage, done, and error events.
- Stop processes with a clear grace-period policy.
- Restrict the working directory through workspace policy.
- Use an explicit environment allowlist and redact sensitive values in logs.
- Read stdout and stderr with bounded buffers and backpressure.
- Terminate process trees predictably.
- Map exit codes and signals into structured agent errors.

## First Milestone

The first useful end-to-end milestone should avoid broad platform support. It should prove the local bridge shape with one IM adapter and one agent adapter:

```text
one chat platform + one local agent CLI + persisted session state
```

Only after this path is stable should the project generalize adapter registration and configuration.
