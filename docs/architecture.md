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

When a transport supports explicit delivery acknowledgement, the adapter should ask the runtime to persist or de-duplicate the normalized inbound event before acknowledging the platform delivery. The foundation layer provides the persistence primitive, and the initial runtime orchestrator boundary wires it through `InboundDelivery` and `ImAdapter::acknowledge_inbound_delivery`: the runtime records a new event or recognizes a duplicate before it calls the adapter acknowledgement. Duplicate detection uses the normalized `EventId`, so IM adapters must namespace provider delivery identifiers by platform and scope before handing events to the runtime. A failed persistence attempt must leave the delivery unacknowledged so the platform can retry according to its own transport semantics. Real provider transports still belong inside concrete IM adapters.

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
