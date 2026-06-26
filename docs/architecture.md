# Architecture Notes

`ferris-agent-bridge` is planned as a local service with three major boundaries:

```text
IM adapters <-> core runtime <-> agent adapters
```

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

## Core Runtime

The core runtime owns behavior that should be independent of any single IM platform or agent CLI.

Responsibilities:

- Session identity and continuity.
- Pending message queueing and batching.
- Access policy.
- Workspace policy.
- Attachment policy.
- Run lifecycle and cancellation.
- State storage and service locks.
- Event normalization between IM and agent sides.

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

## First Milestone

The first useful milestone should avoid broad platform support. It should prove the local relay shape with one IM adapter and one agent adapter:

```text
one chat platform + one local agent CLI + persisted session state
```

Only after this path is stable should the project generalize adapter registration and configuration.
