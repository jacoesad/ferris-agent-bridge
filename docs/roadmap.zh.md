# 路线图

本文档跟踪 `ferris-agent-bridge` 在早期 `0.x` 阶段的计划方向。它是规划指南，不是兼容性承诺。

近期目标是构建一个 local-first bridge：接收归一化 chat events，通过明确的 adapter 边界运行本地 agent CLI，并把回复发回平台，同时让凭证、workspace state 和 policy decisions 留在用户机器上。

## 当前状态

项目已经完成 local daemon foundation 和 runtime foundation：

- `v0.0.1` 建立了初始 Rust CLI、package metadata、README、架构说明、license 和 changelog。
- Milestone 1 已交付本地 daemon lifecycle：前台 `run`、后台 `start`、`status`、`stop`、本地 runtime 文件、带 ownership 的 lock、stale recovery 和 daemon lifecycle 测试（#2）。该里程碑已作为 `v0.1.0` 发布（#3）。
- Milestone 2 加入 versioned config、runtime state storage、stable sessions、平台无关 message/event models、structured logging、redaction 和 runtime error classes（#4）。这些内容包含在版本 `0.2.0` 中。

下一个功能里程碑是 Milestone 3，即 runtime orchestrator。它应通过聚焦 PR 逐步交付，而不是放进一个过大的分支。

## 架构边界

Bridge 有三个主要边界：

```text
IM adapters <-> core runtime <-> agent adapters
```

Core daemon 和 runtime 不应依赖 Lark / Feishu、Slack、Discord 或某个具体 agent CLI。Provider SDK、event transports、OpenAPI clients、message payloads 和 agent process 细节都应放在 adapter 边界之后。

详细架构说明放在 [architecture.zh.md](architecture.zh.md)。本路线图应聚焦状态、版本策略和适合 PR 拆分的 milestone bullets。

[design/](design/) 下的设计文档仍然有必要保留。它们记录 milestone-specific invariants、验收说明和边界场景推理；这些内容如果放进 roadmap，会让路线图变得很重。Roadmap 应链接到设计文档，而不是复制设计细节。

## 版本策略

早期 `0.x` 阶段，release 通常对应已完成的 roadmap milestone，而不是每个合并的 feature PR：

- `v0.0.1`：项目和最小 CLI scaffold
- `v0.1.0`：Milestone 1，local daemon foundation
- `v0.2.0`：Milestone 2，runtime foundations
- `v0.3.0`：Milestone 3，runtime orchestrator
- `v0.4.0`：Milestone 4，first agent adapter
- `v0.5.0`：Milestone 5，first chat platform integration

`v0.2.1` 这类 patch version 保留给 bug fix、release cleanup、文档和已完成 milestone 内的小范围 follow-up improvements。

Release PR 应保持独立，只包含 release metadata、changelog/readme version updates、package metadata fixes 和 publish dry-run fixes。分支命名、合并策略、版本更新、changelog 更新和 release tagging 记录在 [CONTRIBUTING.zh.md](../CONTRIBUTING.zh.md)。

Milestone bullets 是默认 PR 边界：一个 PR 通常应完成一个 bullet。紧密相关的小 bullet 可以合并到一个 PR，过大的 bullet 可以拆成多个聚焦 PR。已完成 bullet 可以标注交付它的 PR。

## Milestone 0: Project Foundation

- Rust crate scaffold 和 package metadata
- 最小 CLI metadata commands、`--help` 和 `--version`
- README、架构说明、changelog 和 licenses
- 初始 roadmap 和 CI workflow（#1）

Milestone 0 完成标准是：仓库具备足够结构，可以通过聚焦 follow-up PR 开始可 review 的功能开发。

## Milestone 1: Local Daemon Foundation

设计说明：[0.1 Daemon 生命周期设计](design/0.1-daemon-lifecycle.zh.md)。

状态：已完成，并作为 `v0.1.0` 发布。

- 便于本地调试的前台 `run` command（#2）
- 后台 `start`、`status` 和 `stop` lifecycle commands（#2）
- 私有 local runtime directory，包含 daemon lock、state、stop request 和 log files（#2）
- 带 ownership 的 daemon locks、stale lock detection 和 PID reuse validation（#2）
- Graceful shutdown，包含清晰 timeout 和 failure behavior（#2）
- macOS daemon lifecycle integration tests，覆盖 start/status/stop、duplicate starts、concurrent starts、foreground stop、file permissions 和 invalid-state fallback stop（#2）
- `v0.1.0` release preparation（#3）

## Milestone 2: Runtime Foundations

设计说明：[0.2 Runtime Foundations 设计](design/0.2-runtime-foundations.zh.md)。

状态：版本 `0.2.0` 已发布 foundation；`0.2.0` 之后的 M2 hardening 继续在 `main` 上完成，并随下一个版本发布。

- Versioned local config 和 profile loading（#4）
- 面向未来 keystore-backed secret sources 的 `SecretInput` 占位（#4）
- 通过共享 atomic write semantics 写入 runtime state storage（#4）
- Unix/macOS 上为新建 runtime JSON files 和缺失 parent directories 使用私有权限（#4）
- Stable session identity 和 continuity model（#4）
- 平台无关 `Message` 和 `Event` domain models（#4）
- 带 session 和 event context 的 structured log events（#4）
- Structured fields 和 inline values 的 secret-value redaction（#4）
- Runtime errors 分为 fatal、recoverable 和 user-visible classes（#4）
- Run records 和本地状态转换：pending、running、completed、failed、cancelled（#8）
- 覆盖 config/state/session/message/event/logging/error 行为的聚焦测试（#4）

## Milestone 3: Runtime Orchestrator

设计说明：[0.3 Runtime Orchestrator 设计](design/0.3-runtime-orchestrator.zh.md)。

目标：在 runtime foundations 之上加入 durable orchestration，但暂不加入真实 IM 或 agent 实现。

- 用于重复投递处理的 durable inbound event ledger（#9）
- Inbound events 的 store-level ack-after-persist persistence primitive（#11）
- 通过初始 `ImAdapter` 和 runtime orchestrator intake 边界接入显式 transport acknowledgement
- Durable outbound outbox records，以及 send 前必须 enqueue 的 persistence primitive
- Outbox consumption state primitives：领取 queued deliveries、持久化 delivery attempts，并在返回调用方前标记 delivered、retryable-failed 或 uncertain
- Single-step outbox worker，包含 bounded retry scheduling/backoff 和 retry-safe normalized outbound adapter outcomes
- Durable per-scope message queue，在 acknowledgement 前与 inbound ledger record 一起持久化，并包含 debounce 和 bounded batching
- Scope 级互斥：同一个 scope 同时最多一个 active run
- Startup recovery，将 pending、running 和 failed runs 恢复为显式状态
- Outbound delivery 遗留为 `delivering` 时的 startup recovery 和显式 reconciliation state，禁止盲目重试
- Workspace policy skeleton，并让 decisions 可测试
- Access policy skeleton，并让 decisions 可测试
- 剩余 `ImAdapter` 能力和 `AgentAdapter` capability traits
- 具体 runtime orchestrator，用于串联 storage、queues、policies 和 adapter boundaries
- Runtime-level tests，证明 duplicate handling、queueing、recovery 和 policy decisions

## Milestone 4: First Agent Adapter

目标：用一个受控 adapter 证明 agent 侧 bridge 能力，并验证 mock end-to-end runtime path。

- Mock 或 echo adapter 作为第一个稳定 adapter target
- 通过 controlled fixture CLI 验证真实 process boundary
- 面向未来真实 CLI adapters 的 process spawning abstraction
- Stdout、stderr、exit-code、timeout 和 cancellation handling
- 内部 `AgentEvent` stream shape
- 确定性的 cancellation 和 timeout behavior
- Mock end-to-end smoke test：mock IM adapter -> runtime -> mock agent adapter -> simulated reply

## Milestone 5: First Chat Platform Integration

目标：用一个 chat platform 和一个 agent adapter 完成第一条最小端到端 bridge path。

- 初始 Lark / Feishu IM adapter
- `ImAdapter` boundary 的第一个具体实现
- Platform-specific auth：`app_secret` 通过本地 encrypted keystore 持久化，绝不落 plaintext config
- Event transport 作为 adapter implementation detail，例如 WebSocket、webhook 或未来 official channel SDK
- 用于发送消息和读取 metadata 的 platform API client
- Provider-aware outbound 幂等与 reconciliation：在 provider 支持时通过幂等 key 或状态查询复用稳定 delivery id，区分确认未接受与 ambiguous outcome，并且绝不盲目重放未解决的 attempt
- Incoming message normalization 和 outgoing text/markdown reply path
- 连接 chat events、runtime sessions 和第一个 agent adapter
- 所选平台的 minimal local configuration
- 面向 remote-trigger 的 minimum safety envelope：owner-only admin commands、allowlisted users/chats、默认拒绝 unknown scopes、workspace allowlists 和清晰 policy logs

## 后续范围

- Additional agent adapters，例如 Claude Code、Codex CLI 和 Trae CLI
- Additional chat platform adapters，例如 Slack、Discord 和 Telegram
- 可在官方 SDK 可用时包装它们的 alternative platform adapter implementations
- Attachment handling
- Streaming updates
- Rich interactive cards
- 更强的 access、workspace 和 attachment policies
- 通过 launchd、systemd 和 Task Scheduler 做 cross-platform service management
- Installer 和 release automation

## 非目标

- 在本仓库内重新实现完整 Lark / Feishu OpenAPI SDK
- 把平台无关 runtime behavior 放进 platform adapter
- 把 WebSocket、webhook 或某个 provider SDK 当成固定架构
- 在第一条端到端路径稳定前承诺 multi-platform parity
