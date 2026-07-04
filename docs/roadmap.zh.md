# 路线图

本文档描述 `ferris-agent-bridge` 在早期 `0.x` 开发阶段的计划方向。它是规划指南，不是兼容性承诺。

项目应保持 Rust-first 和 local-first：

- 本地 agent 进程运行在用户机器上。
- 凭证、文件、session state 和 workspace policy 保持在本地。
- Chat platforms 与 agent CLIs 通过明确的 adapter 边界连接。

## 版本策略

`0.0.x` 线上的 patch release 应限制在发布清理、metadata、文档和小范围 CLI polish。

功能开发从 `0.1.0` 开始。每个 minor version 应有一个清晰的用户可见里程碑，并配套足够测试，避免下个里程碑重建上个里程碑的基础。

## 开发流程

Roadmap 里程碑应在聚焦的 feature branch 上开发。分支命名、合并策略、版本更新、changelog 更新和 release tagging 记录在 [CONTRIBUTING.zh.md](../CONTRIBUTING.zh.md)。

## 实现顺序

实现应从本地服务边界开始，然后向外推进：

- 先构建 daemon lifecycle。
- 加入 core runtime 基础设施（config、state、session、domain models）。
- 加入 runtime orchestrator（run lifecycle、events、policies、adapter contracts）。
- 用一个受控 adapter 和 mock e2e test 证明 agent 侧。
- 在 runtime 和 agent 侧稳定后，加入第一个 chat platform adapter。

这样早期里程碑可以保持独立，不依赖任何单一 chat platform SDK。对 Lark / Feishu 来说，需要时可以用 Rust 实现 OpenAPI HTTP client，而 event transport 留到后续 IM adapter 阶段处理。

Adapter contracts 应基于能力，而不是 SDK。未来的官方 SDK 或 channel API 应该可以在这些 contract 后替换，而不改变 core runtime。

公共接口应描述 bridge 能力，而不是 provider 内部结构。Provider API clients、event transports 和 event dispatchers 是具体平台实现可能用到的构件，但不是每个 IM adapter 的必需形状。未来 adapter 可以直接包装官方 channel SDK，组合独立的 REST 和 event clients，或者自行补齐缺失的一侧。WebSocket 和 webhook 应是 transport 实现，而不是固定架构选择。

## 跨版本关注点

以下关注点跨越多个里程碑，应尽早引入：

- **结构化日志和可观测性** — 在 `0.2.0` 引入。结构化日志事件携带关键字段（session、run、event id），使 runtime 行为可调试，而不依赖 `println!`。在日志输出中脱敏（redact）敏感值（secrets、tokens）。
- **配置兼容性** — 从 `0.2.0` 起 config 结构体携带 `version` 字段。未来版本可以读取旧配置并迁移，不会破坏用户设置。
- **错误处理策略** — 在 `0.2.0` 确定。定义哪些错误是致命的（panic/exit）、哪些可恢复（log + continue）、哪些是用户可见的（作为结构化错误返回）。Adapters 必须遵循相同策略，使跨平台错误行为一致。

## 0.1.0 - Local Daemon Foundation

目标：

提供可用的本地服务基础和基本 lifecycle commands。这个版本应证明项目可以安全管理本地后台进程，然后再连接 chat platforms 或真实 agent CLIs。

设计说明：[0.1 Daemon 生命周期设计](design/0.1-daemon-lifecycle.zh.md)。

包含：

- `run`、`start`、`stop`、`status` CLI commands。
- 便于调试的 `run` foreground mode。
- 本地状态的 runtime directory layout。
- 防止重复服务实例的 atomic lock handling。
- Stale lock detection 和 PID ownership validation。
- 基本 service state reporting。
- 适合本地排障的 minimal logging。
- command parsing 和 daemon lifecycle behavior 的测试。

不包含：

- Chat platform integration。
- Lark / Feishu OpenAPI 或 channel integration。
- 真实 agent CLI execution。
- 跨 chat conversations 的 session continuity。
- Streaming replies。

验收标准：

- `ferris-agent-bridge run` 在前台运行本地 daemon。
- `ferris-agent-bridge start` 启动本地后台服务。
- `ferris-agent-bridge status` 报告服务是否运行。
- `ferris-agent-bridge stop` 干净停止服务。
- 重复 start 不会创建多个 active services。
- 并发 start 会被 lock 串行化。
- Stale locks 和 PID reuse 能被检测出来，且不会停止无关进程。
- Shutdown 有 graceful timeout 和清晰 failure path。
- 实现有聚焦测试覆盖，并以 macOS 作为第一个目标平台工作。

## 0.2.0 - Runtime Foundations

目标：

引入 config、state、session 和 domain model 层。这是基础设施步骤——暂时没有编排逻辑或 adapter 实现。重点是稳定的数据结构和原子持久化基础。

设计说明：[0.2 Runtime Foundations 设计](design/0.2-runtime-foundations.zh.md)。

包含：

- 带 `version` 字段的 profile 和 config loading，为未来迁移预留空间。
- Config 结构体带有 `secret: Option<SecretInput>` 占位字段（暂不实现 keystore）。
- 带原子写入语义的 runtime state storage，并在 Unix/macOS 上为新建 JSON 文件使用私有权限。
- Session identity 和 continuity model。
- Core `message` 和 `event` domain models（平台无关）。
- 带 session 和 event 标识符的结构化日志。
- 日志输出中的敏感值脱敏。
- 错误处理策略（fatal vs. recoverable vs. user-visible）。

不包含：

- Run lifecycle state machine。
- Inbound event ledger 或 outbound outbox。
- Message queueing 或 batching。
- `ImAdapter` 和 `AgentAdapter` trait 定义。
- Policy evaluation。
- 任何 adapter 实现。

验收标准：

- Config 可以通过共享 atomic write 路径加载、验证和保存。
- Sessions 有稳定标识符，可以持久化和重新加载。
- Core domain types（`Message`、`Event`、`Session`）已定义且可测试。
- 结构化日志事件携带 session 和 event 上下文；后续里程碑引入 run 后再携带 run 上下文。
- 类似 secret 的值在日志输出中被脱敏。
- Unix/macOS 上新建的 runtime JSON 文件使用私有权限；显式 Windows ACL hardening 延后处理。
- `0.1.0` 的 daemon foundation 保持完整。

## 0.3.0 - Runtime Orchestrator

目标：

在 `0.2.0` 基础设施之上构建 runtime orchestrator。包括 run lifecycle、event handling、policy evaluation 和 adapter contracts。

包含：

- Run state machine（pending → running → completed / failed / cancelled）。
- 用于处理重复投递的 inbound event ledger。
- Ack-after-persist contract。
- 用于可靠回复投递的 outbound outbox。
- Per-scope message queue 与 debounce（quiet window）和 batching。
- Scope 级互斥：同一 scope 同时最多一个 active run。消息在 run 活跃期间被阻塞，run 完成后批量刷新。
- 具体 runtime orchestrator。
- `ImAdapter` 和 `AgentAdapter` capability traits。
- Workspace policy skeleton。
- Access policy skeleton。
- Startup recovery strategy（将 pending/running/failed runs 恢复为显式状态）。

不包含：

- 完整 IM adapter support。
- 完整 agent adapter support。
- Platform-specific permissions 或 card rendering。
- Platform-specific auth、transport、OpenAPI、message 或 event payload types。

验收标准：

- 一个 run 可以被创建、经历状态转换并完成。
- 重复 inbound events 不会创建重复 runs。
- 同一 scope 内的消息在触发 run 前会被 debounce 和 batch。
- 有 active run 的 scope 不会启动新 run，直到当前 run 完成。
- Runtime restart 可以把 pending、running 或 failed runs 恢复为显式状态。
- Event acknowledgement 只在最小 durable state 记录之后发生。
- Workspace 和 access policy decisions 显式且可测试。
- `0.1.0` 的 daemon foundation 保持完整。

## 0.4.0 - First Agent Adapter

目标：

用一个受控 adapter 证明 agent 侧 bridge 能力，并通过 mock end-to-end smoke test 验证完整的 runtime → agent 管线。

包含：

- 以 mock 或 echo adapter 作为第一个稳定 adapter target。
- 用 controlled fixture CLI 验证真实 process boundary。
- 面向后续真实 CLI adapters 的 process spawning abstraction。
- 基本 stdout、stderr、exit-code、timeout 和 cancellation handling。
- 内部 `AgentEvent` stream shape。
- Cancellation 和 timeout behavior。
- Mock end-to-end smoke test：mock IM adapter → runtime → mock agent adapter，验证完整调度管线，无需真实 chat platform。
- 使用 fake processes 或 controlled adapters 的测试。

不包含：

- 对多个 coding agents 的广泛支持。
- 完整 Claude、Codex 或 Trae compatibility。
- Chat platform integration。

验收标准：

- Runtime 可以启动 adapter run 并接收 structured events。
- Fixture CLI 能证明 spawning、stdout/stderr capture、exit-code mapping、timeout 和 cancellation。
- Cancellation 和 timeout behavior 是确定性的。
- Adapter output 可以映射到通用内部 event model。
- Mock e2e smoke test 通过：一条模拟的 inbound message 流经 runtime 到达 agent adapter，并产生模拟的 reply。

## 0.5.0 - First Chat Platform Integration

目标：

完成第一条最小端到端 bridge 路径。

包含：

- 一个初始 chat platform adapter（Lark / Feishu）。
- `ImAdapter` boundary 的第一个具体实现（接口在 0.3.0 定义）。
- Platform-specific auth：通过本地 AES-256-GCM keystore 管理 `app_secret`。env 和 file 可作为 secret 输入来源，但持久化到本地时必须进 encrypted keystore，不能明文落 config。
- 所选平台的 event transport（WebSocket 或 webhook）。
- 用于发送消息和读取元数据的 platform API client。
- Incoming message normalization。
- Outgoing reply path（文本或 markdown 消息；不包含交互式卡片）。
- 连接 chat events、runtime sessions 和第一个 agent adapter。
- 所选平台的 minimal local configuration。
- 面向远程触发本地 runs 的 minimum safety envelope：
  - 三层访问控制：DM（owner + 允许用户）、群组（owner + 允许群聊）、管理员命令（仅 owner）。
  - Owner 通过平台 API 在运行时解析，定期刷新。
  - 默认拒绝：未知 chats 和 users 在任何本地执行之前被拒绝。

不包含：

- 广泛 multi-platform support。
- Rich interactive cards。
- Advanced attachment handling。
- 超出 minimum safety envelope 的 production-grade permission policy。

验收标准：

- 一条 chat message 可以触发本地 runtime run。
- Runtime 可以调用第一个 agent adapter。
- Reply 可以发送回 chat platform。
- `app_secret` 以加密形式存储在磁盘上（AES-256-GCM），不会以明文形式出现在 config 文件或日志中。
- 未知 chats 和 users 默认拒绝。
- 本地执行前强制检查 workspace allowlists 和 agent command 或 profile allowlists。
- Policy decisions 会记录日志，便于本地排障。
- 失败会产生清晰 local logs 和 user-facing errors。

## 后续里程碑

第一条端到端路径稳定后，再规划未来工作。

可能方向：

- Additional agent adapters（Claude、Codex、Trae）。
- Additional chat platform adapters（Slack、Discord、Telegram）。
- 可在官方 SDK 可用时包装它们的 alternative platform adapter implementations。
- Attachment handling。
- Streaming updates。
- Workspace allowlists 和更强 access policy。
- Cross-platform service management（launchd、systemd、Task Scheduler）。
- Installer 和 release automation。
